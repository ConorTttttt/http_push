//! Relay metrics: detection latency, dispatch latency, event counts and HTTP
//! bandwidth. Exposed as Prometheus text and JSON on the loopback admin
//! listener only (`/admin/v1/metrics`, `/admin/v1/stats`).
//!
//! Definitions:
//! * **detection latency**: `observed_at_ms - occurred_at_ms`, i.e. from the
//!   event at its origin (the conductor's integration time) until the relay
//!   journaled it. Only for sources that report `occurred_at_ms`. Both clocks
//!   are this host's wall clock when the relay runs next to the conductor.
//! * **SSE dispatch latency**: from journaling until the event is written to
//!   an open SSE stream.
//! * **HTTP bytes**: response body bytes as written (including streamed SSE
//!   frames), request body bytes (`content-length`), and an estimate of
//!   HTTP/1.1 header bytes. TLS and TCP overhead are not included.

use axum::body::Body;
use axum::extract::{Request, State};
use axum::middleware::Next;
use axum::response::Response;
use bytes::Bytes;
use http_body::{Frame, SizeHint};
use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Instant;

/// Upper bucket bounds in seconds.
const LATENCY_BUCKETS: &[f64] = &[
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0,
];

#[derive(Debug, Clone)]
pub struct Histogram {
    counts: Vec<u64>,
    sum: f64,
    count: u64,
    min: f64,
    max: f64,
}

impl Default for Histogram {
    fn default() -> Self {
        Self {
            counts: vec![0; LATENCY_BUCKETS.len()],
            sum: 0.0,
            count: 0,
            min: f64::INFINITY,
            max: 0.0,
        }
    }
}

impl Histogram {
    pub fn observe(&mut self, v: f64) {
        let v = v.max(0.0);
        for (i, b) in LATENCY_BUCKETS.iter().enumerate() {
            if v <= *b {
                self.counts[i] += 1;
            }
        }
        self.sum += v;
        self.count += 1;
        self.min = self.min.min(v);
        self.max = self.max.max(v);
    }

    pub fn count(&self) -> u64 {
        self.count
    }

    fn json(&self) -> serde_json::Value {
        if self.count == 0 {
            return serde_json::json!({ "count": 0 });
        }
        serde_json::json!({
            "count": self.count,
            "mean_s": self.sum / self.count as f64,
            "min_s": self.min,
            "max_s": self.max,
        })
    }

    fn prometheus(&self, out: &mut String, name: &str, labels: &str) {
        let sep = if labels.is_empty() { "" } else { "," };
        for (i, b) in LATENCY_BUCKETS.iter().enumerate() {
            out.push_str(&format!(
                "{name}_bucket{{{labels}{sep}le=\"{b}\"}} {}\n",
                self.counts[i]
            ));
        }
        out.push_str(&format!(
            "{name}_bucket{{{labels}{sep}le=\"+Inf\"}} {}\n",
            self.count
        ));
        let l = if labels.is_empty() {
            String::new()
        } else {
            format!("{{{labels}}}")
        };
        out.push_str(&format!("{name}_sum{l} {}\n", self.sum));
        out.push_str(&format!("{name}_count{l} {}\n", self.count));
    }
}

#[derive(Debug, Default)]
pub struct Metrics {
    started: Option<Instant>,
    http_requests: Mutex<BTreeMap<(String, u16), u64>>,
    http_response_body_bytes: Mutex<BTreeMap<String, Arc<AtomicU64>>>,
    http_response_header_bytes: AtomicU64,
    http_request_body_bytes: AtomicU64,
    http_request_header_bytes: AtomicU64,
    events_journaled: Mutex<BTreeMap<(String, String), u64>>,
    duplicates_dropped: Mutex<BTreeMap<String, u64>>,
    detection_latency: Mutex<BTreeMap<String, Histogram>>,
    sse_dispatch_latency: Mutex<Histogram>,
    sse_events_sent: AtomicU64,
    sync_pages_served: AtomicU64,
    sync_events_served: AtomicU64,
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// Normalise a request path to a bounded set of route labels.
pub fn route_label(path: &str) -> &'static str {
    match path {
        "/v1/health" => "/v1/health",
        "/v1/capabilities" => "/v1/capabilities",
        "/v1/subscriptions" => "/v1/subscriptions",
        "/v1/sync" => "/v1/sync",
        "/v1/events" => "/v1/events",
        "/v1/peers" => "/v1/peers",
        p if p.starts_with("/v1/subscriptions/") => "/v1/subscriptions/{id}",
        _ => "other",
    }
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            started: Some(Instant::now()),
            ..Default::default()
        }
    }

    /// Record newly journaled events from one batch.
    pub fn record_journaled(
        &self,
        source: &str,
        envs: &[hc_push_protocol::EventEnvelope],
        duplicates: usize,
    ) {
        {
            let mut m = lock(&self.events_journaled);
            for e in envs {
                *m.entry((source.to_string(), e.kind.as_str().to_string()))
                    .or_default() += 1;
            }
        }
        if duplicates > 0 {
            *lock(&self.duplicates_dropped)
                .entry(source.to_string())
                .or_default() += duplicates as u64;
        }
        let mut lat = lock(&self.detection_latency);
        for e in envs {
            if let Some(occ) = e.occurred_at_ms {
                let secs = e.observed_at_ms.saturating_sub(occ) as f64 / 1000.0;
                lat.entry(source.to_string()).or_default().observe(secs);
            }
        }
    }

    pub fn record_sse_sent(&self, observed_at_ms: u64, now_ms: u64) {
        self.sse_events_sent.fetch_add(1, Ordering::Relaxed);
        lock(&self.sse_dispatch_latency)
            .observe(now_ms.saturating_sub(observed_at_ms) as f64 / 1000.0);
    }

    pub fn record_sync_page(&self, events: usize) {
        self.sync_pages_served.fetch_add(1, Ordering::Relaxed);
        self.sync_events_served
            .fetch_add(events as u64, Ordering::Relaxed);
    }

    fn response_counter(&self, route: &str) -> Arc<AtomicU64> {
        lock(&self.http_response_body_bytes)
            .entry(route.to_string())
            .or_default()
            .clone()
    }

    /// JSON snapshot for `/admin/v1/stats` and test scripts.
    pub fn snapshot(&self) -> serde_json::Value {
        let requests: BTreeMap<String, u64> = lock(&self.http_requests)
            .iter()
            .map(|((r, s), n)| (format!("{r} {s}"), *n))
            .collect();
        let resp_bytes: BTreeMap<String, u64> = lock(&self.http_response_body_bytes)
            .iter()
            .map(|(r, n)| (r.clone(), n.load(Ordering::Relaxed)))
            .collect();
        let journaled: BTreeMap<String, u64> = lock(&self.events_journaled)
            .iter()
            .map(|((s, k), n)| (format!("{s}/{k}"), *n))
            .collect();
        let detection: BTreeMap<String, serde_json::Value> = lock(&self.detection_latency)
            .iter()
            .map(|(s, h)| (s.clone(), h.json()))
            .collect();
        serde_json::json!({
            "uptime_s": self.started.map(|s| s.elapsed().as_secs_f64()).unwrap_or(0.0),
            "http_requests": requests,
            "http_response_body_bytes": resp_bytes,
            "http_response_header_bytes_est": self.http_response_header_bytes.load(Ordering::Relaxed),
            "http_request_body_bytes": self.http_request_body_bytes.load(Ordering::Relaxed),
            "http_request_header_bytes_est": self.http_request_header_bytes.load(Ordering::Relaxed),
            "events_journaled": journaled,
            "duplicates_dropped": *lock(&self.duplicates_dropped),
            "detection_latency": detection,
            "sse_dispatch_latency": lock(&self.sse_dispatch_latency).json(),
            "sse_events_sent": self.sse_events_sent.load(Ordering::Relaxed),
            "sync_pages_served": self.sync_pages_served.load(Ordering::Relaxed),
            "sync_events_served": self.sync_events_served.load(Ordering::Relaxed),
        })
    }

    /// Prometheus text exposition (format 0.0.4).
    pub fn prometheus(&self, gauges: &[(&str, &str, f64)]) -> String {
        let mut o = String::new();
        let esc = |s: &str| s.replace('\\', "\\\\").replace('"', "\\\"");
        o.push_str("# TYPE hcpush_http_requests_total counter\n");
        for ((r, s), n) in lock(&self.http_requests).iter() {
            o.push_str(&format!(
                "hcpush_http_requests_total{{route=\"{}\",status=\"{s}\"}} {n}\n",
                esc(r)
            ));
        }
        o.push_str("# TYPE hcpush_http_response_body_bytes_total counter\n");
        for (r, n) in lock(&self.http_response_body_bytes).iter() {
            o.push_str(&format!(
                "hcpush_http_response_body_bytes_total{{route=\"{}\"}} {}\n",
                esc(r),
                n.load(Ordering::Relaxed)
            ));
        }
        for (name, v) in [
            (
                "hcpush_http_response_header_bytes_estimated_total",
                &self.http_response_header_bytes,
            ),
            (
                "hcpush_http_request_body_bytes_total",
                &self.http_request_body_bytes,
            ),
            (
                "hcpush_http_request_header_bytes_estimated_total",
                &self.http_request_header_bytes,
            ),
            ("hcpush_sse_events_sent_total", &self.sse_events_sent),
            ("hcpush_sync_pages_served_total", &self.sync_pages_served),
            ("hcpush_sync_events_served_total", &self.sync_events_served),
        ] {
            o.push_str(&format!(
                "# TYPE {name} counter\n{name} {}\n",
                v.load(Ordering::Relaxed)
            ));
        }
        o.push_str("# TYPE hcpush_events_journaled_total counter\n");
        for ((s, k), n) in lock(&self.events_journaled).iter() {
            o.push_str(&format!(
                "hcpush_events_journaled_total{{source=\"{}\",kind=\"{}\"}} {n}\n",
                esc(s),
                esc(k)
            ));
        }
        o.push_str("# TYPE hcpush_duplicates_dropped_total counter\n");
        for (s, n) in lock(&self.duplicates_dropped).iter() {
            o.push_str(&format!(
                "hcpush_duplicates_dropped_total{{source=\"{}\"}} {n}\n",
                esc(s)
            ));
        }
        o.push_str("# TYPE hcpush_detection_latency_seconds histogram\n");
        for (s, h) in lock(&self.detection_latency).iter() {
            h.prometheus(
                &mut o,
                "hcpush_detection_latency_seconds",
                &format!("source=\"{}\"", esc(s)),
            );
        }
        o.push_str("# TYPE hcpush_sse_dispatch_latency_seconds histogram\n");
        lock(&self.sse_dispatch_latency).prometheus(
            &mut o,
            "hcpush_sse_dispatch_latency_seconds",
            "",
        );
        for (name, help, v) in gauges {
            o.push_str(&format!(
                "# HELP {name} {help}\n# TYPE {name} gauge\n{name} {v}\n"
            ));
        }
        o
    }
}

fn header_bytes(headers: &axum::http::HeaderMap) -> u64 {
    headers
        .iter()
        .map(|(k, v)| (k.as_str().len() + v.len() + 4) as u64)
        .sum::<u64>()
        + 2
}

/// Axum middleware counting requests and HTTP bytes per route.
pub async fn track(State(metrics): State<Arc<Metrics>>, req: Request, next: Next) -> Response {
    let route = route_label(req.uri().path());
    let req_line = req.method().as_str().len()
        + req
            .uri()
            .path_and_query()
            .map(|p| p.as_str().len())
            .unwrap_or(1)
        + 11;
    metrics.http_request_header_bytes.fetch_add(
        req_line as u64 + header_bytes(req.headers()),
        Ordering::Relaxed,
    );
    if let Some(len) = req
        .headers()
        .get(axum::http::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
    {
        metrics
            .http_request_body_bytes
            .fetch_add(len, Ordering::Relaxed);
    }
    let resp = next.run(req).await;
    *lock(&metrics.http_requests)
        .entry((route.to_string(), resp.status().as_u16()))
        .or_default() += 1;
    metrics
        .http_response_header_bytes
        .fetch_add(17 + header_bytes(resp.headers()), Ordering::Relaxed);
    let counter = metrics.response_counter(route);
    resp.map(|b| Body::new(CountingBody { inner: b, counter }))
}

/// Response body wrapper that counts data bytes as they are written, so
/// long-lived SSE streams are measured too.
struct CountingBody {
    inner: Body,
    counter: Arc<AtomicU64>,
}

impl http_body::Body for CountingBody {
    type Data = Bytes;
    type Error = axum::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, axum::Error>>> {
        let r = Pin::new(&mut self.inner).poll_frame(cx);
        if let Poll::Ready(Some(Ok(frame))) = &r {
            if let Some(d) = frame.data_ref() {
                self.counter.fetch_add(d.len() as u64, Ordering::Relaxed);
            }
        }
        r
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn histogram_buckets_are_cumulative() {
        let mut h = Histogram::default();
        h.observe(0.003);
        h.observe(0.2);
        h.observe(100.0);
        assert_eq!(h.count(), 3);
        let mut out = String::new();
        h.prometheus(&mut out, "x", "");
        assert!(out.contains("x_bucket{le=\"0.005\"} 1"));
        assert!(out.contains("x_bucket{le=\"0.25\"} 2"));
        assert!(out.contains("x_bucket{le=\"60\"} 2"));
        assert!(out.contains("x_bucket{le=\"+Inf\"} 3"));
    }

    #[test]
    fn routes_are_bounded() {
        assert_eq!(
            route_label("/v1/subscriptions/sub_123"),
            "/v1/subscriptions/{id}"
        );
        assert_eq!(route_label("/../etc/passwd"), "other");
    }
}
