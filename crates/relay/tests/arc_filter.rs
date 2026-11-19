//! Storage-arc-aware hints: `arc` on `/v1/sync` and `/v1/events`, and the
//! client's `ClientOptions::arc` against relays with and without support.

mod common;

use axum::extract::Query;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use common::*;
use futures::future::BoxFuture;
use futures::StreamExt;
use hc_push_client::{ClientOptions, MemoryStateStore, PushClient, RelayEndpoint, SyncPolicy};
use hc_push_protocol::{
    Capabilities, ChangeKind, ChangeRef, Cursor, EventEnvelope, RefType, SourceFidelity,
    SourceInfo, SyncPage,
};
use hc_push_source_api::{source_info, ChangeSource, ObservedChange, SourceContext};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;

const Q: u32 = 1 << 30; // a quarter of the location space
const FIRST_HALF: &str = "0-2147483647";

/// Feeds whatever the test sends it into the relay, like the
/// `OpsIntegrated` source does.
struct ScriptedSource {
    rx: mpsc::Receiver<Vec<ObservedChange>>,
}

impl ChangeSource for ScriptedSource {
    fn info(&self) -> SourceInfo {
        source_info("test-located", SourceFidelity::ConductorOpLevel, "scripted")
    }

    fn run(mut self: Box<Self>, ctx: SourceContext) -> BoxFuture<'static, anyhow::Result<()>> {
        Box::pin(async move {
            ctx.health.ok();
            loop {
                tokio::select! {
                    _ = ctx.shutdown.cancelled() => return Ok(()),
                    b = self.rx.recv() => match b {
                        Some(b) => { ctx.sink.submit(b, None).await?; }
                        None => return Ok(()),
                    }
                }
            }
        })
    }
}

fn op(id: &str, loc: Option<u32>) -> ObservedChange {
    let r = ChangeRef::new(RefType::DhtOp, id);
    ObservedChange {
        space: SPACE.into(),
        kind: ChangeKind::OpIntegrated,
        refs: vec![match loc {
            Some(l) => r.with_loc(l),
            None => r,
        }],
        occurred_at_ms: None,
    }
}

/// One op per quarter of the location space, one with an unknown location
/// (e.g. from the `DumpOpTimings` poller) and an app-signal digest.
fn mixed_batch(tag: &str) -> Vec<ObservedChange> {
    let mut v: Vec<_> = (0..4)
        .map(|i| op(&format!("{tag}q{i}"), Some(i * Q + 7)))
        .collect();
    v.push(op(&format!("{tag}unknown"), None));
    v.push(ObservedChange {
        space: SPACE.into(),
        kind: ChangeKind::AppSignal,
        refs: vec![ChangeRef::new(RefType::Digest, format!("sha256:{tag}00"))],
        occurred_at_ms: None,
    });
    v
}

async fn start() -> (TestRelay, mpsc::Sender<Vec<ObservedChange>>) {
    let (tx, rx) = mpsc::channel(8);
    let relay =
        TestRelay::start_custom(None, |_| {}, vec![Box::new(ScriptedSource { rx })], None).await;
    (relay, tx)
}

async fn wait_for_head(relay: &TestRelay, head: u64) {
    for _ in 0..200 {
        if relay.journal.bounds().unwrap().head >= head {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("journal head stuck");
}

fn ref_ids(events: &[EventEnvelope]) -> Vec<String> {
    events.iter().map(|e| e.refs[0].id.clone()).collect()
}

async fn sync_with(
    http: &reqwest::Client,
    url: &str,
    token: &str,
    arc: Option<&str>,
) -> (reqwest::StatusCode, Option<SyncPage>, usize) {
    let mut req = http.get(format!("{url}/v1/sync")).bearer_auth(token);
    if let Some(a) = arc {
        req = req.query(&[("arc", a)]);
    }
    let resp = req.send().await.unwrap();
    let status = resp.status();
    let body = resp.bytes().await.unwrap();
    let page = serde_json::from_slice(&body).ok();
    (status, page, body.len())
}

#[tokio::test]
async fn sync_returns_only_hints_in_the_requested_arc() {
    let (relay, tx) = start().await;
    let http = reqwest::Client::new();
    let caps: Capabilities = http
        .get(format!("{}/v1/capabilities", relay.url))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(caps.arc_filter);
    let sub = subscribe(&http, &relay.url, &[SPACE]).await;
    tx.send(mixed_batch("a")).await.unwrap();
    wait_for_head(&relay, 6).await;

    let (_, all, all_bytes) = sync_with(&http, &relay.url, &sub.token, None).await;
    let all = all.unwrap();
    assert_eq!(all.events.len(), 6);
    // Located hints carry their location on the wire.
    assert_eq!(all.events[1].refs[0].loc, Some(Q + 7));

    let (_, full, _) = sync_with(&http, &relay.url, &sub.token, Some("full")).await;
    assert_eq!(ref_ids(&full.unwrap().events), ref_ids(&all.events));

    let (_, half, _) = sync_with(&http, &relay.url, &sub.token, Some(FIRST_HALF)).await;
    let half = half.unwrap();
    assert_eq!(
        ref_ids(&half.events),
        ["aq0", "aq1", "aunknown", "sha256:a00"]
    );
    assert_eq!(
        half.next_cursor.seq, 6,
        "skipped hints still advance the cursor"
    );

    // Wrapping arc: the last quarter through the first.
    let wrap = format!("{}-{}", 3 * Q, Q - 1);
    let (_, w, _) = sync_with(&http, &relay.url, &sub.token, Some(&wrap)).await;
    assert_eq!(
        ref_ids(&w.unwrap().events),
        ["aq0", "aq3", "aunknown", "sha256:a00"]
    );

    // Zero arc: no op hint with a known location; unknown ones and app
    // signals still arrive, and the response is smaller.
    let (_, zero, zero_bytes) = sync_with(&http, &relay.url, &sub.token, Some("empty")).await;
    let zero = zero.unwrap();
    assert_eq!(ref_ids(&zero.events), ["aunknown", "sha256:a00"]);
    assert_eq!(zero.next_cursor.seq, 6);
    assert!(zero_bytes < all_bytes / 2, "{zero_bytes} vs {all_bytes}");

    for bad in ["", "half", "1-2-3", "0-4294967296"] {
        let (status, _, _) = sync_with(&http, &relay.url, &sub.token, Some(bad)).await;
        assert_eq!(status, 400, "arc={bad:?}");
    }
    relay.stop().await;
}

async fn sse_events(resp: reqwest::Response, n: usize) -> Vec<EventEnvelope> {
    let mut parser = hc_push_client::sse::SseParser::new();
    let mut out = Vec::new();
    let mut body = resp.bytes_stream();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while out.len() < n {
        let chunk = tokio::time::timeout_at(deadline, body.next())
            .await
            .expect("timed out waiting for SSE events");
        let Some(Ok(chunk)) = chunk else { break };
        for ev in parser.push(&chunk).unwrap() {
            if ev.event == "change" {
                out.push(serde_json::from_str(&ev.data).unwrap());
            }
        }
    }
    out
}

#[tokio::test]
async fn event_stream_replays_and_streams_only_hints_in_the_arc() {
    let (relay, tx) = start().await;
    let http = reqwest::Client::new();
    let sub = subscribe(&http, &relay.url, &[SPACE]).await;
    tx.send(mixed_batch("a")).await.unwrap();
    wait_for_head(&relay, 6).await;

    let open = |cursor: Cursor| {
        http.get(format!("{}/v1/events", relay.url))
            .bearer_auth(&sub.token)
            .query(&[("cursor", cursor.to_string()), ("arc", FIRST_HALF.into())])
            .send()
    };
    // Replay from the start, then live events.
    let resp = open(sub.earliest_cursor.clone()).await.unwrap();
    assert_eq!(resp.status(), 200);
    tx.send(mixed_batch("b")).await.unwrap();
    let got = sse_events(resp, 8).await;
    assert_eq!(
        ref_ids(&got),
        [
            "aq0",
            "aq1",
            "aunknown",
            "sha256:a00", // replay
            "bq0",
            "bq1",
            "bunknown",
            "sha256:b00", // live
        ]
    );

    let bad = http
        .get(format!("{}/v1/events", relay.url))
        .bearer_auth(&sub.token)
        .query(&[("arc", "nope")])
        .send()
        .await
        .unwrap();
    assert_eq!(bad.status(), 400);
    relay.stop().await;
}

fn client(url: &str, arc: Option<&str>) -> PushClient {
    PushClient::new(
        vec![RelayEndpoint {
            name: "r".into(),
            url: url.into(),
            registration_key: Some(KEY.into()),
        }],
        vec![SPACE.to_string()],
        Box::new(MemoryStateStore::default()),
        ClientOptions {
            page_size: 2,
            request_timeout: Duration::from_secs(3),
            arc: arc.map(|a| a.parse().unwrap()),
            ..ClientOptions::default()
        },
    )
    .unwrap()
}

async fn client_sync(c: &mut PushClient) -> (Vec<String>, hc_push_client::RelaySyncStats) {
    let log: Arc<Mutex<Vec<EventEnvelope>>> = Arc::default();
    let l = log.clone();
    let mut handler = move |e: &EventEnvelope| {
        l.lock().unwrap().push(e.clone());
        Ok(())
    };
    let report = c.sync(SyncPolicy::FirstAvailable, &mut handler).await;
    let stats = report.outcomes[0].result.clone().unwrap();
    let ids = ref_ids(&log.lock().unwrap());
    (ids, stats)
}

#[tokio::test]
async fn client_arc_is_filtered_by_the_relay() {
    let (relay, tx) = start().await;
    let mut c = client(&relay.url, Some(FIRST_HALF));
    c.ensure_registered().await;
    tx.send(mixed_batch("a")).await.unwrap();
    wait_for_head(&relay, 6).await;
    let (ids, stats) = client_sync(&mut c).await;
    assert_eq!(ids, ["aq0", "aq1", "aunknown", "sha256:a00"]);
    // The relay did the filtering: nothing outside the arc was downloaded.
    assert_eq!(stats.received, 4);
    assert_eq!(stats.outside_arc, 0);
    assert_eq!(stats.cursor.unwrap().seq, 6);
    relay.stop().await;
}

/// A relay from before `arc` existed: rejects unknown query parameters and
/// does not advertise `arc_filter`.
async fn legacy_relay(events: Vec<EventEnvelope>) -> (String, Arc<Mutex<Vec<String>>>) {
    let queries: Arc<Mutex<Vec<String>>> = Arc::default();
    #[derive(serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    #[allow(dead_code)]
    struct OldSyncQuery {
        cursor: Option<String>,
        limit: Option<u32>,
    }
    let q = queries.clone();
    let app = Router::new()
        .route(
            "/v1/capabilities",
            get(|| async {
                // Only the fields an old relay sends; no `arc_filter`.
                Json(serde_json::json!({ "protocol_versions": [1], "peer_hints": false }))
            }),
        )
        .route(
            "/v1/subscriptions",
            post(|| async {
                (
                    axum::http::StatusCode::CREATED,
                    Json(serde_json::json!({
                        "subscription_id": "s1",
                        "token": "t",
                        "spaces": [SPACE],
                        "cursor": "v1.0123abcd.0",
                        "earliest_cursor": "v1.0123abcd.0",
                        "idle_ttl_secs": 3600
                    })),
                )
            }),
        )
        .route(
            "/v1/sync",
            get(
                move |raw: axum::extract::RawQuery,
                      query: Result<
                    Query<OldSyncQuery>,
                    axum::extract::rejection::QueryRejection,
                >| {
                    let q = q.clone();
                    let events = events.clone();
                    async move {
                        q.lock().unwrap().push(raw.0.unwrap_or_default());
                        if query.is_err() {
                            return (
                                axum::http::StatusCode::BAD_REQUEST,
                                Json(serde_json::json!({})),
                            )
                                .into_response();
                        }
                        let head = Cursor::new("0123abcd", events.len() as u64);
                        Json(SyncPage {
                            events,
                            next_cursor: head.clone(),
                            head_cursor: head,
                            has_more: false,
                        })
                        .into_response()
                    }
                },
            ),
        );
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", l.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
    (url, queries)
}

#[tokio::test]
async fn client_filters_itself_when_the_relay_cannot() {
    let mut envs = Vec::new();
    for (i, c) in mixed_batch("a").into_iter().enumerate() {
        let seq = i as u64 + 1;
        envs.push(EventEnvelope {
            v: 1,
            event_id: format!("0123abcd-{seq}"),
            cursor: Cursor::new("0123abcd", seq),
            space: c.space.clone(),
            kind: c.kind,
            change_key: hc_push_protocol::change_key(&c.space, c.kind, &c.refs),
            refs: c.refs,
            observed_at_ms: 1,
            occurred_at_ms: None,
            source: "old".into(),
        });
    }
    let (url, queries) = legacy_relay(envs).await;
    let mut c = client(&url, Some(FIRST_HALF));
    let (ids, stats) = client_sync(&mut c).await;
    assert_eq!(ids, ["aq0", "aq1", "aunknown", "sha256:a00"]);
    assert_eq!(stats.received, 6);
    assert_eq!(stats.outside_arc, 2);
    assert_eq!(stats.cursor.unwrap().seq, 6);
    // The client never sent `arc` to a relay that did not advertise it.
    let qs = queries.lock().unwrap().clone();
    assert!(!qs.is_empty());
    assert!(qs.iter().all(|q| !q.contains("arc=")), "{qs:?}");
}
