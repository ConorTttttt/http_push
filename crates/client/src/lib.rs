//! Lightweight client for Holochain HTTP push relays.
//!
//! Designed for mobile apps that are *not* continuously running:
//!
//! * [`PushClient::sync`] is a bounded, one-shot catch-up: page through every
//!   change since the stored cursor, hand each new change to the app exactly
//!   once (deduplicated across relays), persist the cursor, return. No
//!   background tasks or connections survive the call.
//! * [`PushClient::listen`] is for when the app is in the foreground: an SSE
//!   stream that ends after an idle timeout, after which the app goes idle.
//! * Several independent relays can be configured. Each has its own
//!   subscription and cursor; cursors are never reused across relays.
//!
//! Every event is a *hint*. The app must fetch and validate referenced data
//! through Holochain before trusting it; see `docs/architecture.md`.

pub mod sse;
pub mod state;

use hc_push_protocol::{
    ApiError, Cursor, ErrorBody, ErrorCode, EventEnvelope, SubscribeRequest, SubscribeResponse,
    SyncPage, SyncRequired, SSE_EVENT_CHANGE, SSE_EVENT_SYNC_REQUIRED,
};
use reqwest::StatusCode;
pub use state::{ClientState, FileStateStore, MemoryStateStore, RelayState, StateStore};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// One configured relay.
#[derive(Debug, Clone)]
pub struct RelayEndpoint {
    /// Local name used as the key in client state.
    pub name: String,
    /// Base URL, e.g. `https://relay.example.org`.
    pub url: String,
    /// Operator-issued registration key (if the relay requires one).
    pub registration_key: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ClientOptions {
    pub request_timeout: Duration,
    pub connect_timeout: Duration,
    pub page_size: u32,
    /// Bound on work per relay per sync call.
    pub max_pages_per_sync: u32,
    /// Attempts per request before failing over to the next relay.
    pub attempts_per_request: u32,
    pub retry_base: Duration,
    pub seen_capacity: usize,
    pub label: Option<String>,
}

impl Default for ClientOptions {
    fn default() -> Self {
        Self {
            request_timeout: Duration::from_secs(10),
            connect_timeout: Duration::from_secs(5),
            page_size: 100,
            max_pages_per_sync: 50,
            attempts_per_request: 2,
            retry_base: Duration::from_millis(200),
            seen_capacity: 10_000,
            label: None,
        }
    }
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum RelayError {
    #[error("relay unavailable: {0}")]
    Unavailable(String),
    #[error("unauthorized")]
    Unauthorized,
    #[error("cursor not servable ({code:?})")]
    Gap {
        code: ErrorCode,
        earliest: Option<Cursor>,
    },
    #[error("rate limited")]
    RateLimited(Option<Duration>),
    #[error("request rejected: {0}")]
    Rejected(String),
    #[error("protocol violation: {0}")]
    Protocol(String),
    #[error("application handler failed: {0}")]
    Handler(String),
    #[error("unknown relay {0}")]
    UnknownRelay(String),
}

impl RelayError {
    fn is_transient(&self) -> bool {
        matches!(
            self,
            RelayError::Unavailable(_) | RelayError::RateLimited(_)
        )
    }
}

/// The relay could not serve history after our cursor. The app must
/// reconcile authoritatively through Holochain for these spaces.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GapNotice {
    pub relay: String,
    pub code: ErrorCode,
    pub spaces: Vec<String>,
    pub resumed_from: Option<Cursor>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RelaySyncStats {
    pub pages: u32,
    pub received: usize,
    pub delivered: usize,
    pub duplicates: usize,
    pub cursor: Option<Cursor>,
    /// Stopped because of `max_pages_per_sync`; call sync again.
    pub more_pending: bool,
}

#[derive(Debug, Clone)]
pub struct RelayOutcome {
    pub relay: String,
    pub result: Result<RelaySyncStats, RelayError>,
}

#[derive(Debug, Clone, Default)]
pub struct SyncReport {
    pub outcomes: Vec<RelayOutcome>,
    pub gaps: Vec<GapNotice>,
    pub delivered: usize,
    pub duplicates: usize,
    /// First relay that synced successfully.
    pub served_by: Option<String>,
}

impl SyncReport {
    pub fn any_success(&self) -> bool {
        self.served_by.is_some()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncPolicy {
    /// Try relays in configured order; stop at the first that succeeds.
    /// Cheapest for battery and bandwidth.
    FirstAvailable,
    /// Sync every relay, for maximum coverage. Duplicates are filtered.
    All,
}

#[derive(Debug, Clone, Copy)]
pub struct ListenOptions {
    /// Close the stream after this long without a change event.
    pub idle_timeout: Duration,
    /// Close the stream after this long regardless.
    pub max_duration: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ListenEnd {
    Idle,
    MaxDuration,
    /// The relay asked for a sync (backlog / lag / shutdown); already done.
    SyncedAfterRequest,
    Disconnected,
}

#[derive(Debug, Clone)]
pub struct ListenOutcome {
    pub end: ListenEnd,
    pub delivered: usize,
    pub duplicates: usize,
    pub sync: Option<SyncReport>,
}

/// What the app does with a new change. Returning an error stops the sync
/// without advancing the cursor past the failed event.
pub type Handler<'a> = dyn FnMut(&EventEnvelope) -> anyhow::Result<()> + Send + 'a;

pub struct PushClient {
    http: reqwest::Client,
    endpoints: Vec<RelayEndpoint>,
    spaces: Vec<String>,
    store: Box<dyn StateStore>,
    state: ClientState,
    opts: ClientOptions,
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

async fn error_from_response(resp: reqwest::Response) -> RelayError {
    let status = resp.status();
    let retry_after = resp
        .headers()
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
        .map(Duration::from_secs);
    let body: Option<ApiError> = resp.json::<ErrorBody>().await.ok().map(|b| b.error);
    match status {
        StatusCode::UNAUTHORIZED => RelayError::Unauthorized,
        StatusCode::GONE => RelayError::Gap {
            code: body.as_ref().map(|b| b.code).unwrap_or(ErrorCode::Unknown),
            earliest: body.and_then(|b| b.earliest_cursor),
        },
        StatusCode::TOO_MANY_REQUESTS => RelayError::RateLimited(retry_after),
        s if s.is_server_error() => RelayError::Unavailable(format!("HTTP {s}")),
        s => RelayError::Rejected(format!(
            "HTTP {s}: {}",
            body.map(|b| b.message).unwrap_or_default()
        )),
    }
}

fn transport_error(e: reqwest::Error) -> RelayError {
    RelayError::Unavailable(e.to_string())
}

impl PushClient {
    pub fn new(
        endpoints: Vec<RelayEndpoint>,
        spaces: Vec<String>,
        store: Box<dyn StateStore>,
        opts: ClientOptions,
    ) -> anyhow::Result<Self> {
        if endpoints.is_empty() {
            anyhow::bail!("at least one relay endpoint is required");
        }
        let mut names = std::collections::HashSet::new();
        for e in &endpoints {
            if !names.insert(e.name.clone()) {
                anyhow::bail!("duplicate relay name {}", e.name);
            }
        }
        if spaces.is_empty() {
            anyhow::bail!("at least one space is required");
        }
        let http = reqwest::Client::builder()
            .connect_timeout(opts.connect_timeout)
            // Do not keep sockets open once we go idle.
            .pool_idle_timeout(Duration::from_secs(2))
            .user_agent(concat!("hc-push-client/", env!("CARGO_PKG_VERSION")))
            .build()?;
        let mut state = store
            .load()?
            .unwrap_or_else(|| ClientState::new(opts.seen_capacity));
        state.fix_capacity(opts.seen_capacity);
        Ok(Self {
            http,
            endpoints,
            spaces,
            store,
            state,
            opts,
        })
    }

    pub fn state(&self) -> &ClientState {
        &self.state
    }

    pub fn endpoints(&self) -> &[RelayEndpoint] {
        &self.endpoints
    }

    fn endpoint(&self, name: &str) -> Result<RelayEndpoint, RelayError> {
        self.endpoints
            .iter()
            .find(|e| e.name == name)
            .cloned()
            .ok_or_else(|| RelayError::UnknownRelay(name.to_string()))
    }

    fn save(&self) -> Result<(), RelayError> {
        self.store
            .save(&self.state)
            .map_err(|e| RelayError::Handler(format!("saving client state: {e}")))
    }

    fn url(base: &str, path: &str) -> String {
        format!("{}{}", base.trim_end_matches('/'), path)
    }

    /// Register (or re-register) with one relay.
    pub async fn register(&mut self, name: &str) -> Result<(), RelayError> {
        let ep = self.endpoint(name)?;
        let mut req = self
            .http
            .post(Self::url(&ep.url, "/v1/subscriptions"))
            .timeout(self.opts.request_timeout)
            .json(&SubscribeRequest {
                spaces: self.spaces.clone(),
                label: self.opts.label.clone(),
            });
        if let Some(k) = &ep.registration_key {
            req = req.bearer_auth(k);
        }
        let resp = req.send().await.map_err(transport_error)?;
        if resp.status() != StatusCode::CREATED {
            return Err(error_from_response(resp).await);
        }
        let sub: SubscribeResponse = resp
            .json()
            .await
            .map_err(|e| RelayError::Protocol(e.to_string()))?;
        let journal_id = sub.cursor.journal_id.clone();
        // Keep our position if the relay's journal is unchanged (e.g. our
        // subscription merely expired); otherwise start at the new head.
        let cursor = match self.state.relays.get(name) {
            Some(old) if old.journal_id == journal_id && old.cursor.is_some() => old.cursor.clone(),
            _ => Some(sub.cursor.clone()),
        };
        self.state.relays.insert(
            name.to_string(),
            RelayState {
                url: ep.url.clone(),
                subscription_id: sub.subscription_id,
                token: sub.token,
                journal_id,
                spaces: sub.spaces,
                cursor,
                last_success_ms: Some(now_ms()),
                consecutive_failures: 0,
            },
        );
        self.save()?;
        tracing::info!(relay = name, "registered");
        Ok(())
    }

    /// Register with every relay that has no subscription yet.
    pub async fn ensure_registered(&mut self) -> Vec<(String, Result<(), RelayError>)> {
        let mut out = Vec::new();
        for ep in self.endpoints.clone() {
            let needs = match self.state.relays.get(&ep.name) {
                None => true,
                Some(rs) => rs.url != ep.url,
            };
            if needs {
                let r = self.register(&ep.name).await;
                out.push((ep.name.clone(), r));
            }
        }
        out
    }

    async fn fetch_page(&self, rs: &RelayState) -> Result<SyncPage, RelayError> {
        let mut req = self
            .http
            .get(Self::url(&rs.url, "/v1/sync"))
            .timeout(self.opts.request_timeout)
            .bearer_auth(&rs.token)
            .query(&[("limit", self.opts.page_size.to_string())]);
        if let Some(c) = &rs.cursor {
            req = req.query(&[("cursor", c.to_string())]);
        }
        let resp = req.send().await.map_err(transport_error)?;
        if resp.status() != StatusCode::OK {
            return Err(error_from_response(resp).await);
        }
        let page: SyncPage = resp
            .json()
            .await
            .map_err(|e| RelayError::Protocol(e.to_string()))?;
        // Never accept a cursor from a different journal than we track.
        if page.next_cursor.journal_id != rs.journal_id
            || page
                .events
                .iter()
                .any(|e| e.cursor.journal_id != rs.journal_id)
        {
            return Err(RelayError::Protocol(
                "journal id mismatch in sync page".into(),
            ));
        }
        let mut prev = rs.cursor.as_ref().map(|c| c.seq).unwrap_or(0);
        for e in &page.events {
            if e.seq() <= prev {
                return Err(RelayError::Protocol("events not strictly ascending".into()));
            }
            prev = e.seq();
        }
        if page.next_cursor.seq < prev {
            return Err(RelayError::Protocol("next_cursor behind last event".into()));
        }
        Ok(page)
    }

    async fn fetch_page_with_retry(&self, name: &str) -> Result<SyncPage, RelayError> {
        let mut attempt = 0;
        loop {
            let rs = self
                .state
                .relays
                .get(name)
                .cloned()
                .ok_or(RelayError::Unauthorized)?;
            match self.fetch_page(&rs).await {
                Ok(p) => return Ok(p),
                Err(e) if e.is_transient() && attempt + 1 < self.opts.attempts_per_request => {
                    attempt += 1;
                    let wait = match &e {
                        RelayError::RateLimited(Some(d)) => (*d).min(Duration::from_secs(5)),
                        _ => self.opts.retry_base * 2u32.pow(attempt.min(6)),
                    };
                    tracing::debug!(relay = name, error = %e, ?wait, "retrying");
                    tokio::time::sleep(wait).await;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Deliver one event: dedup, call the handler, advance the cursor.
    fn deliver(
        &mut self,
        name: &str,
        e: &EventEnvelope,
        handler: &mut Handler<'_>,
        stats: &mut RelaySyncStats,
    ) -> Result<(), RelayError> {
        stats.received += 1;
        if self.state.seen.contains(&e.change_key) {
            stats.duplicates += 1;
        } else {
            handler(e).map_err(|err| RelayError::Handler(err.to_string()))?;
            self.state.seen.insert(&e.change_key);
            stats.delivered += 1;
        }
        if let Some(rs) = self.state.relays.get_mut(name) {
            rs.cursor = Some(e.cursor.clone());
        }
        Ok(())
    }

    /// Catch up with one relay.
    pub async fn sync_relay(
        &mut self,
        name: &str,
        handler: &mut Handler<'_>,
        gaps: &mut Vec<GapNotice>,
    ) -> Result<RelaySyncStats, RelayError> {
        self.endpoint(name)?;
        if !self.state.relays.contains_key(name) {
            self.register(name).await?;
        }
        let mut stats = RelaySyncStats::default();
        let mut reregistered = false;
        let mut gap_handled = false;
        loop {
            if stats.pages >= self.opts.max_pages_per_sync {
                stats.more_pending = true;
                break;
            }
            let page = match self.fetch_page_with_retry(name).await {
                Ok(p) => p,
                Err(RelayError::Unauthorized) if !reregistered => {
                    // Subscription expired or was revoked: register again.
                    reregistered = true;
                    self.register(name).await?;
                    continue;
                }
                Err(RelayError::Gap { code, earliest }) if !gap_handled => {
                    gap_handled = true;
                    tracing::warn!(
                        relay = name,
                        ?code,
                        "relay cannot serve history after our cursor; app must reconcile"
                    );
                    let spaces = self.state.relays[name].spaces.clone();
                    match &earliest {
                        Some(e) => {
                            let rs = self.state.relays.get_mut(name).expect("registered");
                            rs.journal_id = e.journal_id.clone();
                            rs.cursor = Some(e.clone());
                            self.save()?;
                        }
                        // No resume point offered: start over at the head.
                        None => {
                            self.state.relays.remove(name);
                            self.register(name).await?;
                        }
                    }
                    gaps.push(GapNotice {
                        relay: name.to_string(),
                        code,
                        spaces,
                        resumed_from: self.state.relays[name].cursor.clone(),
                    });
                    continue;
                }
                Err(e) => {
                    if let Some(rs) = self.state.relays.get_mut(name) {
                        rs.consecutive_failures = rs.consecutive_failures.saturating_add(1);
                    }
                    let _ = self.save();
                    return Err(e);
                }
            };
            stats.pages += 1;
            for e in &page.events {
                if let Err(err) = self.deliver(name, e, handler, &mut stats) {
                    let _ = self.save();
                    return Err(err);
                }
            }
            let rs = self.state.relays.get_mut(name).expect("registered");
            rs.cursor = Some(page.next_cursor.clone());
            rs.last_success_ms = Some(now_ms());
            rs.consecutive_failures = 0;
            stats.cursor = rs.cursor.clone();
            self.save()?;
            if !page.has_more {
                break;
            }
        }
        Ok(stats)
    }

    /// Catch up according to `policy`, deduplicating across relays.
    pub async fn sync(&mut self, policy: SyncPolicy, handler: &mut Handler<'_>) -> SyncReport {
        let mut report = SyncReport::default();
        for ep in self.endpoints.clone() {
            let r = self.sync_relay(&ep.name, handler, &mut report.gaps).await;
            if let Ok(s) = &r {
                report.delivered += s.delivered;
                report.duplicates += s.duplicates;
                if report.served_by.is_none() {
                    report.served_by = Some(ep.name.clone());
                }
            } else if let Err(e) = &r {
                tracing::warn!(relay = %ep.name, error = %e, "relay sync failed; trying next relay");
            }
            let ok = r.is_ok();
            report.outcomes.push(RelayOutcome {
                relay: ep.name.clone(),
                result: r,
            });
            if ok && policy == SyncPolicy::FirstAvailable {
                break;
            }
        }
        report
    }

    /// Foreground notifications from one relay over SSE. Returns once the
    /// stream has been idle for `idle_timeout`, after `max_duration`, when
    /// the relay asks for a sync (which is then performed), or on disconnect.
    pub async fn listen(
        &mut self,
        name: &str,
        opts: ListenOptions,
        handler: &mut Handler<'_>,
    ) -> Result<ListenOutcome, RelayError> {
        use futures::StreamExt;
        self.endpoint(name)?;
        if !self.state.relays.contains_key(name) {
            self.register(name).await?;
        }
        let rs = self.state.relays.get(name).cloned().expect("registered");
        let mut req = self
            .http
            .get(Self::url(&rs.url, "/v1/events"))
            .bearer_auth(&rs.token)
            .header(reqwest::header::ACCEPT, "text/event-stream");
        if let Some(c) = &rs.cursor {
            req = req.header("last-event-id", c.to_string());
        }
        let resp = tokio::time::timeout(self.opts.request_timeout, req.send())
            .await
            .map_err(|_| RelayError::Unavailable("timed out opening event stream".into()))?
            .map_err(transport_error)?;
        if resp.status() != StatusCode::OK {
            return Err(error_from_response(resp).await);
        }
        let mut body = resp.bytes_stream();
        let mut parser = sse::SseParser::new();
        let mut stats = RelaySyncStats::default();
        let started = Instant::now();
        let mut last_change = Instant::now();
        let end = loop {
            let deadline_idle = opts.idle_timeout.saturating_sub(last_change.elapsed());
            let deadline_max = opts.max_duration.saturating_sub(started.elapsed());
            if deadline_max.is_zero() {
                break ListenEnd::MaxDuration;
            }
            if deadline_idle.is_zero() {
                break ListenEnd::Idle;
            }
            let chunk =
                match tokio::time::timeout(deadline_idle.min(deadline_max), body.next()).await {
                    Err(_) => continue, // re-evaluate deadlines
                    Ok(None) => break ListenEnd::Disconnected,
                    Ok(Some(Err(e))) => {
                        tracing::debug!(relay = name, error = %e, "event stream error");
                        break ListenEnd::Disconnected;
                    }
                    Ok(Some(Ok(b))) => b,
                };
            let events = parser
                .push(&chunk)
                .map_err(|e| RelayError::Protocol(e.to_string()))?;
            let mut sync_needed = false;
            for ev in events {
                match ev.event.as_str() {
                    SSE_EVENT_CHANGE => {
                        let env: EventEnvelope = serde_json::from_str(&ev.data)
                            .map_err(|e| RelayError::Protocol(e.to_string()))?;
                        let cur = self.state.relays.get(name).and_then(|r| r.cursor.clone());
                        if env.cursor.journal_id != rs.journal_id
                            || cur.as_ref().is_some_and(|c| env.seq() <= c.seq)
                        {
                            stats.duplicates += 1;
                            continue;
                        }
                        self.deliver(name, &env, handler, &mut stats)?;
                        self.save()?;
                        last_change = Instant::now();
                    }
                    SSE_EVENT_SYNC_REQUIRED => {
                        let sr: Option<SyncRequired> = serde_json::from_str(&ev.data).ok();
                        tracing::info!(relay = name, reason = ?sr.map(|s| s.reason), "relay requested sync");
                        sync_needed = true;
                    }
                    _ => {}
                }
            }
            if sync_needed {
                drop(body);
                let mut gaps = Vec::new();
                let r = self.sync_relay(name, handler, &mut gaps).await;
                let mut report = SyncReport {
                    gaps,
                    ..Default::default()
                };
                if let Ok(s) = &r {
                    report.delivered = s.delivered;
                    report.duplicates = s.duplicates;
                    report.served_by = Some(name.to_string());
                }
                report.outcomes.push(RelayOutcome {
                    relay: name.to_string(),
                    result: r,
                });
                return Ok(ListenOutcome {
                    end: ListenEnd::SyncedAfterRequest,
                    delivered: stats.delivered + report.delivered,
                    duplicates: stats.duplicates + report.duplicates,
                    sync: Some(report),
                });
            }
        };
        if let Some(r) = self.state.relays.get_mut(name) {
            r.last_success_ms = Some(now_ms());
        }
        self.save()?;
        Ok(ListenOutcome {
            end,
            delivered: stats.delivered,
            duplicates: stats.duplicates,
            sync: None,
        })
    }

    /// Delete our subscription on a relay and forget its state.
    pub async fn unsubscribe(&mut self, name: &str) -> Result<(), RelayError> {
        let Some(rs) = self.state.relays.get(name).cloned() else {
            return Ok(());
        };
        let resp = self
            .http
            .delete(Self::url(
                &rs.url,
                &format!("/v1/subscriptions/{}", rs.subscription_id),
            ))
            .timeout(self.opts.request_timeout)
            .bearer_auth(&rs.token)
            .send()
            .await
            .map_err(transport_error)?;
        if !(resp.status().is_success() || resp.status() == StatusCode::UNAUTHORIZED) {
            return Err(error_from_response(resp).await);
        }
        self.state.relays.remove(name);
        self.save()
    }
}
