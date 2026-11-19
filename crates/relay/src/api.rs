//! Public HTTP API (`/v1/...`).

use crate::auth::{self, parse_bearer};
use crate::journal::{Journal, JournalError, StoredSubscription};
use crate::metrics::Metrics;
use crate::rate_limit::RateLimiter;
use crate::settings::RelaySettings;
use crate::sse;
use axum::extract::rejection::JsonRejection;
use axum::extract::{ConnectInfo, DefaultBodyLimit, FromRequestParts, Path, Query, State};
use axum::http::header::{AUTHORIZATION, LOCATION, RETRY_AFTER};
use axum::http::request::Parts;
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use hc_push_protocol::PeersResponse;
use hc_push_protocol::{
    ApiError, ArcFilter, Capabilities, Cursor, ErrorBody, ErrorCode, EventEnvelope, Health,
    HealthStatus, LimitsInfo, RegistrationMode, RetentionInfo, SourceHealth, SourceInfo,
    SourceState, SubscribeRequest, SubscribeResponse, SyncPage, PROTOCOL_VERSION,
};
use hc_push_source_api::{now_ms, CancellationToken, DynPeerHints, HealthHandle};
use serde::Deserialize;
use std::collections::{BTreeSet, HashMap};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{broadcast, Semaphore};
use tower::limit::ConcurrencyLimitLayer;
use tower_http::timeout::TimeoutLayer;
use tower_http::trace::TraceLayer;

/// Shared state of the HTTP layer.
pub struct AppState {
    pub settings: RelaySettings,
    pub journal: Arc<Journal>,
    pub relay_id: String,
    pub live: broadcast::Sender<Arc<Vec<EventEnvelope>>>,
    pub sources: Vec<(SourceInfo, HealthHandle)>,
    pub sub_rate: RateLimiter,
    pub reg_rate: RateLimiter,
    pub sse_slots: Arc<Semaphore>,
    pub sse_per_sub: Arc<Mutex<HashMap<String, usize>>>,
    pub shutdown: CancellationToken,
    pub metrics: Arc<Metrics>,
    /// Signed peer records of the conductor next to this relay, if the
    /// source supports it (`GET /v1/peers`).
    pub peers: Option<DynPeerHints>,
}

impl AppState {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        settings: RelaySettings,
        journal: Arc<Journal>,
        relay_id: String,
        live: broadcast::Sender<Arc<Vec<EventEnvelope>>>,
        sources: Vec<(SourceInfo, HealthHandle)>,
        shutdown: CancellationToken,
        metrics: Arc<Metrics>,
        peers: Option<DynPeerHints>,
    ) -> Self {
        Self {
            sub_rate: RateLimiter::new(settings.rate_per_minute, settings.rate_burst, 100_000),
            reg_rate: RateLimiter::new(
                settings.registration_rate_per_minute,
                settings.registration_rate_per_minute.clamp(1, 10),
                10_000,
            ),
            sse_slots: Arc::new(Semaphore::new(settings.max_sse_connections)),
            sse_per_sub: Arc::new(Mutex::new(HashMap::new())),
            settings,
            journal,
            relay_id,
            live,
            sources,
            shutdown,
            metrics,
            peers,
        }
    }

    pub fn cursor(&self, seq: u64) -> Cursor {
        self.journal.cursor(seq)
    }
}

pub type SharedState = Arc<AppState>;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct ApiErr {
    status: StatusCode,
    body: ApiError,
    retry_after: Option<Duration>,
}

impl ApiErr {
    pub fn new(status: StatusCode, code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            status,
            body: ApiError {
                code,
                message: message.into(),
                earliest_cursor: None,
            },
            retry_after: None,
        }
    }

    pub fn bad_request(msg: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, ErrorCode::BadRequest, msg)
    }

    pub fn unauthorized() -> Self {
        Self::new(
            StatusCode::UNAUTHORIZED,
            ErrorCode::Unauthorized,
            "missing or invalid bearer credential",
        )
    }

    pub fn internal() -> Self {
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            ErrorCode::Internal,
            "internal error",
        )
    }

    pub fn rate_limited(wait: Duration) -> Self {
        let mut e = Self::new(
            StatusCode::TOO_MANY_REQUESTS,
            ErrorCode::RateLimited,
            "rate limit exceeded",
        );
        e.retry_after = Some(wait);
        e
    }

    pub fn capacity(msg: &str) -> Self {
        let mut e = Self::new(StatusCode::SERVICE_UNAVAILABLE, ErrorCode::Capacity, msg);
        e.retry_after = Some(Duration::from_secs(30));
        e
    }

    fn with_earliest(mut self, c: Cursor) -> Self {
        self.body.earliest_cursor = Some(c);
        self
    }

    fn from_journal(state: &AppState, err: JournalError) -> Self {
        match err {
            JournalError::CursorExpired { earliest } => Self::new(
                StatusCode::GONE,
                ErrorCode::CursorExpired,
                "events after this cursor have been pruned; reconcile through Holochain and resume from earliest_cursor",
            )
            .with_earliest(state.cursor(earliest)),
            JournalError::CursorAhead { earliest } => Self::new(
                StatusCode::GONE,
                ErrorCode::CursorUnknownJournal,
                "cursor is not known to this journal",
            )
            .with_earliest(state.cursor(earliest)),
            JournalError::Capacity => Self::capacity("subscription capacity reached"),
            other => {
                tracing::error!(error = %other, "journal error");
                Self::internal()
            }
        }
    }
}

impl IntoResponse for ApiErr {
    fn into_response(self) -> Response {
        let mut resp = (self.status, Json(ErrorBody { error: self.body })).into_response();
        if let Some(wait) = self.retry_after {
            let secs = wait.as_secs().max(1).to_string();
            if let Ok(v) = HeaderValue::from_str(&secs) {
                resp.headers_mut().insert(RETRY_AFTER, v);
            }
        }
        if self.status == StatusCode::UNAUTHORIZED {
            resp.headers_mut().insert(
                axum::http::header::WWW_AUTHENTICATE,
                HeaderValue::from_static("Bearer realm=\"hc-push\""),
            );
        }
        resp
    }
}

/// Run a blocking journal operation off the async executor.
pub async fn blocking<T, F>(state: &SharedState, f: F) -> Result<T, ApiErr>
where
    T: Send + 'static,
    F: FnOnce(&Journal) -> Result<T, JournalError> + Send + 'static,
{
    let journal = state.journal.clone();
    match tokio::task::spawn_blocking(move || f(&journal)).await {
        Ok(Ok(v)) => Ok(v),
        Ok(Err(e)) => Err(ApiErr::from_journal(state, e)),
        Err(e) => {
            tracing::error!(error = %e, "journal task panicked");
            Err(ApiErr::internal())
        }
    }
}

// ---------------------------------------------------------------------------
// Authentication extractor
// ---------------------------------------------------------------------------

/// An authenticated subscription (from `Authorization: Bearer hcp_...`).
pub struct AuthedSub(pub StoredSubscription);

impl FromRequestParts<SharedState> for AuthedSub {
    type Rejection = ApiErr;

    async fn from_request_parts(parts: &mut Parts, state: &SharedState) -> Result<Self, ApiErr> {
        let token = parts
            .headers
            .get(AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(parse_bearer)
            .ok_or_else(ApiErr::unauthorized)?;
        if !token.starts_with(auth::TOKEN_PREFIX) {
            return Err(ApiErr::unauthorized());
        }
        let hash = auth::hash_secret(token);
        let sub = blocking(state, move |j| j.subscription_by_token_hash(&hash))
            .await?
            .ok_or_else(ApiErr::unauthorized)?;
        state
            .sub_rate
            .check(&sub.id)
            .map_err(ApiErr::rate_limited)?;
        // Refresh the idle timer at most once a minute to limit writes.
        let now = now_ms();
        if now.saturating_sub(sub.last_seen_at_ms) > 60_000 {
            let id = sub.id.clone();
            let _ = blocking(state, move |j| j.touch_subscription(&id, now)).await;
        }
        Ok(AuthedSub(sub))
    }
}

fn client_addr(state: &AppState, headers: &HeaderMap, peer: SocketAddr) -> String {
    if state.settings.trust_forwarded_for {
        if let Some(last) = headers
            .get("x-forwarded-for")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.rsplit(',').next())
        {
            let last = last.trim();
            if !last.is_empty() && last.len() <= 64 {
                return last.to_string();
            }
        }
    }
    peer.ip().to_string()
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

async fn health(State(state): State<SharedState>) -> Result<Json<Health>, ApiErr> {
    let bounds = blocking(&state, |j| j.bounds()).await?;
    let sources: Vec<SourceHealth> = state
        .sources
        .iter()
        .map(|(info, h)| {
            let (st, last_ok) = h.snapshot();
            SourceHealth {
                id: info.id.clone(),
                state: st,
                last_ok_at_ms: last_ok,
            }
        })
        .collect();
    let status = if sources.iter().all(|s| s.state == SourceState::Connected) {
        HealthStatus::Ok
    } else {
        HealthStatus::Degraded
    };
    Ok(Json(Health {
        status,
        journal_id: state.journal.journal_id().to_string(),
        head_cursor: state.cursor(bounds.head),
        sources,
    }))
}

async fn capabilities(State(state): State<SharedState>) -> Json<Capabilities> {
    let s = &state.settings;
    Json(Capabilities {
        protocol_versions: vec![PROTOCOL_VERSION],
        software: format!("hc-push-relay/{}", env!("CARGO_PKG_VERSION")),
        relay_id: state.relay_id.clone(),
        journal_id: state.journal.journal_id().to_string(),
        spaces: s.advertise_spaces.then(|| s.spaces.clone()),
        sources: state.sources.iter().map(|(i, _)| i.clone()).collect(),
        retention: RetentionInfo {
            max_events: s.retention.max_events,
            max_age_secs: s.retention.max_age.as_secs(),
        },
        limits: LimitsInfo {
            max_page_size: s.max_page_size,
            max_spaces_per_subscription: s.max_spaces_per_subscription as u32,
            max_body_bytes: s.max_body_bytes as u32,
            sse_replay_limit: s.sse_replay_limit,
        },
        transports: vec!["sse".into(), "sync".into()],
        peer_hints: state.peers.is_some(),
        arc_filter: true,
        registration: s.registration_mode,
    })
}

async fn subscribe(
    State(state): State<SharedState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Result<Json<SubscribeRequest>, JsonRejection>,
) -> Result<Response, ApiErr> {
    // Rate-limit registration attempts per client address before doing any
    // work, including failed authentication attempts.
    let addr = client_addr(&state, &headers, peer);
    state.reg_rate.check(&addr).map_err(ApiErr::rate_limited)?;

    if state.settings.registration_mode == RegistrationMode::Key {
        let key = headers
            .get(AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(parse_bearer)
            .ok_or_else(ApiErr::unauthorized)?;
        if !state.settings.registration_keys.verify(key) {
            return Err(ApiErr::unauthorized());
        }
    }

    let Json(req) = body.map_err(|e| match e {
        JsonRejection::BytesRejection(_) => ApiErr::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            ErrorCode::PayloadTooLarge,
            "request body too large",
        ),
        other => ApiErr::bad_request(format!("invalid request body: {}", other.body_text())),
    })?;

    if req.spaces.is_empty() {
        return Err(ApiErr::bad_request("spaces must not be empty"));
    }
    if req.spaces.len() > state.settings.max_spaces_per_subscription {
        return Err(ApiErr::bad_request("too many spaces"));
    }
    let mut spaces = BTreeSet::new();
    for s in &req.spaces {
        if !hc_push_protocol::is_valid_space_id(s) {
            return Err(ApiErr::bad_request("malformed space id"));
        }
        if !state.settings.spaces.iter().any(|served| served == s) {
            return Err(ApiErr::new(
                StatusCode::FORBIDDEN,
                ErrorCode::Forbidden,
                "space is not served by this relay",
            ));
        }
        spaces.insert(s.clone());
    }
    if let Some(label) = &req.label {
        if !hc_push_protocol::is_valid_label(label) {
            return Err(ApiErr::bad_request("malformed label"));
        }
    }
    let spaces: Vec<String> = spaces.into_iter().collect();

    let id = auth::new_subscription_id();
    let token = auth::new_token();
    let token_hash = auth::hash_secret(&token);
    let max = state.settings.max_subscriptions;
    let (id2, spaces2, label) = (id.clone(), spaces.clone(), req.label.clone());
    let bounds = blocking(&state, move |j| {
        j.create_subscription(&id2, &token_hash, &spaces2, label.as_deref(), now_ms(), max)?;
        j.bounds()
    })
    .await?;
    tracing::info!(subscription = %id, spaces = spaces.len(), "subscription created");

    let body = SubscribeResponse {
        subscription_id: id.clone(),
        token,
        spaces,
        cursor: state.cursor(bounds.head),
        earliest_cursor: state.cursor(bounds.pruned_through),
        idle_ttl_secs: state.settings.subscription_idle_ttl.as_secs(),
    };
    let mut resp = (StatusCode::CREATED, Json(body)).into_response();
    if let Ok(v) = HeaderValue::from_str(&format!("/v1/subscriptions/{id}")) {
        resp.headers_mut().insert(LOCATION, v);
    }
    resp.headers_mut()
        .insert("cache-control", HeaderValue::from_static("no-store"));
    Ok(resp)
}

async fn unsubscribe(
    State(state): State<SharedState>,
    AuthedSub(sub): AuthedSub,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiErr> {
    if id != sub.id {
        return Err(ApiErr::new(
            StatusCode::FORBIDDEN,
            ErrorCode::Forbidden,
            "token does not own this subscription",
        ));
    }
    blocking(&state, move |j| j.delete_subscription(&id)).await?;
    tracing::info!(subscription = %sub.id, "subscription deleted");
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SyncQuery {
    pub cursor: Option<String>,
    pub limit: Option<u32>,
    /// Storage arc filter; see [`hc_push_protocol::ArcFilter`].
    pub arc: Option<String>,
}

/// Parse the optional `arc` query parameter.
pub fn parse_arc(raw: Option<&str>) -> Result<Option<ArcFilter>, ApiErr> {
    raw.map(|a| a.parse::<ArcFilter>())
        .transpose()
        .map_err(|e| ApiErr::bad_request(e.to_string()))
}

/// Parse and check a client cursor against this journal. `None` means "from
/// the beginning of the retained window".
pub fn resolve_cursor(state: &AppState, raw: Option<&str>) -> Result<Option<u64>, ApiErr> {
    let Some(raw) = raw else { return Ok(None) };
    let cursor: Cursor = raw
        .parse()
        .map_err(|_| ApiErr::bad_request("malformed cursor"))?;
    if cursor.journal_id != state.journal.journal_id() {
        let earliest = state
            .journal
            .bounds()
            .map(|b| b.pruned_through)
            .unwrap_or(0);
        return Err(ApiErr::new(
            StatusCode::GONE,
            ErrorCode::CursorUnknownJournal,
            "cursor was issued by a different journal; cursors are not interchangeable between relays",
        )
        .with_earliest(state.cursor(earliest)));
    }
    Ok(Some(cursor.seq))
}

async fn sync(
    State(state): State<SharedState>,
    AuthedSub(sub): AuthedSub,
    query: Result<Query<SyncQuery>, axum::extract::rejection::QueryRejection>,
) -> Result<Response, ApiErr> {
    let Query(q) = query.map_err(|e| ApiErr::bad_request(e.body_text()))?;
    let limit = q.limit.unwrap_or(state.settings.default_page_size);
    if limit == 0 || limit > state.settings.max_page_size {
        return Err(ApiErr::bad_request(format!(
            "limit must be between 1 and {}",
            state.settings.max_page_size
        )));
    }
    let after = resolve_cursor(&state, q.cursor.as_deref())?;
    let arc = parse_arc(q.arc.as_deref())?;
    let spaces = sub.spaces.clone();
    let res = blocking(&state, move |j| {
        let after = match after {
            Some(a) => a,
            None => j.bounds()?.pruned_through,
        };
        j.read_after(after, &spaces, arc.as_ref(), limit)
    })
    .await?;
    state.metrics.record_sync_page(res.events.len());
    let page = SyncPage {
        events: res.events,
        next_cursor: state.cursor(res.next_seq),
        head_cursor: state.cursor(res.head),
        has_more: res.has_more,
    };
    let mut resp = Json(page).into_response();
    resp.headers_mut()
        .insert("cache-control", HeaderValue::from_static("no-store"));
    Ok(resp)
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PeersQuery {
    pub space: String,
}

/// `GET /v1/peers?space=...`: signed peer records of this relay's own
/// conductor, so a client's conductor can gossip with it directly.
async fn peers(
    State(state): State<SharedState>,
    AuthedSub(sub): AuthedSub,
    query: Result<Query<PeersQuery>, axum::extract::rejection::QueryRejection>,
) -> Result<Response, ApiErr> {
    let Query(q) = query.map_err(|e| ApiErr::bad_request(e.body_text()))?;
    if !sub.spaces.contains(&q.space) {
        return Err(ApiErr::new(
            StatusCode::FORBIDDEN,
            ErrorCode::Forbidden,
            "space is not part of this subscription",
        ));
    }
    let Some(provider) = &state.peers else {
        return Err(ApiErr::new(
            StatusCode::NOT_FOUND,
            ErrorCode::NotFound,
            "this relay does not offer peer hints",
        ));
    };
    let agent_infos = provider.peer_hints(&q.space).await.map_err(|e| {
        tracing::warn!(error = %e, "peer hints unavailable");
        ApiErr::new(
            StatusCode::SERVICE_UNAVAILABLE,
            ErrorCode::Unavailable,
            "conductor unavailable",
        )
    })?;
    let mut resp = Json(PeersResponse {
        space: q.space,
        agent_infos,
    })
    .into_response();
    resp.headers_mut()
        .insert("cache-control", HeaderValue::from_static("no-store"));
    Ok(resp)
}

async fn not_found() -> ApiErr {
    ApiErr::new(
        StatusCode::NOT_FOUND,
        ErrorCode::NotFound,
        "no such endpoint",
    )
}

/// Build the public router.
pub fn router(state: SharedState) -> Router {
    let request_routes = Router::new()
        .route("/v1/health", get(health))
        .route("/v1/capabilities", get(capabilities))
        .route("/v1/subscriptions", post(subscribe))
        .route("/v1/subscriptions/{id}", delete(unsubscribe))
        .route("/v1/sync", get(sync))
        .route("/v1/peers", get(peers))
        .layer(ConcurrencyLimitLayer::new(
            state.settings.max_concurrent_requests,
        ))
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            state.settings.request_timeout,
        ));
    // SSE is long-lived: excluded from the request timeout and concurrency
    // limit, bounded by its own slot semaphore instead.
    let stream_routes = Router::new().route("/v1/events", get(sse::events));

    request_routes
        .merge(stream_routes)
        .fallback(not_found)
        .layer(DefaultBodyLimit::max(state.settings.max_body_bytes))
        .layer(axum::middleware::from_fn_with_state(
            state.metrics.clone(),
            crate::metrics::track,
        ))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}
