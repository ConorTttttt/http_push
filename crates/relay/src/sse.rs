//! `GET /v1/events`: Server-Sent Events for clients in the foreground.
//!
//! The stream is a *latency optimisation* over `/v1/sync`, never the source of
//! truth. Each event's SSE `id` is its cursor, so a standard `EventSource`
//! reconnects with `Last-Event-ID`. When the backlog after the client's
//! cursor is larger than the replay limit, or the client lags behind the live
//! broadcast, the relay sends `sync_required` and closes the stream; the
//! client then pages through `/v1/sync` and reconnects.

use crate::api::{blocking, resolve_cursor, ApiErr, AuthedSub, SharedState};
use axum::extract::{Query, State};
use axum::http::HeaderMap;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use futures::Stream;
use hc_push_protocol::{
    EventEnvelope, SyncRequired, SyncRequiredReason, SSE_EVENT_CHANGE, SSE_EVENT_SYNC_REQUIRED,
};
use hc_push_source_api::now_ms;
use serde::Deserialize;
use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast::error::RecvError;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventsQuery {
    pub cursor: Option<String>,
}

/// Releases the per-subscription SSE slot when the stream is dropped.
struct PerSubGuard {
    map: Arc<Mutex<HashMap<String, usize>>>,
    id: String,
}

impl Drop for PerSubGuard {
    fn drop(&mut self) {
        let mut m = self.map.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(n) = m.get_mut(&self.id) {
            *n -= 1;
            if *n == 0 {
                m.remove(&self.id);
            }
        }
    }
}

fn change_event(env: &EventEnvelope) -> Event {
    Event::default()
        .event(SSE_EVENT_CHANGE)
        .id(env.cursor.to_string())
        .json_data(env)
        .unwrap_or_else(|_| Event::default().comment("serialization error"))
}

fn sync_required_event(state: &SharedState, reason: SyncRequiredReason, seq: u64) -> Event {
    let body = SyncRequired {
        reason,
        cursor: state.cursor(seq),
    };
    Event::default()
        .event(SSE_EVENT_SYNC_REQUIRED)
        .json_data(body)
        .unwrap_or_else(|_| Event::default().comment("serialization error"))
}

pub async fn events(
    State(state): State<SharedState>,
    AuthedSub(sub): AuthedSub,
    headers: HeaderMap,
    query: Result<Query<EventsQuery>, axum::extract::rejection::QueryRejection>,
) -> Result<Response, ApiErr> {
    let Query(q) = query.map_err(|e| ApiErr::bad_request(e.body_text()))?;
    // Per the SSE spec, Last-Event-ID (sent by reconnecting EventSources)
    // takes precedence over the query parameter.
    let raw = headers
        .get("last-event-id")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
        .or(q.cursor);
    let start = resolve_cursor(&state, raw.as_deref())?;

    // Bounded resources: a global slot and a per-subscription count.
    let permit = state
        .sse_slots
        .clone()
        .try_acquire_owned()
        .map_err(|_| ApiErr::capacity("too many open event streams"))?;
    let guard = {
        let mut m = state.sse_per_sub.lock().unwrap_or_else(|e| e.into_inner());
        let n = m.entry(sub.id.clone()).or_insert(0);
        if *n >= state.settings.max_sse_per_subscription {
            return Err(ApiErr::capacity(
                "too many open event streams for this subscription",
            ));
        }
        *n += 1;
        PerSubGuard {
            map: state.sse_per_sub.clone(),
            id: sub.id.clone(),
        }
    };

    // Subscribe to live events *before* reading the backlog so nothing that
    // commits in between is missed; duplicates are filtered by sequence.
    let mut live = state.live.subscribe();
    let spaces = sub.spaces.clone();
    let replay_limit = state.settings.sse_replay_limit;
    let replay = match start {
        Some(after) => {
            let spaces = spaces.clone();
            Some(blocking(&state, move |j| j.read_after(after, &spaces, replay_limit)).await?)
        }
        None => None,
    };
    let mut last_seq = match (&replay, start) {
        (Some(r), _) => r.next_seq,
        (None, _) => blocking(&state, |j| j.bounds()).await?.head,
    };
    tracing::debug!(subscription = %sub.id, from = last_seq, "event stream opened");

    let shutdown = state.shutdown.clone();
    let st = state.clone();
    let stream = async_stream::stream! {
        let _permit = permit;
        let _guard = guard;
        if let Some(r) = replay {
            if r.has_more {
                yield Ok::<Event, Infallible>(sync_required_event(&st, SyncRequiredReason::BacklogTooLarge, start.unwrap_or(0)));
                return;
            }
            for env in &r.events {
                st.metrics.record_sse_sent(env.observed_at_ms, now_ms());
                yield Ok(change_event(env));
            }
        }
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    yield Ok(sync_required_event(&st, SyncRequiredReason::Shutdown, last_seq));
                    return;
                }
                msg = live.recv() => match msg {
                    Ok(batch) => {
                        for env in batch.iter() {
                            if env.seq() <= last_seq {
                                continue;
                            }
                            last_seq = env.seq();
                            if spaces.contains(&env.space) {
                                st.metrics.record_sse_sent(env.observed_at_ms, now_ms());
                                yield Ok(change_event(env));
                            }
                        }
                    }
                    Err(RecvError::Lagged(_)) => {
                        yield Ok(sync_required_event(&st, SyncRequiredReason::Lagged, last_seq));
                        return;
                    }
                    Err(RecvError::Closed) => return,
                }
            }
        }
    };
    let sse: Sse<_> =
        Sse::new(Box::pin(stream)
            as std::pin::Pin<
                Box<dyn Stream<Item = Result<Event, Infallible>> + Send>,
            >)
        .keep_alive(KeepAlive::new().interval(state.settings.sse_keepalive));
    let mut resp = sse.into_response();
    resp.headers_mut().insert(
        "cache-control",
        axum::http::HeaderValue::from_static("no-store"),
    );
    // Disable proxy buffering (nginx) so events are delivered promptly.
    resp.headers_mut().insert(
        "x-accel-buffering",
        axum::http::HeaderValue::from_static("no"),
    );
    Ok(resp)
}
