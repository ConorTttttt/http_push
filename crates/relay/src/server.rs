//! Wires sources, ingest, maintenance and the HTTP listeners together.

use crate::api::{self, AppState, SharedState};
use crate::ingest;
use crate::journal::Journal;
use crate::metrics::Metrics;
use crate::mock::MockInjector;
use crate::settings::RelaySettings;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use hc_push_source_api::{
    now_ms, CancellationToken, ChangeSink, ChangeSource, DynPeerHints, HealthHandle, SourceContext,
};
use serde::Deserialize;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::{broadcast, mpsc};

pub struct ServeOptions {
    pub settings: RelaySettings,
    pub journal: Arc<Journal>,
    /// Operator-chosen relay id; defaults to the journal's random id.
    pub relay_id: Option<String>,
    pub sources: Vec<Box<dyn ChangeSource>>,
    pub public: TcpListener,
    /// Loopback-only operator listener (mock injection, stats).
    pub admin: Option<TcpListener>,
    pub mock: Option<MockInjector>,
    /// Peer records of the relay's own conductor (`GET /v1/peers`).
    pub peers: Option<DynPeerHints>,
    pub shutdown: CancellationToken,
}

/// Run the relay until `shutdown` is cancelled, then drain and return.
pub async fn serve(opts: ServeOptions) -> anyhow::Result<()> {
    let ServeOptions {
        settings,
        journal,
        relay_id,
        sources,
        public,
        admin,
        mock,
        peers,
        shutdown,
    } = opts;
    let metrics = Arc::new(Metrics::new());

    let (live, _) = broadcast::channel(1024);
    let (batch_tx, batch_rx) = mpsc::channel(64);

    // Start change sources.
    let sources_shutdown = CancellationToken::new();
    let mut source_infos = Vec::new();
    let mut source_tasks = Vec::new();
    for source in sources {
        let info = source.info();
        let health = HealthHandle::default();
        let j = journal.clone();
        let id = info.id.clone();
        let checkpoint = tokio::task::spawn_blocking(move || j.checkpoint(&id)).await??;
        let ctx = SourceContext {
            sink: ChangeSink::new(info.id.clone(), batch_tx.clone()),
            checkpoint,
            health: health.clone(),
            shutdown: sources_shutdown.child_token(),
        };
        let id = info.id.clone();
        source_tasks.push(tokio::spawn(async move {
            if let Err(e) = source.run(ctx).await {
                tracing::error!(source = %id, error = %e, "change source stopped with error");
            }
        }));
        tracing::info!(source = %info.id, fidelity = ?info.fidelity, "change source started");
        source_infos.push((info, health));
    }
    drop(batch_tx);
    let ingest_task = tokio::spawn(ingest::run(
        journal.clone(),
        settings.spaces.clone(),
        batch_rx,
        live.clone(),
        metrics.clone(),
    ));

    // Periodic retention + subscription expiry.
    let maint_journal = journal.clone();
    let maint_shutdown = shutdown.clone();
    let interval = settings.maintenance_interval;
    let idle_ttl = settings.subscription_idle_ttl;
    let maint_task = tokio::spawn(async move {
        let mut t = tokio::time::interval(interval);
        loop {
            tokio::select! {
                _ = maint_shutdown.cancelled() => break,
                _ = t.tick() => {
                    let j = maint_journal.clone();
                    let r = tokio::task::spawn_blocking(move || {
                        let now = now_ms();
                        Ok::<_, crate::journal::JournalError>((j.prune(now)?, j.prune_subscriptions(now, idle_ttl)?))
                    }).await;
                    match r {
                        Ok(Ok((e, s))) if e > 0 || s > 0 => tracing::info!(pruned_events = e, expired_subscriptions = s, "retention applied"),
                        Ok(Ok(_)) => {}
                        Ok(Err(e)) => tracing::error!(error = %e, "retention failed"),
                        Err(e) => tracing::error!(error = %e, "retention task panicked"),
                    }
                }
            }
        }
    });

    let relay_id = relay_id.unwrap_or_else(|| journal.relay_id().to_string());
    let state: SharedState = Arc::new(AppState::new(
        settings,
        journal.clone(),
        relay_id.clone(),
        live,
        source_infos,
        shutdown.clone(),
        metrics,
        peers,
    ));

    let public_addr = public.local_addr()?;
    tracing::info!(%public_addr, relay_id = %relay_id, journal_id = %journal.journal_id(), "relay listening");
    let public_srv = axum::serve(
        public,
        api::router(state.clone()).into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown.clone().cancelled_owned());

    let admin_srv = admin.map(|listener| {
        let addr = listener.local_addr().ok();
        tracing::info!(admin_addr = ?addr, "admin listener (loopback only)");
        axum::serve(
            listener,
            admin_router(AdminState {
                app: state.clone(),
                mock,
            }),
        )
        .with_graceful_shutdown(shutdown.clone().cancelled_owned())
    });

    match admin_srv {
        Some(a) => {
            let (p, a) = tokio::join!(public_srv, a);
            p?;
            a?;
        }
        None => public_srv.await?,
    }

    // Drain: stop sources, let ingest finish pending batches.
    tracing::info!("shutting down: stopping change sources");
    sources_shutdown.cancel();
    let drain = async {
        for t in source_tasks {
            let _ = t.await;
        }
        let _ = ingest_task.await;
    };
    if tokio::time::timeout(Duration::from_secs(10), drain)
        .await
        .is_err()
    {
        tracing::warn!("timed out waiting for sources to stop");
    }
    maint_task.abort();
    tracing::info!("relay stopped");
    Ok(())
}

// ---------------------------------------------------------------------------
// Admin (loopback) listener
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct AdminState {
    app: SharedState,
    mock: Option<MockInjector>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct InjectBody {
    space: String,
    ids: Vec<String>,
}

async fn admin_inject(
    State(st): State<AdminState>,
    Json(body): Json<InjectBody>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let Some(mock) = &st.mock else {
        return Err((StatusCode::NOT_FOUND, "mock source not enabled".into()));
    };
    if body.ids.is_empty() || body.ids.len() > 1000 {
        return Err((
            StatusCode::BAD_REQUEST,
            "ids must contain 1..=1000 entries".into(),
        ));
    }
    let new = mock
        .inject(&body.space, &body.ids)
        .await
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    Ok(Json(
        serde_json::json!({ "submitted": body.ids.len(), "new": new }),
    ))
}

async fn admin_stats(State(st): State<AdminState>) -> Result<Json<serde_json::Value>, StatusCode> {
    let j = st.app.journal.clone();
    let (bounds, events, subs) = tokio::task::spawn_blocking(move || {
        Ok::<_, crate::journal::JournalError>((
            j.bounds()?,
            j.event_count()?,
            j.subscription_count()?,
        ))
    })
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let sse_open = st.app.settings.max_sse_connections - st.app.sse_slots.available_permits();
    Ok(Json(serde_json::json!({
        "relay_id": st.app.relay_id,
        "journal_id": st.app.journal.journal_id(),
        "head": bounds.head,
        "pruned_through": bounds.pruned_through,
        "events_retained": events,
        "subscriptions": subs,
        "sse_open": sse_open,
        "metrics": st.app.metrics.snapshot(),
    })))
}

async fn admin_metrics(State(st): State<AdminState>) -> Result<String, StatusCode> {
    let j = st.app.journal.clone();
    let (bounds, events, subs) = tokio::task::spawn_blocking(move || {
        Ok::<_, crate::journal::JournalError>((
            j.bounds()?,
            j.event_count()?,
            j.subscription_count()?,
        ))
    })
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let sse_open =
        (st.app.settings.max_sse_connections - st.app.sse_slots.available_permits()) as f64;
    Ok(st.app.metrics.prometheus(&[
        (
            "hcpush_journal_head_seq",
            "Highest journal sequence",
            bounds.head as f64,
        ),
        (
            "hcpush_journal_pruned_through_seq",
            "Events up to here were pruned",
            bounds.pruned_through as f64,
        ),
        (
            "hcpush_journal_events_retained",
            "Events currently retained",
            events as f64,
        ),
        ("hcpush_subscriptions", "Active subscriptions", subs as f64),
        ("hcpush_sse_streams_open", "Open SSE streams", sse_open),
    ]))
}

fn admin_router(state: AdminState) -> Router {
    Router::new()
        .route("/admin/v1/mock/changes", post(admin_inject))
        .route("/admin/v1/stats", get(admin_stats))
        .route("/admin/v1/metrics", get(admin_metrics))
        .layer(axum::extract::DefaultBodyLimit::max(256 * 1024))
        .with_state(state)
}
