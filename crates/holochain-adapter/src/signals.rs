//! Signal-driven sources: app signal digests and, with the
//! `ops-integrated-signal` feature, the event-driven `OpsIntegrated` system
//! signal from a patched conductor (see `upstream/`).
//!
//! Signals are best effort: they are not replayed after a reconnect and the
//! conductor drops them for lagging connections. Durability comes from the
//! `DumpOpTimings` poller, which always runs alongside.

use crate::{connect_app, HolochainConfig, APP_SIGNAL_SOURCE_ID};
use futures::future::BoxFuture;
use hc_push_protocol::{ChangeKind, ChangeRef, RefType, SourceFidelity, SourceInfo, SourceState};
use hc_push_source_api::{source_info, Backoff, ChangeSource, ObservedChange, SourceContext};
use holochain_client::AppWebsocket;
use holochain_types::prelude::DnaHash;
use holochain_types::signal::Signal;
use sha2::{Digest, Sha256};
use std::time::Duration;

/// `sha256:<hex>` of a signal payload. The payload itself is never stored
/// or forwarded by the relay.
pub fn signal_digest(payload: &[u8]) -> String {
    format!("sha256:{}", hex::encode(Sha256::digest(payload)))
}

/// Map a conductor app signal to a change, if it is one we forward.
pub fn signal_to_change(sig: &Signal) -> Option<ObservedChange> {
    let (dna, payload): (&DnaHash, &[u8]) = match sig {
        Signal::App {
            cell_id, signal, ..
        } => (cell_id.dna_hash(), &signal.clone().into_inner().0),
        Signal::AppDirect {
            cell_id, signal, ..
        } => (cell_id.dna_hash(), signal.as_slice()),
        _ => return None,
    };
    // Borrow checker: compute the digest before returning.
    let digest = signal_digest(payload);
    Some(ObservedChange {
        space: dna.to_string(),
        kind: ChangeKind::AppSignal,
        refs: vec![ChangeRef::new(RefType::Digest, digest)],
        occurred_at_ms: Some(hc_push_source_api::now_ms()),
    })
}

/// Changes for ops reported by an `OpsIntegrated` notification.
/// `ops` are `(op hash, accepted)`.
pub fn integrated_ops_to_changes(
    space: &str,
    when_integrated_ms: u64,
    ops: impl IntoIterator<Item = (String, bool)>,
) -> Vec<ObservedChange> {
    ops.into_iter()
        .map(|(op_hash, accepted)| ObservedChange {
            space: space.to_string(),
            kind: if accepted {
                ChangeKind::OpIntegrated
            } else {
                ChangeKind::OpRejected
            },
            refs: vec![ChangeRef::new(RefType::DhtOp, op_hash)],
            occurred_at_ms: Some(when_integrated_ms),
        })
        .collect()
}

/// Map `SystemSignal::OpsIntegrated` (patched conductor) to changes.
#[cfg(feature = "ops-integrated-signal")]
pub fn ops_integrated_signal_to_changes(sig: &Signal) -> Vec<ObservedChange> {
    use holochain_types::prelude::RecordValidity;
    use holochain_types::signal::SystemSignal;
    match sig {
        Signal::System(SystemSignal::OpsIntegrated {
            dna_hash,
            when_integrated,
            ops,
        }) => integrated_ops_to_changes(
            &dna_hash.to_string(),
            (when_integrated.as_micros().max(0) / 1000) as u64,
            ops.iter().map(|o| {
                (
                    o.op_hash.to_string(),
                    o.validity == RecordValidity::Accepted,
                )
            }),
        ),
        _ => Vec::new(),
    }
}

/// Ask the conductor to deliver `OpsIntegrated` on this connection.
#[cfg(feature = "ops-integrated-signal")]
pub async fn subscribe_ops_integrated(app: &AppWebsocket) -> anyhow::Result<()> {
    app.subscribe_ops_integrated()
        .await
        .map_err(|e| anyhow::anyhow!("conductor rejected SubscribeOpsIntegrated: {e:?}"))
}

type SignalMap = fn(&Signal) -> Vec<ObservedChange>;

/// Connect, register `map` as the signal handler, journal what it returns,
/// check liveness, reconnect with backoff. Shared by all signal sources.
async fn run_signal_loop(
    cfg: HolochainConfig,
    ctx: SourceContext,
    label: &'static str,
    map: SignalMap,
    subscribe_ops: bool,
) -> anyhow::Result<()> {
    let _ = subscribe_ops;
    let mut backoff = Backoff::new(Duration::from_millis(500), Duration::from_secs(30));
    'outer: loop {
        let app = tokio::select! {
            _ = ctx.shutdown.cancelled() => break 'outer,
            r = connect_app(&cfg) => r,
        };
        let app: AppWebsocket = match app {
            Ok(a) => {
                backoff.reset();
                a
            }
            Err(e) => {
                ctx.health.set_state(SourceState::Disconnected);
                let d = backoff.next_delay();
                tracing::warn!(source = label, error = %e, retry_in = ?d, "signal source cannot reach conductor");
                tokio::select! {
                    _ = ctx.shutdown.cancelled() => break 'outer,
                    _ = tokio::time::sleep(d) => continue 'outer,
                }
            }
        };
        let (tx, mut rx) = tokio::sync::mpsc::channel::<ObservedChange>(4096);
        app.on_signal(move |sig| {
            for c in map(&sig) {
                if tx.try_send(c).is_err() {
                    tracing::warn!(
                        source = label,
                        "signal buffer full; dropping (poller will reconcile)"
                    );
                }
            }
        })
        .await;
        #[cfg(feature = "ops-integrated-signal")]
        if subscribe_ops {
            if let Err(e) = subscribe_ops_integrated(&app).await {
                ctx.health.set_state(SourceState::Disconnected);
                let d = backoff.next_delay();
                tracing::warn!(source = label, error = %e, retry_in = ?d, "could not subscribe to OpsIntegrated");
                tokio::select! {
                    _ = ctx.shutdown.cancelled() => break 'outer,
                    _ = tokio::time::sleep(d) => continue 'outer,
                }
            }
        }
        ctx.health.ok();
        let mut liveness = tokio::time::interval(Duration::from_secs(10));
        liveness.tick().await;
        loop {
            tokio::select! {
                _ = ctx.shutdown.cancelled() => break 'outer,
                c = rx.recv() => {
                    let Some(first) = c else { continue 'outer };
                    let mut batch = vec![first];
                    while batch.len() < 1024 {
                        match rx.try_recv() {
                            Ok(c) => batch.push(c),
                            Err(_) => break,
                        }
                    }
                    if let Err(e) = ctx.sink.submit(batch, None).await {
                        tracing::error!(source = label, error = %e, "failed to journal signal changes");
                    }
                    ctx.health.ok();
                }
                _ = liveness.tick() => {
                    if let Err(e) = app.app_info().await {
                        tracing::warn!(source = label, error = ?e, "app websocket lost; reconnecting");
                        ctx.health.set_state(SourceState::Disconnected);
                        continue 'outer;
                    }
                    ctx.health.ok();
                }
            }
        }
    }
    ctx.health.set_state(SourceState::Stopped);
    Ok(())
}

pub struct AppSignalSource {
    pub(crate) cfg: HolochainConfig,
}

impl ChangeSource for AppSignalSource {
    fn info(&self) -> SourceInfo {
        source_info(
            APP_SIGNAL_SOURCE_ID,
            SourceFidelity::ConductorAppSignal,
            "Digests of app signals emitted by zomes on this conductor (local events only; payloads are not forwarded)",
        )
    }

    fn run(self: Box<Self>, ctx: SourceContext) -> BoxFuture<'static, anyhow::Result<()>> {
        Box::pin(run_signal_loop(
            self.cfg,
            ctx,
            APP_SIGNAL_SOURCE_ID,
            |s| signal_to_change(s).into_iter().collect(),
            false,
        ))
    }
}

/// Event-driven op notifications from a conductor with the `OpsIntegrated`
/// upstream patch. Journals immediately; the poller later finds the same ops
/// and the journal drops them as duplicates.
#[cfg(feature = "ops-integrated-signal")]
pub struct OpsIntegratedSource {
    pub(crate) cfg: HolochainConfig,
}

#[cfg(feature = "ops-integrated-signal")]
impl ChangeSource for OpsIntegratedSource {
    fn info(&self) -> SourceInfo {
        source_info(
            crate::OPS_INTEGRATED_SOURCE_ID,
            SourceFidelity::ConductorOpLevel,
            "Event-driven SystemSignal::OpsIntegrated (patched conductor): op hashes as soon as this conductor integrates them; best effort, reconciled by the DumpOpTimings poller",
        )
    }

    fn run(self: Box<Self>, ctx: SourceContext) -> BoxFuture<'static, anyhow::Result<()>> {
        Box::pin(run_signal_loop(
            self.cfg,
            ctx,
            crate::OPS_INTEGRATED_SOURCE_ID,
            ops_integrated_signal_to_changes,
            true,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signal_digest_hides_payload() {
        let d = signal_digest(b"secret payload");
        assert!(d.starts_with("sha256:"));
        assert!(!d.contains("secret"));
        assert!(hc_push_protocol::is_valid_ref_id(&d));
    }

    #[test]
    fn integrated_ops_map_to_kinds_with_event_time() {
        let c = integrated_ops_to_changes(
            "uhC0kX",
            1234,
            [("uhCQkA".to_string(), true), ("uhCQkB".to_string(), false)],
        );
        assert_eq!(c[0].kind, ChangeKind::OpIntegrated);
        assert_eq!(c[1].kind, ChangeKind::OpRejected);
        assert_eq!(c[0].occurred_at_ms, Some(1234));
        assert_eq!(c[1].refs[0], ChangeRef::new(RefType::DhtOp, "uhCQkB"));
    }
}
