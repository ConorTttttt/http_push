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

/// One op reported by an `OpsIntegrated` notification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntegratedOpHint {
    pub op_hash: String,
    pub accepted: bool,
    /// Kitsune2 DHT location of the op (its basis location), if known.
    pub loc: Option<u32>,
}

impl From<(String, bool)> for IntegratedOpHint {
    fn from((op_hash, accepted): (String, bool)) -> Self {
        Self {
            op_hash,
            accepted,
            loc: None,
        }
    }
}

impl From<(String, bool, Option<u32>)> for IntegratedOpHint {
    fn from((op_hash, accepted, loc): (String, bool, Option<u32>)) -> Self {
        Self {
            op_hash,
            accepted,
            loc,
        }
    }
}

/// Kitsune2 DHT location of an op with this basis.
///
/// Holochain builds an op's Kitsune2 id from the op hash core and the basis
/// hash's four location bytes (`DhtOpHash::to_located_k2_op_id`), and
/// `holochain_p2p` sets Kitsune2's op location callback to read those bytes
/// as a little-endian `u32`. That is exactly `HoloHash::get_loc` of the
/// basis, so an op is held by an agent iff its storage arc contains this
/// value.
pub fn basis_loc(basis: &holochain_types::prelude::AnyLinkableHash) -> u32 {
    basis.get_loc()
}

/// Changes for ops reported by an `OpsIntegrated` notification, one per op.
pub fn integrated_ops_to_changes(
    space: &str,
    when_integrated_ms: u64,
    ops: impl IntoIterator<Item = impl Into<IntegratedOpHint>>,
) -> Vec<ObservedChange> {
    ops.into_iter()
        .map(|op| {
            let op: IntegratedOpHint = op.into();
            let r = ChangeRef::new(RefType::DhtOp, op.op_hash);
            ObservedChange {
                space: space.to_string(),
                kind: if op.accepted {
                    ChangeKind::OpIntegrated
                } else {
                    ChangeKind::OpRejected
                },
                refs: vec![match op.loc {
                    Some(l) => r.with_loc(l),
                    None => r,
                }],
                occurred_at_ms: Some(when_integrated_ms),
            }
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
                    Some(basis_loc(&o.basis_hash)),
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
        let c = integrated_ops_to_changes("uhC0kX", 1, [("uhCQkC".to_string(), true, Some(9))]);
        assert_eq!(c[0].refs[0].loc, Some(9));
    }

    #[test]
    fn basis_location_is_the_kitsune2_op_location() {
        use holochain_types::prelude::{AnyLinkableHash, EntryHash};
        // 32 core bytes followed by 4 location bytes, as in a real hash.
        let mut raw = vec![3u8; 32];
        raw.extend_from_slice(&[0x78, 0x56, 0x34, 0x12]);
        let basis = AnyLinkableHash::from(EntryHash::from_raw_36(raw.clone()));
        // What holochain_p2p's `OpId::set_loc_callback` computes from the
        // located op id (op core ++ basis loc bytes).
        let k2 = u32::from_le_bytes(raw[32..36].try_into().unwrap());
        assert_eq!(k2, 0x1234_5678);
        assert_eq!(basis_loc(&basis), k2);
    }
}
