//! Holochain conductor change sources for the HTTP push relay.
//!
//! Uses only **supported, public conductor APIs** (verified against
//! Holochain 0.7.1 / `holochain_client` 0.9.1):
//!
//! | Need                          | API used                                           |
//! |-------------------------------|----------------------------------------------------|
//! | Connect without exposing admin| `AdminWebsocket` on loopback, only to issue a single-use `IssueAppAuthenticationToken` and (if needed) `AttachAppInterface` bound to loopback; then dropped |
//! | Which spaces (DNAs) exist     | `AppInfo.cell_info` on the app websocket           |
//! | Op-level change observation   | `AppRequest::DumpOpTimings { dna_hash, cursor, limit }` (since 0.7.0-rc.5) |
//! | App-level events              | `AppWebsocket::on_signal` (`Signal::App`, `Signal::AppDirect`) |
//!
//! # What this is not
//!
//! This is **not** a Kitsune2 gossip integration. `DumpOpTimings` reports
//! the ops *this conductor* holds for a DNA (its storage arc), after they
//! have passed through Holochain's own validation pipeline. It is polled,
//! not pushed. Op data is never read or forwarded: only op hashes. See
//! `docs/holochain-integration.md` for the gap analysis.

use futures::future::BoxFuture;
use hc_push_protocol::{ChangeKind, ChangeRef, RefType, SourceFidelity, SourceInfo, SourceState};
use hc_push_source_api::{
    source_info, Backoff, CancellationToken, ChangeSource, ObservedChange, SourceContext,
};
use holochain_client::{
    AdminWebsocket, AllowedOrigins, AppWebsocket, CellInfo, ClientAgentSigner,
    IssueAppAuthenticationTokenPayload,
};
use holochain_conductor_api::{OpTimingDump, OpTimingsCursor};
use holochain_types::prelude::{DhtOpHash, DnaHash, RecordValidity};
use holochain_types::signal::Signal;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::time::Duration;

/// Websocket `Origin` the relay presents to the conductor.
pub const ORIGIN: &str = "hc-push-relay";
pub const OP_TIMINGS_SOURCE_ID: &str = "holochain-op-timings";
pub const APP_SIGNAL_SOURCE_ID: &str = "holochain-app-signals";

/// Stop re-scanning an op that has been pending validation for this many
/// newer ops; it is then only reported if it was integrated before the
/// next scan passes it.
const MAX_PENDING_WINDOW: usize = 20_000;
/// Upper bound on `DumpOpTimings` pages read per DNA per poll.
const MAX_PAGES_PER_POLL: usize = 10;

#[derive(Debug, Clone)]
pub struct HolochainConfig {
    /// Admin websocket `host:port` (loopback / shared network namespace).
    pub admin_addr: String,
    pub app_id: String,
    /// Use this existing app interface instead of finding/attaching one.
    pub app_port: Option<u16>,
    pub poll_interval: Duration,
    pub page_limit: u32,
    pub op_timings: bool,
    pub signals: bool,
}

/// Result of [`discover`].
pub struct Discovered {
    /// DNA hashes (base64) of the app's cells.
    pub spaces: Vec<String>,
    pub sources: Vec<Box<dyn ChangeSource>>,
}

fn admin_host(admin_addr: &str) -> &str {
    admin_addr
        .rsplit_once(':')
        .map(|(h, _)| h.trim_start_matches('[').trim_end_matches(']'))
        .unwrap_or("127.0.0.1")
}

/// Explicit loopback bind address for an attached app interface, derived
/// from the admin address. `None` (conductor default: loopback) when the
/// admin host is a name such as `localhost` or a non-loopback address.
fn loopback_bind_addr(admin_addr: &str) -> Option<String> {
    match admin_host(admin_addr).parse::<std::net::IpAddr>() {
        Ok(ip) if ip.is_loopback() => Some(ip.to_string()),
        _ => None,
    }
}

/// Open an authenticated app websocket. The admin connection is used only
/// for token issuance (and interface attachment) and is dropped before
/// returning.
pub async fn connect_app(cfg: &HolochainConfig) -> anyhow::Result<AppWebsocket> {
    let admin = AdminWebsocket::connect(cfg.admin_addr.as_str(), Some(ORIGIN.to_string()))
        .await
        .map_err(|e| anyhow::anyhow!("admin websocket {}: {e:?}", cfg.admin_addr))?;
    let port = match cfg.app_port {
        Some(p) => p,
        None => {
            let existing = admin
                .list_app_interfaces()
                .await
                .map_err(|e| anyhow::anyhow!("list_app_interfaces: {e:?}"))?
                .into_iter()
                .find(|i| {
                    i.allowed_origins.is_allowed(ORIGIN)
                        && i.installed_app_id
                            .as_deref()
                            .is_none_or(|a| a == cfg.app_id)
                });
            match existing {
                Some(i) => i.port,
                None => {
                    // Port 0: the conductor picks a free port. Bind to the
                    // same loopback address we reach the admin port on
                    // (the conductor's default binds both 127.0.0.1 and ::1,
                    // which fails on IPv4-only hosts). Never a public address.
                    let bind = loopback_bind_addr(&cfg.admin_addr);
                    let origins =
                        AllowedOrigins::Origins([ORIGIN.to_string()].into_iter().collect());
                    let p = admin
                        .attach_app_interface(0, bind, origins, Some(cfg.app_id.clone()))
                        .await
                        .map_err(|e| anyhow::anyhow!("attach_app_interface: {e:?}"))?;
                    tracing::info!(port = p, "attached loopback app interface for the relay");
                    p
                }
            }
        }
    };
    let issued = admin
        .issue_app_auth_token(IssueAppAuthenticationTokenPayload::for_installed_app_id(
            cfg.app_id.clone(),
        ))
        .await
        .map_err(|e| anyhow::anyhow!("issue_app_auth_token: {e:?}"))?;
    drop(admin);
    let host = admin_host(&cfg.admin_addr);
    // The relay never signs zome calls; this signer stays empty. Holochain
    // agent keys never leave the conductor's keystore.
    let signer = ClientAgentSigner::default();
    let app = AppWebsocket::connect(
        (host, port),
        issued.token,
        signer.into(),
        Some(ORIGIN.to_string()),
    )
    .await
    .map_err(|e| anyhow::anyhow!("app websocket {host}:{port}: {e:?}"))?;
    Ok(app)
}

/// Unique DNA hashes of all provisioned and cloned cells of the app.
pub fn app_dnas(app: &AppWebsocket) -> Vec<DnaHash> {
    let mut out: Vec<DnaHash> = Vec::new();
    for cells in app.cached_app_info().cell_info.values() {
        for c in cells {
            let dna = match c {
                CellInfo::Provisioned(p) => p.cell_id.dna_hash().clone(),
                CellInfo::Cloned(c) => c.cell_id.dna_hash().clone(),
                _ => continue,
            };
            if !out.contains(&dna) {
                out.push(dna);
            }
        }
    }
    out
}

/// Connect (retrying until `shutdown`), discover spaces, build sources.
pub async fn discover(
    cfg: HolochainConfig,
    shutdown: CancellationToken,
) -> anyhow::Result<Discovered> {
    let mut backoff = Backoff::new(Duration::from_millis(500), Duration::from_secs(30));
    let app = loop {
        match connect_app(&cfg).await {
            Ok(app) => break app,
            Err(e) => {
                let d = backoff.next_delay();
                tracing::warn!(error = %e, retry_in = ?d, "waiting for the Holochain conductor");
                tokio::select! {
                    _ = shutdown.cancelled() => anyhow::bail!("shut down before the conductor became available"),
                    _ = tokio::time::sleep(d) => {}
                }
            }
        }
    };
    let dnas = app_dnas(&app);
    drop(app);
    if dnas.is_empty() {
        anyhow::bail!("app {} has no cells", cfg.app_id);
    }
    let spaces: Vec<String> = dnas.iter().map(|d| d.to_string()).collect();
    tracing::info!(app_id = %cfg.app_id, ?spaces, "discovered Holochain spaces");
    let mut sources: Vec<Box<dyn ChangeSource>> = Vec::new();
    if cfg.op_timings {
        sources.push(Box::new(OpTimingsSource {
            cfg: cfg.clone(),
            dnas: dnas.clone(),
        }));
    }
    if cfg.signals {
        sources.push(Box::new(AppSignalSource { cfg: cfg.clone() }));
    }
    Ok(Discovered { spaces, sources })
}

// ---------------------------------------------------------------------------
// DumpOpTimings poller
// ---------------------------------------------------------------------------

/// Serializable resume position for one DNA (exclusive cursor).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScanCursor {
    pub when_received: i64,
    pub op_hash: String,
}

impl ScanCursor {
    fn to_hc(&self) -> anyhow::Result<OpTimingsCursor> {
        Ok(OpTimingsCursor {
            when_received: self.when_received,
            hash: DhtOpHash::try_from(self.op_hash.as_str())
                .map_err(|e| anyhow::anyhow!("bad op hash in checkpoint: {e:?}"))?,
        })
    }

    fn of(t: &OpTimingDump) -> Self {
        Self {
            when_received: t.when_received.as_micros(),
            op_hash: t.op_hash.to_string(),
        }
    }
}

/// Checkpoint persisted atomically with the journaled events.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpTimingsCheckpoint {
    pub v: u32,
    /// DNA hash (base64) -> exclusive scan start. `None` = from the start.
    pub dnas: BTreeMap<String, Option<ScanCursor>>,
}

/// Outcome of classifying one page of op timings.
#[derive(Debug, Default)]
pub struct ScanStep {
    pub changes: Vec<ObservedChange>,
    /// Where the next scan must start (exclusive).
    pub next: Option<ScanCursor>,
    pub pending: usize,
}

/// Pure classification logic, unit-testable without a conductor.
///
/// `start` is the exclusive cursor the scan began at; `pages` are the
/// consecutive pages returned by `DumpOpTimings`. Ops that reached a final
/// state are turned into changes; the next scan starts just before the first
/// op still pending, so it is re-examined until it concludes. Re-reporting
/// concluded ops after it is harmless: the journal de-duplicates by
/// `change_key`.
///
/// `reached_end` is false when the scan stopped at its per-poll page budget.
/// Holding the low-water mark then would restart every poll at the same
/// pending op and never reach newer ops, so the cursor advances past the
/// scanned region instead (pending ops in it are then not re-checked).
pub fn classify(
    space: &str,
    start: Option<ScanCursor>,
    timings: &[OpTimingDump],
    reached_end: bool,
) -> ScanStep {
    let mut step = ScanStep::default();
    let mut prev = start;
    let mut low_water: Option<Option<ScanCursor>> = None;
    let mut since_low_water = 0usize;
    for t in timings {
        let integrated = t.when_integrated.is_some();
        let abandoned = t.abandoned_at.is_some();
        if !integrated && !abandoned {
            step.pending += 1;
            if low_water.is_none() {
                low_water = Some(prev.clone());
            }
        } else if integrated {
            let kind = match t.validation_status {
                Some(RecordValidity::Rejected) => ChangeKind::OpRejected,
                _ => ChangeKind::OpIntegrated,
            };
            step.changes.push(ObservedChange {
                space: space.to_string(),
                kind,
                refs: vec![ChangeRef::new(RefType::DhtOp, t.op_hash.to_string())],
            });
        }
        if low_water.is_some() {
            since_low_water += 1;
        }
        prev = Some(ScanCursor::of(t));
    }
    step.next = match low_water {
        Some(lw) if reached_end && since_low_water <= MAX_PENDING_WINDOW => lw,
        Some(_) => {
            tracing::warn!(
                space,
                pending = step.pending,
                "backlog behind ops pending validation exceeds the scan budget; advancing past them"
            );
            prev
        }
        None => prev,
    };
    step
}

pub struct OpTimingsSource {
    cfg: HolochainConfig,
    dnas: Vec<DnaHash>,
}

impl OpTimingsSource {
    async fn scan_dna(
        &self,
        app: &AppWebsocket,
        dna: &DnaHash,
        start: Option<ScanCursor>,
    ) -> anyhow::Result<ScanStep> {
        let space = dna.to_string();
        let mut all = Vec::new();
        let mut cursor = start.as_ref().map(|c| c.to_hc()).transpose()?;
        // Bound the work per poll.
        let mut reached_end = false;
        for _ in 0..MAX_PAGES_PER_POLL {
            let page = app
                .dump_op_timings(dna.clone(), cursor.clone(), Some(self.cfg.page_limit))
                .await
                .map_err(|e| anyhow::anyhow!("dump_op_timings: {e:?}"))?;
            let n = page.timings.len();
            all.extend(page.timings);
            match page.cursor {
                Some(c) if n as u32 >= self.cfg.page_limit => cursor = Some(c),
                _ => {
                    reached_end = true;
                    break;
                }
            }
        }
        Ok(classify(&space, start, &all, reached_end))
    }
}

impl ChangeSource for OpTimingsSource {
    fn info(&self) -> SourceInfo {
        source_info(
            OP_TIMINGS_SOURCE_ID,
            SourceFidelity::ConductorOpLevel,
            "Holochain DumpOpTimings poller: op hashes this conductor has integrated (its storage arc only); hints, not validated data",
        )
    }

    fn run(self: Box<Self>, ctx: SourceContext) -> BoxFuture<'static, anyhow::Result<()>> {
        Box::pin(async move {
            let mut cp: OpTimingsCheckpoint = ctx
                .checkpoint
                .as_deref()
                .and_then(|s| serde_json::from_str(s).ok())
                .unwrap_or(OpTimingsCheckpoint {
                    v: 1,
                    ..Default::default()
                });
            let mut backoff = Backoff::new(Duration::from_millis(500), Duration::from_secs(30));
            'outer: loop {
                let app = tokio::select! {
                    _ = ctx.shutdown.cancelled() => break 'outer,
                    r = connect_app(&self.cfg) => r,
                };
                let app = match app {
                    Ok(a) => {
                        backoff.reset();
                        a
                    }
                    Err(e) => {
                        ctx.health.set_state(SourceState::Disconnected);
                        let d = backoff.next_delay();
                        tracing::warn!(error = %e, retry_in = ?d, "op-timings source cannot reach conductor");
                        tokio::select! {
                            _ = ctx.shutdown.cancelled() => break 'outer,
                            _ = tokio::time::sleep(d) => continue 'outer,
                        }
                    }
                };
                ctx.health.ok();
                loop {
                    for dna in &self.dnas {
                        let key = dna.to_string();
                        let start = cp.dnas.get(&key).cloned().flatten();
                        let step = match self.scan_dna(&app, dna, start.clone()).await {
                            Ok(s) => s,
                            Err(e) => {
                                tracing::warn!(error = %e, "op-timings poll failed; reconnecting");
                                ctx.health.set_state(SourceState::Disconnected);
                                continue 'outer;
                            }
                        };
                        let moved = step.next != start;
                        if step.changes.is_empty() && !moved {
                            continue;
                        }
                        let mut next_cp = cp.clone();
                        next_cp.dnas.insert(key, step.next.clone());
                        let n = step.changes.len();
                        match ctx
                            .sink
                            .submit(step.changes, Some(serde_json::to_string(&next_cp)?))
                            .await
                        {
                            Ok(new) => {
                                cp = next_cp;
                                if new > 0 {
                                    tracing::info!(space = %dna, observed = n, new, pending = step.pending, "ops integrated");
                                }
                            }
                            // Keep the old cursor: the same ops are re-read next time.
                            Err(e) => tracing::error!(error = %e, "failed to journal op changes"),
                        }
                    }
                    ctx.health.ok();
                    tokio::select! {
                        _ = ctx.shutdown.cancelled() => break 'outer,
                        _ = tokio::time::sleep(self.cfg.poll_interval) => {}
                    }
                }
            }
            ctx.health.set_state(SourceState::Stopped);
            Ok(())
        })
    }
}

// ---------------------------------------------------------------------------
// App signal digests
// ---------------------------------------------------------------------------

/// `sha256:<hex>` of a signal payload. The payload itself is never stored
/// or forwarded by the relay.
pub fn signal_digest(payload: &[u8]) -> String {
    format!("sha256:{}", hex::encode(Sha256::digest(payload)))
}

/// Map a conductor signal to a change, if it is one we forward.
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
    })
}

pub struct AppSignalSource {
    cfg: HolochainConfig,
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
        Box::pin(async move {
            let mut backoff = Backoff::new(Duration::from_millis(500), Duration::from_secs(30));
            'outer: loop {
                let app = tokio::select! {
                    _ = ctx.shutdown.cancelled() => break 'outer,
                    r = connect_app(&self.cfg) => r,
                };
                let app = match app {
                    Ok(a) => {
                        backoff.reset();
                        a
                    }
                    Err(e) => {
                        ctx.health.set_state(SourceState::Disconnected);
                        let d = backoff.next_delay();
                        tracing::warn!(error = %e, retry_in = ?d, "signal source cannot reach conductor");
                        tokio::select! {
                            _ = ctx.shutdown.cancelled() => break 'outer,
                            _ = tokio::time::sleep(d) => continue 'outer,
                        }
                    }
                };
                let (tx, mut rx) = tokio::sync::mpsc::channel::<ObservedChange>(1024);
                app.on_signal(move |sig| {
                    if let Some(c) = signal_to_change(&sig) {
                        if tx.try_send(c).is_err() {
                            tracing::warn!("signal buffer full; dropping signal digest");
                        }
                    }
                })
                .await;
                ctx.health.ok();
                let mut liveness = tokio::time::interval(Duration::from_secs(10));
                liveness.tick().await;
                loop {
                    tokio::select! {
                        _ = ctx.shutdown.cancelled() => break 'outer,
                        c = rx.recv() => {
                            let Some(first) = c else { continue 'outer };
                            let mut batch = vec![first];
                            while batch.len() < 256 {
                                match rx.try_recv() {
                                    Ok(c) => batch.push(c),
                                    Err(_) => break,
                                }
                            }
                            if let Err(e) = ctx.sink.submit(batch, None).await {
                                tracing::error!(error = %e, "failed to journal signal digests");
                            }
                            ctx.health.ok();
                        }
                        _ = liveness.tick() => {
                            if let Err(e) = app.app_info().await {
                                tracing::warn!(error = ?e, "app websocket lost; reconnecting");
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
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use holochain_types::prelude::Timestamp;

    fn op(n: u8) -> DhtOpHash {
        DhtOpHash::from_raw_32(vec![n; 32])
    }

    fn t(n: u8, received: i64, integrated: bool, status: Option<RecordValidity>) -> OpTimingDump {
        OpTimingDump {
            op_hash: op(n),
            when_received: Timestamp::from_micros(received),
            when_integrated: integrated.then(|| Timestamp::from_micros(received + 10)),
            abandoned_at: None,
            validation_status: status,
            locally_validated: Some(true),
        }
    }

    #[test]
    fn integrated_ops_become_changes_and_cursor_advances() {
        let pages = [
            t(1, 100, true, Some(RecordValidity::Accepted)),
            t(2, 200, true, Some(RecordValidity::Rejected)),
        ];
        let step = classify("dna", None, &pages, true);
        assert_eq!(step.changes.len(), 2);
        assert_eq!(step.changes[0].kind, ChangeKind::OpIntegrated);
        assert_eq!(step.changes[1].kind, ChangeKind::OpRejected);
        assert_eq!(step.changes[0].refs[0].ref_type, RefType::DhtOp);
        assert_eq!(step.changes[0].refs[0].id, op(1).to_string());
        assert_eq!(step.next, Some(ScanCursor::of(&pages[1])));
        assert_eq!(step.pending, 0);
    }

    #[test]
    fn pending_op_holds_the_low_water_mark() {
        let start = Some(ScanCursor {
            when_received: 50,
            op_hash: op(9).to_string(),
        });
        let pages = [
            t(1, 100, true, Some(RecordValidity::Accepted)),
            t(2, 200, false, None), // still validating
            t(3, 300, true, Some(RecordValidity::Accepted)),
        ];
        let step = classify("dna", start, &pages, true);
        assert_eq!(
            step.changes.len(),
            2,
            "concluded ops are reported immediately"
        );
        assert_eq!(step.pending, 1);
        // Next scan resumes right after op 1, so op 2 is re-examined.
        assert_eq!(step.next, Some(ScanCursor::of(&pages[0])));

        // First op pending: resume from the original start.
        let start = Some(ScanCursor {
            when_received: 50,
            op_hash: op(9).to_string(),
        });
        let step = classify("dna", start.clone(), &[t(2, 200, false, None)], true);
        assert_eq!(step.next, start);
    }

    #[test]
    fn budget_exhausted_scan_advances_past_pending_ops() {
        // A stuck pending op followed by more ops than one poll can read
        // must not pin every future scan to the same position.
        let pages = [
            t(1, 100, false, None),
            t(2, 200, true, Some(RecordValidity::Accepted)),
        ];
        let step = classify("dna", None, &pages, false);
        assert_eq!(step.changes.len(), 1);
        assert_eq!(step.next, Some(ScanCursor::of(&pages[1])));
    }

    #[test]
    fn abandoned_ops_are_skipped_without_blocking() {
        let mut a = t(1, 100, false, None);
        a.abandoned_at = Some(Timestamp::from_micros(150));
        let step = classify("dna", None, &[a.clone()], true);
        assert!(step.changes.is_empty());
        assert_eq!(step.next, Some(ScanCursor::of(&a)));
    }

    #[test]
    fn empty_page_keeps_cursor() {
        let start = Some(ScanCursor {
            when_received: 1,
            op_hash: op(1).to_string(),
        });
        let step = classify("dna", start.clone(), &[], true);
        assert_eq!(step.next, start);
    }

    #[test]
    fn scan_cursor_roundtrips_through_holochain_type() {
        let c = ScanCursor::of(&t(7, 1234, true, None));
        let hc = c.to_hc().unwrap();
        assert_eq!(hc.when_received, 1234);
        assert_eq!(hc.hash, op(7));
    }

    #[test]
    fn signal_digest_hides_payload() {
        let d = signal_digest(b"secret payload");
        assert!(d.starts_with("sha256:"));
        assert!(!d.contains("secret"));
        assert!(hc_push_protocol::is_valid_ref_id(&d));
    }
}
