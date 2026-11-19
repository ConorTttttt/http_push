//! `DumpOpTimings` poller: the durable reconciliation path.
//!
//! Event-driven notifications (see `signals.rs`) are a latency optimisation
//! only. This poller's cursor is checkpointed atomically with the journal, so
//! after any disconnect, missed signal or relay restart it resumes from the
//! last journaled position and reports every op integrated in between.

use crate::{connect_app, HolochainConfig, OP_TIMINGS_SOURCE_ID};
use futures::future::BoxFuture;
use hc_push_protocol::{ChangeKind, ChangeRef, RefType, SourceFidelity, SourceInfo, SourceState};
use hc_push_source_api::{source_info, Backoff, ChangeSource, ObservedChange, SourceContext};
use holochain_client::AppWebsocket;
use holochain_conductor_api::{OpTimingDump, OpTimingsCursor, OpTimingsDump};
use holochain_types::prelude::{DhtOpHash, DnaHash, RecordValidity};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

/// Stop re-scanning an op that has been pending validation for this many
/// newer ops; it is then only reported if it was integrated before the
/// next scan passes it.
const MAX_PENDING_WINDOW: usize = 20_000;
/// Upper bound on `DumpOpTimings` pages read per DNA per poll.
const MAX_PAGES_PER_POLL: usize = 10;

/// The one conductor call the poller needs. Implemented by the app websocket
/// and by test doubles.
pub trait OpTimingsApi: Send + Sync {
    fn dump_op_timings<'a>(
        &'a self,
        dna: &'a DnaHash,
        cursor: Option<OpTimingsCursor>,
        limit: u32,
    ) -> BoxFuture<'a, anyhow::Result<OpTimingsDump>>;
}

impl OpTimingsApi for AppWebsocket {
    fn dump_op_timings<'a>(
        &'a self,
        dna: &'a DnaHash,
        cursor: Option<OpTimingsCursor>,
        limit: u32,
    ) -> BoxFuture<'a, anyhow::Result<OpTimingsDump>> {
        Box::pin(async move {
            AppWebsocket::dump_op_timings(self, dna.clone(), cursor, Some(limit))
                .await
                .map_err(|e| anyhow::anyhow!("dump_op_timings: {e:?}"))
        })
    }
}

/// Opens a connection to the conductor (reconnects reuse it).
pub trait OpTimingsConnector: Send + Sync + 'static {
    fn connect(&self) -> BoxFuture<'_, anyhow::Result<Box<dyn OpTimingsApi>>>;
}

/// Connects through the app websocket (see [`connect_app`]).
pub struct WsConnector(pub HolochainConfig);

impl OpTimingsConnector for WsConnector {
    fn connect(&self) -> BoxFuture<'_, anyhow::Result<Box<dyn OpTimingsApi>>> {
        Box::pin(async move {
            let app = connect_app(&self.0).await?;
            Ok(Box::new(app) as Box<dyn OpTimingsApi>)
        })
    }
}

/// Serializable resume position for one DNA (exclusive cursor).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScanCursor {
    pub when_received: i64,
    pub op_hash: String,
}

impl ScanCursor {
    pub(crate) fn to_hc(&self) -> anyhow::Result<OpTimingsCursor> {
        Ok(OpTimingsCursor {
            when_received: self.when_received,
            hash: DhtOpHash::try_from(self.op_hash.as_str())
                .map_err(|e| anyhow::anyhow!("bad op hash in checkpoint: {e:?}"))?,
        })
    }

    pub fn of(t: &OpTimingDump) -> Self {
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
                occurred_at_ms: t
                    .when_integrated
                    .map(|w| (w.as_micros().max(0) / 1000) as u64),
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
    connector: Arc<dyn OpTimingsConnector>,
    dnas: Vec<DnaHash>,
    poll_interval: Duration,
    page_limit: u32,
}

impl OpTimingsSource {
    pub fn new(
        connector: Arc<dyn OpTimingsConnector>,
        dnas: Vec<DnaHash>,
        poll_interval: Duration,
        page_limit: u32,
    ) -> Self {
        Self {
            connector,
            dnas,
            poll_interval,
            page_limit: page_limit.max(1),
        }
    }

    async fn scan_dna(
        &self,
        app: &dyn OpTimingsApi,
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
                .dump_op_timings(dna, cursor.clone(), self.page_limit)
                .await?;
            let n = page.timings.len();
            all.extend(page.timings);
            match page.cursor {
                Some(c) if n as u32 >= self.page_limit => cursor = Some(c),
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
                    r = self.connector.connect() => r,
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
                        let step = match self.scan_dna(app.as_ref(), dna, start.clone()).await {
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
                        _ = tokio::time::sleep(self.poll_interval) => {}
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
    fn integrated_ops_carry_their_integration_time() {
        let step = classify(
            "dna",
            None,
            &[t(1, 5_000_000, true, Some(RecordValidity::Accepted))],
            true,
        );
        assert_eq!(step.changes[0].occurred_at_ms, Some(5_000));
    }
}
