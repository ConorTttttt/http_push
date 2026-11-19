//! Ingest loop: change sources -> journal -> live broadcast.

use crate::journal::Journal;
use crate::metrics::Metrics;
use hc_push_protocol::{is_valid_ref_id, is_valid_space_id, EventEnvelope};
use hc_push_source_api::{now_ms, ChangeBatch, ObservedChange};
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc};

/// Upper bound on changes in one batch; sources must page.
pub const MAX_BATCH: usize = 5_000;
/// Upper bound on refs in one change.
pub const MAX_REFS: usize = 64;

fn sanitize(spaces: &HashSet<String>, c: &ObservedChange) -> bool {
    is_valid_space_id(&c.space)
        && spaces.contains(&c.space)
        && !c.refs.is_empty()
        && c.refs.len() <= MAX_REFS
        && c.refs.iter().all(|r| is_valid_ref_id(&r.id))
}

/// Run until every [`hc_push_source_api::ChangeSink`] has been dropped.
pub async fn run(
    journal: Arc<Journal>,
    served_spaces: Vec<String>,
    mut rx: mpsc::Receiver<ChangeBatch>,
    live: broadcast::Sender<Arc<Vec<EventEnvelope>>>,
    metrics: Arc<Metrics>,
) {
    let spaces: HashSet<String> = served_spaces.into_iter().collect();
    while let Some(batch) = rx.recv().await {
        if batch.changes.len() > MAX_BATCH {
            tracing::warn!(source = %batch.source_id, n = batch.changes.len(), "batch too large");
            batch.ack(Err(anyhow::anyhow!("batch exceeds {MAX_BATCH} changes")));
            continue;
        }
        let (valid, invalid): (Vec<_>, Vec<_>) = batch
            .changes
            .iter()
            .cloned()
            .partition(|c| sanitize(&spaces, c));
        if !invalid.is_empty() {
            tracing::warn!(source = %batch.source_id, dropped = invalid.len(), "dropping malformed or out-of-scope changes");
        }
        let valid_count = valid.len();
        let j = journal.clone();
        let source_id = batch.source_id.clone();
        let checkpoint = batch.checkpoint.clone();
        let res = tokio::task::spawn_blocking(move || {
            j.append(&source_id, &valid, checkpoint.as_deref(), now_ms())
        })
        .await;
        match res {
            Ok(Ok(envs)) => {
                let n = envs.len();
                metrics.record_journaled(&batch.source_id, &envs, valid_count - n);
                if n > 0 {
                    tracing::debug!(source = %batch.source_id, new = n, head = envs.last().map(|e| e.seq()), "journaled changes");
                    // No receivers is fine: nobody is listening right now.
                    let _ = live.send(Arc::new(envs));
                }
                batch.ack(Ok(n));
            }
            Ok(Err(e)) => {
                tracing::error!(source = %batch.source_id, error = %e, "journal append failed");
                batch.ack(Err(e.into()));
            }
            Err(e) => {
                tracing::error!(error = %e, "journal append task panicked");
                batch.ack(Err(anyhow::anyhow!("journal task panicked")));
            }
        }
    }
    tracing::debug!("ingest loop finished");
}
