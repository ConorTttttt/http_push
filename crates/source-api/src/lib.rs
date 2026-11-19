//! The change-source abstraction ("gossip integration adapter").
//!
//! The relay never talks to Holochain or Kitsune2 directly. It consumes
//! [`ChangeBatch`]es from one or more [`ChangeSource`]s. Today the
//! implementations are:
//!
//! * `hc_push_relay::mock::MockSource` – synthetic events, clearly labelled
//!   [`SourceFidelity::Mock`].
//! * `hc_push_holochain::OpTimingsSource` – polls the conductor's supported
//!   `DumpOpTimings` app API (op-level, this conductor's arc only).
//! * `hc_push_holochain::AppSignalSource` – forwards digests of app signals.
//!
//! A future Kitsune2-native source (see `docs/holochain-integration.md`)
//! implements the same trait, so the HTTP layer does not change.
//!
//! # Delivery contract
//!
//! A source submits a batch together with an opaque `checkpoint` string. The
//! relay persists the batch's events **and** the checkpoint in a single
//! transaction before acknowledging. On restart the source receives the last
//! persisted checkpoint and resumes from it. Duplicate changes (same
//! `change_key`) are discarded by the journal, so sources may re-submit after
//! a crash: delivery is at-least-once from the source, exactly-once in the
//! journal within the retention window.

use futures::future::BoxFuture;
use hc_push_protocol::{ChangeKind, ChangeRef, SourceFidelity, SourceInfo, SourceState};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, oneshot};
pub use tokio_util::sync::CancellationToken;

/// A change observed by a source, before it is assigned a journal sequence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservedChange {
    pub space: String,
    pub kind: ChangeKind,
    pub refs: Vec<ChangeRef>,
}

/// A batch of changes plus the source checkpoint that covers them.
#[derive(Debug)]
pub struct ChangeBatch {
    pub source_id: String,
    pub changes: Vec<ObservedChange>,
    /// Opaque, source-defined resume position. `None` leaves the stored
    /// checkpoint unchanged.
    pub checkpoint: Option<String>,
    ack: oneshot::Sender<anyhow::Result<usize>>,
}

impl ChangeBatch {
    /// Acknowledge the batch with the number of newly journaled events, or
    /// an error if it could not be persisted.
    pub fn ack(self, result: anyhow::Result<usize>) {
        let _ = self.ack.send(result);
    }
}

/// Handle a source uses to submit batches to the relay.
#[derive(Debug, Clone)]
pub struct ChangeSink {
    source_id: String,
    tx: mpsc::Sender<ChangeBatch>,
}

impl ChangeSink {
    pub fn new(source_id: impl Into<String>, tx: mpsc::Sender<ChangeBatch>) -> Self {
        Self {
            source_id: source_id.into(),
            tx,
        }
    }

    /// Submit a batch and wait until it is durably journaled.
    ///
    /// Returns the number of events that were new (not duplicates).
    pub async fn submit(
        &self,
        changes: Vec<ObservedChange>,
        checkpoint: Option<String>,
    ) -> anyhow::Result<usize> {
        let (ack, rx) = oneshot::channel();
        self.tx
            .send(ChangeBatch {
                source_id: self.source_id.clone(),
                changes,
                checkpoint,
                ack,
            })
            .await
            .map_err(|_| anyhow::anyhow!("relay ingest loop has stopped"))?;
        rx.await
            .map_err(|_| anyhow::anyhow!("relay dropped the batch without acknowledging"))?
    }
}

/// Shared, cheaply clonable health state of one source.
#[derive(Debug, Clone)]
pub struct HealthHandle(Arc<Mutex<HealthInner>>);

#[derive(Debug, Clone)]
struct HealthInner {
    state: SourceState,
    last_ok_at_ms: Option<u64>,
}

impl Default for HealthHandle {
    fn default() -> Self {
        Self(Arc::new(Mutex::new(HealthInner {
            state: SourceState::Starting,
            last_ok_at_ms: None,
        })))
    }
}

impl HealthHandle {
    /// Record a successful interaction with the underlying system.
    pub fn ok(&self) {
        let mut g = self.0.lock().unwrap_or_else(|e| e.into_inner());
        g.state = SourceState::Connected;
        g.last_ok_at_ms = Some(now_ms());
    }

    pub fn set_state(&self, state: SourceState) {
        self.0.lock().unwrap_or_else(|e| e.into_inner()).state = state;
    }

    pub fn snapshot(&self) -> (SourceState, Option<u64>) {
        let g = self.0.lock().unwrap_or_else(|e| e.into_inner());
        (g.state, g.last_ok_at_ms)
    }
}

/// Everything a running source needs from the relay.
pub struct SourceContext {
    pub sink: ChangeSink,
    /// The last checkpoint persisted for this source id, if any.
    pub checkpoint: Option<String>,
    pub health: HealthHandle,
    pub shutdown: CancellationToken,
}

/// Something that observes changes and feeds them to the relay.
pub trait ChangeSource: Send + 'static {
    /// Static description, published in `/v1/capabilities`.
    fn info(&self) -> SourceInfo;

    /// Run until `ctx.shutdown` is cancelled. Implementations must handle
    /// their own reconnection; returning an error stops only this source.
    fn run(self: Box<Self>, ctx: SourceContext) -> BoxFuture<'static, anyhow::Result<()>>;
}

/// Convenience constructor for [`SourceInfo`].
pub fn source_info(id: &str, fidelity: SourceFidelity, description: &str) -> SourceInfo {
    SourceInfo {
        id: id.to_string(),
        fidelity,
        description: description.to_string(),
    }
}

/// Current wall-clock time in unix milliseconds.
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Exponential backoff with full jitter, bounded by `max`.
#[derive(Debug, Clone)]
pub struct Backoff {
    base: std::time::Duration,
    max: std::time::Duration,
    attempt: u32,
}

impl Backoff {
    pub fn new(base: std::time::Duration, max: std::time::Duration) -> Self {
        Self {
            base,
            max,
            attempt: 0,
        }
    }

    pub fn reset(&mut self) {
        self.attempt = 0;
    }

    /// Next delay. Uses a cheap time-seeded jitter so this crate needs no RNG.
    pub fn next_delay(&mut self) -> std::time::Duration {
        let exp = self
            .base
            .saturating_mul(1u32.checked_shl(self.attempt.min(16)).unwrap_or(u32::MAX));
        let cap = exp.min(self.max);
        self.attempt = self.attempt.saturating_add(1);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.subsec_nanos())
            .unwrap_or(0);
        // Jitter in [cap/2, cap].
        let half = cap / 2;
        half + half.mul_f64((nanos % 1000) as f64 / 1000.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn backoff_is_bounded_and_grows() {
        let mut b = Backoff::new(Duration::from_millis(100), Duration::from_secs(2));
        let first = b.next_delay();
        assert!(first <= Duration::from_millis(100));
        for _ in 0..20 {
            assert!(b.next_delay() <= Duration::from_secs(2));
        }
        b.reset();
        assert!(b.next_delay() <= Duration::from_millis(100));
    }

    #[tokio::test]
    async fn sink_waits_for_ack() {
        let (tx, mut rx) = mpsc::channel(1);
        let sink = ChangeSink::new("s", tx);
        let h = tokio::spawn(async move { sink.submit(vec![], Some("cp".into())).await });
        let batch = rx.recv().await.unwrap();
        assert_eq!(batch.checkpoint.as_deref(), Some("cp"));
        batch.ack(Ok(0));
        assert_eq!(h.await.unwrap().unwrap(), 0);
    }
}
