//! MOCK change source.
//!
//! Produces synthetic [`ChangeKind::MockChange`] events with
//! [`RefType::Mock`] references. These never correspond to DHT data and are
//! labelled [`SourceFidelity::Mock`] in `/v1/capabilities` and in every
//! envelope's `source` field (`"mock"`). It exists so the HTTP protocol,
//! journal and clients can be developed and tested without a conductor.
//!
//! Events are produced (a) on an optional timer and (b) through
//! [`MockInjector`], exposed on the loopback-only admin listener.

use futures::future::BoxFuture;
use hc_push_protocol::{ChangeKind, ChangeRef, RefType, SourceFidelity, SourceInfo, SourceState};
use hc_push_source_api::{source_info, ChangeSource, ObservedChange, SourceContext};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

pub const MOCK_SOURCE_ID: &str = "mock";

type InjectMsg = (Vec<ObservedChange>, oneshot::Sender<anyhow::Result<usize>>);

pub struct MockSource {
    spaces: Vec<String>,
    interval: Option<Duration>,
    rx: mpsc::Receiver<InjectMsg>,
}

/// Handle used to inject synthetic changes.
#[derive(Clone, Debug)]
pub struct MockInjector {
    spaces: Vec<String>,
    tx: mpsc::Sender<InjectMsg>,
}

impl MockInjector {
    /// Inject one change per id into `space`. Returns how many were new.
    pub async fn inject(&self, space: &str, ids: &[String]) -> anyhow::Result<usize> {
        if !self.spaces.iter().any(|s| s == space) {
            anyhow::bail!("space is not served by the mock source");
        }
        let changes = ids
            .iter()
            .map(|id| ObservedChange {
                space: space.to_string(),
                kind: ChangeKind::MockChange,
                refs: vec![ChangeRef::new(RefType::Mock, id.clone())],
            })
            .collect();
        let (tx, rx) = oneshot::channel();
        self.tx
            .send((changes, tx))
            .await
            .map_err(|_| anyhow::anyhow!("mock source is not running"))?;
        rx.await
            .map_err(|_| anyhow::anyhow!("mock source dropped request"))?
    }
}

/// Create a mock source and its injector.
pub fn mock_source(spaces: Vec<String>, interval: Option<Duration>) -> (MockSource, MockInjector) {
    let (tx, rx) = mpsc::channel(64);
    (
        MockSource {
            spaces: spaces.clone(),
            interval,
            rx,
        },
        MockInjector { spaces, tx },
    )
}

impl ChangeSource for MockSource {
    fn info(&self) -> SourceInfo {
        source_info(
            MOCK_SOURCE_ID,
            SourceFidelity::Mock,
            "MOCK: synthetic changes for development and tests; not DHT data",
        )
    }

    fn run(mut self: Box<Self>, ctx: SourceContext) -> BoxFuture<'static, anyhow::Result<()>> {
        Box::pin(async move {
            // The checkpoint is the tick counter, so generated ids stay
            // unique across restarts.
            let mut tick: u64 = ctx
                .checkpoint
                .as_deref()
                .and_then(|c| c.parse().ok())
                .unwrap_or(0);
            ctx.health.ok();
            let mut timer = self.interval.map(|d| {
                let mut t = tokio::time::interval(d);
                t.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                t
            });
            loop {
                tokio::select! {
                    _ = ctx.shutdown.cancelled() => break,
                    msg = self.rx.recv() => {
                        let Some((changes, reply)) = msg else { break };
                        let r = ctx.sink.submit(changes, None).await;
                        let _ = reply.send(r);
                        ctx.health.ok();
                    }
                    _ = async { timer.as_mut().unwrap().tick().await }, if timer.is_some() => {
                        tick += 1;
                        let changes = self.spaces.iter().map(|s| ObservedChange {
                            space: s.clone(),
                            kind: ChangeKind::MockChange,
                            refs: vec![ChangeRef::new(RefType::Mock, format!("tick-{tick}"))],
                        }).collect();
                        if let Err(e) = ctx.sink.submit(changes, Some(tick.to_string())).await {
                            tracing::warn!(error = %e, "mock source submit failed");
                        }
                        ctx.health.ok();
                    }
                }
            }
            ctx.health.set_state(SourceState::Stopped);
            Ok(())
        })
    }
}
