//! Regression: missed integration events and relay restarts never prevent
//! eventual delivery.
//!
//! Uses the real `OpTimingsSource` (the durable `DumpOpTimings` poller) against
//! a scripted conductor, plus an event-driven source that, like
//! `SystemSignal::OpsIntegrated`, is best effort: notices sent while the relay
//! is down, or dropped, are lost. The poller's checkpoint, stored atomically
//! with the journal, must reconcile everything.

#![cfg(feature = "holochain")]

mod common;

use common::*;
use futures::future::BoxFuture;
use hc_push_client::{ClientOptions, MemoryStateStore, PushClient, RelayEndpoint, SyncPolicy};
use hc_push_holochain::types::{
    DhtOpHash, DnaHash, OpTimingDump, OpTimingsCursor, OpTimingsDump, RecordValidity, Timestamp,
};
use hc_push_holochain::{
    integrated_ops_to_changes, OpTimingsApi, OpTimingsConnector, OpTimingsSource,
    OP_TIMINGS_SOURCE_ID,
};
use hc_push_protocol::{ChangeKind, EventEnvelope, SourceFidelity, SourceInfo};
use hc_push_source_api::{source_info, ChangeSource, SourceContext};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::broadcast;

const EVENT_SOURCE_ID: &str = "test-ops-integrated";

fn dna() -> DnaHash {
    DnaHash::from_raw_32(vec![7; 32])
}

fn op(n: u8) -> DhtOpHash {
    DhtOpHash::from_raw_32(vec![n; 32])
}

/// A conductor's DHT store as seen through `DumpOpTimings`.
#[derive(Clone, Default)]
struct FakeConductor {
    ops: Arc<Mutex<Vec<OpTimingDump>>>,
    up: Arc<AtomicBool>,
}

impl FakeConductor {
    fn new() -> Self {
        let c = Self::default();
        c.up.store(true, Ordering::SeqCst);
        c
    }

    /// Receive an op; `integrated_at` is `None` while it is still validating.
    fn receive(&self, n: u8, received_ms: i64, integrated_at: Option<i64>) {
        let mut ops = self.ops.lock().unwrap();
        ops.retain(|t| t.op_hash != op(n));
        ops.push(OpTimingDump {
            op_hash: op(n),
            when_received: Timestamp::from_micros(received_ms * 1000),
            when_integrated: integrated_at.map(|t| Timestamp::from_micros(t * 1000)),
            abandoned_at: None,
            validation_status: integrated_at.map(|_| RecordValidity::Accepted),
            locally_validated: Some(true),
        });
        ops.sort_by(|a, b| {
            (a.when_received.as_micros(), a.op_hash.get_raw_39())
                .cmp(&(b.when_received.as_micros(), b.op_hash.get_raw_39()))
        });
    }
}

impl OpTimingsApi for FakeConductor {
    fn dump_op_timings<'a>(
        &'a self,
        _dna: &'a DnaHash,
        cursor: Option<OpTimingsCursor>,
        limit: u32,
    ) -> BoxFuture<'a, anyhow::Result<OpTimingsDump>> {
        Box::pin(async move {
            if !self.up.load(Ordering::SeqCst) {
                anyhow::bail!("conductor down");
            }
            let ops = self.ops.lock().unwrap();
            let page: Vec<OpTimingDump> = ops
                .iter()
                .filter(|t| match &cursor {
                    None => true,
                    Some(c) => {
                        (t.when_received.as_micros(), t.op_hash.get_raw_39())
                            > (c.when_received, c.hash.get_raw_39())
                    }
                })
                .take(limit as usize)
                .cloned()
                .collect();
            let cursor = page.last().map(|t| OpTimingsCursor {
                when_received: t.when_received.as_micros(),
                hash: t.op_hash.clone(),
            });
            Ok(OpTimingsDump {
                timings: page,
                cursor,
            })
        })
    }
}

impl OpTimingsConnector for FakeConductor {
    fn connect(&self) -> BoxFuture<'_, anyhow::Result<Box<dyn OpTimingsApi>>> {
        Box::pin(async move {
            if !self.up.load(Ordering::SeqCst) {
                anyhow::bail!("conductor down");
            }
            Ok(Box::new(self.clone()) as Box<dyn OpTimingsApi>)
        })
    }
}

/// Integration notice: `(op number, integrated_at_ms)`.
type Notice = (u8, u64);

/// Best-effort event source, like the `OpsIntegrated` signal: it only sees
/// notices broadcast while it is running.
struct EventSource {
    rx: broadcast::Receiver<Notice>,
}

impl ChangeSource for EventSource {
    fn info(&self) -> SourceInfo {
        source_info(
            EVENT_SOURCE_ID,
            SourceFidelity::ConductorOpLevel,
            "test event path",
        )
    }

    fn run(mut self: Box<Self>, ctx: SourceContext) -> BoxFuture<'static, anyhow::Result<()>> {
        Box::pin(async move {
            ctx.health.ok();
            loop {
                tokio::select! {
                    _ = ctx.shutdown.cancelled() => return Ok(()),
                    n = self.rx.recv() => match n {
                        Ok((n, at)) => {
                            let changes = integrated_ops_to_changes(
                                &dna().to_string(),
                                at,
                                [(op(n).to_string(), true)],
                            );
                            ctx.sink.submit(changes, None).await?;
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(broadcast::error::RecvError::Closed) => return Ok(()),
                    }
                }
            }
        })
    }
}

async fn start(
    bind: &str,
    path: &std::path::Path,
    conductor: &FakeConductor,
    events: &broadcast::Sender<Notice>,
    poll: Duration,
) -> TestRelay {
    let poller = OpTimingsSource::new(Arc::new(conductor.clone()), vec![dna()], poll, 2);
    let event_source = EventSource {
        rx: events.subscribe(),
    };
    TestRelay::start_custom_at(
        bind,
        Some(path),
        |s| s.spaces.push(dna().to_string()),
        vec![Box::new(poller), Box::new(event_source)],
        None,
    )
    .await
}

async fn sync_ids(client: &mut PushClient) -> Vec<EventEnvelope> {
    let log: Arc<Mutex<Vec<EventEnvelope>>> = Arc::default();
    let l = log.clone();
    let mut handler = move |e: &EventEnvelope| {
        l.lock().unwrap().push(e.clone());
        Ok(())
    };
    let report = client.sync(SyncPolicy::FirstAvailable, &mut handler).await;
    assert!(report.any_success(), "{:?}", report.outcomes);
    assert!(report.gaps.is_empty());
    let out = log.lock().unwrap().clone();
    out
}

fn ops_of(events: &[EventEnvelope]) -> Vec<String> {
    events.iter().map(|e| e.refs[0].id.clone()).collect()
}

async fn wait_for_head(relay: &TestRelay, head: u64) {
    for _ in 0..200 {
        if relay.journal.bounds().unwrap().head >= head {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!(
        "journal head stuck at {}",
        relay.journal.bounds().unwrap().head
    );
}

#[tokio::test]
async fn missed_integration_events_and_relay_restart_are_reconciled() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("journal.sqlite3");
    let conductor = FakeConductor::new();
    let (events, _) = broadcast::channel::<Notice>(64);
    let now = hc_push_source_api::now_ms() as i64;

    // 1. Relay up. The poller runs once at start, then only every minute, so
    //    anything journaled before the restart arrives via the event path.
    let relay = start(
        "127.0.0.1:0",
        &path,
        &conductor,
        &events,
        Duration::from_secs(60),
    )
    .await;
    let bind = relay.addr.to_string();
    let mut client = PushClient::new(
        vec![RelayEndpoint {
            name: "b".into(),
            url: relay.url.clone(),
            registration_key: Some(KEY.into()),
        }],
        vec![dna().to_string()],
        Box::new(MemoryStateStore::default()),
        ClientOptions {
            page_size: 3,
            ..ClientOptions::default()
        },
    )
    .unwrap();
    client.ensure_registered().await;
    tokio::time::sleep(Duration::from_millis(100)).await; // initial poll done

    for n in 1..=3u8 {
        conductor.receive(n, now + n as i64, Some(now + 10 + n as i64));
        events.send((n, (now + 10 + n as i64) as u64)).unwrap();
    }
    wait_for_head(&relay, 3).await;
    let first = sync_ids(&mut client).await;
    assert_eq!(ops_of(&first), [1, 2, 3].map(|n| op(n).to_string()));
    assert!(
        first.iter().all(|e| e.source == EVENT_SOURCE_ID),
        "delivered by the event path"
    );
    assert!(first.iter().all(|e| e.occurred_at_ms.is_some()));

    // 2. Relay goes down. Ops integrate meanwhile; their notices are lost
    //    (no receiver), and op 4 is still validating.
    relay.stop().await;
    conductor.receive(4, now + 20, None);
    for n in 5..=6u8 {
        conductor.receive(n, now + 20 + n as i64, Some(now + 30 + n as i64));
        let _ = events.send((n, (now + 30 + n as i64) as u64)); // nobody listening
    }

    // 3. Relay restarts on the same journal with a fast poller. It resumes
    //    from its persisted checkpoint and finds 5 and 6; 1-3 are dedup'd.
    let relay = start(
        &bind,
        &path,
        &conductor,
        &events,
        Duration::from_millis(100),
    )
    .await;
    wait_for_head(&relay, 5).await;
    let second = sync_ids(&mut client).await;
    assert_eq!(ops_of(&second), [5, 6].map(|n| op(n).to_string()));
    assert!(second.iter().all(|e| e.source == OP_TIMINGS_SOURCE_ID));

    // 4. Op 4 finishes validation later, and its notice is dropped while the
    //    relay is up. The poller's low-water mark held it; it arrives anyway.
    conductor.receive(4, now + 20, Some(now + 50));
    wait_for_head(&relay, 6).await;
    let third = sync_ids(&mut client).await;
    assert_eq!(ops_of(&third), [op(4).to_string()]);
    assert_eq!(third[0].kind, ChangeKind::OpIntegrated);

    // 5. Conductor down, then back: the poller reconnects and continues.
    conductor.up.store(false, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(300)).await;
    conductor.receive(7, now + 60, Some(now + 61));
    conductor.up.store(true, Ordering::SeqCst);
    wait_for_head(&relay, 7).await;
    let fourth = sync_ids(&mut client).await;
    assert_eq!(ops_of(&fourth), [op(7).to_string()]);

    // Every op exactly once, in journal order, no gaps.
    let all: Vec<EventEnvelope> = [first, second, third, fourth].concat();
    let seqs: Vec<u64> = all.iter().map(|e| e.seq()).collect();
    assert_eq!(seqs, (1..=7).collect::<Vec<_>>());
    assert_eq!(relay.journal.event_count().unwrap(), 7);

    // Detection latency was recorded for both paths.
    let stats: serde_json::Value = reqwest::get(format!("http://{}/admin/v1/stats", relay.admin))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let det = &stats["metrics"]["detection_latency"];
    assert!(det[OP_TIMINGS_SOURCE_ID]["count"].as_u64().unwrap() >= 3);
    relay.stop().await;
}
