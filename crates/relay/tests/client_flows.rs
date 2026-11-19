//! End-to-end flows: real relays on loopback sockets + the portable client.

mod common;

use common::*;
use hc_push_client::{
    ClientOptions, FileStateStore, ListenEnd, ListenOptions, MemoryStateStore, PushClient,
    RelayEndpoint, SyncPolicy,
};
use hc_push_protocol::{ErrorCode, EventEnvelope};
use std::sync::{Arc, Mutex};
use std::time::Duration;

fn endpoint(name: &str, url: &str) -> RelayEndpoint {
    RelayEndpoint {
        name: name.into(),
        url: url.into(),
        registration_key: Some(KEY.into()),
    }
}

fn opts() -> ClientOptions {
    ClientOptions {
        page_size: 4,
        retry_base: Duration::from_millis(10),
        request_timeout: Duration::from_secs(3),
        connect_timeout: Duration::from_secs(1),
        ..ClientOptions::default()
    }
}

type Log = Arc<Mutex<Vec<EventEnvelope>>>;

fn collector() -> (Log, impl FnMut(&EventEnvelope) -> anyhow::Result<()> + Send) {
    let log: Log = Arc::default();
    let l = log.clone();
    (log, move |e: &EventEnvelope| {
        l.lock().unwrap().push(e.clone());
        Ok(())
    })
}

fn ids(log: &Log) -> Vec<String> {
    log.lock()
        .unwrap()
        .iter()
        .map(|e| e.refs[0].id.clone())
        .collect()
}

/// Phase 4: connect, subscribe, disconnect, miss events, reconnect,
/// retrieve, dedup, persist cursor.
#[tokio::test]
async fn disconnected_client_recovers_missed_events() {
    let relay = TestRelay::start(None).await;
    let dir = tempfile::tempdir().unwrap();
    let state_path = dir.path().join("client.json");
    let new_client = || {
        PushClient::new(
            vec![endpoint("a", &relay.url)],
            vec![SPACE.into()],
            Box::new(FileStateStore::new(&state_path)),
            opts(),
        )
        .unwrap()
    };

    // 1-2. Connect and subscribe.
    let mut client = new_client();
    assert!(client
        .ensure_registered()
        .await
        .iter()
        .all(|(_, r)| r.is_ok()));

    // Foreground: a live notification arrives over SSE.
    let (log, mut handler) = collector();
    let mock = relay.mock.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(150)).await;
        mock.inject(SPACE, &["live".to_string()]).await.unwrap();
    });
    let out = client
        .listen(
            "a",
            ListenOptions {
                idle_timeout: Duration::from_millis(600),
                max_duration: Duration::from_secs(5),
            },
            &mut handler,
        )
        .await
        .unwrap();
    assert_eq!(out.end, ListenEnd::Idle);
    assert_eq!(ids(&log), ["live"]);

    // 3-4. App goes to the background (client dropped); events are missed,
    // including a duplicate of one it already saw.
    drop(client);
    for i in 0..9 {
        relay.inject(SPACE, &[&format!("missed-{i}")]).await;
    }
    relay.inject(SPACE, &["live"]).await; // duplicate change: journal drops it

    // 5-8. Reconnect from persisted state, catch up, persist cursor.
    let mut client = new_client();
    let (log, mut handler) = collector();
    let report = client.sync(SyncPolicy::FirstAvailable, &mut handler).await;
    assert!(report.gaps.is_empty());
    assert_eq!(report.served_by.as_deref(), Some("a"));
    assert_eq!(report.delivered, 9);
    assert_eq!(
        ids(&log),
        (0..9).map(|i| format!("missed-{i}")).collect::<Vec<_>>()
    );
    let stats = report.outcomes[0].result.as_ref().unwrap();
    assert_eq!(stats.pages, 3, "paged with page_size=4");
    let saved = FileStateStore::new(&state_path);
    let st = hc_push_client::StateStore::load(&saved).unwrap().unwrap();
    assert_eq!(st.relays["a"].cursor.as_ref().unwrap().seq, 10);

    // Syncing again is a no-op: nothing is delivered twice.
    let (log, mut handler) = collector();
    let report = client.sync(SyncPolicy::FirstAvailable, &mut handler).await;
    assert_eq!(report.delivered, 0);
    assert!(log.lock().unwrap().is_empty());
    relay.stop().await;
}

/// A handler failure must not advance the cursor past the failed event.
#[tokio::test]
async fn handler_failure_keeps_cursor_before_failed_event() {
    let relay = TestRelay::start(None).await;
    let mut client = PushClient::new(
        vec![endpoint("a", &relay.url)],
        vec![SPACE.into()],
        Box::new(MemoryStateStore::default()),
        opts(),
    )
    .unwrap();
    client.ensure_registered().await;
    relay.inject(SPACE, &["ok-1", "boom", "ok-2"]).await;
    let mut handler = |e: &EventEnvelope| {
        if e.refs[0].id == "boom" {
            anyhow::bail!("disk full")
        }
        Ok(())
    };
    let report = client.sync(SyncPolicy::FirstAvailable, &mut handler).await;
    assert!(!report.any_success());
    assert_eq!(client.state().relays["a"].cursor.as_ref().unwrap().seq, 1);
    let (log, mut handler) = collector();
    client.sync(SyncPolicy::FirstAvailable, &mut handler).await;
    assert_eq!(ids(&log), ["boom", "ok-2"]);
    relay.stop().await;
}

/// Retention expiry surfaces a gap notice and the client resumes.
#[tokio::test]
async fn client_reports_gap_after_retention_expiry() {
    let relay = TestRelay::start_with(None, |s| s.retention.max_events = 3).await;
    let mut client = PushClient::new(
        vec![endpoint("a", &relay.url)],
        vec![SPACE.into()],
        Box::new(MemoryStateStore::default()),
        opts(),
    )
    .unwrap();
    client.ensure_registered().await;
    for i in 0..8 {
        relay.inject(SPACE, &[&format!("g{i}")]).await;
    }
    relay.journal.prune(hc_push_source_api::now_ms()).unwrap();
    let (log, mut handler) = collector();
    let report = client.sync(SyncPolicy::FirstAvailable, &mut handler).await;
    assert_eq!(report.gaps.len(), 1);
    assert_eq!(report.gaps[0].code, ErrorCode::CursorExpired);
    assert_eq!(report.gaps[0].spaces, vec![SPACE.to_string()]);
    assert_eq!(
        ids(&log),
        ["g5", "g6", "g7"],
        "retained tail still delivered"
    );
    relay.stop().await;
}

/// Unreachable relay: reported, counted, never fatal.
#[tokio::test]
async fn unavailable_relay_is_reported_not_fatal() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let dead = format!("http://{}", listener.local_addr().unwrap());
    drop(listener);
    let mut client = PushClient::new(
        vec![endpoint("dead", &dead)],
        vec![SPACE.into()],
        Box::new(MemoryStateStore::default()),
        opts(),
    )
    .unwrap();
    let (_log, mut handler) = collector();
    let report = client.sync(SyncPolicy::All, &mut handler).await;
    assert!(!report.any_success());
    assert!(matches!(
        report.outcomes[0].result,
        Err(hc_push_client::RelayError::Unavailable(_))
    ));
}

/// Phase 5: two independent relays with different coverage; the client
/// dedups across them, keeps separate cursors, and fails over.
#[tokio::test]
async fn two_independent_relays_failover_and_dedup() {
    let a = TestRelay::start(None).await;
    let b = TestRelay::start(None).await;
    let mut client = PushClient::new(
        vec![endpoint("a", &a.url), endpoint("b", &b.url)],
        vec![SPACE.into()],
        Box::new(MemoryStateStore::default()),
        opts(),
    )
    .unwrap();
    assert!(client
        .ensure_registered()
        .await
        .iter()
        .all(|(_, r)| r.is_ok()));

    // Overlapping but non-identical coverage: both relays observe x1 and x2
    // (same change => same change_key), only A observes a-only, only B
    // observes b-only.
    a.inject(SPACE, &["x1", "x2", "a-only"]).await;
    b.inject(SPACE, &["b-only", "x2", "x1"]).await;

    let (log, mut handler) = collector();
    let report = client.sync(SyncPolicy::All, &mut handler).await;
    assert_eq!(report.delivered, 4);
    assert_eq!(report.duplicates, 2);
    let mut got = ids(&log);
    got.sort();
    assert_eq!(got, ["a-only", "b-only", "x1", "x2"]);
    let st = client.state();
    assert_ne!(st.relays["a"].journal_id, st.relays["b"].journal_id);

    // Relay A goes away. New changes reach B only.
    a.stop().await;
    b.inject(SPACE, &["after-a-died"]).await;
    let (log, mut handler) = collector();
    let report = client.sync(SyncPolicy::FirstAvailable, &mut handler).await;
    assert_eq!(report.served_by.as_deref(), Some("b"));
    assert!(
        report.outcomes[0].result.is_err(),
        "a was tried first and failed"
    );
    assert_eq!(ids(&log), ["after-a-died"]);
    assert_eq!(client.state().relays["a"].consecutive_failures, 1);
    b.stop().await;
}

/// The relay's sync_required makes `listen` fall back to `/v1/sync`.
#[tokio::test]
async fn listen_falls_back_to_sync_on_large_backlog() {
    let relay = TestRelay::start_with(None, |s| s.sse_replay_limit = 2).await;
    let mut client = PushClient::new(
        vec![endpoint("a", &relay.url)],
        vec![SPACE.into()],
        Box::new(MemoryStateStore::default()),
        opts(),
    )
    .unwrap();
    client.ensure_registered().await;
    for i in 0..6 {
        relay.inject(SPACE, &[&format!("bl{i}")]).await;
    }
    let (log, mut handler) = collector();
    let out = client
        .listen(
            "a",
            ListenOptions {
                idle_timeout: Duration::from_secs(2),
                max_duration: Duration::from_secs(5),
            },
            &mut handler,
        )
        .await
        .unwrap();
    assert_eq!(out.end, ListenEnd::SyncedAfterRequest);
    assert_eq!(ids(&log).len(), 6);
    relay.stop().await;
}

/// Subscription revoked on the relay: the client re-registers and, because
/// the journal is unchanged, keeps its position.
#[tokio::test]
async fn client_reregisters_after_subscription_loss() {
    let relay = TestRelay::start(None).await;
    let mut client = PushClient::new(
        vec![endpoint("a", &relay.url)],
        vec![SPACE.into()],
        Box::new(MemoryStateStore::default()),
        opts(),
    )
    .unwrap();
    client.ensure_registered().await;
    let old_sub = client.state().relays["a"].subscription_id.clone();
    relay.journal.delete_subscription(&old_sub).unwrap();
    relay.inject(SPACE, &["while-revoked"]).await;
    let (log, mut handler) = collector();
    let report = client.sync(SyncPolicy::FirstAvailable, &mut handler).await;
    assert!(report.any_success());
    assert_ne!(client.state().relays["a"].subscription_id, old_sub);
    assert_eq!(ids(&log), ["while-revoked"]);
    relay.stop().await;
}
