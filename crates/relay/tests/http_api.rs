//! HTTP API tests against a real relay on a loopback socket (mock source).

mod common;

use common::*;
use futures::StreamExt;
use hc_push_protocol::{
    Capabilities, Cursor, ErrorBody, ErrorCode, EventEnvelope, Health, SourceFidelity, SyncPage,
    SyncRequired, SyncRequiredReason,
};
use std::time::Duration;

#[tokio::test]
async fn health_and_capabilities() {
    let r = TestRelay::start(None).await;
    let http = reqwest::Client::new();
    let h: Health = http
        .get(format!("{}/v1/health", r.url))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(h.journal_id, r.journal.journal_id());
    assert_eq!(h.head_cursor.seq, 0);
    let c: Capabilities = http
        .get(format!("{}/v1/capabilities", r.url))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(c.protocol_versions, vec![1]);
    assert_eq!(c.sources.len(), 1);
    assert_eq!(
        c.sources[0].fidelity,
        SourceFidelity::Mock,
        "mock must be labelled"
    );
    assert!(c.spaces.unwrap().contains(&SPACE.to_string()));
    // Unknown paths and wrong methods.
    assert_eq!(
        http.get(format!("{}/v2/sync", r.url))
            .send()
            .await
            .unwrap()
            .status(),
        404
    );
    assert_eq!(
        http.put(format!("{}/v1/sync", r.url))
            .send()
            .await
            .unwrap()
            .status(),
        405
    );
    r.stop().await;
}

#[tokio::test]
async fn subscription_requires_registration_key_and_valid_input() {
    let r = TestRelay::start(None).await;
    let http = reqwest::Client::new();
    let url = format!("{}/v1/subscriptions", r.url);
    let body = serde_json::json!({ "spaces": [SPACE] });

    let resp = http.post(&url).json(&body).send().await.unwrap();
    assert_eq!(resp.status(), 401);
    assert!(resp.headers().contains_key("www-authenticate"));
    let resp = http
        .post(&url)
        .bearer_auth("wrong-key-wrong-key")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);

    let cases = [
        (serde_json::json!({ "spaces": [] }), 400),
        (serde_json::json!({ "spaces": ["not a space"] }), 400),
        (serde_json::json!({ "spaces": ["uhC0kNotServed"] }), 403),
        (
            serde_json::json!({ "spaces": [SPACE], "agent_private_key": "x" }),
            400,
        ),
        (
            serde_json::json!({ "spaces": [SPACE], "label": "bad\u{0007}" }),
            400,
        ),
    ];
    for (b, status) in cases {
        let resp = http
            .post(&url)
            .bearer_auth(KEY)
            .json(&b)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), status, "body {b}");
        let err: ErrorBody = resp.json().await.unwrap();
        assert!(!err.error.message.is_empty());
    }
    // Oversized body.
    let huge = serde_json::json!({ "spaces": [SPACE], "label": "x".repeat(64 * 1024) });
    let resp = http
        .post(&url)
        .bearer_auth(KEY)
        .json(&huge)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 413);

    let sub = subscribe(&http, &r.url, &[SPACE, SPACE]).await;
    assert_eq!(
        sub.spaces,
        vec![SPACE.to_string()],
        "spaces are de-duplicated"
    );
    assert!(sub.token.starts_with("hcp_"));
    assert_eq!(sub.cursor.journal_id, r.journal.journal_id());
    r.stop().await;
}

#[tokio::test]
async fn sync_and_events_reject_unauthorized_access() {
    let r = TestRelay::start(None).await;
    let http = reqwest::Client::new();
    for path in ["/v1/sync", "/v1/events"] {
        let resp = http.get(format!("{}{path}", r.url)).send().await.unwrap();
        assert_eq!(resp.status(), 401, "{path} without token");
        let resp = http
            .get(format!("{}{path}", r.url))
            .bearer_auth("hcp_forgedtokenforgedtokenforgedtokenforgedtok")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 401, "{path} with forged token");
        // A registration key is not an access token.
        let resp = http
            .get(format!("{}{path}", r.url))
            .bearer_auth(KEY)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 401, "{path} with registration key");
    }
    // Deleting someone else's subscription is forbidden.
    let a = subscribe(&http, &r.url, &[SPACE]).await;
    let b = subscribe(&http, &r.url, &[SPACE]).await;
    let resp = http
        .delete(format!("{}/v1/subscriptions/{}", r.url, b.subscription_id))
        .bearer_auth(&a.token)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 403);
    // Deleting your own works and revokes the token.
    let resp = http
        .delete(format!("{}/v1/subscriptions/{}", r.url, a.subscription_id))
        .bearer_auth(&a.token)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204);
    assert_eq!(
        sync_page(&http, &r.url, &a.token, None, 10).await.status(),
        401
    );
    r.stop().await;
}

#[tokio::test]
async fn sync_orders_pages_and_filters_spaces() {
    let r = TestRelay::start(None).await;
    let http = reqwest::Client::new();
    let sub = subscribe(&http, &r.url, &[SPACE]).await;
    let ids: Vec<String> = (0..25).map(|i| format!("op-{i:02}")).collect();
    for (i, id) in ids.iter().enumerate() {
        r.inject(SPACE, &[id]).await;
        if i % 5 == 0 {
            r.inject(OTHER_SPACE, &[&format!("other-{i}")]).await;
        }
    }
    let mut cursor = sub.cursor.to_string();
    let mut seen = Vec::new();
    let mut pages = 0;
    loop {
        let resp = sync_page(&http, &r.url, &sub.token, Some(&cursor), 10).await;
        assert_eq!(resp.status(), 200);
        assert_eq!(resp.headers()["cache-control"], "no-store");
        let page: SyncPage = resp.json().await.unwrap();
        pages += 1;
        for e in &page.events {
            assert_eq!(e.space, SPACE, "other spaces are filtered out");
            assert_eq!(e.v, 1);
            assert_eq!(e.source, "mock");
            if let Some(last) = seen.last() {
                let last: &EventEnvelope = last;
                assert!(e.seq() > last.seq(), "strictly ascending");
            }
            seen.push(e.clone());
        }
        cursor = page.next_cursor.to_string();
        if !page.has_more {
            assert_eq!(page.next_cursor, page.head_cursor);
            break;
        }
    }
    assert_eq!(pages, 3);
    let got: Vec<String> = seen.iter().map(|e| e.refs[0].id.clone()).collect();
    assert_eq!(got, ids, "delivered in injection order");
    // Caught up: an empty page with the same cursor.
    let page: SyncPage = sync_page(&http, &r.url, &sub.token, Some(&cursor), 10)
        .await
        .json()
        .await
        .unwrap();
    assert!(page.events.is_empty() && !page.has_more);
    assert_eq!(page.next_cursor.to_string(), cursor);
    // Bad limits and cursors.
    assert_eq!(
        sync_page(&http, &r.url, &sub.token, None, 0).await.status(),
        400
    );
    assert_eq!(
        sync_page(&http, &r.url, &sub.token, None, 100_000)
            .await
            .status(),
        400
    );
    assert_eq!(
        sync_page(&http, &r.url, &sub.token, Some("garbage"), 10)
            .await
            .status(),
        400
    );
    r.stop().await;
}

#[tokio::test]
async fn duplicate_changes_are_journaled_once() {
    let r = TestRelay::start(None).await;
    let http = reqwest::Client::new();
    let sub = subscribe(&http, &r.url, &[SPACE]).await;
    assert_eq!(r.inject(SPACE, &["a", "b", "a"]).await, 2);
    assert_eq!(r.inject(SPACE, &["b", "c"]).await, 1);
    let page: SyncPage = sync_page(&http, &r.url, &sub.token, Some(&sub.cursor.to_string()), 50)
        .await
        .json()
        .await
        .unwrap();
    let ids: Vec<_> = page.events.iter().map(|e| e.refs[0].id.as_str()).collect();
    assert_eq!(ids, ["a", "b", "c"]);
    // Sequence numbers have no gaps from discarded duplicates.
    let seqs: Vec<_> = page.events.iter().map(|e| e.seq()).collect();
    assert_eq!(seqs, [1, 2, 3]);
    r.stop().await;
}

#[tokio::test]
async fn retention_expiry_returns_410_with_resume_point() {
    let r = TestRelay::start_with(None, |s| {
        s.retention.max_events = 5;
    })
    .await;
    let http = reqwest::Client::new();
    let sub = subscribe(&http, &r.url, &[SPACE]).await;
    for i in 0..12 {
        r.inject(SPACE, &[&format!("e{i}")]).await;
    }
    let pruned = r.journal.prune(hc_push_source_api::now_ms()).unwrap();
    assert_eq!(pruned, 7);
    let resp = sync_page(&http, &r.url, &sub.token, Some(&sub.cursor.to_string()), 50).await;
    assert_eq!(resp.status(), 410);
    let err: ErrorBody = resp.json().await.unwrap();
    assert_eq!(err.error.code, ErrorCode::CursorExpired);
    let earliest = err.error.earliest_cursor.unwrap();
    assert_eq!(earliest.seq, 7);
    let page: SyncPage = sync_page(&http, &r.url, &sub.token, Some(&earliest.to_string()), 50)
        .await
        .json()
        .await
        .unwrap();
    assert_eq!(page.events.len(), 5);
    // A cursor ahead of the head is not known to this journal.
    let ahead = Cursor::new(r.journal.journal_id(), 999).to_string();
    let resp = sync_page(&http, &r.url, &sub.token, Some(&ahead), 50).await;
    assert_eq!(resp.status(), 410);
    r.stop().await;
}

#[tokio::test]
async fn rate_limit_returns_429_with_retry_after() {
    let r = TestRelay::start_with(None, |s| {
        s.rate_per_minute = 60;
        s.rate_burst = 3;
    })
    .await;
    let http = reqwest::Client::new();
    let sub = subscribe(&http, &r.url, &[SPACE]).await;
    for _ in 0..3 {
        assert_eq!(
            sync_page(&http, &r.url, &sub.token, None, 10)
                .await
                .status(),
            200
        );
    }
    let resp = sync_page(&http, &r.url, &sub.token, None, 10).await;
    assert_eq!(resp.status(), 429);
    assert!(resp.headers().contains_key("retry-after"));
    r.stop().await;
}

#[tokio::test]
async fn registration_is_rate_limited_per_address() {
    let r = TestRelay::start_with(None, |s| s.registration_rate_per_minute = 2).await;
    let http = reqwest::Client::new();
    let url = format!("{}/v1/subscriptions", r.url);
    let mut statuses = Vec::new();
    for _ in 0..4 {
        let resp = http
            .post(&url)
            .bearer_auth("wrong-key-wrong-key")
            .json(&serde_json::json!({ "spaces": [SPACE] }))
            .send()
            .await
            .unwrap();
        statuses.push(resp.status().as_u16());
    }
    assert_eq!(statuses, [401, 401, 429, 429], "key guessing is throttled");
    r.stop().await;
}

async fn read_sse_events(
    resp: reqwest::Response,
    n: usize,
    timeout: Duration,
) -> Vec<(String, String)> {
    let mut parser = hc_push_client::sse::SseParser::new();
    let mut out = Vec::new();
    let mut body = resp.bytes_stream();
    let deadline = tokio::time::Instant::now() + timeout;
    while out.len() < n {
        let chunk = tokio::time::timeout_at(deadline, body.next())
            .await
            .expect("timed out waiting for SSE events");
        let Some(Ok(chunk)) = chunk else { break };
        for ev in parser.push(&chunk).unwrap() {
            out.push((ev.event, ev.data));
        }
    }
    out
}

#[tokio::test]
async fn sse_delivers_live_notification_and_replays_from_cursor() {
    let r = TestRelay::start(None).await;
    let http = reqwest::Client::new();
    let sub = subscribe(&http, &r.url, &[SPACE]).await;
    r.inject(SPACE, &["before-1", "before-2"]).await;

    // Connect with the subscription cursor: replays the two missed events,
    // then streams live ones.
    let resp = http
        .get(format!("{}/v1/events", r.url))
        .bearer_auth(&sub.token)
        .header("last-event-id", sub.cursor.to_string())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert!(resp.headers()["content-type"]
        .to_str()
        .unwrap()
        .starts_with("text/event-stream"));
    let mock = r.mock.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(200)).await;
        mock.inject(OTHER_SPACE, &["not-for-us".to_string()])
            .await
            .unwrap();
        mock.inject(SPACE, &["live-1".to_string()]).await.unwrap();
    });
    let evs = read_sse_events(resp, 3, Duration::from_secs(5)).await;
    let ids: Vec<String> = evs
        .iter()
        .map(|(name, data)| {
            assert_eq!(name, "change");
            serde_json::from_str::<EventEnvelope>(data).unwrap().refs[0]
                .id
                .clone()
        })
        .collect();
    assert_eq!(ids, ["before-1", "before-2", "live-1"]);
    r.stop().await;
}

#[tokio::test]
async fn sse_large_backlog_requests_sync_and_streams_are_bounded() {
    let r = TestRelay::start_with(None, |s| {
        s.sse_replay_limit = 3;
        s.max_sse_per_subscription = 1;
    })
    .await;
    let http = reqwest::Client::new();
    let sub = subscribe(&http, &r.url, &[SPACE]).await;
    for i in 0..5 {
        r.inject(SPACE, &[&format!("b{i}")]).await;
    }
    let resp = http
        .get(format!("{}/v1/events?cursor={}", r.url, sub.cursor))
        .bearer_auth(&sub.token)
        .send()
        .await
        .unwrap();
    let evs = read_sse_events(resp, 1, Duration::from_secs(5)).await;
    assert_eq!(evs[0].0, "sync_required");
    let sr: SyncRequired = serde_json::from_str(&evs[0].1).unwrap();
    assert_eq!(sr.reason, SyncRequiredReason::BacklogTooLarge);
    assert_eq!(sr.cursor, sub.cursor);

    // One live stream per subscription.
    let first = http
        .get(format!("{}/v1/events", r.url))
        .bearer_auth(&sub.token)
        .send()
        .await
        .unwrap();
    assert_eq!(first.status(), 200);
    let second = http
        .get(format!("{}/v1/events", r.url))
        .bearer_auth(&sub.token)
        .send()
        .await
        .unwrap();
    assert_eq!(second.status(), 503);
    assert_eq!(
        second.json::<ErrorBody>().await.unwrap().error.code,
        ErrorCode::Capacity
    );
    // Closing the first stream frees the slot.
    drop(first);
    let mut ok = false;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        let again = http
            .get(format!("{}/v1/events", r.url))
            .bearer_auth(&sub.token)
            .send()
            .await
            .unwrap();
        if again.status() == 200 {
            ok = true;
            break;
        }
    }
    assert!(ok, "slot released after disconnect");
    r.stop().await;
}

#[tokio::test]
async fn shutdown_tells_streams_to_sync() {
    let r = TestRelay::start(None).await;
    let http = reqwest::Client::new();
    let sub = subscribe(&http, &r.url, &[SPACE]).await;
    let resp = http
        .get(format!("{}/v1/events", r.url))
        .bearer_auth(&sub.token)
        .send()
        .await
        .unwrap();
    let token = r.shutdown.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        token.cancel();
    });
    let evs = read_sse_events(resp, 1, Duration::from_secs(5)).await;
    assert_eq!(evs[0].0, "sync_required");
    let sr: SyncRequired = serde_json::from_str(&evs[0].1).unwrap();
    assert_eq!(sr.reason, SyncRequiredReason::Shutdown);
    tokio::time::timeout(Duration::from_secs(15), r.handle)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn admin_listener_injects_mock_changes() {
    let r = TestRelay::start(None).await;
    let http = reqwest::Client::new();
    let resp = http
        .post(format!("http://{}/admin/v1/mock/changes", r.admin))
        .json(&serde_json::json!({ "space": SPACE, "ids": ["x", "y"] }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let v: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(v["new"], 2);
    let resp = http
        .post(format!("http://{}/admin/v1/mock/changes", r.admin))
        .json(&serde_json::json!({ "space": "uhC0kNotServed", "ids": ["x"] }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    let stats: serde_json::Value = http
        .get(format!("http://{}/admin/v1/stats", r.admin))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(stats["head"], 2);
    // Admin routes are not exposed on the public listener.
    let resp = http
        .post(format!("{}/admin/v1/mock/changes", r.url))
        .json(&serde_json::json!({ "space": SPACE, "ids": ["z"] }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
    r.stop().await;
}

#[tokio::test]
async fn state_survives_relay_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("journal.sqlite3");
    let http = reqwest::Client::new();

    let r = TestRelay::start(Some(&path)).await;
    let journal_id = r.journal.journal_id().to_string();
    let sub = subscribe(&http, &r.url, &[SPACE]).await;
    r.inject(SPACE, &["p1", "p2"]).await;
    r.stop().await;

    let r = TestRelay::start(Some(&path)).await;
    assert_eq!(
        r.journal.journal_id(),
        journal_id,
        "journal identity persists"
    );
    r.inject(SPACE, &["p2", "p3"]).await; // p2 is a duplicate across the restart
                                          // The token issued before the restart still works and the old cursor
                                          // still resolves.
    let page: SyncPage = sync_page(&http, &r.url, &sub.token, Some(&sub.cursor.to_string()), 50)
        .await
        .json()
        .await
        .unwrap();
    let ids: Vec<_> = page.events.iter().map(|e| e.refs[0].id.as_str()).collect();
    assert_eq!(ids, ["p1", "p2", "p3"]);
    assert_eq!(page.head_cursor.seq, 3);
    r.stop().await;
}

#[tokio::test]
async fn cursors_are_not_interchangeable_between_relays() {
    let a = TestRelay::start(None).await;
    let b = TestRelay::start(None).await;
    assert_ne!(a.journal.journal_id(), b.journal.journal_id());
    let http = reqwest::Client::new();
    let sub_a = subscribe(&http, &a.url, &[SPACE]).await;
    let sub_b = subscribe(&http, &b.url, &[SPACE]).await;
    // A token from relay A is meaningless to relay B.
    assert_eq!(
        sync_page(&http, &b.url, &sub_a.token, None, 10)
            .await
            .status(),
        401
    );
    // A cursor from relay A is rejected by relay B with a resume point.
    let resp = sync_page(
        &http,
        &b.url,
        &sub_b.token,
        Some(&sub_a.cursor.to_string()),
        10,
    )
    .await;
    assert_eq!(resp.status(), 410);
    let err: ErrorBody = resp.json().await.unwrap();
    assert_eq!(err.error.code, ErrorCode::CursorUnknownJournal);
    assert_eq!(
        err.error.earliest_cursor.unwrap().journal_id,
        b.journal.journal_id()
    );
    a.stop().await;
    b.stop().await;
}
