#![allow(dead_code)]

use hc_push_relay::auth::RegistrationKeys;
use hc_push_relay::mock::{mock_source, MockInjector};
use hc_push_relay::{serve, Journal, RelaySettings, ServeOptions};
use hc_push_source_api::{CancellationToken, ChangeSource, DynPeerHints};
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

pub const KEY: &str = "test-registration-key-0001";
pub const SPACE: &str = "uhC0kTestSpaceAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
pub const OTHER_SPACE: &str = "uhC0kOtherSpaceBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB";

pub struct TestRelay {
    pub addr: SocketAddr,
    pub admin: SocketAddr,
    pub url: String,
    pub journal: Arc<Journal>,
    pub mock: MockInjector,
    pub shutdown: CancellationToken,
    pub handle: tokio::task::JoinHandle<anyhow::Result<()>>,
}

impl TestRelay {
    pub async fn start(path: Option<&Path>) -> Self {
        Self::start_with(path, |_| {}).await
    }

    pub async fn start_with(path: Option<&Path>, f: impl FnOnce(&mut RelaySettings)) -> Self {
        Self::start_custom(path, f, Vec::new(), None).await
    }

    /// Start with the mock source plus `extra` sources and optional peer hints.
    pub async fn start_custom(
        path: Option<&Path>,
        f: impl FnOnce(&mut RelaySettings),
        extra: Vec<Box<dyn ChangeSource>>,
        peers: Option<DynPeerHints>,
    ) -> Self {
        Self::start_custom_at("127.0.0.1:0", path, f, extra, peers).await
    }

    /// Like [`Self::start_custom`] but on a given public address, so a test
    /// can restart a relay where its clients expect it.
    pub async fn start_custom_at(
        bind: &str,
        path: Option<&Path>,
        f: impl FnOnce(&mut RelaySettings),
        extra: Vec<Box<dyn ChangeSource>>,
        peers: Option<DynPeerHints>,
    ) -> Self {
        let mut settings = RelaySettings {
            spaces: vec![SPACE.to_string(), OTHER_SPACE.to_string()],
            registration_keys: RegistrationKeys::new([KEY]),
            // Generous defaults so tests are not rate limited by accident.
            rate_per_minute: 6000,
            rate_burst: 1000,
            registration_rate_per_minute: 6000,
            ..RelaySettings::default()
        };
        f(&mut settings);
        let journal = Arc::new(match path {
            Some(p) => Journal::open(p, settings.retention).unwrap(),
            None => Journal::open_in_memory(settings.retention).unwrap(),
        });
        let (src, mock) = mock_source(settings.spaces.clone(), None);
        let public = tokio::net::TcpListener::bind(bind).await.unwrap();
        let admin_l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = public.local_addr().unwrap();
        let admin = admin_l.local_addr().unwrap();
        let shutdown = CancellationToken::new();
        let handle = tokio::spawn(serve(ServeOptions {
            settings,
            journal: journal.clone(),
            relay_id: None,
            sources: std::iter::once(Box::new(src) as Box<dyn ChangeSource>)
                .chain(extra)
                .collect(),
            public,
            admin: Some(admin_l),
            mock: Some(mock.clone()),
            peers,
            shutdown: shutdown.clone(),
        }));
        let url = format!("http://{addr}");
        // Wait until the listener answers.
        let http = reqwest::Client::new();
        for _ in 0..100 {
            if http.get(format!("{url}/v1/health")).send().await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Self {
            addr,
            admin,
            url,
            journal,
            mock,
            shutdown,
            handle,
        }
    }

    pub async fn inject(&self, space: &str, ids: &[&str]) -> usize {
        let ids: Vec<String> = ids.iter().map(|s| s.to_string()).collect();
        self.mock.inject(space, &ids).await.unwrap()
    }

    pub async fn stop(self) {
        self.shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(15), self.handle)
            .await
            .expect("relay did not stop in time")
            .unwrap()
            .unwrap();
    }
}

pub async fn subscribe(
    http: &reqwest::Client,
    url: &str,
    spaces: &[&str],
) -> hc_push_protocol::SubscribeResponse {
    let resp = http
        .post(format!("{url}/v1/subscriptions"))
        .bearer_auth(KEY)
        .json(&serde_json::json!({ "spaces": spaces }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201, "{:?}", resp.text().await);
    resp.json().await.unwrap()
}

pub async fn sync_page(
    http: &reqwest::Client,
    url: &str,
    token: &str,
    cursor: Option<&str>,
    limit: u32,
) -> reqwest::Response {
    let mut req = http
        .get(format!("{url}/v1/sync"))
        .bearer_auth(token)
        .query(&[("limit", limit.to_string())]);
    if let Some(c) = cursor {
        req = req.query(&[("cursor", c)]);
    }
    req.send().await.unwrap()
}
