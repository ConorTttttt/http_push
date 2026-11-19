//! Runtime settings of a relay instance (independent of how they are parsed).

use crate::auth::RegistrationKeys;
use crate::journal::Retention;
use hc_push_protocol::RegistrationMode;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct RelaySettings {
    pub retention: Retention,
    /// Spaces clients may subscribe to (union of configured and
    /// source-declared spaces).
    pub spaces: Vec<String>,
    /// Publish `spaces` in `/v1/capabilities`.
    pub advertise_spaces: bool,
    pub registration_mode: RegistrationMode,
    pub registration_keys: RegistrationKeys,
    pub max_subscriptions: u64,
    pub subscription_idle_ttl: Duration,
    pub max_spaces_per_subscription: usize,
    pub default_page_size: u32,
    pub max_page_size: u32,
    pub sse_replay_limit: u32,
    pub max_sse_connections: usize,
    pub max_sse_per_subscription: usize,
    pub sse_keepalive: Duration,
    pub max_body_bytes: usize,
    pub request_timeout: Duration,
    pub max_concurrent_requests: usize,
    pub rate_per_minute: u32,
    pub rate_burst: u32,
    pub registration_rate_per_minute: u32,
    /// Use the last `X-Forwarded-For` hop as the client address. Only enable
    /// behind a reverse proxy that overwrites the header.
    pub trust_forwarded_for: bool,
    pub maintenance_interval: Duration,
}

impl Default for RelaySettings {
    fn default() -> Self {
        Self {
            retention: Retention::default(),
            spaces: Vec::new(),
            advertise_spaces: true,
            registration_mode: RegistrationMode::Key,
            registration_keys: RegistrationKeys::default(),
            max_subscriptions: 10_000,
            subscription_idle_ttl: Duration::from_secs(30 * 24 * 3600),
            max_spaces_per_subscription: 16,
            default_page_size: 100,
            max_page_size: 500,
            sse_replay_limit: 200,
            max_sse_connections: 1024,
            max_sse_per_subscription: 4,
            sse_keepalive: Duration::from_secs(15),
            max_body_bytes: 16 * 1024,
            request_timeout: Duration::from_secs(10),
            max_concurrent_requests: 256,
            rate_per_minute: 120,
            rate_burst: 30,
            registration_rate_per_minute: 10,
            trust_forwarded_for: false,
            maintenance_interval: Duration::from_secs(60),
        }
    }
}
