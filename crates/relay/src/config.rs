//! Command-line / environment configuration of the relay binary.
//!
//! Every option can be set through an `HC_PUSH_*` environment variable, which
//! is how the OCI image is configured.

use crate::auth::RegistrationKeys;
use crate::journal::Retention;
use crate::settings::RelaySettings;
use anyhow::{bail, Context};
use clap::{Parser, ValueEnum};
use hc_push_protocol::RegistrationMode;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum SourceKind {
    /// Synthetic events only (development and tests).
    Mock,
    /// Real Holochain conductor via its app API.
    Holochain,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum RegistrationArg {
    Key,
    Open,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum LogFormat {
    Json,
    Pretty,
}

#[derive(Debug, Clone, Parser)]
#[command(
    name = "hc-push-relay",
    version,
    about = "Holochain HTTP push and catch-up relay"
)]
pub struct Cli {
    /// Public listener. Put a TLS-terminating reverse proxy in front of it.
    #[arg(long, env = "HC_PUSH_BIND", default_value = "127.0.0.1:8787")]
    pub bind: SocketAddr,

    /// Operator listener (mock injection, stats). Must be loopback.
    #[arg(long, env = "HC_PUSH_ADMIN_BIND")]
    pub admin_bind: Option<SocketAddr>,

    /// Allow a non-loopback admin listener. Never do this on a public host.
    #[arg(
        long,
        env = "HC_PUSH_DANGER_ADMIN_NON_LOOPBACK",
        default_value_t = false
    )]
    pub danger_admin_non_loopback: bool,

    /// Directory for the persistent journal.
    #[arg(long, env = "HC_PUSH_DATA_DIR", default_value = "./data")]
    pub data_dir: PathBuf,

    #[arg(long, env = "HC_PUSH_SOURCE", value_enum, default_value = "mock")]
    pub source: SourceKind,

    /// Spaces to serve (comma separated). Required for the mock source; for
    /// the Holochain source they are discovered from the app's cells and this
    /// list, if given, restricts them.
    #[arg(long, env = "HC_PUSH_SPACES", value_delimiter = ',')]
    pub spaces: Vec<String>,

    /// Stable public identifier for this relay (defaults to a random id
    /// stored in the journal).
    #[arg(long, env = "HC_PUSH_RELAY_ID")]
    pub relay_id: Option<String>,

    #[arg(long, env = "HC_PUSH_REGISTRATION", value_enum, default_value = "key")]
    pub registration: RegistrationArg,

    /// Registration keys (comma separated). Prefer the _FILE variant.
    #[arg(
        long,
        env = "HC_PUSH_REGISTRATION_KEYS",
        value_delimiter = ',',
        hide_env_values = true
    )]
    pub registration_keys: Vec<String>,

    /// File with one registration key per line.
    #[arg(long, env = "HC_PUSH_REGISTRATION_KEYS_FILE")]
    pub registration_keys_file: Option<PathBuf>,

    #[arg(long, env = "HC_PUSH_RETENTION_MAX_EVENTS", default_value_t = 100_000)]
    pub retention_max_events: u64,

    #[arg(long, env = "HC_PUSH_RETENTION_MAX_AGE_SECS", default_value_t = 7 * 24 * 3600)]
    pub retention_max_age_secs: u64,

    #[arg(long, env = "HC_PUSH_MAX_SUBSCRIPTIONS", default_value_t = 10_000)]
    pub max_subscriptions: u64,

    #[arg(long, env = "HC_PUSH_SUBSCRIPTION_IDLE_TTL_SECS", default_value_t = 30 * 24 * 3600)]
    pub subscription_idle_ttl_secs: u64,

    #[arg(long, env = "HC_PUSH_RATE_LIMIT_PER_MINUTE", default_value_t = 120)]
    pub rate_limit_per_minute: u32,

    #[arg(
        long,
        env = "HC_PUSH_REGISTRATION_RATE_PER_MINUTE",
        default_value_t = 10
    )]
    pub registration_rate_per_minute: u32,

    #[arg(long, env = "HC_PUSH_MAX_SSE_CONNECTIONS", default_value_t = 1024)]
    pub max_sse_connections: usize,

    #[arg(long, env = "HC_PUSH_TRUST_FORWARDED_FOR", default_value_t = false)]
    pub trust_forwarded_for: bool,

    #[arg(long, env = "HC_PUSH_ADVERTISE_SPACES", default_value_t = true, action = clap::ArgAction::Set)]
    pub advertise_spaces: bool,

    /// Mock source: emit one synthetic change per space every N ms (0 = off).
    #[arg(long, env = "HC_PUSH_MOCK_INTERVAL_MS", default_value_t = 0)]
    pub mock_interval_ms: u64,

    #[arg(long, env = "HC_PUSH_LOG_FORMAT", value_enum, default_value = "json")]
    pub log_format: LogFormat,

    /// Conductor admin websocket (host:port). Must not be publicly reachable.
    #[arg(long, env = "HC_PUSH_HC_ADMIN_ADDR", default_value = "127.0.0.1:4444")]
    pub hc_admin_addr: String,

    /// Installed app id to observe.
    #[arg(long, env = "HC_PUSH_HC_APP_ID")]
    pub hc_app_id: Option<String>,

    /// Existing app interface port; if unset one is attached on loopback.
    #[arg(long, env = "HC_PUSH_HC_APP_PORT")]
    pub hc_app_port: Option<u16>,

    #[arg(long, env = "HC_PUSH_HC_POLL_INTERVAL_MS", default_value_t = 2000)]
    pub hc_poll_interval_ms: u64,

    #[arg(long, env = "HC_PUSH_HC_PAGE_LIMIT", default_value_t = 500)]
    pub hc_page_limit: u32,

    /// Observe DHT ops through `DumpOpTimings`.
    #[arg(long, env = "HC_PUSH_HC_OP_TIMINGS", default_value_t = true, action = clap::ArgAction::Set)]
    pub hc_op_timings: bool,

    /// Forward digests of app signals.
    #[arg(long, env = "HC_PUSH_HC_SIGNALS", default_value_t = true, action = clap::ArgAction::Set)]
    pub hc_signals: bool,

    /// Subscribe to the event-driven `OpsIntegrated` system signal when the
    /// conductor supports it (requires the upstream patch in `upstream/` and a
    /// relay built with `--features ops-integrated-signal`). Polling always
    /// continues as the durable reconciliation path.
    #[arg(long, env = "HC_PUSH_HC_OPS_INTEGRATED", default_value_t = true, action = clap::ArgAction::Set)]
    pub hc_ops_integrated: bool,

    /// Serve the conductor's own signed agent infos on `GET /v1/peers`.
    #[arg(long, env = "HC_PUSH_HC_PEER_HINTS", default_value_t = true, action = clap::ArgAction::Set)]
    pub hc_peer_hints: bool,
}

impl Cli {
    pub fn validate(&self) -> anyhow::Result<()> {
        if let Some(a) = self.admin_bind {
            if !a.ip().is_loopback() && !self.danger_admin_non_loopback {
                bail!("HC_PUSH_ADMIN_BIND must be a loopback address (got {a})");
            }
        }
        for s in &self.spaces {
            if !hc_push_protocol::is_valid_space_id(s) {
                bail!("invalid space id in HC_PUSH_SPACES: {s:?}");
            }
        }
        if self.source == SourceKind::Mock && self.spaces.is_empty() {
            bail!("the mock source requires HC_PUSH_SPACES");
        }
        if self.source == SourceKind::Holochain && self.hc_app_id.is_none() {
            bail!("the holochain source requires HC_PUSH_HC_APP_ID");
        }
        if self.retention_max_events == 0 || self.retention_max_age_secs == 0 {
            bail!("retention limits must be positive");
        }
        Ok(())
    }

    pub fn registration_keys(&self) -> anyhow::Result<RegistrationKeys> {
        let mut keys = self.registration_keys.clone();
        if let Some(path) = &self.registration_keys_file {
            let text = std::fs::read_to_string(path)
                .with_context(|| format!("reading {}", path.display()))?;
            keys.extend(
                text.lines()
                    .map(str::trim)
                    .filter(|l| !l.is_empty() && !l.starts_with('#'))
                    .map(str::to_string),
            );
        }
        if keys.iter().any(|k| k.trim().len() < 16) {
            bail!("registration keys must be at least 16 characters");
        }
        Ok(RegistrationKeys::new(keys))
    }

    /// Build runtime settings. `spaces` is the final served space list.
    pub fn settings(&self, spaces: Vec<String>) -> anyhow::Result<RelaySettings> {
        let registration_keys = self.registration_keys()?;
        let registration_mode = match self.registration {
            RegistrationArg::Key => {
                if registration_keys.is_empty() {
                    bail!("HC_PUSH_REGISTRATION=key requires HC_PUSH_REGISTRATION_KEYS or HC_PUSH_REGISTRATION_KEYS_FILE");
                }
                RegistrationMode::Key
            }
            RegistrationArg::Open => RegistrationMode::Open,
        };
        Ok(RelaySettings {
            retention: Retention {
                max_events: self.retention_max_events,
                max_age: Duration::from_secs(self.retention_max_age_secs),
            },
            spaces,
            advertise_spaces: self.advertise_spaces,
            registration_mode,
            registration_keys,
            max_subscriptions: self.max_subscriptions,
            subscription_idle_ttl: Duration::from_secs(self.subscription_idle_ttl_secs),
            rate_per_minute: self.rate_limit_per_minute,
            registration_rate_per_minute: self.registration_rate_per_minute,
            max_sse_connections: self.max_sse_connections,
            trust_forwarded_for: self.trust_forwarded_for,
            ..RelaySettings::default()
        })
    }
}
