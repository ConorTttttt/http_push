//! `hc-push-client`: command-line demonstration of the lightweight client.
//!
//! ```text
//! hc-push-client --state client.json --space SPACE \
//!     --relay a=http://127.0.0.1:8787 --relay b=http://127.0.0.1:8788 \
//!     --registration-key KEY  register|sync|listen|status|unsubscribe
//! ```
//!
//! Delivered events are printed to stdout as JSON lines; diagnostics go to
//! stderr.

use anyhow::Context;
use clap::{Parser, Subcommand, ValueEnum};
use hc_push_client::{
    ClientOptions, FileStateStore, ListenOptions, PushClient, RelayEndpoint, SyncPolicy,
};
use hc_push_protocol::EventEnvelope;
use std::path::PathBuf;
use std::time::Duration;

#[derive(Parser, Debug)]
#[command(
    name = "hc-push-client",
    version,
    about = "Lightweight Holochain HTTP push client"
)]
struct Cli {
    /// Client state file (subscriptions, cursors, dedup set).
    #[arg(
        long,
        env = "HC_PUSH_CLIENT_STATE",
        default_value = "hc-push-client.json"
    )]
    state: PathBuf,

    /// Relay as NAME=URL, in priority order. Repeat for several relays.
    #[arg(
        long = "relay",
        env = "HC_PUSH_CLIENT_RELAYS",
        value_delimiter = ',',
        required = true
    )]
    relays: Vec<String>,

    /// Space (DNA hash) to follow. Repeatable.
    #[arg(
        long = "space",
        env = "HC_PUSH_CLIENT_SPACES",
        value_delimiter = ',',
        required = true
    )]
    spaces: Vec<String>,

    /// Registration key used for every relay (unless overridden).
    #[arg(long, env = "HC_PUSH_CLIENT_REGISTRATION_KEY", hide_env_values = true)]
    registration_key: Option<String>,

    /// Per-relay registration key as NAME=KEY. Repeatable.
    #[arg(long = "relay-key", hide_env_values = true)]
    relay_keys: Vec<String>,

    /// Label shown to relay operators.
    #[arg(long)]
    label: Option<String>,

    #[arg(long, default_value_t = 100)]
    page_size: u32,

    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Policy {
    First,
    All,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Register with every configured relay that has no subscription yet.
    Register,
    /// Catch up on missed changes, then exit.
    Sync {
        #[arg(long, value_enum, default_value = "first")]
        policy: Policy,
    },
    /// Hold an SSE stream open while "in the foreground", then exit.
    Listen {
        /// Relay name (default: first configured).
        #[arg(long)]
        relay: Option<String>,
        /// Exit after this many seconds without a change.
        #[arg(long, default_value_t = 10)]
        idle_secs: u64,
        /// Exit after this many seconds in any case.
        #[arg(long, default_value_t = 60)]
        max_secs: u64,
    },
    /// Show stored subscriptions and cursors (tokens redacted).
    Status,
    /// Delete the subscription on a relay.
    Unsubscribe {
        #[arg(long)]
        relay: String,
    },
}

fn print_event(e: &EventEnvelope) -> anyhow::Result<()> {
    println!("{}", serde_json::to_string(e)?);
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("hc_push_client=info")),
        )
        .init();
    let cli = Cli::parse();

    let mut endpoints = Vec::new();
    for r in &cli.relays {
        let (name, url) = r
            .split_once('=')
            .with_context(|| format!("--relay must be NAME=URL, got {r:?}"))?;
        let key = cli
            .relay_keys
            .iter()
            .filter_map(|k| k.split_once('='))
            .find(|(n, _)| *n == name)
            .map(|(_, k)| k.to_string())
            .or_else(|| cli.registration_key.clone());
        endpoints.push(RelayEndpoint {
            name: name.to_string(),
            url: url.to_string(),
            registration_key: key,
        });
    }
    let first = endpoints[0].name.clone();
    let opts = ClientOptions {
        label: cli.label.clone(),
        page_size: cli.page_size,
        ..ClientOptions::default()
    };
    let mut client = PushClient::new(
        endpoints,
        cli.spaces.clone(),
        Box::new(FileStateStore::new(&cli.state)),
        opts,
    )?;

    match cli.cmd {
        Cmd::Register => {
            let results = client.ensure_registered().await;
            if results.is_empty() {
                eprintln!("already registered with every relay");
            }
            let mut failed = 0;
            for (name, r) in results {
                match r {
                    Ok(()) => eprintln!("registered with {name}"),
                    Err(e) => {
                        failed += 1;
                        eprintln!("registration with {name} failed: {e}");
                    }
                }
            }
            if failed > 0 && client.state().relays.is_empty() {
                anyhow::bail!("no relay accepted the registration");
            }
        }
        Cmd::Sync { policy } => {
            let policy = match policy {
                Policy::First => SyncPolicy::FirstAvailable,
                Policy::All => SyncPolicy::All,
            };
            let mut handler = print_event;
            let report = client.sync(policy, &mut handler).await;
            for o in &report.outcomes {
                match &o.result {
                    Ok(s) => {
                        eprintln!(
                        "relay {}: ok pages={} received={} delivered={} duplicates={} cursor={}{}",
                        o.relay,
                        s.pages,
                        s.received,
                        s.delivered,
                        s.duplicates,
                        s.cursor.as_ref().map(|c| c.to_string()).unwrap_or_default(),
                        if s.more_pending { " (more pending)" } else { "" }
                    )
                    }
                    Err(e) => eprintln!("relay {}: FAILED: {e}", o.relay),
                }
            }
            for g in &report.gaps {
                eprintln!(
                    "GAP on relay {} ({:?}): reconcile spaces {:?} through Holochain; resumed from {:?}",
                    g.relay,
                    g.code,
                    g.spaces,
                    g.resumed_from.as_ref().map(|c| c.to_string())
                );
            }
            eprintln!(
                "sync done: delivered={} duplicates={} served_by={}",
                report.delivered,
                report.duplicates,
                report.served_by.as_deref().unwrap_or("NONE")
            );
            if !report.any_success() {
                anyhow::bail!("no relay could be synced");
            }
        }
        Cmd::Listen {
            relay,
            idle_secs,
            max_secs,
        } => {
            let relay = relay.unwrap_or(first);
            let mut handler = print_event;
            let out = client
                .listen(
                    &relay,
                    ListenOptions {
                        idle_timeout: Duration::from_secs(idle_secs),
                        max_duration: Duration::from_secs(max_secs),
                    },
                    &mut handler,
                )
                .await?;
            eprintln!(
                "listen ended: {:?} delivered={} duplicates={}",
                out.end, out.delivered, out.duplicates
            );
        }
        Cmd::Status => {
            for (name, rs) in &client.state().relays {
                eprintln!(
                    "{name}: url={} subscription={} journal={} cursor={} failures={}",
                    rs.url,
                    rs.subscription_id,
                    rs.journal_id,
                    rs.cursor
                        .as_ref()
                        .map(|c| c.to_string())
                        .unwrap_or_default(),
                    rs.consecutive_failures
                );
            }
            eprintln!("dedup set: {} change keys", client.state().seen.len());
        }
        Cmd::Unsubscribe { relay } => {
            client.unsubscribe(&relay).await?;
            eprintln!("unsubscribed from {relay}");
        }
    }
    Ok(())
}
