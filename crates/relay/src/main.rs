use anyhow::Context;
use clap::Parser;
use hc_push_relay::config::{Cli, LogFormat, SourceKind};
use hc_push_relay::mock::{mock_source, MockInjector};
use hc_push_relay::{serve, Journal, ServeOptions};
use hc_push_source_api::{CancellationToken, ChangeSource};
use std::sync::Arc;
use std::time::Duration;
use tracing_subscriber::EnvFilter;

fn init_tracing(format: LogFormat) {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,tower_http=info"));
    let builder = tracing_subscriber::fmt().with_env_filter(filter);
    match format {
        LogFormat::Json => builder.json().flatten_event(true).init(),
        LogFormat::Pretty => builder.init(),
    }
}

async fn shutdown_signal(token: CancellationToken) {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let term = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut s) => {
                s.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };
    #[cfg(not(unix))]
    let term = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => {},
        _ = term => {},
    }
    tracing::info!("shutdown signal received");
    token.cancel();
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    init_tracing(cli.log_format);
    cli.validate()?;

    let shutdown = CancellationToken::new();
    tokio::spawn(shutdown_signal(shutdown.clone()));

    let (sources, spaces, mock): (
        Vec<Box<dyn ChangeSource>>,
        Vec<String>,
        Option<MockInjector>,
    ) = match cli.source {
        SourceKind::Mock => {
            let interval =
                (cli.mock_interval_ms > 0).then(|| Duration::from_millis(cli.mock_interval_ms));
            let (src, inj) = mock_source(cli.spaces.clone(), interval);
            tracing::warn!("running with the MOCK change source: events are synthetic");
            (vec![Box::new(src)], cli.spaces.clone(), Some(inj))
        }
        SourceKind::Holochain => build_holochain(&cli, &shutdown).await?,
    };
    if spaces.is_empty() {
        anyhow::bail!("no spaces to serve");
    }
    let settings = cli.settings(spaces)?;

    std::fs::create_dir_all(&cli.data_dir)
        .with_context(|| format!("creating {}", cli.data_dir.display()))?;
    let path = cli.data_dir.join("journal.sqlite3");
    let journal = Arc::new(
        Journal::open(&path, settings.retention)
            .with_context(|| format!("opening journal {}", path.display()))?,
    );

    let public = tokio::net::TcpListener::bind(cli.bind)
        .await
        .with_context(|| format!("binding {}", cli.bind))?;
    let admin = match cli.admin_bind {
        Some(a) => Some(
            tokio::net::TcpListener::bind(a)
                .await
                .with_context(|| format!("binding admin {a}"))?,
        ),
        None => None,
    };

    serve(ServeOptions {
        settings,
        journal,
        relay_id: cli.relay_id.clone(),
        sources,
        public,
        admin,
        mock,
        shutdown,
    })
    .await
}

#[cfg(feature = "holochain")]
async fn build_holochain(
    cli: &Cli,
    shutdown: &CancellationToken,
) -> anyhow::Result<(
    Vec<Box<dyn ChangeSource>>,
    Vec<String>,
    Option<MockInjector>,
)> {
    use hc_push_holochain::{discover, HolochainConfig};
    let cfg = HolochainConfig {
        admin_addr: cli.hc_admin_addr.clone(),
        app_id: cli.hc_app_id.clone().expect("validated"),
        app_port: cli.hc_app_port,
        poll_interval: Duration::from_millis(cli.hc_poll_interval_ms.max(100)),
        page_limit: cli.hc_page_limit.clamp(1, 5000),
        op_timings: cli.hc_op_timings,
        signals: cli.hc_signals,
    };
    let built = discover(cfg, shutdown.clone()).await?;
    let mut spaces = built.spaces;
    if !cli.spaces.is_empty() {
        spaces.retain(|s| cli.spaces.contains(s));
    }
    Ok((built.sources, spaces, None))
}

#[cfg(not(feature = "holochain"))]
async fn build_holochain(
    _cli: &Cli,
    _shutdown: &CancellationToken,
) -> anyhow::Result<(
    Vec<Box<dyn ChangeSource>>,
    Vec<String>,
    Option<MockInjector>,
)> {
    anyhow::bail!("this binary was built without the `holochain` feature")
}
