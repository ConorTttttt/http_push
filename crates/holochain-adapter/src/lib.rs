//! Holochain conductor change sources for the HTTP push relay.
//!
//! Uses only **supported, public conductor APIs** (verified against
//! Holochain 0.7.1 / `holochain_client` 0.9.1), plus one optional upstream
//! extension:
//!
//! | Need                          | API used                                           |
//! |-------------------------------|----------------------------------------------------|
//! | Connect without exposing admin| `AdminWebsocket` on loopback, only to issue a single-use `IssueAppAuthenticationToken` and (if needed) `AttachAppInterface` bound to loopback; then dropped |
//! | Which spaces (DNAs) exist     | `AppInfo.cell_info` on the app websocket           |
//! | Durable op-level reconciliation | `AppRequest::DumpOpTimings { dna_hash, cursor, limit }` (since 0.7.0-rc.5), polled with a checkpointed cursor ([`optimings`]) |
//! | Event-driven op notification  | `SystemSignal::OpsIntegrated` + `AppRequest::SubscribeOpsIntegrated`: **not in released Holochain**; requires the patch in `upstream/` and the `ops-integrated-signal` feature ([`signals`]) |
//! | App-level events              | `AppWebsocket::on_signal` (`Signal::App`, `Signal::AppDirect`) |
//! | Peer hints for direct gossip  | `AppRequest::AgentInfo`, filtered to this conductor's own agent ([`peers`]) |
//!
//! # What this is not
//!
//! The relay never takes part in Kitsune2 gossip and never forwards op data:
//! only op hashes and the conductor's own signed peer records. Clients
//! synchronize data through their own conductor using native Kitsune2 gossip
//! and validation. See `docs/holochain-integration.md`.

pub mod optimings;
pub mod peers;
pub mod signals;

pub use optimings::{
    classify, OpTimingsApi, OpTimingsCheckpoint, OpTimingsConnector, OpTimingsSource, ScanCursor,
    ScanStep, WsConnector,
};
pub use peers::ConductorPeers;
#[cfg(feature = "ops-integrated-signal")]
pub use signals::OpsIntegratedSource;
pub use signals::{integrated_ops_to_changes, signal_digest, signal_to_change, AppSignalSource};

/// Holochain types used by this crate's public API, for callers and tests.
pub mod types {
    pub use holochain_conductor_api::{OpTimingDump, OpTimingsCursor, OpTimingsDump};
    pub use holochain_types::prelude::{DhtOpHash, DnaHash, RecordValidity, Timestamp};
}

use hc_push_source_api::{Backoff, CancellationToken, ChangeSource, DynPeerHints};
use holochain_client::{
    AdminWebsocket, AllowedOrigins, AppWebsocket, CellInfo, ClientAgentSigner,
    IssueAppAuthenticationTokenPayload,
};
use holochain_types::prelude::DnaHash;
use std::sync::Arc;
use std::time::Duration;

/// Websocket `Origin` the relay presents to the conductor.
pub const ORIGIN: &str = "hc-push-relay";
pub const OP_TIMINGS_SOURCE_ID: &str = "holochain-op-timings";
pub const APP_SIGNAL_SOURCE_ID: &str = "holochain-app-signals";
pub const OPS_INTEGRATED_SOURCE_ID: &str = "holochain-ops-integrated";

#[derive(Debug, Clone)]
pub struct HolochainConfig {
    /// Admin websocket `host:port` (loopback / shared network namespace).
    pub admin_addr: String,
    pub app_id: String,
    /// Use this existing app interface instead of finding/attaching one.
    pub app_port: Option<u16>,
    pub poll_interval: Duration,
    pub page_limit: u32,
    pub op_timings: bool,
    pub signals: bool,
    /// Use the event-driven `OpsIntegrated` signal if this build and the
    /// conductor support it. Polling continues regardless.
    pub ops_integrated: bool,
    /// Serve this conductor's own agent infos on `GET /v1/peers`.
    pub peer_hints: bool,
}

/// Result of [`discover`].
pub struct Discovered {
    /// DNA hashes (base64) of the app's cells.
    pub spaces: Vec<String>,
    pub sources: Vec<Box<dyn ChangeSource>>,
    pub peers: Option<DynPeerHints>,
    /// Whether the event-driven `OpsIntegrated` source is active.
    pub event_driven: bool,
}

fn admin_host(admin_addr: &str) -> &str {
    admin_addr
        .rsplit_once(':')
        .map(|(h, _)| h.trim_start_matches('[').trim_end_matches(']'))
        .unwrap_or("127.0.0.1")
}

/// Explicit loopback bind address for an attached app interface, derived
/// from the admin address. `None` (conductor default: loopback) when the
/// admin host is a name such as `localhost` or a non-loopback address.
fn loopback_bind_addr(admin_addr: &str) -> Option<String> {
    match admin_host(admin_addr).parse::<std::net::IpAddr>() {
        Ok(ip) if ip.is_loopback() => Some(ip.to_string()),
        _ => None,
    }
}

/// Open an authenticated app websocket. The admin connection is used only
/// for token issuance (and interface attachment) and is dropped before
/// returning.
pub async fn connect_app(cfg: &HolochainConfig) -> anyhow::Result<AppWebsocket> {
    let admin = AdminWebsocket::connect(cfg.admin_addr.as_str(), Some(ORIGIN.to_string()))
        .await
        .map_err(|e| anyhow::anyhow!("admin websocket {}: {e:?}", cfg.admin_addr))?;
    let port = match cfg.app_port {
        Some(p) => p,
        None => {
            let existing = admin
                .list_app_interfaces()
                .await
                .map_err(|e| anyhow::anyhow!("list_app_interfaces: {e:?}"))?
                .into_iter()
                .find(|i| {
                    i.allowed_origins.is_allowed(ORIGIN)
                        && i.installed_app_id
                            .as_deref()
                            .is_none_or(|a| a == cfg.app_id)
                });
            match existing {
                Some(i) => i.port,
                None => {
                    // Port 0: the conductor picks a free port. Bind to the
                    // same loopback address we reach the admin port on
                    // (the conductor's default binds both 127.0.0.1 and ::1,
                    // which fails on IPv4-only hosts). Never a public address.
                    let bind = loopback_bind_addr(&cfg.admin_addr);
                    let origins =
                        AllowedOrigins::Origins([ORIGIN.to_string()].into_iter().collect());
                    let p = admin
                        .attach_app_interface(0, bind, origins, Some(cfg.app_id.clone()))
                        .await
                        .map_err(|e| anyhow::anyhow!("attach_app_interface: {e:?}"))?;
                    tracing::info!(port = p, "attached loopback app interface for the relay");
                    p
                }
            }
        }
    };
    let issued = admin
        .issue_app_auth_token(IssueAppAuthenticationTokenPayload::for_installed_app_id(
            cfg.app_id.clone(),
        ))
        .await
        .map_err(|e| anyhow::anyhow!("issue_app_auth_token: {e:?}"))?;
    drop(admin);
    let host = admin_host(&cfg.admin_addr);
    // The relay never signs zome calls; this signer stays empty. Holochain
    // agent keys never leave the conductor's keystore.
    let signer = ClientAgentSigner::default();
    let app = AppWebsocket::connect(
        (host, port),
        issued.token,
        signer.into(),
        Some(ORIGIN.to_string()),
    )
    .await
    .map_err(|e| anyhow::anyhow!("app websocket {host}:{port}: {e:?}"))?;
    Ok(app)
}

/// Unique DNA hashes of all provisioned and cloned cells of the app.
pub fn app_dnas(app: &AppWebsocket) -> Vec<DnaHash> {
    let mut out: Vec<DnaHash> = Vec::new();
    for cells in app.cached_app_info().cell_info.values() {
        for c in cells {
            let dna = match c {
                CellInfo::Provisioned(p) => p.cell_id.dna_hash().clone(),
                CellInfo::Cloned(c) => c.cell_id.dna_hash().clone(),
                _ => continue,
            };
            if !out.contains(&dna) {
                out.push(dna);
            }
        }
    }
    out
}

/// Connect (retrying until `shutdown`), discover spaces, build sources.
pub async fn discover(
    cfg: HolochainConfig,
    shutdown: CancellationToken,
) -> anyhow::Result<Discovered> {
    let mut backoff = Backoff::new(Duration::from_millis(500), Duration::from_secs(30));
    let app = loop {
        match connect_app(&cfg).await {
            Ok(app) => break app,
            Err(e) => {
                let d = backoff.next_delay();
                tracing::warn!(error = %e, retry_in = ?d, "waiting for the Holochain conductor");
                tokio::select! {
                    _ = shutdown.cancelled() => anyhow::bail!("shut down before the conductor became available"),
                    _ = tokio::time::sleep(d) => {}
                }
            }
        }
    };
    let dnas = app_dnas(&app);
    let event_driven = cfg.ops_integrated && supports_ops_integrated(&app).await;
    drop(app);
    if dnas.is_empty() {
        anyhow::bail!("app {} has no cells", cfg.app_id);
    }
    let spaces: Vec<String> = dnas.iter().map(|d| d.to_string()).collect();
    tracing::info!(app_id = %cfg.app_id, ?spaces, event_driven, "discovered Holochain spaces");
    let mut sources: Vec<Box<dyn ChangeSource>> = Vec::new();
    if cfg.op_timings {
        sources.push(Box::new(OpTimingsSource::new(
            Arc::new(WsConnector(cfg.clone())),
            dnas.clone(),
            cfg.poll_interval,
            cfg.page_limit,
        )));
    }
    if cfg.signals {
        sources.push(Box::new(AppSignalSource { cfg: cfg.clone() }));
    }
    #[cfg(feature = "ops-integrated-signal")]
    if event_driven {
        sources.push(Box::new(OpsIntegratedSource { cfg: cfg.clone() }));
    }
    let peers: Option<DynPeerHints> = cfg
        .peer_hints
        .then(|| Arc::new(ConductorPeers::new(cfg.clone())) as DynPeerHints);
    Ok(Discovered {
        spaces,
        sources,
        peers,
        event_driven,
    })
}

/// Probe whether the conductor accepts `SubscribeOpsIntegrated`.
#[cfg(feature = "ops-integrated-signal")]
async fn supports_ops_integrated(app: &AppWebsocket) -> bool {
    match signals::subscribe_ops_integrated(app).await {
        Ok(()) => true,
        Err(e) => {
            tracing::warn!(error = %e, "conductor does not support OpsIntegrated (needs the upstream patch); polling only");
            false
        }
    }
}

#[cfg(not(feature = "ops-integrated-signal"))]
async fn supports_ops_integrated(_app: &AppWebsocket) -> bool {
    tracing::info!(
        "relay built without the `ops-integrated-signal` feature; polling DumpOpTimings only"
    );
    false
}
