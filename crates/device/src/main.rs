//! `hc-push-device catch-up`: the device side of a wake-up, after the HTTP
//! sync.
//!
//! 1. Connect to the local conductor (loopback admin + app websocket; the
//!    admin interface is never exposed beyond the device).
//! 2. Wait until this node's own agent has (re)joined the space.
//! 3. Hand the relays' signed peer records to the conductor
//!    (`AddAgentInfo`). The conductor verifies each signature on insert;
//!    this tool never interprets them as trusted.
//! 4. Optionally ask the conductor to gossip with those peers next
//!    (`RequestGossipRound`, patched Holochain + Kitsune2 only).
//! 5. Poll the conductor's own `DumpOpTimings` until every hinted op in this
//!    node's storage arc is held, integrated, `Accepted` and locally
//!    validated, and report timings and Kitsune2 byte counts as JSON.
//!
//! The data itself moves over Kitsune2 between conductors; nothing here
//! fetches or validates op content.

use anyhow::Context;
use base64::Engine;
use clap::{Parser, Subcommand};
use hc_push_device::{
    k2_bytes, parse_agent_info, progress, select_required, AgentSummary, OpState,
};
use hc_push_holochain::{connect_app, HolochainConfig, ORIGIN};
use hc_push_protocol::{ArcFilter, EventEnvelope};
use holochain_client::{AdminWebsocket, AppWebsocket};
use holochain_types::prelude::{DnaHash, RecordValidity};
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Parser, Debug)]
#[command(
    name = "hc-push-device",
    version,
    about = "Device-side catch-up helper"
)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Add relay peer hints to the local conductor and wait for catch-up.
    CatchUp(CatchUp),
}

#[derive(clap::Args, Debug)]
struct CatchUp {
    /// Local conductor admin websocket (loopback).
    #[arg(long, default_value = "127.0.0.1:4444")]
    admin: String,
    #[arg(long)]
    app_id: String,
    /// DNA hash (base64) of the space to catch up.
    #[arg(long)]
    dna: String,
    /// Hints from `hc-push-client sync` (one JSON envelope per line).
    /// Repeatable; the same op hinted by several relays counts once.
    #[arg(long = "hints", required = true)]
    hints: Vec<PathBuf>,
    /// Peer records from `hc-push-client peers` (JSON array). Repeatable.
    #[arg(long = "peers")]
    peers: Vec<PathBuf>,
    /// This node's storage arc (`full`, `empty` or `start-end,...`).
    #[arg(long, default_value = "full")]
    arc: ArcFilter,
    /// Ask the conductor to gossip with the hinted peers next.
    #[arg(long)]
    request_round: bool,
    /// Give up after this many seconds.
    #[arg(long, default_value_t = 300)]
    timeout_s: u64,
    /// Unix ms the conductor process was started; times are reported
    /// relative to it (default: when this tool started).
    #[arg(long)]
    t0_ms: Option<u64>,
    #[arg(long)]
    out: Option<PathBuf>,
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn read_hints(paths: &[PathBuf]) -> anyhow::Result<Vec<EventEnvelope>> {
    let mut out = Vec::new();
    for p in paths {
        let text = std::fs::read_to_string(p).with_context(|| p.display().to_string())?;
        for line in text.lines().filter(|l| !l.trim().is_empty()) {
            out.push(serde_json::from_str(line).with_context(|| p.display().to_string())?);
        }
    }
    Ok(out)
}

/// Encoded `AgentInfoSigned` strings, as `AddAgentInfo` takes them.
fn read_peers(paths: &[PathBuf]) -> anyhow::Result<Vec<String>> {
    let mut out = Vec::new();
    for p in paths {
        let text = std::fs::read_to_string(p).with_context(|| p.display().to_string())?;
        let v: Vec<serde_json::Value> =
            serde_json::from_str(&text).with_context(|| p.display().to_string())?;
        out.extend(v.iter().map(|x| x.to_string()));
    }
    Ok(out)
}

async fn agents(app: &AppWebsocket, dna: &DnaHash) -> Vec<AgentSummary> {
    app.agent_info(Some(vec![dna.clone()]))
        .await
        .unwrap_or_default()
        .iter()
        .filter_map(|s| parse_agent_info(s))
        .collect()
}

async fn op_states(app: &AppWebsocket, dna: &DnaHash) -> anyhow::Result<HashMap<String, OpState>> {
    let mut out = HashMap::new();
    let mut cursor = None;
    loop {
        let page = app
            .dump_op_timings(dna.clone(), cursor, Some(1000))
            .await
            .map_err(|e| anyhow::anyhow!("dump_op_timings: {e:?}"))?;
        let n = page.timings.len();
        for t in page.timings {
            out.insert(
                t.op_hash.to_string(),
                OpState {
                    integrated: t.when_integrated.is_some(),
                    accepted: t.validation_status == Some(RecordValidity::Accepted),
                    locally_validated: t.locally_validated == Some(true),
                },
            );
        }
        cursor = page.cursor;
        if n < 1000 || cursor.is_none() {
            return Ok(out);
        }
    }
}

#[cfg(feature = "request-gossip-round")]
async fn request_round(app: &AppWebsocket, dna: &DnaHash, url: &str) -> Result<(), String> {
    let url = <kitsune2_api::Url as std::str::FromStr>::from_str(url).map_err(|e| e.to_string())?;
    app.request_gossip_round(dna.clone(), url)
        .await
        .map_err(|e| format!("{e:?}"))
}

#[cfg(not(feature = "request-gossip-round"))]
async fn request_round(_app: &AppWebsocket, _dna: &DnaHash, _url: &str) -> Result<(), String> {
    Err("built without the `request-gossip-round` feature (needs patched Holochain)".into())
}

async fn catch_up(a: CatchUp) -> anyhow::Result<serde_json::Value> {
    let started = now_ms();
    let t0 = a.t0_ms.unwrap_or(started);
    let rel = |t: u64| t.saturating_sub(t0);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(a.timeout_s);
    let dna = DnaHash::try_from(a.dna.as_str()).map_err(|e| anyhow::anyhow!("--dna: {e:?}"))?;

    let hints = read_hints(&a.hints)?;
    let selection = select_required(&hints, &a.arc);
    let peers = read_peers(&a.peers)?;
    let peer_urls: Vec<String> = peers
        .iter()
        .filter_map(|p| parse_agent_info(p).and_then(|s| s.url))
        .collect();

    // 1. Connect (the conductor may still be starting).
    let cfg = HolochainConfig {
        admin_addr: a.admin.clone(),
        app_id: a.app_id.clone(),
        app_port: None,
        poll_interval: Duration::from_secs(1),
        page_limit: 1000,
        op_timings: false,
        signals: false,
        ops_integrated: false,
        peer_hints: false,
    };
    let (admin, app) = loop {
        let r = async {
            let admin = AdminWebsocket::connect(a.admin.as_str(), Some(ORIGIN.to_string()))
                .await
                .map_err(|e| anyhow::anyhow!("{e:?}"))?;
            let app = connect_app(&cfg).await?;
            anyhow::Ok((admin, app))
        }
        .await;
        match r {
            Ok(x) => break x,
            Err(e) if tokio::time::Instant::now() < deadline => {
                tracing::debug!(error = %e, "conductor not ready");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(e) => return Err(e.context("conductor did not become reachable")),
        }
    };
    let t_connected = now_ms();
    let own = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode(app.cached_app_info().agent_pub_key.get_raw_32());

    // 2. Own agent back in the space.
    let t_joined = loop {
        if agents(&app, &dna).await.iter().any(|s| s.agent == own) {
            break now_ms();
        }
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "own agent never joined"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };

    // 3. Peer records from the relays; the conductor verifies signatures.
    let mut add_error = None;
    if !peers.is_empty() {
        if let Err(e) = admin.add_agent_info(peers.clone()).await {
            add_error = Some(format!("{e:?}"));
        }
    }
    let t_peers_added = now_ms();
    let t_peers_known = loop {
        let known = agents(&app, &dna).await;
        if peer_urls
            .iter()
            .all(|u| known.iter().any(|s| s.url.as_deref() == Some(u.as_str())))
        {
            break Some(now_ms());
        }
        if tokio::time::Instant::now() >= deadline {
            break None;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };

    // 4. Optionally: gossip with those peers next.
    let mut round_results = Vec::new();
    let mut t_round_requested = None;
    if a.request_round {
        for u in &peer_urls {
            let r = request_round(&app, &dna, u).await;
            round_results.push(serde_json::json!({ "url": u, "ok": r.is_ok(), "error": r.err() }));
        }
        t_round_requested = Some(now_ms());
    }

    // 5. Wait for the required ops.
    let mut t_first_fetched = None;
    let mut held_at_start = None;
    let (t_synced, last) = loop {
        let states = op_states(&app, &dna).await?;
        let p = progress(&selection.required, &states);
        // Ops the node already held before this wake-up (e.g. its own) do
        // not count as fetched.
        let initial = *held_at_start.get_or_insert(p.held);
        if p.held > initial && t_first_fetched.is_none() {
            t_first_fetched = Some(now_ms());
        }
        if p.complete() {
            break (Some(now_ms()), p);
        }
        if tokio::time::Instant::now() >= deadline {
            break (None, p);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };

    let k2 = app
        .dump_network_stats()
        .await
        .map(|s| k2_bytes(&s.transport_stats))
        .map_err(|e| anyhow::anyhow!("dump_network_stats: {e:?}"))?;
    let own_info = agents(&app, &dna)
        .await
        .into_iter()
        .find(|s| s.agent == own);
    let ms = |t: Option<u64>| t.map(rel);
    Ok(serde_json::json!({
        "synced": t_synced.is_some(),
        "arc": a.arc.to_string(),
        "hints": {
            "received": hints.len(),
            "required": selection.required.len(),
            "outside_arc": selection.outside_arc,
            "unlocated_not_required": selection.unlocated_not_required,
            "other": selection.other,
        },
        "progress": last,
        "required_already_held_at_start": held_at_start,
        "peers": { "records": peers.len(), "urls": peer_urls, "add_error": add_error },
        "request_round": { "requested": a.request_round, "results": round_results },
        "t_ms": {
            "tool_started": rel(started),
            "connected": rel(t_connected),
            "own_agent_joined": rel(t_joined),
            "peers_added": rel(t_peers_added),
            "peers_known": ms(t_peers_known),
            "round_requested": ms(t_round_requested),
            "first_required_op_fetched": ms(t_first_fetched),
            "all_required_ops_validated": ms(t_synced),
        },
        "k2_bytes_open_connections": k2,
        "own_agent": own_info,
    }))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()),
        )
        .with_writer(std::io::stderr)
        .init();
    let Cmd::CatchUp(a) = Cli::parse().cmd;
    let out = a.out.clone();
    let report = catch_up(a).await?;
    let text = serde_json::to_string_pretty(&report)?;
    if let Some(p) = out {
        std::fs::write(&p, &text)?;
    }
    println!("{text}");
    if report["synced"] != serde_json::Value::Bool(true) {
        std::process::exit(3);
    }
    Ok(())
}
