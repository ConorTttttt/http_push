//! Peer hints: the relay conductor's own signed Kitsune2 agent infos.
//!
//! A lightweight node that was offline adds these to its own conductor
//! (Holochain `AddAgentInfo`) so native Kitsune2 gossip can reach this
//! always-on peer directly, without a third party. The records are signed by
//! the conductor's agents; the receiving conductor verifies the signature, so
//! the relay cannot forge a peer. Only this conductor's *local* agents are
//! returned: the relay does not act as a bootstrap or peer directory.

use crate::{connect_app, HolochainConfig};
use futures::future::BoxFuture;
use hc_push_source_api::PeerHints;
use holochain_client::AppWebsocket;
use holochain_types::prelude::DnaHash;

pub struct ConductorPeers {
    cfg: HolochainConfig,
    conn: tokio::sync::Mutex<Option<AppWebsocket>>,
}

impl ConductorPeers {
    pub fn new(cfg: HolochainConfig) -> Self {
        Self {
            cfg,
            conn: tokio::sync::Mutex::new(None),
        }
    }
}

/// The agent id and tombstone flag of a JSON-encoded `AgentInfoSigned`.
pub fn agent_of(signed_json: &str) -> Option<(Vec<u8>, bool)> {
    let outer: serde_json::Value = serde_json::from_str(signed_json).ok()?;
    let inner = outer.get("agentInfo")?.as_str()?;
    let info: kitsune2_api::AgentInfo = serde_json::from_str(inner).ok()?;
    Some((info.agent.to_vec(), info.is_tombstone))
}

/// Keep only live records of `local_agent` (raw 32-byte ed25519 key).
pub fn local_agent_infos(infos: Vec<String>, local_agent: &[u8]) -> Vec<String> {
    infos
        .into_iter()
        .filter(|s| matches!(agent_of(s), Some((a, false)) if a == local_agent))
        .collect()
}

impl PeerHints for ConductorPeers {
    fn peer_hints(&self, space: &str) -> BoxFuture<'_, anyhow::Result<Vec<String>>> {
        let space = space.to_string();
        Box::pin(async move {
            let dna = DnaHash::try_from(space.as_str())
                .map_err(|e| anyhow::anyhow!("bad space id: {e:?}"))?;
            let mut guard = self.conn.lock().await;
            if guard.is_none() {
                *guard = Some(connect_app(&self.cfg).await?);
            }
            let app = guard.as_ref().expect("just connected");
            let infos = match app.agent_info(Some(vec![dna])).await {
                Ok(i) => i,
                Err(e) => {
                    // Drop the connection; the next request reconnects.
                    *guard = None;
                    anyhow::bail!("agent_info: {e:?}");
                }
            };
            Ok(local_agent_infos(infos, app.my_pub_key.get_raw_32()))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn signed(agent: &[u8], tombstone: bool) -> String {
        use base64::Engine;
        let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD;
        let inner = serde_json::json!({
            "agent": b64.encode(agent),
            "space": b64.encode([9u8; 32]),
            "createdAt": "1700000000000000",
            "expiresAt": "1700000600000000",
            "isTombstone": tombstone,
            "url": null,
            "storageArc": null,
        });
        serde_json::json!({ "agentInfo": inner.to_string(), "signature": "AAAA" }).to_string()
    }

    #[test]
    fn keeps_only_live_local_agent() {
        let me = [1u8; 32];
        let other = [2u8; 32];
        let infos = vec![
            signed(&me, false),
            signed(&other, false),
            signed(&me, true),
            "junk".into(),
        ];
        let kept = local_agent_infos(infos.clone(), &me);
        assert_eq!(kept, vec![infos[0].clone()]);
    }
}
