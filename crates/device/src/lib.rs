//! Device-side catch-up logic, kept free of I/O so it can be unit tested.
//!
//! After a phone wakes up it syncs hints over HTTP while its conductor is
//! still stopped, starts the conductor, and lets Kitsune2 fetch the data.
//! This module decides which hinted ops the device is *required* to end up
//! holding (those in its own storage arc) and measures progress from the
//! conductor's own `DumpOpTimings`.

use hc_push_protocol::{event_loc, ArcFilter, ChangeKind, EventEnvelope, RefType};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};

/// How the hints received over HTTP relate to this node's storage arc.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct HintSelection {
    /// Accepted-op hints this node must hold once caught up.
    pub required: BTreeSet<String>,
    /// Op hints with a known location outside the arc. The relay normally
    /// filters these out; they can still arrive from relays without
    /// `arc_filter` support.
    pub outside_arc: usize,
    /// Op hints with no location (e.g. from the `DumpOpTimings` poller).
    /// Required only for a full arc; otherwise the node cannot tell whether
    /// it should hold them.
    pub unlocated_not_required: usize,
    /// Hints that are not about accepted ops (app signals, rejected ops).
    pub other: usize,
}

/// Split hints into those the node must hold and the rest.
pub fn select_required<'a>(
    hints: impl IntoIterator<Item = &'a EventEnvelope>,
    arc: &ArcFilter,
) -> HintSelection {
    let mut out = HintSelection::default();
    let mut seen = BTreeSet::new();
    for e in hints {
        if !seen.insert(e.change_key.clone()) {
            continue; // same op hinted by several relays
        }
        let is_op = e.kind == ChangeKind::OpIntegrated
            && e.refs.len() == 1
            && e.refs[0].ref_type == RefType::DhtOp;
        if !is_op {
            out.other += 1;
            continue;
        }
        match event_loc(&e.refs) {
            Some(loc) if arc.contains(loc) => {
                out.required.insert(e.refs[0].id.clone());
            }
            Some(_) => out.outside_arc += 1,
            None if arc.is_full() => {
                out.required.insert(e.refs[0].id.clone());
            }
            None => out.unlocated_not_required += 1,
        }
    }
    out
}

/// What the device's conductor reports about one op.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpState {
    pub integrated: bool,
    pub accepted: bool,
    pub locally_validated: bool,
}

impl OpState {
    /// Held, integrated, accepted and validated by this conductor itself.
    pub fn done(&self) -> bool {
        self.integrated && self.accepted && self.locally_validated
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct Progress {
    pub required: usize,
    /// Present in the conductor's DHT store (any state).
    pub held: usize,
    /// [`OpState::done`].
    pub done: usize,
}

impl Progress {
    pub fn complete(&self) -> bool {
        self.done == self.required
    }
}

pub fn progress(required: &BTreeSet<String>, states: &HashMap<String, OpState>) -> Progress {
    let mut p = Progress {
        required: required.len(),
        ..Default::default()
    };
    for op in required {
        if let Some(s) = states.get(op) {
            p.held += 1;
            if s.done() {
                p.done += 1;
            }
        }
    }
    p
}

/// The fields of a Kitsune2 `AgentInfo` this tool reads. Ids are
/// base64url (no padding) of the 32-byte key.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentSummary {
    pub agent: String,
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default)]
    pub storage_arc: Option<(u32, u32)>,
    #[serde(default)]
    pub is_tombstone: bool,
    pub expires_at: serde_json::Value,
}

/// Parse an encoded `AgentInfoSigned` (`{"agentInfo": "<json>", ...}`).
/// Only reads it; signature verification is the conductor's job when the
/// record is added to its peer store.
pub fn parse_agent_info(encoded: &str) -> Option<AgentSummary> {
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Signed {
        agent_info: String,
    }
    let signed: Signed = serde_json::from_str(encoded).ok()?;
    serde_json::from_str(&signed.agent_info).ok()
}

/// Bytes on the conductor's open Kitsune2 connections, per remote peer key.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct K2Bytes {
    pub send_bytes: u64,
    pub recv_bytes: u64,
    pub connections: BTreeMap<String, (u64, u64)>,
}

pub fn k2_bytes(stats: &kitsune2_api::TransportStats) -> K2Bytes {
    let mut out = K2Bytes::default();
    for c in &stats.connections {
        out.send_bytes += c.send_bytes;
        out.recv_bytes += c.recv_bytes;
        let e = out.connections.entry(c.pub_key.clone()).or_default();
        e.0 += c.send_bytes;
        e.1 += c.recv_bytes;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use hc_push_protocol::{change_key, ChangeRef, Cursor};

    fn hint(seq: u64, kind: ChangeKind, r: ChangeRef) -> EventEnvelope {
        EventEnvelope {
            v: 1,
            event_id: format!("j-{seq}"),
            cursor: Cursor::new("0123abcd", seq),
            space: "s".into(),
            kind,
            change_key: change_key("s", kind, std::slice::from_ref(&r)),
            refs: vec![r],
            observed_at_ms: 0,
            occurred_at_ms: None,
            source: "t".into(),
        }
    }

    fn op(seq: u64, id: &str, loc: Option<u32>) -> EventEnvelope {
        let r = ChangeRef::new(RefType::DhtOp, id);
        hint(
            seq,
            ChangeKind::OpIntegrated,
            match loc {
                Some(l) => r.with_loc(l),
                None => r,
            },
        )
    }

    fn sample() -> Vec<EventEnvelope> {
        vec![
            op(1, "low", Some(10)),
            op(2, "high", Some(u32::MAX - 10)),
            op(3, "unknown", None),
            op(4, "low", Some(10)), // the same op hinted by a second relay
            hint(5, ChangeKind::AppSignal, ChangeRef::new(RefType::Digest, "sha256:00")),
            hint(6, ChangeKind::OpRejected, ChangeRef::new(RefType::DhtOp, "bad")),
        ]
    }

    #[test]
    fn full_arc_requires_every_accepted_op() {
        let s = select_required(&sample(), &ArcFilter::full());
        assert_eq!(
            s.required.iter().map(String::as_str).collect::<Vec<_>>(),
            ["high", "low", "unknown"]
        );
        assert_eq!((s.outside_arc, s.unlocated_not_required, s.other), (0, 0, 2));
    }

    #[test]
    fn partial_arc_requires_only_located_ops_inside_it() {
        let arc: ArcFilter = "0-2147483647".parse().unwrap();
        let s = select_required(&sample(), &arc);
        assert_eq!(s.required.iter().collect::<Vec<_>>(), ["low"]);
        assert_eq!((s.outside_arc, s.unlocated_not_required), (1, 1));
        // A wrapping arc that holds the top of the location space.
        let wrap: ArcFilter = format!("{}-5", u32::MAX - 100).parse().unwrap();
        let s = select_required(&sample(), &wrap);
        assert_eq!(s.required.iter().collect::<Vec<_>>(), ["high"]);
    }

    #[test]
    fn zero_arc_requires_nothing() {
        let s = select_required(&sample(), &ArcFilter::empty());
        assert!(s.required.is_empty());
        assert_eq!((s.outside_arc, s.unlocated_not_required), (2, 1));
        assert!(progress(&s.required, &HashMap::new()).complete());
    }

    #[test]
    fn progress_counts_only_ops_this_conductor_validated() {
        let required: BTreeSet<String> = ["a", "b", "c"].map(String::from).into();
        let st = |integrated, accepted, locally_validated| OpState {
            integrated,
            accepted,
            locally_validated,
        };
        let mut states = HashMap::new();
        states.insert("a".to_string(), st(true, true, true));
        states.insert("b".to_string(), st(false, false, false)); // still in limbo
        states.insert("c".to_string(), st(true, true, false)); // not validated here
        states.insert("x".to_string(), st(true, true, true)); // not required
        let p = progress(&required, &states);
        assert_eq!((p.required, p.held, p.done), (3, 3, 1));
        assert!(!p.complete());
    }

    #[test]
    fn reads_agent_info_without_trusting_it() {
        let inner = serde_json::json!({
            "agent": "AAAA", "space": "BBBB", "createdAt": "1", "expiresAt": "2",
            "isTombstone": false, "url": "http://127.0.0.1:1/x", "storageArc": [0, 4294967295u32]
        });
        let enc = serde_json::json!({ "agentInfo": inner.to_string(), "signature": "c2ln" }).to_string();
        let a = parse_agent_info(&enc).unwrap();
        assert_eq!(a.url.as_deref(), Some("http://127.0.0.1:1/x"));
        assert_eq!(a.storage_arc, Some((0, u32::MAX)));
        assert!(parse_agent_info("{}").is_none());
    }
}
