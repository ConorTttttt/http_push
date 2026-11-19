//! Wire types for the Holochain HTTP push relay protocol, version 1.
//!
//! This crate is deliberately free of any async runtime, HTTP stack or
//! Holochain dependency so that it can be compiled for Android and iOS and
//! shared verbatim by relays and lightweight clients.
//!
//! # Trust model
//!
//! Nothing in this protocol carries validated DHT data. Every reference in an
//! [`EventEnvelope`] is an *opaque hint* that "something changed" on the
//! relay's conductor. A client must fetch and validate the referenced data via
//! a Holochain validation path (its own conductor, or a conductor it trusts)
//! before acting on it. A malicious relay can lie about, delay or omit hints;
//! it cannot forge Holochain actions because it never supplies signed data.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fmt;
use std::str::FromStr;

/// The protocol version implemented by this crate.
pub const PROTOCOL_VERSION: u32 = 1;

/// Path prefix for every v1 endpoint.
pub const API_PREFIX: &str = "/v1";

/// SSE event name carrying one [`EventEnvelope`] in its `data` field.
pub const SSE_EVENT_CHANGE: &str = "change";
/// SSE event name telling the client to stop listening and call `/v1/sync`.
pub const SSE_EVENT_SYNC_REQUIRED: &str = "sync_required";

/// Maximum accepted length of a space identifier.
pub const MAX_SPACE_ID_LEN: usize = 128;
/// Maximum accepted length of a change reference identifier.
pub const MAX_REF_ID_LEN: usize = 256;
/// Maximum accepted length of a client label.
pub const MAX_LABEL_LEN: usize = 64;

// ---------------------------------------------------------------------------
// Identifiers and validation
// ---------------------------------------------------------------------------

/// Returns true if `s` is an acceptable space identifier.
///
/// For the Holochain source this is the base64url (`uhC0k…`) encoding of a
/// DNA hash. The rule is intentionally generic (URL-safe base64 alphabet,
/// bounded length) so that the mock source and future Kitsune2 space ids fit.
pub fn is_valid_space_id(s: &str) -> bool {
    !s.is_empty() && s.len() <= MAX_SPACE_ID_LEN && s.bytes().all(is_url_safe_byte)
}

/// Returns true if `s` is an acceptable change reference id.
pub fn is_valid_ref_id(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= MAX_REF_ID_LEN
        && s.bytes()
            .all(|b| is_url_safe_byte(b) || b == b':' || b == b'.')
}

fn is_url_safe_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'-' || b == b'_'
}

/// Returns true if `s` is an acceptable client label.
pub fn is_valid_label(s: &str) -> bool {
    s.len() <= MAX_LABEL_LEN && s.chars().all(|c| !c.is_control())
}

// ---------------------------------------------------------------------------
// Cursor
// ---------------------------------------------------------------------------

/// A position in one relay's journal.
///
/// Rendered as `v1.<journal_id>.<seq>`. Clients must treat it as opaque. A
/// cursor is only meaningful to the journal that issued it: presenting it to a
/// different relay, or to the same relay after its journal was reset, yields
/// `410 Gone` with [`ErrorCode::CursorUnknownJournal`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Cursor {
    /// Random identifier of the journal instance that issued this cursor.
    pub journal_id: String,
    /// Sequence number of the last event the holder has processed.
    /// `0` means "before the first event".
    pub seq: u64,
}

impl Cursor {
    pub fn new(journal_id: impl Into<String>, seq: u64) -> Self {
        Self {
            journal_id: journal_id.into(),
            seq,
        }
    }
}

impl fmt::Display for Cursor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "v1.{}.{}", self.journal_id, self.seq)
    }
}

/// Error returned when a cursor string cannot be parsed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("malformed cursor")]
pub struct CursorParseError;

impl FromStr for Cursor {
    type Err = CursorParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.len() > 96 {
            return Err(CursorParseError);
        }
        let mut parts = s.split('.');
        let (Some("v1"), Some(journal_id), Some(seq), None) =
            (parts.next(), parts.next(), parts.next(), parts.next())
        else {
            return Err(CursorParseError);
        };
        if journal_id.is_empty()
            || journal_id.len() > 64
            || !journal_id
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(CursorParseError);
        }
        if seq.is_empty() || seq.len() > 20 || !seq.bytes().all(|b| b.is_ascii_digit()) {
            return Err(CursorParseError);
        }
        let seq = seq.parse().map_err(|_| CursorParseError)?;
        Ok(Cursor::new(journal_id, seq))
    }
}

impl Serialize for Cursor {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for Cursor {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

// ---------------------------------------------------------------------------
// Events
// ---------------------------------------------------------------------------

/// What kind of change an event announces.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChangeKind {
    /// A DHT op held by the relay's conductor finished validation and was
    /// integrated as valid. Observed through the conductor's `DumpOpTimings`
    /// app API. The op may have arrived by publish, gossip or local authoring.
    OpIntegrated,
    /// A DHT op held by the relay's conductor was integrated as *rejected*.
    OpRejected,
    /// A zome on the relay's conductor emitted an app signal. Only a digest of
    /// the payload is forwarded.
    AppSignal,
    /// Synthetic change produced by the mock source. Never real DHT data.
    MockChange,
    /// A kind introduced by a newer relay. Clients should treat it as a
    /// generic "something changed in this space" hint.
    #[serde(other)]
    Unknown,
}

impl ChangeKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            ChangeKind::OpIntegrated => "op_integrated",
            ChangeKind::OpRejected => "op_rejected",
            ChangeKind::AppSignal => "app_signal",
            ChangeKind::MockChange => "mock_change",
            ChangeKind::Unknown => "unknown",
        }
    }
}

impl FromStr for ChangeKind {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, ()> {
        Ok(match s {
            "op_integrated" => ChangeKind::OpIntegrated,
            "op_rejected" => ChangeKind::OpRejected,
            "app_signal" => ChangeKind::AppSignal,
            "mock_change" => ChangeKind::MockChange,
            _ => ChangeKind::Unknown,
        })
    }
}

/// The type of an opaque change reference.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RefType {
    /// A Holochain `DhtOpHash` (base64url, `uhCQk…`).
    DhtOp,
    /// A content digest (`sha256:<hex>`) of data the relay will not forward.
    Digest,
    /// An identifier produced by the mock source.
    Mock,
    #[serde(other)]
    Unknown,
}

impl RefType {
    pub fn as_str(&self) -> &'static str {
        match self {
            RefType::DhtOp => "dht_op",
            RefType::Digest => "digest",
            RefType::Mock => "mock",
            RefType::Unknown => "unknown",
        }
    }
}

/// An opaque pointer to changed data. Never contains the data itself.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ChangeRef {
    #[serde(rename = "type")]
    pub ref_type: RefType,
    pub id: String,
    /// Kitsune2 DHT location of the referenced op, if the source knows it.
    ///
    /// For a Holochain `DhtOpHash` this is the location of the op's *basis*
    /// hash (`AnyLinkableHash::get_loc()`), which is the location Kitsune2
    /// uses to decide which agents' storage arcs hold the op. Like the rest
    /// of the hint it is unverified: a client must not trust it beyond
    /// deciding which hints to look at. Not part of [`change_key`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub loc: Option<u32>,
}

impl ChangeRef {
    pub fn new(ref_type: RefType, id: impl Into<String>) -> Self {
        Self {
            ref_type,
            id: id.into(),
            loc: None,
        }
    }

    /// Attach the DHT location of the referenced op.
    pub fn with_loc(mut self, loc: u32) -> Self {
        self.loc = Some(loc);
        self
    }
}

/// The DHT location of an event: `Some` only when every ref carries the same
/// location (true for op hints, which have exactly one ref). `None` means the
/// location is unknown.
pub fn event_loc(refs: &[ChangeRef]) -> Option<u32> {
    let first = refs.first()?.loc?;
    refs.iter().all(|r| r.loc == Some(first)).then_some(first)
}

// ---------------------------------------------------------------------------
// Storage arcs
// ---------------------------------------------------------------------------

/// Maximum number of ranges in an [`ArcFilter`].
pub const MAX_ARC_RANGES: usize = 8;

/// An inclusive range of Kitsune2 DHT locations. When `start > end` the
/// range wraps past `u32::MAX`, as `kitsune2_api::DhtArc::Arc` does.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DhtArcRange {
    pub start: u32,
    pub end: u32,
}

impl DhtArcRange {
    /// Every location.
    pub const FULL: DhtArcRange = DhtArcRange {
        start: 0,
        end: u32::MAX,
    };

    pub fn contains(&self, loc: u32) -> bool {
        if self.start <= self.end {
            self.start <= loc && loc <= self.end
        } else {
            loc >= self.start || loc <= self.end
        }
    }
}

/// The storage arc(s) a client holds, used to select which op hints it
/// receives (`arc` query parameter of `/v1/sync` and `/v1/events`).
///
/// No ranges means a zero arc. Text form: `empty`, `full`, or up to
/// [`MAX_ARC_RANGES`] comma-separated `start-end` pairs of decimal `u32`
/// (inclusive, wrapping when `start > end`), e.g. `0-1073741823`.
///
/// Filtering is conservative: an event whose location is unknown (see
/// [`event_loc`]) is always admitted, because the relay cannot tell whether
/// it belongs to the arc. Clients must not treat such hints as data they
/// are required to hold.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct ArcFilter {
    ranges: Vec<DhtArcRange>,
}

impl ArcFilter {
    /// A zero arc: no located hints are admitted.
    pub fn empty() -> Self {
        Self::default()
    }

    /// A full arc: every hint is admitted.
    pub fn full() -> Self {
        Self {
            ranges: vec![DhtArcRange::FULL],
        }
    }

    pub fn from_ranges(ranges: Vec<DhtArcRange>) -> Result<Self, ArcParseError> {
        if ranges.len() > MAX_ARC_RANGES {
            return Err(ArcParseError);
        }
        Ok(Self { ranges })
    }

    pub fn ranges(&self) -> &[DhtArcRange] {
        &self.ranges
    }

    /// True for a zero arc.
    pub fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }

    /// True if some range covers every location (`0-4294967295`, or a
    /// wrapping range such as `5-4`).
    pub fn is_full(&self) -> bool {
        self.ranges
            .iter()
            .any(|r| r.start == r.end.wrapping_add(1) || (r.start == 0 && r.end == u32::MAX))
    }

    pub fn contains(&self, loc: u32) -> bool {
        self.ranges.iter().any(|r| r.contains(loc))
    }

    /// Whether an event with these refs is admitted: its location is unknown,
    /// or it lies inside the arc.
    pub fn admits(&self, refs: &[ChangeRef]) -> bool {
        match event_loc(refs) {
            None => true,
            Some(loc) => self.contains(loc),
        }
    }
}

impl fmt::Display for ArcFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.ranges.is_empty() {
            return f.write_str("empty");
        }
        for (i, r) in self.ranges.iter().enumerate() {
            if i > 0 {
                f.write_str(",")?;
            }
            write!(f, "{}-{}", r.start, r.end)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("malformed arc: expected `empty`, `full` or up to 8 comma-separated `start-end` u32 pairs")]
pub struct ArcParseError;

impl FromStr for ArcFilter {
    type Err = ArcParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "empty" => return Ok(Self::empty()),
            "full" => return Ok(Self::full()),
            _ => {}
        }
        let mut ranges = Vec::new();
        for part in s.split(',') {
            let (a, b) = part.split_once('-').ok_or(ArcParseError)?;
            let parse = |x: &str| {
                if x.is_empty() || !x.bytes().all(|c| c.is_ascii_digit()) {
                    return Err(ArcParseError);
                }
                x.parse::<u32>().map_err(|_| ArcParseError)
            };
            ranges.push(DhtArcRange {
                start: parse(a)?,
                end: parse(b)?,
            });
        }
        Self::from_ranges(ranges)
    }
}

/// Relay-independent identity of a change.
///
/// Two relays that observe the same DHT op compute the same key, so a client
/// subscribed to both can deduplicate across relays. Computed as the first 16
/// bytes (hex) of `SHA-256(space 0x00 kind 0x00 (ref_type 0x00 ref_id 0x00)*)`.
pub fn change_key(space: &str, kind: ChangeKind, refs: &[ChangeRef]) -> String {
    let mut h = Sha256::new();
    h.update(space.as_bytes());
    h.update([0]);
    h.update(kind.as_str().as_bytes());
    h.update([0]);
    for r in refs {
        h.update(r.ref_type.as_str().as_bytes());
        h.update([0]);
        h.update(r.id.as_bytes());
        h.update([0]);
    }
    hex::encode(&h.finalize()[..16])
}

/// One change notification. Identical shape on the SSE stream and in sync
/// pages.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventEnvelope {
    /// Protocol version of this envelope.
    pub v: u32,
    /// Relay-scoped unique event id: `<journal_id>-<seq>`.
    pub event_id: String,
    /// Cursor pointing *at* this event. Persist it after processing.
    pub cursor: Cursor,
    /// DNA / Kitsune2 space the change belongs to.
    pub space: String,
    pub kind: ChangeKind,
    /// Relay-independent identity of the change; see [`change_key`].
    pub change_key: String,
    /// Opaque references. Fetch and validate through Holochain before use.
    pub refs: Vec<ChangeRef>,
    /// When the relay observed the change (unix milliseconds, relay clock).
    /// Diagnostic only: never use for ordering.
    pub observed_at_ms: u64,
    /// When the change happened at its origin, if the source reports it
    /// (e.g. the conductor's integration time). Diagnostic only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub occurred_at_ms: Option<u64>,
    /// Identifier of the relay change source that produced the event.
    pub source: String,
}

impl EventEnvelope {
    pub fn seq(&self) -> u64 {
        self.cursor.seq
    }
}

/// Payload of the `sync_required` SSE event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncRequired {
    pub reason: SyncRequiredReason,
    /// The last cursor the stream delivered (or the cursor the client
    /// connected with). Resume `/v1/sync` from here.
    pub cursor: Cursor,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SyncRequiredReason {
    /// The backlog after the client's cursor exceeds the SSE replay limit.
    BacklogTooLarge,
    /// The client fell behind the live stream.
    Lagged,
    /// The relay is shutting down.
    Shutdown,
    #[serde(other)]
    Unknown,
}

// ---------------------------------------------------------------------------
// Subscriptions
// ---------------------------------------------------------------------------

/// `POST /v1/subscriptions` request body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubscribeRequest {
    /// Spaces (DNA hashes) the client wants hints for.
    pub spaces: Vec<String>,
    /// Optional human-readable label, for operator diagnostics only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
}

/// `POST /v1/subscriptions` response body (`201 Created`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubscribeResponse {
    pub subscription_id: String,
    /// Bearer token for `/v1/events` and `/v1/sync`. Shown once; the relay
    /// stores only its hash. Unrelated to any Holochain agent key.
    pub token: String,
    pub spaces: Vec<String>,
    /// The relay's current head. Start syncing from here to receive only new
    /// changes, or from [`SubscribeResponse::earliest_cursor`] for the whole
    /// retained backlog.
    pub cursor: Cursor,
    pub earliest_cursor: Cursor,
    /// Subscription expires if unused for this long.
    pub idle_ttl_secs: u64,
}

// ---------------------------------------------------------------------------
// Sync
// ---------------------------------------------------------------------------

/// `GET /v1/sync` response body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncPage {
    /// Events strictly after the request cursor, ascending by sequence,
    /// filtered to the subscription's spaces.
    pub events: Vec<EventEnvelope>,
    /// Cursor to persist and send next. May be greater than the last event's
    /// cursor when events for other spaces were skipped.
    pub next_cursor: Cursor,
    /// The relay's head at the time of the request.
    pub head_cursor: Cursor,
    /// True if more events are available; call again with `next_cursor`.
    pub has_more: bool,
}

// ---------------------------------------------------------------------------
// Peer hints
// ---------------------------------------------------------------------------

/// `GET /v1/peers?space=...` response body.
///
/// Signed peer records of the relay's own conductor for `space`, as Kitsune2
/// `AgentInfoSigned` JSON strings. A client hands them to its own conductor
/// (Holochain `AddAgentInfo`), which verifies the signatures, so that it can
/// gossip directly with this always-on peer. Not validated or interpreted by
/// the relay protocol.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeersResponse {
    pub space: String,
    pub agent_infos: Vec<String>,
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    BadRequest,
    Unauthorized,
    Forbidden,
    NotFound,
    /// The events after the cursor were pruned by the retention policy. The
    /// client must reconcile through Holochain, then resume from
    /// `earliest_cursor`.
    CursorExpired,
    /// The cursor was issued by a different journal (another relay, or this
    /// relay before a reset). Resume from `earliest_cursor` after reconciling.
    CursorUnknownJournal,
    PayloadTooLarge,
    RateLimited,
    /// A bounded resource (subscriptions, SSE slots) is exhausted.
    Capacity,
    Internal,
    Unavailable,
    #[serde(other)]
    Unknown,
}

/// Error response body: `{"error": {...}}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorBody {
    pub error: ApiError,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiError {
    pub code: ErrorCode,
    pub message: String,
    /// Present on cursor errors: the oldest cursor the relay can serve.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub earliest_cursor: Option<Cursor>,
}

// ---------------------------------------------------------------------------
// Capabilities and health
// ---------------------------------------------------------------------------

/// How faithfully a change source reflects real DHT state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceFidelity {
    /// Synthetic events; no relation to any DHT.
    Mock,
    /// Op-level observations from a real conductor's supported app API
    /// (`DumpOpTimings`). Covers only this conductor's storage arc.
    ConductorOpLevel,
    /// App signals emitted by zomes on a real conductor.
    ConductorAppSignal,
    /// Direct Kitsune2 integration. Not implemented.
    Kitsune2,
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceInfo {
    pub id: String,
    pub fidelity: SourceFidelity,
    pub description: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RegistrationMode {
    /// `POST /v1/subscriptions` requires an operator-issued registration key.
    Key,
    /// Anyone may register (rate limited).
    Open,
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetentionInfo {
    pub max_events: u64,
    pub max_age_secs: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LimitsInfo {
    pub max_page_size: u32,
    pub max_spaces_per_subscription: u32,
    pub max_body_bytes: u32,
    pub sse_replay_limit: u32,
}

/// `GET /v1/capabilities` response body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Capabilities {
    pub protocol_versions: Vec<u32>,
    pub software: String,
    pub relay_id: String,
    pub journal_id: String,
    /// Spaces served, unless the operator disabled advertising them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spaces: Option<Vec<String>>,
    pub sources: Vec<SourceInfo>,
    pub retention: RetentionInfo,
    pub limits: LimitsInfo,
    /// Notification transports offered: `"sse"`, and later `"web_push"`.
    pub transports: Vec<String>,
    /// `GET /v1/peers` is available (signed peer records of the relay's
    /// own conductor).
    #[serde(default)]
    pub peer_hints: bool,
    /// `/v1/sync` and `/v1/events` accept an `arc` parameter (see
    /// [`ArcFilter`]).
    #[serde(default)]
    pub arc_filter: bool,
    pub registration: RegistrationMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HealthStatus {
    Ok,
    Degraded,
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceState {
    Starting,
    Connected,
    Disconnected,
    Stopped,
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceHealth {
    pub id: String,
    pub state: SourceState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_ok_at_ms: Option<u64>,
}

/// `GET /v1/health` response body. Deliberately minimal: no error strings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Health {
    pub status: HealthStatus,
    pub journal_id: String,
    pub head_cursor: Cursor,
    pub sources: Vec<SourceHealth>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_roundtrip() {
        let c = Cursor::new("0123abcd", 42);
        assert_eq!(c.to_string(), "v1.0123abcd.42");
        assert_eq!("v1.0123abcd.42".parse::<Cursor>().unwrap(), c);
        let json = serde_json::to_string(&c).unwrap();
        assert_eq!(json, "\"v1.0123abcd.42\"");
        assert_eq!(serde_json::from_str::<Cursor>(&json).unwrap(), c);
    }

    #[test]
    fn cursor_rejects_garbage() {
        for bad in [
            "",
            "v1",
            "v2.abcd.1",
            "v1..1",
            "v1.ABCD.1",
            "v1.abcd.",
            "v1.abcd.-1",
            "v1.abcd.1.2",
            "v1.abcd.99999999999999999999999",
            "v1.zz.1",
        ] {
            assert!(bad.parse::<Cursor>().is_err(), "accepted {bad:?}");
        }
    }

    #[test]
    fn change_key_is_relay_independent_and_order_sensitive() {
        let a = [ChangeRef::new(RefType::DhtOp, "uhCQkAAA")];
        let k1 = change_key("uhC0kX", ChangeKind::OpIntegrated, &a);
        let k2 = change_key("uhC0kX", ChangeKind::OpIntegrated, &a);
        assert_eq!(k1, k2);
        assert_eq!(k1.len(), 32);
        assert_ne!(k1, change_key("uhC0kY", ChangeKind::OpIntegrated, &a));
        assert_ne!(k1, change_key("uhC0kX", ChangeKind::OpRejected, &a));
        // No ambiguity from concatenation.
        let b = [ChangeRef::new(RefType::Mock, "ab")];
        let c = [
            ChangeRef::new(RefType::Mock, "a"),
            ChangeRef::new(RefType::Mock, "b"),
        ];
        assert_ne!(
            change_key("s", ChangeKind::MockChange, &b),
            change_key("s", ChangeKind::MockChange, &c)
        );
    }

    #[test]
    fn unknown_kinds_are_tolerated() {
        let k: ChangeKind = serde_json::from_str("\"future_kind\"").unwrap();
        assert_eq!(k, ChangeKind::Unknown);
        let r: RefType = serde_json::from_str("\"future_ref\"").unwrap();
        assert_eq!(r, RefType::Unknown);
    }

    #[test]
    fn space_and_ref_validation() {
        assert!(is_valid_space_id(
            "uhC0kCWCo2DXc0mKCI9fMbNpZhZ0tAuL5BDA0vfrS1b_t4u_v-aT"
        ));
        assert!(!is_valid_space_id(""));
        assert!(!is_valid_space_id("has space"));
        assert!(!is_valid_space_id("../etc"));
        assert!(!is_valid_space_id(&"a".repeat(MAX_SPACE_ID_LEN + 1)));
        assert!(is_valid_ref_id("sha256:abcdef"));
        assert!(!is_valid_ref_id("a/b"));
        assert!(is_valid_label("Pixel 9 (work)"));
        assert!(!is_valid_label("bad\nlabel"));
    }

    #[test]
    fn arc_ranges_match_kitsune2_semantics() {
        let plain = DhtArcRange { start: 10, end: 20 };
        assert!(plain.contains(10) && plain.contains(20) && plain.contains(15));
        assert!(!plain.contains(9) && !plain.contains(21));
        // Wraps past u32::MAX.
        let wrap = DhtArcRange {
            start: u32::MAX - 5,
            end: 5,
        };
        for loc in [u32::MAX - 5, u32::MAX, 0, 5] {
            assert!(wrap.contains(loc), "{loc}");
        }
        for loc in [6, u32::MAX - 6, 1 << 31] {
            assert!(!wrap.contains(loc), "{loc}");
        }
        let single = DhtArcRange { start: 7, end: 7 };
        assert!(single.contains(7) && !single.contains(8));
        assert!(DhtArcRange::FULL.contains(0) && DhtArcRange::FULL.contains(u32::MAX));
    }

    #[test]
    fn arc_filter_parses_and_admits_conservatively() {
        let half: ArcFilter = "0-2147483647".parse().unwrap();
        assert_eq!(half.to_string(), "0-2147483647");
        let op = |loc: Option<u32>| {
            let r = ChangeRef::new(RefType::DhtOp, "uhCQkAAA");
            vec![match loc {
                Some(l) => r.with_loc(l),
                None => r,
            }]
        };
        assert!(half.admits(&op(Some(5))));
        assert!(!half.admits(&op(Some(1 << 31))));
        // Unknown location: always admitted.
        assert!(half.admits(&op(None)));
        let zero: ArcFilter = "empty".parse().unwrap();
        assert!(zero.is_empty());
        assert!(!zero.admits(&op(Some(0))));
        assert!(zero.admits(&op(None)));
        assert!(zero.admits(&[ChangeRef::new(RefType::Digest, "sha256:00")]));
        let full: ArcFilter = "full".parse().unwrap();
        assert!(full.is_full() && !half.is_full() && !zero.is_full());
        assert!("5-4".parse::<ArcFilter>().unwrap().is_full());
        assert!(full.admits(&op(Some(u32::MAX))));
        assert_eq!(full.to_string(), "0-4294967295");
        let two: ArcFilter = "4294967290-5,100-200".parse().unwrap();
        assert_eq!(two.ranges().len(), 2);
        assert!(two.contains(150) && two.contains(3) && !two.contains(50));
        assert_eq!(two.to_string().parse::<ArcFilter>().unwrap(), two);
        for bad in [
            "",
            "-",
            "1-",
            "-1",
            "a-b",
            "1-2,",
            "1-4294967296",
            "+1-2",
            "1..2",
            " 1-2",
            "0-1,0-1,0-1,0-1,0-1,0-1,0-1,0-1,0-1",
        ] {
            assert!(bad.parse::<ArcFilter>().is_err(), "accepted {bad:?}");
        }
    }

    #[test]
    fn event_location_requires_agreeing_refs() {
        let a = ChangeRef::new(RefType::DhtOp, "a").with_loc(1);
        let b = ChangeRef::new(RefType::DhtOp, "b").with_loc(2);
        let n = ChangeRef::new(RefType::DhtOp, "c");
        assert_eq!(event_loc(std::slice::from_ref(&a)), Some(1));
        assert_eq!(event_loc(&[a.clone(), a.clone()]), Some(1));
        assert_eq!(event_loc(&[a.clone(), b]), None);
        assert_eq!(event_loc(&[a, n]), None);
        assert_eq!(event_loc(&[]), None);
    }

    #[test]
    fn location_is_optional_on_the_wire_and_not_part_of_the_key() {
        let plain = ChangeRef::new(RefType::DhtOp, "uhCQkAAA");
        assert_eq!(
            serde_json::to_string(&plain).unwrap(),
            r#"{"type":"dht_op","id":"uhCQkAAA"}"#
        );
        let located = plain.clone().with_loc(42);
        let json = serde_json::to_string(&located).unwrap();
        assert_eq!(json, r#"{"type":"dht_op","id":"uhCQkAAA","loc":42}"#);
        assert_eq!(serde_json::from_str::<ChangeRef>(&json).unwrap(), located);
        assert_eq!(
            change_key("s", ChangeKind::OpIntegrated, &[plain]),
            change_key("s", ChangeKind::OpIntegrated, &[located])
        );
    }

    #[test]
    fn subscribe_request_rejects_unknown_fields() {
        let r = serde_json::from_str::<SubscribeRequest>(r#"{"spaces":["a"],"agent_key":"x"}"#);
        assert!(r.is_err());
    }
}
