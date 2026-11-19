//! Persistent, bounded event journal backed by SQLite.
//!
//! * Every accepted change gets a monotonically increasing sequence number
//!   (`AUTOINCREMENT`, so numbers are never reused after pruning).
//! * Changes are de-duplicated by their relay-independent `change_key`.
//! * Retention is bounded by event count and age. Pruning always removes a
//!   *prefix* of the sequence, recorded as `pruned_through`, so a cursor is
//!   either fully servable or explicitly expired, never silently gappy.
//! * Source checkpoints are written in the same transaction as the events
//!   they cover.
//! * Subscriptions are stored with a SHA-256 hash of their bearer token only.

use hc_push_protocol::{
    change_key, ChangeKind, ChangeRef, Cursor, EventEnvelope, PROTOCOL_VERSION,
};
use hc_push_source_api::ObservedChange;
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use std::path::Path;
use std::sync::Mutex;
use std::time::Duration;

/// v2 adds `events.occurred_at_ms` (v1 journals are migrated in place).
const SCHEMA_VERSION: i64 = 2;

/// Retention policy.
#[derive(Debug, Clone, Copy)]
pub struct Retention {
    pub max_events: u64,
    pub max_age: Duration,
}

impl Default for Retention {
    fn default() -> Self {
        Self {
            max_events: 100_000,
            max_age: Duration::from_secs(7 * 24 * 3600),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum JournalError {
    #[error("cursor expired; earliest servable seq is {earliest}")]
    CursorExpired { earliest: u64 },
    #[error("cursor is ahead of this journal's head")]
    CursorAhead { earliest: u64 },
    #[error("subscription capacity reached")]
    Capacity,
    #[error(transparent)]
    Db(#[from] rusqlite::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

pub type JResult<T> = Result<T, JournalError>;

/// Sequence bounds of the journal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Bounds {
    /// Highest assigned sequence number (0 if empty).
    pub head: u64,
    /// Every event with `seq <= pruned_through` has been pruned.
    pub pruned_through: u64,
}

/// Result of [`Journal::read_after`].
#[derive(Debug, Clone)]
pub struct ReadResult {
    pub events: Vec<EventEnvelope>,
    pub has_more: bool,
    /// Sequence to resume from.
    pub next_seq: u64,
    pub head: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredSubscription {
    pub id: String,
    pub spaces: Vec<String>,
    pub label: Option<String>,
    pub created_at_ms: u64,
    pub last_seen_at_ms: u64,
}

pub struct Journal {
    conn: Mutex<Connection>,
    journal_id: String,
    relay_id: String,
    retention: Retention,
}

impl std::fmt::Debug for Journal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Journal")
            .field("journal_id", &self.journal_id)
            .field("relay_id", &self.relay_id)
            .finish()
    }
}

fn random_hex(bytes: usize) -> String {
    let mut buf = vec![0u8; bytes];
    rand::Rng::fill(&mut rand::thread_rng(), buf.as_mut_slice());
    hex::encode(buf)
}

impl Journal {
    /// Open (or create) the journal at `path`.
    pub fn open(path: &Path, retention: Retention) -> JResult<Self> {
        let conn = Connection::open(path)?;
        Self::init(conn, retention)
    }

    /// In-memory journal for tests.
    pub fn open_in_memory(retention: Retention) -> JResult<Self> {
        Self::init(Connection::open_in_memory()?, retention)
    }

    fn init(conn: Connection, retention: Retention) -> JResult<Self> {
        conn.busy_timeout(Duration::from_secs(5))?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS meta (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS events (
                seq            INTEGER PRIMARY KEY AUTOINCREMENT,
                change_key     TEXT NOT NULL UNIQUE,
                space          TEXT NOT NULL,
                kind           TEXT NOT NULL,
                refs           TEXT NOT NULL,
                source         TEXT NOT NULL,
                observed_at_ms INTEGER NOT NULL,
                occurred_at_ms INTEGER
            );
            CREATE INDEX IF NOT EXISTS events_space_seq ON events(space, seq);
            CREATE INDEX IF NOT EXISTS events_observed ON events(observed_at_ms);
            CREATE TABLE IF NOT EXISTS source_checkpoints (
                source_id  TEXT PRIMARY KEY,
                checkpoint TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS subscriptions (
                id              TEXT PRIMARY KEY,
                token_hash      TEXT NOT NULL UNIQUE,
                spaces          TEXT NOT NULL,
                label           TEXT,
                created_at_ms   INTEGER NOT NULL,
                last_seen_at_ms INTEGER NOT NULL
            );
            "#,
        )?;

        let get = |conn: &Connection, k: &str| -> rusqlite::Result<Option<String>> {
            conn.query_row("SELECT value FROM meta WHERE key = ?1", [k], |r| r.get(0))
                .optional()
        };
        let put = |conn: &Connection, k: &str, v: &str| -> rusqlite::Result<()> {
            conn.execute(
                "INSERT INTO meta(key, value) VALUES (?1, ?2) ON CONFLICT(key) DO NOTHING",
                params![k, v],
            )
            .map(|_| ())
        };

        match get(&conn, "schema_version")? {
            None => put(&conn, "schema_version", &SCHEMA_VERSION.to_string())?,
            Some(v) if v == SCHEMA_VERSION.to_string() => {}
            Some(v) if v == "1" => {
                // v1 -> v2: nullable column, existing rows keep NULL.
                conn.execute_batch(
                    "BEGIN;
                     ALTER TABLE events ADD COLUMN occurred_at_ms INTEGER;
                     UPDATE meta SET value = '2' WHERE key = 'schema_version';
                     COMMIT;",
                )?;
            }
            Some(v) => {
                return Err(JournalError::Db(rusqlite::Error::InvalidParameterName(
                    format!("unsupported journal schema version {v}"),
                )))
            }
        }
        put(&conn, "journal_id", &random_hex(8))?;
        put(&conn, "relay_id", &random_hex(8))?;
        put(&conn, "pruned_through", "0")?;
        let journal_id = get(&conn, "journal_id")?.expect("just inserted");
        let relay_id = get(&conn, "relay_id")?.expect("just inserted");

        Ok(Self {
            conn: Mutex::new(conn),
            journal_id,
            relay_id,
            retention,
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Connection> {
        self.conn.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn journal_id(&self) -> &str {
        &self.journal_id
    }

    pub fn relay_id(&self) -> &str {
        &self.relay_id
    }

    pub fn retention(&self) -> Retention {
        self.retention
    }

    pub fn cursor(&self, seq: u64) -> Cursor {
        Cursor::new(self.journal_id.clone(), seq)
    }

    fn bounds_locked(conn: &Connection) -> JResult<Bounds> {
        let head: i64 = conn
            .query_row(
                "SELECT COALESCE((SELECT seq FROM sqlite_sequence WHERE name = 'events'), 0)",
                [],
                |r| r.get(0),
            )
            .optional()?
            .unwrap_or(0);
        let pruned: String = conn.query_row(
            "SELECT value FROM meta WHERE key = 'pruned_through'",
            [],
            |r| r.get(0),
        )?;
        Ok(Bounds {
            head: head.max(0) as u64,
            pruned_through: pruned.parse().unwrap_or(0),
        })
    }

    pub fn bounds(&self) -> JResult<Bounds> {
        Self::bounds_locked(&self.lock())
    }

    /// Append changes from `source_id`, skipping duplicates, and store the
    /// source checkpoint in the same transaction. Returns the new envelopes.
    pub fn append(
        &self,
        source_id: &str,
        changes: &[ObservedChange],
        checkpoint: Option<&str>,
        now_ms: u64,
    ) -> JResult<Vec<EventEnvelope>> {
        let mut conn = self.lock();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let mut out = Vec::new();
        {
            // Check first rather than `INSERT OR IGNORE`: an ignored insert
            // still advances the AUTOINCREMENT counter, leaving gaps.
            let mut exists = tx.prepare_cached("SELECT 1 FROM events WHERE change_key = ?1")?;
            let mut insert = tx.prepare_cached(
                "INSERT INTO events(change_key, space, kind, refs, source, observed_at_ms, occurred_at_ms)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            )?;
            for c in changes {
                let key = change_key(&c.space, c.kind, &c.refs);
                if exists.exists([&key])? {
                    continue;
                }
                let refs = serde_json::to_string(&c.refs)?;
                let n = insert.execute(params![
                    key,
                    c.space,
                    c.kind.as_str(),
                    refs,
                    source_id,
                    now_ms as i64,
                    c.occurred_at_ms.map(|t| t as i64)
                ])?;
                if n == 1 {
                    let seq = tx.last_insert_rowid() as u64;
                    out.push(self.envelope(
                        seq,
                        key,
                        c.space.clone(),
                        c.kind,
                        c.refs.clone(),
                        (now_ms, c.occurred_at_ms),
                        source_id.to_string(),
                    ));
                }
            }
        }
        if let Some(cp) = checkpoint {
            tx.execute(
                "INSERT INTO source_checkpoints(source_id, checkpoint) VALUES (?1, ?2)
                 ON CONFLICT(source_id) DO UPDATE SET checkpoint = excluded.checkpoint",
                params![source_id, cp],
            )?;
        }
        tx.commit()?;
        Ok(out)
    }

    #[allow(clippy::too_many_arguments)]
    fn envelope(
        &self,
        seq: u64,
        change_key: String,
        space: String,
        kind: ChangeKind,
        refs: Vec<ChangeRef>,
        (observed_at_ms, occurred_at_ms): (u64, Option<u64>),
        source: String,
    ) -> EventEnvelope {
        EventEnvelope {
            v: PROTOCOL_VERSION,
            event_id: format!("{}-{}", self.journal_id, seq),
            cursor: self.cursor(seq),
            space,
            kind,
            change_key,
            refs,
            observed_at_ms,
            occurred_at_ms,
            source,
        }
    }

    pub fn checkpoint(&self, source_id: &str) -> JResult<Option<String>> {
        Ok(self
            .lock()
            .query_row(
                "SELECT checkpoint FROM source_checkpoints WHERE source_id = ?1",
                [source_id],
                |r| r.get(0),
            )
            .optional()?)
    }

    /// Read up to `limit` events in `spaces` with `seq > after`.
    pub fn read_after(&self, after: u64, spaces: &[String], limit: u32) -> JResult<ReadResult> {
        let conn = self.lock();
        let bounds = Self::bounds_locked(&conn)?;
        if after < bounds.pruned_through {
            return Err(JournalError::CursorExpired {
                earliest: bounds.pruned_through,
            });
        }
        if after > bounds.head {
            return Err(JournalError::CursorAhead {
                earliest: bounds.pruned_through,
            });
        }
        let spaces_json = serde_json::to_string(spaces)?;
        let mut stmt = conn.prepare_cached(
            "SELECT seq, change_key, space, kind, refs, source, observed_at_ms, occurred_at_ms FROM events
             WHERE seq > ?1 AND space IN (SELECT value FROM json_each(?2))
             ORDER BY seq ASC LIMIT ?3",
        )?;
        let rows = stmt.query_map(
            params![after as i64, spaces_json, i64::from(limit) + 1],
            |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, String>(3)?,
                    r.get::<_, String>(4)?,
                    r.get::<_, String>(5)?,
                    r.get::<_, i64>(6)?,
                    r.get::<_, Option<i64>>(7)?,
                ))
            },
        )?;
        let mut events = Vec::new();
        for row in rows {
            let (seq, key, space, kind, refs, source, observed, occurred) = row?;
            let refs: Vec<ChangeRef> = serde_json::from_str(&refs)?;
            events.push(self.envelope(
                seq as u64,
                key,
                space,
                kind.parse().unwrap_or(ChangeKind::Unknown),
                refs,
                (observed.max(0) as u64, occurred.map(|t| t.max(0) as u64)),
                source,
            ));
        }
        let has_more = events.len() > limit as usize;
        if has_more {
            events.truncate(limit as usize);
        }
        let next_seq = if has_more {
            events.last().map(|e| e.seq()).unwrap_or(after)
        } else {
            bounds.head
        };
        Ok(ReadResult {
            events,
            has_more,
            next_seq,
            head: bounds.head,
        })
    }

    /// Apply the retention policy. Returns the number of pruned events.
    pub fn prune(&self, now_ms: u64) -> JResult<u64> {
        let mut conn = self.lock();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let bounds = Self::bounds_locked(&tx)?;
        let by_count = bounds.head.saturating_sub(self.retention.max_events);
        let age_cutoff = now_ms.saturating_sub(self.retention.max_age.as_millis() as u64);
        let by_age: i64 = tx.query_row(
            "SELECT COALESCE(MAX(seq), 0) FROM events WHERE observed_at_ms < ?1",
            [age_cutoff as i64],
            |r| r.get(0),
        )?;
        let through = by_count.max(by_age.max(0) as u64);
        if through <= bounds.pruned_through {
            return Ok(0);
        }
        let n = tx.execute("DELETE FROM events WHERE seq <= ?1", [through as i64])?;
        tx.execute(
            "UPDATE meta SET value = ?1 WHERE key = 'pruned_through'",
            [through.to_string()],
        )?;
        tx.commit()?;
        Ok(n as u64)
    }

    pub fn event_count(&self) -> JResult<u64> {
        let n: i64 = self
            .lock()
            .query_row("SELECT COUNT(*) FROM events", [], |r| r.get(0))?;
        Ok(n.max(0) as u64)
    }

    // ----------------------------------------------------------------------
    // Subscriptions
    // ----------------------------------------------------------------------

    pub fn create_subscription(
        &self,
        id: &str,
        token_hash: &str,
        spaces: &[String],
        label: Option<&str>,
        now_ms: u64,
        max_subscriptions: u64,
    ) -> JResult<()> {
        let mut conn = self.lock();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let count: i64 = tx.query_row("SELECT COUNT(*) FROM subscriptions", [], |r| r.get(0))?;
        if count.max(0) as u64 >= max_subscriptions {
            return Err(JournalError::Capacity);
        }
        tx.execute(
            "INSERT INTO subscriptions(id, token_hash, spaces, label, created_at_ms, last_seen_at_ms)
             VALUES (?1, ?2, ?3, ?4, ?5, ?5)",
            params![id, token_hash, serde_json::to_string(spaces)?, label, now_ms as i64],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn subscription_by_token_hash(
        &self,
        token_hash: &str,
    ) -> JResult<Option<StoredSubscription>> {
        let conn = self.lock();
        let row = conn
            .query_row(
                "SELECT id, spaces, label, created_at_ms, last_seen_at_ms
                 FROM subscriptions WHERE token_hash = ?1",
                [token_hash],
                |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, Option<String>>(2)?,
                        r.get::<_, i64>(3)?,
                        r.get::<_, i64>(4)?,
                    ))
                },
            )
            .optional()?;
        let Some((id, spaces, label, created, seen)) = row else {
            return Ok(None);
        };
        Ok(Some(StoredSubscription {
            id,
            spaces: serde_json::from_str(&spaces)?,
            label,
            created_at_ms: created.max(0) as u64,
            last_seen_at_ms: seen.max(0) as u64,
        }))
    }

    pub fn touch_subscription(&self, id: &str, now_ms: u64) -> JResult<()> {
        self.lock().execute(
            "UPDATE subscriptions SET last_seen_at_ms = ?2 WHERE id = ?1",
            params![id, now_ms as i64],
        )?;
        Ok(())
    }

    pub fn delete_subscription(&self, id: &str) -> JResult<bool> {
        Ok(self
            .lock()
            .execute("DELETE FROM subscriptions WHERE id = ?1", [id])?
            == 1)
    }

    /// Remove subscriptions idle for longer than `idle_ttl`.
    pub fn prune_subscriptions(&self, now_ms: u64, idle_ttl: Duration) -> JResult<u64> {
        let cutoff = now_ms.saturating_sub(idle_ttl.as_millis() as u64);
        Ok(self.lock().execute(
            "DELETE FROM subscriptions WHERE last_seen_at_ms < ?1",
            [cutoff as i64],
        )? as u64)
    }

    pub fn subscription_count(&self) -> JResult<u64> {
        let n: i64 = self
            .lock()
            .query_row("SELECT COUNT(*) FROM subscriptions", [], |r| r.get(0))?;
        Ok(n.max(0) as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hc_push_protocol::RefType;

    fn change(space: &str, id: &str) -> ObservedChange {
        ObservedChange {
            space: space.into(),
            kind: ChangeKind::MockChange,
            refs: vec![ChangeRef::new(RefType::Mock, id)],
            occurred_at_ms: None,
        }
    }

    fn spaces(s: &[&str]) -> Vec<String> {
        s.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn append_assigns_monotonic_seq_and_dedups() {
        let j = Journal::open_in_memory(Retention::default()).unwrap();
        let a = j
            .append(
                "m",
                &[change("s1", "a"), change("s1", "b"), change("s1", "a")],
                None,
                1,
            )
            .unwrap();
        assert_eq!(a.iter().map(|e| e.seq()).collect::<Vec<_>>(), vec![1, 2]);
        // Re-submitting the same change later is still a duplicate.
        let b = j
            .append("m", &[change("s1", "b"), change("s1", "c")], None, 2)
            .unwrap();
        assert_eq!(b.len(), 1);
        assert_eq!(b[0].seq(), 3);
        assert_eq!(
            j.bounds().unwrap(),
            Bounds {
                head: 3,
                pruned_through: 0
            }
        );
    }

    #[test]
    fn read_after_pages_and_filters_by_space() {
        let j = Journal::open_in_memory(Retention::default()).unwrap();
        for i in 0..10 {
            let s = if i % 2 == 0 { "even" } else { "odd" };
            j.append("m", &[change(s, &i.to_string())], None, 1)
                .unwrap();
        }
        let r = j.read_after(0, &spaces(&["even"]), 3).unwrap();
        assert_eq!(
            r.events.iter().map(|e| e.seq()).collect::<Vec<_>>(),
            vec![1, 3, 5]
        );
        assert!(r.has_more);
        assert_eq!(r.next_seq, 5);
        let r = j.read_after(r.next_seq, &spaces(&["even"]), 3).unwrap();
        assert_eq!(
            r.events.iter().map(|e| e.seq()).collect::<Vec<_>>(),
            vec![7, 9]
        );
        assert!(!r.has_more);
        // Cursor advances to head even though seq 10 is in another space.
        assert_eq!(r.next_seq, 10);
        assert_eq!(r.head, 10);
    }

    #[test]
    fn retention_by_count_expires_old_cursors() {
        let j = Journal::open_in_memory(Retention {
            max_events: 3,
            max_age: Duration::from_secs(3600),
        })
        .unwrap();
        for i in 0..6 {
            j.append("m", &[change("s", &i.to_string())], None, 1000)
                .unwrap();
        }
        assert_eq!(j.prune(1000).unwrap(), 3);
        assert_eq!(
            j.bounds().unwrap(),
            Bounds {
                head: 6,
                pruned_through: 3
            }
        );
        assert!(matches!(
            j.read_after(2, &spaces(&["s"]), 10),
            Err(JournalError::CursorExpired { earliest: 3 })
        ));
        let r = j.read_after(3, &spaces(&["s"]), 10).unwrap();
        assert_eq!(r.events.len(), 3);
        assert!(matches!(
            j.read_after(7, &spaces(&["s"]), 10),
            Err(JournalError::CursorAhead { .. })
        ));
    }

    #[test]
    fn retention_by_age_prunes_a_prefix() {
        let j = Journal::open_in_memory(Retention {
            max_events: 1000,
            max_age: Duration::from_millis(100),
        })
        .unwrap();
        j.append(
            "m",
            &[change("s", "old1"), change("s", "old2")],
            None,
            1_000,
        )
        .unwrap();
        j.append("m", &[change("s", "new")], None, 5_000).unwrap();
        assert_eq!(j.prune(5_050).unwrap(), 2);
        assert_eq!(j.bounds().unwrap().pruned_through, 2);
        // Sequence numbers are never reused after pruning.
        let e = j.append("m", &[change("s", "newer")], None, 5_060).unwrap();
        assert_eq!(e[0].seq(), 4);
    }

    #[test]
    fn checkpoint_is_atomic_with_events() {
        let j = Journal::open_in_memory(Retention::default()).unwrap();
        assert_eq!(j.checkpoint("src").unwrap(), None);
        j.append("src", &[change("s", "x")], Some("cp1"), 1)
            .unwrap();
        assert_eq!(j.checkpoint("src").unwrap().as_deref(), Some("cp1"));
        j.append("src", &[], Some("cp2"), 1).unwrap();
        assert_eq!(j.checkpoint("src").unwrap().as_deref(), Some("cp2"));
        j.append("src", &[], None, 1).unwrap();
        assert_eq!(j.checkpoint("src").unwrap().as_deref(), Some("cp2"));
    }

    #[test]
    fn persists_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("j.sqlite3");
        let (jid, rid) = {
            let j = Journal::open(&path, Retention::default()).unwrap();
            j.append("m", &[change("s", "a"), change("s", "b")], Some("cp"), 1)
                .unwrap();
            j.create_subscription("sub1", "hash1", &spaces(&["s"]), Some("phone"), 1, 10)
                .unwrap();
            (j.journal_id().to_string(), j.relay_id().to_string())
        };
        let j = Journal::open(&path, Retention::default()).unwrap();
        assert_eq!(j.journal_id(), jid);
        assert_eq!(j.relay_id(), rid);
        assert_eq!(j.bounds().unwrap().head, 2);
        assert_eq!(j.checkpoint("m").unwrap().as_deref(), Some("cp"));
        let sub = j.subscription_by_token_hash("hash1").unwrap().unwrap();
        assert_eq!(sub.id, "sub1");
        assert_eq!(sub.label.as_deref(), Some("phone"));
        let e = j.append("m", &[change("s", "c")], None, 2).unwrap();
        assert_eq!(e[0].seq(), 3);
    }

    #[test]
    fn migrates_v1_journal_and_keeps_occurred_at() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v1.sqlite3");
        {
            // A journal as created by schema v1.
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch(
                "CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
                 CREATE TABLE events (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT, change_key TEXT NOT NULL UNIQUE,
                    space TEXT NOT NULL, kind TEXT NOT NULL, refs TEXT NOT NULL,
                    source TEXT NOT NULL, observed_at_ms INTEGER NOT NULL);
                 INSERT INTO meta VALUES ('schema_version','1'),('journal_id','abcd'),
                    ('relay_id','r'),('pruned_through','0');
                 INSERT INTO events(change_key, space, kind, refs, source, observed_at_ms)
                    VALUES ('k1','s','mock_change','[{\"type\":\"mock\",\"id\":\"old\"}]','mock',5);",
            )
            .unwrap();
        }
        let j = Journal::open(&path, Retention::default()).unwrap();
        assert_eq!(j.journal_id(), "abcd");
        let mut c = change("s", "new");
        c.occurred_at_ms = Some(42);
        j.append("m", &[c], None, 50).unwrap();
        let r = j.read_after(0, &spaces(&["s"]), 10).unwrap();
        assert_eq!(r.events.len(), 2);
        assert_eq!(r.events[0].occurred_at_ms, None);
        assert_eq!(r.events[1].occurred_at_ms, Some(42));
        drop(j);
        // Reopening a migrated journal is a no-op.
        Journal::open(&path, Retention::default()).unwrap();
    }

    #[test]
    fn subscription_capacity_and_idle_pruning() {
        let j = Journal::open_in_memory(Retention::default()).unwrap();
        j.create_subscription("a", "ha", &spaces(&["s"]), None, 0, 2)
            .unwrap();
        j.create_subscription("b", "hb", &spaces(&["s"]), None, 0, 2)
            .unwrap();
        assert!(matches!(
            j.create_subscription("c", "hc", &spaces(&["s"]), None, 0, 2),
            Err(JournalError::Capacity)
        ));
        j.touch_subscription("b", 10_000).unwrap();
        assert_eq!(
            j.prune_subscriptions(10_500, Duration::from_secs(1))
                .unwrap(),
            1
        );
        assert!(j.subscription_by_token_hash("ha").unwrap().is_none());
        assert!(j.delete_subscription("b").unwrap());
        assert!(!j.delete_subscription("b").unwrap());
    }
}
