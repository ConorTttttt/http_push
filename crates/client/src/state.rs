//! Persistent client state: per-relay subscriptions and cursors, plus a
//! bounded set of already-delivered change keys for cross-relay dedup.

use hc_push_protocol::Cursor;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};

pub const STATE_VERSION: u32 = 1;

/// Everything the client remembers about one relay. Cursors are never shared
/// between relays: each relay's journal has its own sequence space.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelayState {
    pub url: String,
    pub subscription_id: String,
    /// Relay bearer token. Not a Holochain key. On mobile, keep this in the
    /// platform keystore if the state file is not otherwise protected.
    pub token: String,
    pub journal_id: String,
    pub spaces: Vec<String>,
    /// Last cursor fully processed from this relay.
    pub cursor: Option<Cursor>,
    #[serde(default)]
    pub last_success_ms: Option<u64>,
    #[serde(default)]
    pub consecutive_failures: u32,
}

/// Bounded FIFO set of change keys already handed to the application.
#[derive(Debug, Clone, Default)]
pub struct SeenSet {
    cap: usize,
    order: VecDeque<String>,
    set: HashSet<String>,
}

impl SeenSet {
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            cap: cap.max(1),
            order: VecDeque::new(),
            set: HashSet::new(),
        }
    }

    pub fn contains(&self, key: &str) -> bool {
        self.set.contains(key)
    }

    /// Insert; returns false if already present.
    pub fn insert(&mut self, key: &str) -> bool {
        if self.set.contains(key) {
            return false;
        }
        self.order.push_back(key.to_string());
        self.set.insert(key.to_string());
        while self.order.len() > self.cap {
            if let Some(old) = self.order.pop_front() {
                self.set.remove(&old);
            }
        }
        true
    }

    pub fn len(&self) -> usize {
        self.order.len()
    }

    pub fn is_empty(&self) -> bool {
        self.order.is_empty()
    }

    fn set_capacity(&mut self, cap: usize) {
        self.cap = cap.max(1);
        while self.order.len() > self.cap {
            if let Some(old) = self.order.pop_front() {
                self.set.remove(&old);
            }
        }
    }
}

impl Serialize for SeenSet {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        self.order.serialize(s)
    }
}

impl<'de> Deserialize<'de> for SeenSet {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let order: VecDeque<String> = VecDeque::deserialize(d)?;
        let set = order.iter().cloned().collect();
        Ok(Self {
            cap: order.len().max(1),
            order,
            set,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientState {
    pub version: u32,
    pub relays: BTreeMap<String, RelayState>,
    pub seen: SeenSet,
}

impl ClientState {
    pub fn new(seen_capacity: usize) -> Self {
        Self {
            version: STATE_VERSION,
            relays: BTreeMap::new(),
            seen: SeenSet::with_capacity(seen_capacity),
        }
    }

    pub(crate) fn fix_capacity(&mut self, cap: usize) {
        self.seen.set_capacity(cap);
    }
}

/// Where client state lives. Mobile apps implement this over their sandbox
/// storage or keystore.
pub trait StateStore: Send + Sync {
    fn load(&self) -> anyhow::Result<Option<ClientState>>;
    fn save(&self, state: &ClientState) -> anyhow::Result<()>;
}

/// JSON file with atomic replace (write temp file, fsync, rename).
#[derive(Debug, Clone)]
pub struct FileStateStore {
    path: PathBuf,
}

impl FileStateStore {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl StateStore for FileStateStore {
    fn load(&self) -> anyhow::Result<Option<ClientState>> {
        match std::fs::read(&self.path) {
            Ok(bytes) => {
                let st: ClientState = serde_json::from_slice(&bytes)?;
                if st.version != STATE_VERSION {
                    anyhow::bail!("unsupported client state version {}", st.version);
                }
                Ok(Some(st))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    fn save(&self, state: &ClientState) -> anyhow::Result<()> {
        use std::io::Write;
        if let Some(dir) = self.path.parent() {
            if !dir.as_os_str().is_empty() {
                std::fs::create_dir_all(dir)?;
            }
        }
        let tmp = self.path.with_extension("tmp");
        {
            let mut f = std::fs::File::create(&tmp)?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                f.set_permissions(std::fs::Permissions::from_mode(0o600))?;
            }
            f.write_all(&serde_json::to_vec_pretty(state)?)?;
            f.sync_all()?;
        }
        std::fs::rename(&tmp, &self.path)?;
        Ok(())
    }
}

/// In-memory store (tests, or apps that persist state themselves).
#[derive(Debug, Default)]
pub struct MemoryStateStore(std::sync::Mutex<Option<ClientState>>);

impl StateStore for MemoryStateStore {
    fn load(&self) -> anyhow::Result<Option<ClientState>> {
        Ok(self.0.lock().unwrap_or_else(|e| e.into_inner()).clone())
    }

    fn save(&self, state: &ClientState) -> anyhow::Result<()> {
        *self.0.lock().unwrap_or_else(|e| e.into_inner()) = Some(state.clone());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seen_set_is_bounded_fifo() {
        let mut s = SeenSet::with_capacity(2);
        assert!(s.insert("a"));
        assert!(!s.insert("a"));
        assert!(s.insert("b"));
        assert!(s.insert("c"));
        assert!(!s.contains("a"));
        assert!(s.contains("b") && s.contains("c"));
        assert_eq!(s.len(), 2);
    }

    #[test]
    fn file_store_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileStateStore::new(dir.path().join("sub/state.json"));
        assert!(store.load().unwrap().is_none());
        let mut st = ClientState::new(10);
        st.seen.insert("k1");
        st.relays.insert(
            "a".into(),
            RelayState {
                url: "http://x".into(),
                subscription_id: "sub_1".into(),
                token: "hcp_x".into(),
                journal_id: "ab".into(),
                spaces: vec!["s".into()],
                cursor: Some(Cursor::new("ab", 3)),
                last_success_ms: None,
                consecutive_failures: 0,
            },
        );
        store.save(&st).unwrap();
        let back = store.load().unwrap().unwrap();
        assert!(back.seen.contains("k1"));
        assert_eq!(back.relays["a"].cursor, Some(Cursor::new("ab", 3)));
    }
}
