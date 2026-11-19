//! HTTP credentials. Entirely separate from Holochain agent keys: the relay
//! never sees, stores or derives an agent's signing key.

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use rand::RngCore;
use sha2::{Digest, Sha256};

/// Prefix of subscription bearer tokens, to make leaked tokens greppable.
pub const TOKEN_PREFIX: &str = "hcp_";

/// Generate a new random 256-bit subscription token.
pub fn new_token() -> String {
    let mut buf = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut buf);
    format!("{TOKEN_PREFIX}{}", URL_SAFE_NO_PAD.encode(buf))
}

/// Generate a new subscription id.
pub fn new_subscription_id() -> String {
    let mut buf = [0u8; 12];
    rand::thread_rng().fill_bytes(&mut buf);
    format!("sub_{}", hex::encode(buf))
}

/// Hash a secret for storage / lookup. Tokens have 256 bits of entropy, so an
/// unsalted SHA-256 is sufficient (no dictionary to attack).
pub fn hash_secret(secret: &str) -> String {
    hex::encode(Sha256::digest(secret.as_bytes()))
}

/// Extract the bearer credential from an `Authorization` header value.
pub fn parse_bearer(value: &str) -> Option<&str> {
    let (scheme, rest) = value.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("bearer") {
        return None;
    }
    let token = rest.trim();
    if token.is_empty() || token.len() > 256 || !token.is_ascii() {
        return None;
    }
    Some(token)
}

/// Operator-issued registration keys, stored hashed.
#[derive(Debug, Clone, Default)]
pub struct RegistrationKeys {
    hashes: Vec<[u8; 32]>,
}

impl RegistrationKeys {
    pub fn new<I, S>(keys: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        Self {
            hashes: keys
                .into_iter()
                .map(|k| k.as_ref().trim().to_string())
                .filter(|k| !k.is_empty())
                .map(|k| Sha256::digest(k.as_bytes()).into())
                .collect(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.hashes.is_empty()
    }

    /// Constant-time (with respect to key contents) membership check.
    pub fn verify(&self, presented: &str) -> bool {
        let h: [u8; 32] = Sha256::digest(presented.as_bytes()).into();
        let mut ok = 0u8;
        for k in &self.hashes {
            let mut diff = 0u8;
            for (a, b) in k.iter().zip(h.iter()) {
                diff |= a ^ b;
            }
            ok |= (diff == 0) as u8;
        }
        ok == 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokens_are_unique_and_prefixed() {
        let a = new_token();
        let b = new_token();
        assert_ne!(a, b);
        assert!(a.starts_with(TOKEN_PREFIX));
        assert_eq!(a.len(), TOKEN_PREFIX.len() + 43);
        assert_ne!(hash_secret(&a), hash_secret(&b));
    }

    #[test]
    fn bearer_parsing() {
        assert_eq!(parse_bearer("Bearer abc"), Some("abc"));
        assert_eq!(parse_bearer("bearer  abc "), Some("abc"));
        assert_eq!(parse_bearer("Basic abc"), None);
        assert_eq!(parse_bearer("Bearer "), None);
        assert_eq!(parse_bearer("Bearer"), None);
    }

    #[test]
    fn registration_keys() {
        let k = RegistrationKeys::new(["alpha", " beta ", ""]);
        assert!(k.verify("alpha"));
        assert!(k.verify("beta"));
        assert!(!k.verify("gamma"));
        assert!(!k.verify(""));
        assert!(RegistrationKeys::new(Vec::<String>::new()).is_empty());
    }
}
