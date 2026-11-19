//! Small in-memory token-bucket rate limiter with a bounded key set.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

#[derive(Debug)]
struct Bucket {
    tokens: f64,
    last: Instant,
}

#[derive(Debug)]
pub struct RateLimiter {
    rate_per_sec: f64,
    burst: f64,
    max_keys: usize,
    buckets: Mutex<HashMap<String, Bucket>>,
}

impl RateLimiter {
    /// `per_minute` sustained requests with bursts up to `burst`.
    pub fn new(per_minute: u32, burst: u32, max_keys: usize) -> Self {
        Self {
            rate_per_sec: f64::from(per_minute.max(1)) / 60.0,
            burst: f64::from(burst.max(1)),
            max_keys: max_keys.max(16),
            buckets: Mutex::new(HashMap::new()),
        }
    }

    /// Take one token for `key`. On refusal returns how long to wait.
    pub fn check(&self, key: &str) -> Result<(), Duration> {
        self.check_at(key, Instant::now())
    }

    fn check_at(&self, key: &str, now: Instant) -> Result<(), Duration> {
        let mut map = self.buckets.lock().unwrap_or_else(|e| e.into_inner());
        if !map.contains_key(key) && map.len() >= self.max_keys {
            // Bound memory: drop buckets that have fully refilled (idle keys).
            let (rate, burst) = (self.rate_per_sec, self.burst);
            map.retain(|_, b| b.tokens + now.duration_since(b.last).as_secs_f64() * rate < burst);
            if map.len() >= self.max_keys {
                return Err(Duration::from_secs(1));
            }
        }
        let b = map.entry(key.to_string()).or_insert(Bucket {
            tokens: self.burst,
            last: now,
        });
        let elapsed = now.duration_since(b.last).as_secs_f64();
        b.tokens = (b.tokens + elapsed * self.rate_per_sec).min(self.burst);
        b.last = now;
        if b.tokens >= 1.0 {
            b.tokens -= 1.0;
            Ok(())
        } else {
            let wait = (1.0 - b.tokens) / self.rate_per_sec;
            Err(Duration::from_secs_f64(wait.max(0.001)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn burst_then_refill() {
        let rl = RateLimiter::new(60, 3, 100);
        let t0 = Instant::now();
        for _ in 0..3 {
            assert!(rl.check_at("k", t0).is_ok());
        }
        let wait = rl.check_at("k", t0).unwrap_err();
        assert!(wait <= Duration::from_secs(1));
        assert!(rl.check_at("other", t0).is_ok());
        assert!(rl.check_at("k", t0 + Duration::from_millis(1100)).is_ok());
    }

    #[test]
    fn key_set_is_bounded() {
        let rl = RateLimiter::new(60, 1, 16);
        let t0 = Instant::now();
        for i in 0..16 {
            assert!(rl.check_at(&format!("k{i}"), t0).is_ok());
        }
        // All 16 buckets are drained, so a new key is refused rather than
        // growing the map.
        assert!(rl.check_at("new", t0).is_err());
        // Once buckets refill they are evicted and new keys are accepted.
        assert!(rl.check_at("new", t0 + Duration::from_secs(5)).is_ok());
    }
}
