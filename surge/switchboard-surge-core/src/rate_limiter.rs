use std::sync::Arc;
use std::time::{Duration, Instant};
use dashmap::DashMap;

/// Rate limiter for ticker processing to prevent CPU overload
pub struct TickerRateLimiter {
    /// Last sent time for each symbol
    last_sent: Arc<DashMap<String, Instant>>,
    /// Minimum interval between updates for the same symbol
    min_interval: Duration,
}

impl TickerRateLimiter {
    /// Create a new rate limiter with specified minimum interval in milliseconds
    pub fn new(min_interval_ms: u64) -> Self {
        Self {
            last_sent: Arc::new(DashMap::new()),
            min_interval: Duration::from_millis(min_interval_ms),
        }
    }

    /// Check if we should send this ticker based on rate limiting
    /// Returns true if enough time has passed since last update for this symbol
    pub fn should_send(&self, symbol: &str) -> bool {
        let now = Instant::now();
        
        // Try to get existing entry
        if let Some(mut entry) = self.last_sent.get_mut(symbol) {
            let last_time = *entry;
            if now.duration_since(last_time) >= self.min_interval {
                *entry = now;
                true
            } else {
                false
            }
        } else {
            // New symbol - add it
            self.last_sent.insert(symbol.to_string(), now);
            true
        }
    }
    
    /// Clean up old entries to prevent memory leak
    pub async fn cleanup(&self, max_age: Duration) {
        let now = Instant::now();
        
        // DashMap's retain is more efficient than mutex version
        self.last_sent.retain(|_, last_time| {
            now.duration_since(*last_time) < max_age
        });
    }
    
    /// Get number of tracked symbols
    pub fn symbol_count(&self) -> usize {
        self.last_sent.len()
    }
    
    /// Clear all entries
    pub fn clear(&self) {
        self.last_sent.clear();
    }
}

/// API rate limiter for exchange requests
pub struct ApiRateLimiter {
    /// Request timestamps for each endpoint
    requests: Arc<DashMap<String, Vec<Instant>>>,
    /// Maximum requests per window
    max_requests: usize,
    /// Time window for rate limiting
    window: Duration,
}

impl ApiRateLimiter {
    /// Create a new API rate limiter
    pub fn new(max_requests: usize, window_seconds: u64) -> Self {
        Self {
            requests: Arc::new(DashMap::new()),
            max_requests,
            window: Duration::from_secs(window_seconds),
        }
    }
    
    /// Check if request is allowed for the given endpoint
    pub fn is_allowed(&self, endpoint: &str) -> bool {
        let now = Instant::now();
        
        let mut entry = self.requests.entry(endpoint.to_string()).or_default();
        let timestamps = entry.value_mut();
        
        // Remove old timestamps outside the window
        timestamps.retain(|&timestamp| now.duration_since(timestamp) < self.window);
        
        // Check if we're under the limit
        if timestamps.len() < self.max_requests {
            timestamps.push(now);
            true
        } else {
            false
        }
    }
    
    /// Get delay until next request is allowed
    pub fn delay_until_allowed(&self, endpoint: &str) -> Option<Duration> {
        let now = Instant::now();
        
        if let Some(entry) = self.requests.get(endpoint) {
            let timestamps = entry.value();
            if timestamps.len() >= self.max_requests {
                // Find the oldest timestamp
                if let Some(&oldest) = timestamps.first() {
                    let window_start = oldest + self.window;
                    if window_start > now {
                        return Some(window_start - now);
                    }
                }
            }
        }
        
        None
    }
    
    /// Clean up old entries
    pub async fn cleanup(&self) {
        let now = Instant::now();
        
        self.requests.retain(|_, timestamps| {
            timestamps.retain(|&timestamp| now.duration_since(timestamp) < self.window);
            !timestamps.is_empty()
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::sleep;

    #[tokio::test]
    async fn test_ticker_rate_limiter() {
        let limiter = TickerRateLimiter::new(100); // 100ms interval
        
        // First call should be allowed
        assert!(limiter.should_send("BTCUSDT"));
        
        // Immediate second call should be denied
        assert!(!limiter.should_send("BTCUSDT"));
        
        // Wait and try again
        sleep(Duration::from_millis(110)).await;
        assert!(limiter.should_send("BTCUSDT"));
    }
    
    #[tokio::test]
    async fn test_api_rate_limiter() {
        let limiter = ApiRateLimiter::new(2, 1); // 2 requests per second
        
        assert!(limiter.is_allowed("test_endpoint"));
        assert!(limiter.is_allowed("test_endpoint"));
        assert!(!limiter.is_allowed("test_endpoint")); // Third should be denied
        
        // Wait and try again
        sleep(Duration::from_secs(1)).await;
        assert!(limiter.is_allowed("test_endpoint"));
    }
}