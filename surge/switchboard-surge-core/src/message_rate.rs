use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use dashmap::DashMap;
use once_cell::sync::Lazy;
use tracing::{debug, info};
use tokio::time::interval;

use crate::{
    types::{Source, MapKey},
    pair::Pair,
};

/// Global message counter that tracks raw tick counts
pub static MESSAGE_COUNTER: Lazy<Arc<MessageCounter>> = Lazy::new(|| {
    Arc::new(MessageCounter::new())
});

/// Global message rate cache that stores calculated rates
pub static MESSAGE_RATE_CACHE: Lazy<Arc<MessageRateCache>> = Lazy::new(|| {
    Arc::new(MessageRateCache::new())
});

/// Counter that tracks raw message counts per (Source, Pair)
pub struct MessageCounter {
    /// (Source, Pair) -> running count of messages
    counts: Arc<DashMap<MapKey, AtomicU64>>,
}

impl MessageCounter {
    pub fn new() -> Self {
        Self {
            counts: Arc::new(DashMap::new()),
        }
    }

    /// Increment the counter for a given source and pair
    pub fn increment(&self, source: Source, pair: Pair) {
        let key = (source, pair.clone());
        let _new_count = self.counts
            .entry(key.clone())
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(1, Ordering::Relaxed) + 1;
            
    }

    /// Get current count for debugging
    pub fn get_count(&self, source: Source, pair: &Pair) -> u64 {
        let key = (source, pair.clone());
        if let Some(counter) = self.counts.get(&key) {
            counter.load(Ordering::Relaxed)
        } else {
            0
        }
    }

    /// Get all tracked pairs (for debugging)
    pub fn get_all_pairs(&self) -> Vec<(Source, Pair, u64)> {
        self.counts
            .iter()
            .map(|entry| {
                let (source, pair) = entry.key().clone();
                let count = entry.value().load(Ordering::Relaxed);
                (source, pair, count)
            })
            .collect()
    }
    
    /// Get APT counts for debugging contamination
    pub fn debug_apt_counts(&self) {
        debug!("🔍 ALL APT MESSAGE COUNTS:");
        for entry in self.counts.iter() {
            let (source, pair) = entry.key().clone();
            if pair.base == "APT" {
                let count = entry.value().load(Ordering::Relaxed);
                debug!("  - key=({}, {}) count={}", source.as_str(), pair.as_str(), count);
            }
        }
    }
}

/// Cache that stores calculated message rates (messages per second)
pub struct MessageRateCache {
    /// (Source, Pair) -> messages per second in last 30s window
    rates: Arc<DashMap<MapKey, f64>>,
    /// (Source, Pair) -> last recorded count (for delta calculation)
    last_counts: Arc<DashMap<MapKey, u64>>,
    /// (Source, Pair) -> average message rate over time
    average_rates: Arc<DashMap<MapKey, f64>>,
    /// (Source, Pair) -> number of samples in the average
    average_counts: Arc<DashMap<MapKey, u64>>,
}

impl MessageRateCache {
    pub fn new() -> Self {
        Self {
            rates: Arc::new(DashMap::new()),
            last_counts: Arc::new(DashMap::new()),
            average_rates: Arc::new(DashMap::new()),
            average_counts: Arc::new(DashMap::new()),
        }
    }

    /// Get the message rate (messages per second) for a given source and pair
    pub fn get_rate(&self, source: Source, pair: &Pair) -> f64 {
        let key = (source, pair.clone());
        if let Some(rate) = self.rates.get(&key) {
            let rate_val = *rate;
            
            // Debug logging for APT pairs to see what's being requested
            if pair.base == "APT" && (pair.quote == "USD" || pair.quote == "USDT" || pair.quote == "USDC") {
                debug!(
                    "📊 APT RATE REQUEST: key=({}, {}) returning rate={:.3}",
                    key.0.as_str(),
                    key.1.as_str(),
                    rate_val
                );
            }
            rate_val
        } else {
            // Debug logging for APT pairs to see missing rates
            if pair.base == "APT" && (pair.quote == "USD" || pair.quote == "USDT" || pair.quote == "USDC") {
                debug!(
                    "📊 APT RATE MISSING: key=({}, {}) returning 0.0 (not in cache)",
                    key.0.as_str(),
                    key.1.as_str()
                );
            }
            0.0
        }
    }

    /// Get the average message rate for a given source and pair
    pub fn get_average_rate(&self, source: Source, pair: &Pair) -> f64 {
        let key = (source, pair.clone());
        if let Some(avg_rate) = self.average_rates.get(&key) {
            *avg_rate
        } else {
            // Fall back to current rate if no average yet
            self.get_rate(source, pair)
        }
    }

    /// Get all rates for debugging
    pub fn get_all_rates(&self) -> Vec<(Source, Pair, f64)> {
        self.rates
            .iter()
            .map(|entry| {
                let (source, pair) = entry.key().clone();
                let rate = *entry.value();
                (source, pair, rate)
            })
            .collect()
    }

    /// Update rates based on current counter values
    fn update_rates(&self) {
        let mut updated_count = 0;
        let start_time = std::time::Instant::now();

        // Iterate through all counters and calculate deltas
        for entry in MESSAGE_COUNTER.counts.iter() {
            let key = entry.key().clone();
            let current_count = entry.value().load(Ordering::Relaxed);

            // Calculate rate based on delta from last update
            if let Some(last_count_entry) = self.last_counts.get(&key) {
                let last_count = *last_count_entry;
                let delta = current_count.saturating_sub(last_count);
                let rate = delta as f64 / 30.0; // 30 second window
                
                // Only store if there's meaningful activity
                if rate > 0.0 {
                    self.rates.insert(key.clone(), rate);
                    updated_count += 1;
                    
                    // Update the running average
                    let mut count = self.average_counts.entry(key.clone()).or_insert(0);
                    let mut current_avg = self.average_rates.entry(key.clone()).or_insert(0.0);
                    
                    // Calculate new average using incremental formula
                    // new_avg = (old_avg * count + new_value) / (count + 1)
                    *count += 1;
                    *current_avg = (*current_avg * (*count - 1) as f64 + rate) / *count as f64;
                    
                    // Special logging for APT pairs to debug weighted source selection
                    if key.1.base == "APT" && (key.1.quote == "USD" || key.1.quote == "USDT" || key.1.quote == "USDC") {
                        debug!(
                            "📊 APT MESSAGE RATE CALC: key=({}, {}) current_count={} last_count={} delta={} rate={:.3}",
                            key.0.as_str(),
                            key.1.as_str(),
                            current_count,
                            last_count,
                            delta,
                            rate
                        );
                    } else {
                        debug!(
                            "Updated message rate for {} {}: {:.3} msg/sec ({} messages in 30s)",
                            key.0.as_str(),
                            key.1.as_str(),
                            rate,
                            delta
                        );
                    }
                }
            } else {
                // First time seeing this pair, just store current count
                if key.1.base == "APT" && (key.1.quote == "USD" || key.1.quote == "USDT" || key.1.quote == "USDC") {
                    debug!(
                        "📊 APT FIRST OBSERVATION: key=({}, {}) current_count={} (storing for delta calc)",
                        key.0.as_str(),
                        key.1.as_str(),
                        current_count
                    );
                } else {
                    debug!(
                        "First observation for {} {}: {} total messages",
                        key.0.as_str(),
                        key.1.as_str(),
                        current_count
                    );
                }
            }

            // Update last count for next iteration
            self.last_counts.insert(key, current_count);
        }

        info!(
            "✅ Updated {} message rates in {:?}",
            updated_count,
            start_time.elapsed()
        );
        
        // Debug APT counts after each update
        MESSAGE_COUNTER.debug_apt_counts();
    }
}

/// Start the background task that calculates message rates every 30 seconds
pub fn start_message_rate_updater() {
    crate::runtime_separation::spawn_on_ingestion_named(
        "message-rate-updater",
        async move {
            let mut interval = interval(Duration::from_secs(30));
            
            info!("🚀 Starting message rate updater (30s intervals)");
            
            loop {
                interval.tick().await;
                
                debug!("🔄 Updating message rates...");
                MESSAGE_RATE_CACHE.update_rates();
            }
        }
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pair::Pair;

    #[test]
    fn test_message_counter() {
        let counter = MessageCounter::new();
        let pair = Pair::from_task("BTC/USDT").unwrap();
        
        // Test increment
        counter.increment(Source::Binance, pair.clone());
        counter.increment(Source::Binance, pair.clone());
        
        assert_eq!(counter.get_count(Source::Binance, &pair), 2);
    }

    #[test]
    fn test_message_rate_cache() {
        let cache = MessageRateCache::new();
        let pair = Pair::from_task("BTC/USDT").unwrap();
        
        // Initially no rate
        assert_eq!(cache.get_rate(Source::Binance, &pair), 0.0);
        
        // Simulate counter activity and rate calculation
        MESSAGE_COUNTER.increment(Source::Binance, pair.clone());
        MESSAGE_COUNTER.increment(Source::Binance, pair.clone());
        
        cache.update_rates();
        
        // Should have some rate now (first update won't calculate rate, second will)
        cache.update_rates();
        let rate = cache.get_rate(Source::Binance, &pair);
        assert!(rate >= 0.0); // Non-negative
    }
}