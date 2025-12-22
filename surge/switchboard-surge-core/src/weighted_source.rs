use anyhow::{anyhow, Result};
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use std::sync::atomic::{AtomicBool, Ordering};
use tracing::{debug, info, warn};
use once_cell::sync::Lazy;

use crate::{
    hub::{WeightedSourceEntry, WEIGHTED_SOURCE_CACHE},
    pair::Pair,
    volume_cache::VolumeFetcher,
    message_rate::MESSAGE_RATE_CACHE,
    Source,
};

/// Stablecoins we consider equivalent to USD
const USD_STABLECOINS: &[&str] = &["USDT", "USDC", "FDUSD", "USD"];

/// AUTO source update interval in seconds (2 minutes for production)
const AUTO_SOURCE_UPDATE_INTERVAL_SECS: u64 = 2*60;

/// Global flag to track if an update is in progress
static UPDATE_IN_PROGRESS: AtomicBool = AtomicBool::new(false);

/// Global flag to track if AUTO cache is ready for subscribeAll
/// Set to true after first full AUTO cache update completes (~10-11 seconds after startup)
pub static AUTO_CACHE_READY: AtomicBool = AtomicBool::new(false);

/// Dedicated thread pool for weighted source calculations
static WEIGHTED_CALC_POOL: Lazy<tokio::runtime::Runtime> = Lazy::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_name("weighted-calc")
        .enable_all()
        .build()
        .expect("Failed to create weighted calc runtime")
});

/// Check if a currency is a stablecoin
fn is_stablecoin(currency: &str) -> bool {
    USD_STABLECOINS.contains(&currency.to_uppercase().as_str())
}

/// Check if a currency is USD or USD-equivalent
pub fn is_usd_equivalent(currency: &str) -> bool {
    currency.to_uppercase() == "USD" || is_stablecoin(currency)
}

/// Get all equivalent quotes for a given quote currency
pub fn get_equivalent_quotes(quote: &str) -> Vec<String> {
    if quote.to_uppercase() == "USD" {
        // For USD, prioritize USDT first (highest volume), then other stablecoins, then USD
        vec![
            "USDT".to_string(),
            "USDC".to_string(), 
            "FDUSD".to_string(),
            "USD".to_string(),
        ]
    } else if is_stablecoin(quote) {
        // For stablecoins, just return the exact match
        vec![quote.to_string()]
    } else {
        // For other currencies, just return the exact match
        vec![quote.to_string()]
    }
}

/// Weighted source calculator that periodically updates the cache
pub struct WeightedSourceCalculator {
    volume_fetcher: &'static VolumeFetcher,
}

impl Default for WeightedSourceCalculator {
    fn default() -> Self {
        Self::new()
    }
}

/// Get all USD pairs from the weighted source cache (for debugging)
pub fn get_usd_pairs_from_cache() -> Vec<String> {
    WEIGHTED_SOURCE_CACHE
        .iter()
        .filter(|entry| entry.key().quote == "USD")
        .map(|entry| entry.key().base.clone())
        .collect()
}

/// Trigger an immediate AUTO cache update (used when new symbols are discovered)
pub fn trigger_auto_cache_update() {
    crate::runtime_separation::spawn_on_ingestion_named(
        "auto-cache-immediate-update",
        async {
            info!("🚀 Triggered immediate AUTO cache update for newly discovered symbols");
            let calculator = AutoSourceCalculator::new();
            if let Err(e) = calculator.update_all_auto_sources().await {
                warn!("Failed immediate AUTO cache update: {}", e);
            } else {
                info!("✅ AUTO cache updated with new symbols");
            }
        }
    );
}

impl WeightedSourceCalculator {
    pub fn new() -> Self {
        Self {
            volume_fetcher: VolumeFetcher::global(),
        }
    }

    /// Get weighted source from cache synchronously (no async/blocking)
    pub fn get_from_cache_sync(&self, pair: &Pair) -> Option<(Source, Pair)> {
        // First try exact match
        if let Some(entry) = WEIGHTED_SOURCE_CACHE.get(pair) {
            if !entry.is_expired() {
                return Some((entry.source, entry.actual_pair.clone()));
            }
        }

        // If requesting USD, try USD equivalents
        if is_usd_equivalent(&pair.quote) {
            let usd_pair = Pair {
                base: pair.base.clone(),
                quote: "USD".to_string(),
            };

            if let Some(entry) = WEIGHTED_SOURCE_CACHE.get(&usd_pair) {
                if !entry.is_expired() {
                    return Some((entry.source, entry.actual_pair.clone()));
                }
            }
        }

        None
    }

    /// Start the background task that periodically updates weighted sources
    pub fn start_background_updates() {
        crate::runtime_separation::spawn_on_ingestion_named(
            "weighted-source-updater",
            async move {
            let calculator = WeightedSourceCalculator::new();

            // Do a volume-based initialization to discover all available feeds
            info!("🚀 Performing initial volume-based weighted source cache initialization...");
            if let Err(e) = calculator.update_all_weighted_sources_volume_based_blocking().await {
                warn!("Failed initial volume-based weighted source update: {}", e);
            }

            // Then do incremental updates every 1 minute
            let mut interval = tokio::time::interval(Duration::from_secs(60)); // 1 minute
            let mut updates_since_full = 0;

            loop {
                interval.tick().await;
                updates_since_full += 1;

                // Do a full update before cache entries expire (12 hours = 720 intervals)
                // Do it at 660 intervals (11 hours) to ensure we refresh before expiry
                if updates_since_full >= 660 {
                    info!("🔄 Performing full weighted source cache refresh (11 hour mark)...");
                    if let Err(e) = calculator.update_all_weighted_sources_blocking().await {
                        warn!("Failed full weighted source refresh: {}", e);
                    }
                    updates_since_full = 0;
                } else {
                    info!("🔄 Performing incremental weighted source cache update...");
                    if let Err(e) = calculator.update_incremental_weighted_sources().await {
                        warn!("Failed incremental weighted source update: {}", e);
                    }
                }
            }
        });
    }

    /// Update all weighted sources based on current volume data (for initialization)
    pub async fn update_all_weighted_sources_volume_based(&self) -> Result<()> {
        let start = std::time::Instant::now();

        // Get all available pairs from all exchanges
        let mut all_pairs: HashSet<Pair> = HashSet::new();

        // Collect pairs from active exchanges only
        for source in crate::exchange_config::get_active_exchanges() {
            if let Ok(pairs) = crate::exchange_config::get_cached_pairs_if_enabled(source) {
                all_pairs.extend(pairs);
            }
        }
        
        // IMPORTANT: Also include all pairs from the hub (both active and inactive)
        // The hub.keys() only returns actively streaming pairs
        // We need to check the prices cache which contains all pairs ever seen
        let hub = crate::SurgeHub::global();
        
        // Get all pairs from the hub's price cache (includes inactive pairs)
        for (source, pair) in hub.all_known_pairs() {
            // Skip weighted/auto sources and exchange-specific pairs
            if source != Source::Weighted && source != Source::Auto && !pair.base.contains(':') {
                all_pairs.insert(pair);
            }
        }

        // Group pairs by base currency
        let mut base_to_pairs: HashMap<String, Vec<Pair>> = HashMap::new();
        for pair in all_pairs {
            base_to_pairs.entry(pair.base.clone()).or_default().push(pair);
        }

        let mut updated_count = 0;

        // For each unique base currency, calculate weighted sources
        for (base, pairs) in base_to_pairs {
            // Group by quote currency to handle different quote options
            let mut quote_groups: HashMap<String, Vec<(Source, Pair)>> = HashMap::new();

            for pair in pairs {
                // Check each exchange for this pair
                for source in crate::exchange_config::get_active_exchanges() {
                    // Get the actual symbol format for this exchange
                    let pair_to_check = pair.clone();
                    let symbol = if source == Source::Weighted {
                        None // Skip weighted sources
                    } else {
                        crate::exchange_config::get_cached_pairs_if_enabled(source)
                            .ok()
                            .and_then(|cached_pairs| cached_pairs.iter().find(|p| **p == pair_to_check).cloned())
                    };

                    if let Some(actual_pair) = symbol {
                        quote_groups.entry(pair.quote.clone())
                            .or_default()
                            .push((source, actual_pair));
                    }
                }
            }

            // Now calculate weighted source for each quote option
            for (quote, sources) in &quote_groups {
                // Create the canonical pair for this quote
                let canonical_pair = Pair {
                    base: base.clone(),
                    quote: quote.clone(),
                };

                // Find the source with highest volume
                let mut best_source: Option<(Source, Pair, f64)> = None;

                for (source, pair) in sources {
                    let volume = self.volume_fetcher.get_volume(*source, pair).await;

                    if let Some((_, _, best_volume)) = &best_source {
                        if volume > *best_volume {
                            best_source = Some((*source, pair.clone(), volume));
                        }
                    } else {
                        best_source = Some((*source, pair.clone(), volume));
                    }
                }

                if let Some((source, actual_pair, volume)) = best_source {
                    // Always create entries, even for 0 volume pairs (they exist but are inactive)
                    // Get secondary source based on volume (different exchange than primary)
                    let secondary = self.volume_fetcher.get_secondary_source(&actual_pair, source);
                    
                    let entry = WeightedSourceEntry {
                        source,
                        actual_pair: actual_pair.clone(),
                        secondary_source: secondary.as_ref().map(|(s, _, _)| *s),
                        secondary_pair: secondary.as_ref().map(|(_, p, _)| p.clone()),
                        calculated_at: get_timestamp_ms(),
                    };

                    WEIGHTED_SOURCE_CACHE.insert(canonical_pair.clone(), entry);
                    updated_count += 1;

                    debug!(
                        "Updated weighted source for {}: {} with {} (volume: {:.2})",
                        canonical_pair.as_str(),
                        source.as_str(),
                        actual_pair.as_str(),
                        volume
                    );
                }
            }

            // Special handling for USD quotes - create synthetic USD pairs
            if quote_groups.keys().any(|q| is_usd_equivalent(q)) {
                let usd_pair = Pair {
                    base: base.clone(),
                    quote: "USD".to_string(),
                };
                
                // Debug logging for BNB
                if base == "BNB" {
                    info!("🔍 Processing BNB/USD - Found quotes: {:?}", quote_groups.keys().collect::<Vec<_>>());
                }

                // Find highest volume among all USD equivalents
                let mut best_usd_source: Option<(Source, Pair, f64)> = None;

                for quote in get_equivalent_quotes("USD") {
                    if let Some(sources) = quote_groups.get(&quote) {
                        for (source, pair) in sources {
                            let volume = self.volume_fetcher.get_volume(*source, pair).await;

                            if let Some((_, _, best_volume)) = &best_usd_source {
                                if volume > *best_volume {
                                    best_usd_source = Some((*source, pair.clone(), volume));
                                }
                            } else {
                                best_usd_source = Some((*source, pair.clone(), volume));
                            }
                        }
                    }
                }

                if let Some((source, actual_pair, volume)) = best_usd_source {
                    // Get secondary source based on volume (different exchange than primary)
                    let secondary = self.volume_fetcher.get_secondary_source(&actual_pair, source);
                    
                    // Always insert USD pairs, even with 0 volume, so they appear in feed lists
                    let entry = WeightedSourceEntry {
                        source,
                        actual_pair: actual_pair.clone(),
                        secondary_source: secondary.as_ref().map(|(s, _, _)| *s),
                        secondary_pair: secondary.as_ref().map(|(_, p, _)| p.clone()),
                        calculated_at: get_timestamp_ms(),
                    };

                    WEIGHTED_SOURCE_CACHE.insert(usd_pair.clone(), entry);
                    updated_count += 1;

                    // Special logging for BNB and major pairs
                    if base == "BNB" {
                        info!(
                            "✅ BNB/USD ADDED TO CACHE: {}/USD → {} on {} (volume: {:.2}), Secondary: {}",
                            base,
                            actual_pair.as_str(),
                            source.as_str(),
                            volume,
                            if let Some((sec_source, sec_pair, _)) = &secondary {
                                format!("{} on {}", sec_pair.as_str(), sec_source.as_str())
                            } else {
                                "None".to_string()
                            }
                        );
                    } else if base == "BTC" || base == "ETH" || base == "APT" {
                        info!(
                            "✅ {}/USD VOLUME-BASED: {} on {} (vol: {:.2}), Secondary: {}",
                            base,
                            actual_pair.as_str(),
                            source.as_str(),
                            volume,
                            if let Some((sec_source, sec_pair, _)) = &secondary {
                                format!("{} on {}", sec_pair.as_str(), sec_source.as_str())
                            } else {
                                "None".to_string()
                            }
                        );
                    } else {
                        debug!(
                            "Updated weighted USD source for {}/USD: {} with {} (volume: {:.2})",
                            base,
                            source.as_str(),
                            actual_pair.as_str(),
                            volume
                        );
                    }
                }
            }
        }

        info!(
            "✅ Volume-based initialization updated {} weighted source entries in {:?}",
            updated_count,
            start.elapsed()
        );

        Ok(())
    }

    /// Update all weighted sources based on current message rates (for runtime updates)
    pub async fn update_all_weighted_sources(&self) -> Result<()> {
        let start = std::time::Instant::now();

        // Get all available pairs from all exchanges
        let mut all_pairs: HashSet<Pair> = HashSet::new();

        // Collect pairs from active exchanges only
        for source in crate::exchange_config::get_active_exchanges() {
            if let Ok(pairs) = crate::exchange_config::get_cached_pairs_if_enabled(source) {
                all_pairs.extend(pairs);
            }
        }
        
        // IMPORTANT: Also include all pairs from the hub (both active and inactive)
        // The hub.keys() only returns actively streaming pairs
        // We need to check the prices cache which contains all pairs ever seen
        let hub = crate::SurgeHub::global();
        
        // Get all pairs from the hub's price cache (includes inactive pairs)
        for (source, pair) in hub.all_known_pairs() {
            // Skip weighted/auto sources and exchange-specific pairs
            if source != Source::Weighted && source != Source::Auto && !pair.base.contains(':') {
                all_pairs.insert(pair);
            }
        }

        // Group pairs by base currency
        let mut base_to_pairs: HashMap<String, Vec<Pair>> = HashMap::new();
        for pair in all_pairs {
            base_to_pairs.entry(pair.base.clone()).or_default().push(pair);
        }

        let mut updated_count = 0;

        // For each unique base currency, calculate weighted sources
        for (base, pairs) in base_to_pairs {
            // Group by quote currency to handle different quote options
            let mut quote_groups: HashMap<String, Vec<(Source, Pair)>> = HashMap::new();

            for pair in pairs {
                // Check each exchange for this pair
                for source in crate::exchange_config::get_active_exchanges() {
                    // Get the actual symbol format for this exchange
                    let pair_to_check = pair.clone();
                    let symbol = if source == Source::Weighted {
                        None // Skip weighted sources
                    } else {
                        crate::exchange_config::get_cached_pairs_if_enabled(source)
                            .ok()
                            .and_then(|cached_pairs| cached_pairs.iter().find(|p| **p == pair_to_check).cloned())
                    };

                    if let Some(actual_pair) = symbol {
                        quote_groups.entry(pair.quote.clone())
                            .or_default()
                            .push((source, actual_pair));
                    }
                }
            }

            // Now calculate weighted source for each quote option
            for (quote, sources) in &quote_groups {
                // Create the canonical pair for this quote
                let canonical_pair = Pair {
                    base: base.clone(),
                    quote: quote.clone(),
                };

                // Find the source with highest message rate AND second highest from different exchange
                let mut all_sources: Vec<(Source, Pair, f64)> = Vec::new();
                
                for (source, pair) in sources {
                    let message_rate = MESSAGE_RATE_CACHE.get_rate(*source, pair);

                    // Debug logging for BTC pairs
                    if canonical_pair.base == "BTC" && canonical_pair.quote == "USD" {
                        info!("🔍 BTC DEBUG: {} on {} = {:.3} msg/sec", 
                            pair.as_str(), source.as_str(), message_rate);
                    }
                    
                    if message_rate > 0.0 {
                        all_sources.push((*source, pair.clone(), message_rate));
                    }
                }
                
                // Sort by message rate descending
                all_sources.sort_by(|a, b| b.2.partial_cmp(&a.2).unwrap_or(std::cmp::Ordering::Equal));
                
                if let Some((primary_source, primary_pair, message_rate)) = all_sources.first() {
                    // Get secondary source based on volume (different exchange than primary)
                    let volume_fetcher = crate::volume_cache::VolumeFetcher::global();
                    let secondary = volume_fetcher.get_secondary_source(primary_pair, *primary_source);
                    
                    let entry = WeightedSourceEntry {
                        source: *primary_source,
                        actual_pair: primary_pair.clone(),
                        secondary_source: secondary.as_ref().map(|(s, _, _)| *s),
                        secondary_pair: secondary.as_ref().map(|(_, p, _)| p.clone()),
                        calculated_at: get_timestamp_ms(),
                    };

                    WEIGHTED_SOURCE_CACHE.insert(canonical_pair.clone(), entry);
                    updated_count += 1;

                    if canonical_pair.base == "APT" {
                        if let Some((sec_source, sec_pair, _)) = &secondary {
                            debug!(
                                "✅ APT/{} SELECTION: Primary={} on {}, Secondary={} on {} (rate: {:.3} msg/sec)",
                                canonical_pair.quote,
                                primary_pair.as_str(), primary_source.as_str(),
                                sec_pair.as_str(), sec_source.as_str(),
                                message_rate
                            );
                        } else {
                            debug!(
                                "✅ APT/{} SELECTION: Primary={} on {} (rate: {:.3} msg/sec, no secondary)",
                                canonical_pair.quote,
                                primary_pair.as_str(), primary_source.as_str(),
                                message_rate
                            );
                        }
                    } else {
                        debug!(
                            "Updated weighted source for {}: {} with {} (rate: {:.3} msg/sec)",
                            canonical_pair.as_str(),
                            primary_source.as_str(),
                            primary_pair.as_str(),
                            message_rate
                        );
                    }
                }
            }

            // Special handling for USD quotes - create synthetic USD pairs
            if quote_groups.keys().any(|q| is_usd_equivalent(q)) {
                let usd_pair = Pair {
                    base: base.clone(),
                    quote: "USD".to_string(),
                };
                
                // Debug logging for BNB
                if base == "BNB" {
                    debug!("🔍 DEBUG: Processing BNB/USD - Found quotes: {:?}", quote_groups.keys().collect::<Vec<_>>());
                }

                // Find highest message rate among all USD equivalents AND secondary source
                let mut all_usd_sources: Vec<(Source, Pair, f64)> = Vec::new();

                for quote in get_equivalent_quotes("USD") {
                    if let Some(sources) = quote_groups.get(&quote) {
                        for (source, pair) in sources {
                            let message_rate = MESSAGE_RATE_CACHE.get_rate(*source, pair);

                            // Debug logging for BTC/USD pairs  
                            if base == "BTC" {
                                debug!("🔍 BTC/USD DEBUG: {} on {} = {:.3} msg/sec", 
                                    pair.as_str(), source.as_str(), message_rate);
                            }

                            // Debug logging for APT/USD pairs to diagnose selection issue
                            if base == "APT" {
                                debug!("🔍 APT/USD DEBUG: {} on {} = {:.3} msg/sec", 
                                    pair.as_str(), source.as_str(), message_rate);
                            }
                            
                            if message_rate > 0.0 {
                                all_usd_sources.push((*source, pair.clone(), message_rate));
                            }
                        }
                    }
                }
                
                // Sort by message rate descending
                all_usd_sources.sort_by(|a, b| b.2.partial_cmp(&a.2).unwrap_or(std::cmp::Ordering::Equal));

                if let Some((primary_source, primary_pair, message_rate)) = all_usd_sources.first() {
                    // Only log for major tokens to reduce noise
                    if base == "BTC" || base == "ETH" || base == "APT" || base == "BNB" {
                        debug!("🔍 WEIGHTED_SOURCE: Getting secondary for {}/USD primary: {} on {}", 
                            base, primary_pair.as_str(), primary_source.as_str());
                    }
                    
                    // Get secondary source based on volume (different exchange than primary)
                    let volume_fetcher = crate::volume_cache::VolumeFetcher::global();
                    let secondary = volume_fetcher.get_secondary_source(primary_pair, *primary_source);
                    
                    if let Some((sec_source, sec_pair, sec_volume)) = &secondary {
                        if base == "BTC" || base == "ETH" || base == "APT" || base == "BNB" {
                            debug!("✅ WEIGHTED_SOURCE: Secondary found for {}/USD: {} on {} (vol: {:.2})", 
                                base, sec_pair.as_str(), sec_source.as_str(), sec_volume);
                        }
                    } else {
                        if base == "BTC" || base == "ETH" || base == "APT" || base == "BNB" {
                            debug!("⚠️ WEIGHTED_SOURCE: No secondary found for {}/USD", base);
                        }
                    }
                    
                    // Always insert USD pairs, even with 0 message rate, so they appear in feed lists
                    let entry = WeightedSourceEntry {
                        source: *primary_source,
                        actual_pair: primary_pair.clone(),
                        secondary_source: secondary.as_ref().map(|(s, _, _)| *s),
                        secondary_pair: secondary.as_ref().map(|(_, p, _)| p.clone()),
                        calculated_at: get_timestamp_ms(),
                    };

                    WEIGHTED_SOURCE_CACHE.insert(usd_pair.clone(), entry);
                    updated_count += 1;

                    // Special logging for BNB and APT
                    if base == "BNB" {
                        debug!(
                            "✅ BNB/USD ADDED TO CACHE: {}/USD → {} on {} (rate: {:.3} msg/sec)",
                            base,
                            primary_pair.as_str(),
                            primary_source.as_str(),
                            message_rate
                        );
                    } else if base == "APT" {
                        debug!(
                            "✅ APT/USD FINAL SELECTION: {}/USD → {} on {} (rate: {:.3} msg/sec)",
                            base,
                            primary_pair.as_str(),
                            primary_source.as_str(),
                            message_rate
                        );
                    } else {
                        debug!(
                            "Updated weighted USD source for {}/USD: {} with {} (rate: {:.3} msg/sec)",
                            base,
                            primary_source.as_str(),
                            primary_pair.as_str(),
                            message_rate
                        );
                    }
                }
            }
        }

        info!(
            "✅ Updated {} weighted source entries in {:?}",
            updated_count,
            start.elapsed()
        );

        Ok(())
    }

    /// Run the full update in a dedicated thread pool to avoid blocking async runtime
    pub async fn update_all_weighted_sources_blocking(&self) -> Result<()> {
        // Check if an update is already in progress
        if UPDATE_IN_PROGRESS.load(Ordering::Relaxed) {
            debug!("Weighted source update already in progress, skipping");
            return Ok(());
        }

        // Set the flag
        UPDATE_IN_PROGRESS.store(true, Ordering::Relaxed);

        // Clone self for the blocking task
        let calculator = WeightedSourceCalculator::new();

        // Run the heavy calculation in dedicated thread pool
        let result = WEIGHTED_CALC_POOL.spawn(async move {
            calculator.update_all_weighted_sources().await
        }).await?;

        // Clear the flag
        UPDATE_IN_PROGRESS.store(false, Ordering::Relaxed);

        result
    }

    /// Run the volume-based initialization in a dedicated thread pool to avoid blocking async runtime
    pub async fn update_all_weighted_sources_volume_based_blocking(&self) -> Result<()> {
        // Check if an update is already in progress
        if UPDATE_IN_PROGRESS.load(Ordering::Relaxed) {
            debug!("Weighted source update already in progress, skipping volume-based init");
            return Ok(());
        }

        // Set the flag
        UPDATE_IN_PROGRESS.store(true, Ordering::Relaxed);

        // Clone self for the blocking task
        let calculator = WeightedSourceCalculator::new();

        // Run the heavy calculation in dedicated thread pool
        let result = WEIGHTED_CALC_POOL.spawn(async move {
            calculator.update_all_weighted_sources_volume_based().await
        }).await?;

        // Clear the flag
        UPDATE_IN_PROGRESS.store(false, Ordering::Relaxed);

        result
    }

    /// Incremental update - now performs full update every time for responsiveness
    pub async fn update_incremental_weighted_sources(&self) -> Result<()> {
        let start = std::time::Instant::now();

        // Just do a full update every time since we're running every minute now
        let result = self.update_all_weighted_sources().await;
        
        match &result {
            Ok(_count) => {
                info!("✅ Incrementally updated all weighted source entries in {:?}", start.elapsed());
            }
            Err(e) => {
                warn!("Failed incremental weighted source update: {}", e);
            }
        }

        result.map(|_| ())
    }

    /// Get the weighted source for a specific pair
    /// Returns (Source, actual_pair) where actual_pair might be different (e.g., BTC/FDUSD for BTC/USD)
    pub async fn get_weighted_source(&self, pair: &Pair) -> Result<(Source, Pair)> {
        // Check cache first
        if let Some(entry) = WEIGHTED_SOURCE_CACHE.get(pair) {
            let age_ms = get_timestamp_ms() - entry.calculated_at;
            if age_ms < 120_000 { // Cache is fresh (< 2 minutes)
                return Ok((entry.source, entry.actual_pair.clone()));
            }
        }

        // If not in cache or stale, calculate on demand
        self.calculate_weighted_source_for_pair(pair).await
    }

    /// Calculate weighted source for a specific pair on demand
    async fn calculate_weighted_source_for_pair(&self, pair: &Pair) -> Result<(Source, Pair)> {
        let quotes_to_check = if is_usd_equivalent(&pair.quote) {
            // For USD or stablecoins, check all equivalents
            get_equivalent_quotes(&pair.quote)
        } else {
            // For other currencies, just check exact match
            vec![pair.quote.clone()]
        };

        let mut all_options: Vec<(Source, Pair, f64)> = Vec::new();

        // Check each exchange for each quote variant
        for quote in quotes_to_check {
            let check_pair = Pair {
                base: pair.base.clone(),
                quote: quote.to_string(),
            };

            for source in crate::exchange_config::get_active_exchanges() {
                // Check if this exchange has this pair
                let has_pair = if source == Source::Weighted {
                    false // Skip weighted sources
                } else {
                    crate::exchange_config::get_cached_pairs_if_enabled(source)
                        .ok()
                        .map(|pairs| pairs.contains(&check_pair))
                        .unwrap_or(false)
                };

                if has_pair {
                    let message_rate = MESSAGE_RATE_CACHE.get_rate(source, &check_pair);
                    if message_rate > 0.0 {
                        all_options.push((source, check_pair.clone(), message_rate));
                    }
                }
            }
        }
        
        // Sort by message rate descending
        all_options.sort_by(|a, b| b.2.partial_cmp(&a.2).unwrap_or(std::cmp::Ordering::Equal));

        if let Some((primary_source, primary_pair, message_rate)) = all_options.first() {
            // Get secondary source based on volume (different exchange)
            let volume_fetcher = crate::volume_cache::VolumeFetcher::global();
            let secondary = volume_fetcher.get_secondary_source(&primary_pair, *primary_source);
            
            debug!(
                "On-demand weighted calculation for {}: primary={} on {} (rate: {:.3} msg/sec), secondary={:?}",
                pair.as_str(),
                primary_pair.as_str(),
                primary_source.as_str(),
                message_rate,
                secondary.as_ref().map(|(s, _, _)| s.as_str())
            );

            // Cache the result with secondary source from volume data
            let entry = WeightedSourceEntry {
                source: *primary_source,
                actual_pair: primary_pair.clone(),
                secondary_source: secondary.as_ref().map(|(s, _, _)| *s),
                secondary_pair: secondary.map(|(_, p, _)| p),
                calculated_at: get_timestamp_ms(),
            };
            WEIGHTED_SOURCE_CACHE.insert(pair.clone(), entry);

            Ok((*primary_source, primary_pair.clone()))
        } else {
            Err(anyhow!("No source found for pair {}", pair.as_str()))
        }
    }
}

/// Get current timestamp in milliseconds
fn get_timestamp_ms() -> u64 {
    crate::clock_sync::get_corrected_timestamp_ms()
}

/// Calculator for AUTO source selection based on multiple quality metrics
pub struct AutoSourceCalculator {
    is_volume_only_mode: bool,
}

impl AutoSourceCalculator {
    pub fn new() -> Self {
        Self {
            is_volume_only_mode: true,  // Start with volume-only mode
        }
    }
    
    pub fn new_with_mode(volume_only: bool) -> Self {
        Self {
            is_volume_only_mode: volume_only,
        }
    }

    /// Get from AUTO cache without blocking
    pub fn get_from_cache_sync(&self, pair: &Pair) -> Option<(Source, Pair)> {
        use crate::hub::AUTO_SOURCE_CACHE;
        
        // Check the cache for exact pair
        if let Some(entry) = AUTO_SOURCE_CACHE.get(pair) {
            if !entry.is_expired() {
                return Some((entry.source, entry.actual_pair.clone()));
            }
        }
        
        // If pair has a USD-equivalent quote (USDT, USDC, etc), check USD cache
        if is_usd_equivalent(&pair.quote) && pair.quote != "USD" {
            let usd_pair = Pair {
                base: pair.base.clone(),
                quote: "USD".to_string(),
            };
            
            if let Some(entry) = AUTO_SOURCE_CACHE.get(&usd_pair) {
                if !entry.is_expired() {
                    return Some((entry.source, entry.actual_pair.clone()));
                }
            }
        }
        
        None
    }

    /// Calculate AUTO score for a source/pair combination with dynamic normalization
    fn calculate_auto_score(&self, source: Source, pair: &Pair, max_values: &(f64, f64, f64, f64)) -> f64 {
        // Force Pyth to always win for its native stablecoin pairs
        // These are canonical stablecoin peg prices that Pyth specializes in
        const PYTH_STABLECOINS: &[&str] = &["PYUSD", "USDS", "USDG"];
        if source == Source::Pyth && PYTH_STABLECOINS.contains(&pair.base.as_str()) {
            return 1.0;  // Perfect score - Pyth always selected as primary for these
        }

        let volume_fetcher = crate::volume_cache::VolumeFetcher::global();

        // Get symbol format for this exchange
        let symbol = match source {
            Source::Binance => pair.as_binance_str(),
            Source::Bybit => pair.as_bybit_str(),
            Source::Okx => pair.as_okx_str(),
            Source::Coinbase => pair.as_coinbase_str().replace("/", "-"),
            Source::Bitget => pair.as_bitget_str(),
            Source::Pyth => pair.as_pyth_str(),
            Source::Titan => pair.as_titan_str(),
            Source::Weighted | Source::Auto => return 0.0,
        };
        
        let (max_rate, min_spread, max_volume, max_depth) = *max_values;
        
        // In volume-only mode (first 2 minutes), only use volume for scoring
        if self.is_volume_only_mode {
            let volume = volume_fetcher.get_volume_sync(source, pair);
            let normalized_volume = if max_volume > 0.0 {
                (volume / max_volume).min(1.0)
            } else {
                0.0
            };
            return normalized_volume;  // 100% volume-based scoring
        }
        
        // Component 1: Average message rate (35%)
        let avg_message_rate = MESSAGE_RATE_CACHE.get_average_rate(source, pair);
        let normalized_message_rate = if max_rate > 0.0 {
            (avg_message_rate / max_rate).min(1.0)
        } else {
            0.0
        };
        
        // Component 2: Spread quality (25%)
        let spread = volume_fetcher.get_spread(source, &symbol).unwrap_or(100.0);
        let spread_quality = if spread < 100.0 {
            // Normalize: spread of 0% = 1.0, spread of 0.5% = 0.0
            // Linear scale where tighter spread = better quality
            // Cap at 0.5% since anything wider is poor quality
            let max_acceptable_spread = 0.5;
            ((max_acceptable_spread - spread) / max_acceptable_spread).max(0.0).min(1.0)
        } else {
            0.0  // No spread data or invalid
        };
        
        // Component 3: Volume (20%)
        let volume = volume_fetcher.get_volume_sync(source, pair);
        let normalized_volume = if max_volume > 0.0 {
            (volume / max_volume).min(1.0)
        } else {
            0.0
        };
        
        // Component 4: Market depth (15%)
        let (bid_size, ask_size) = volume_fetcher.get_market_depth(source, &symbol)
            .unwrap_or((0.0, 0.0));
        let total_depth = bid_size + ask_size;
        let normalized_depth = if max_depth > 0.0 {
            (total_depth / max_depth).min(1.0)
        } else {
            0.0
        };
        
        // Calculate weighted score - emphasize message rate for real-time price discovery
        let score = 0.60 * normalized_message_rate  // 60% - message rate is critical for real-time
                  + 0.15 * spread_quality            // 15% - spread matters but less than rate
                  + 0.15 * normalized_volume         // 15% - volume quality
                  + 0.10 * normalized_depth;         // 10% - market depth

        // Debug logging for BTC Binance pairs to understand scoring
        if pair.base == "BTC" && source == Source::Binance && (pair.quote == "FDUSD" || pair.quote == "USDT") {
            info!(
                "[DEBUG COMPONENTS] BTC/{} Binance: rate={:.3} ({:.1}/{:.1}) spread={:.3} ({:.4}%/{:.4}%) vol={:.3} (${:.0}B/${:.0}B) depth={:.3} | SCORE={:.3}",
                pair.quote,
                normalized_message_rate, avg_message_rate, max_rate,
                spread_quality, spread, min_spread,
                normalized_volume, volume / 1_000_000_000.0, max_volume / 1_000_000_000.0,
                normalized_depth,
                score
            );
        }

        score
    }

    /// Update AUTO source cache for all pairs
    pub async fn update_all_auto_sources(&self) -> Result<()> {
        use crate::hub::AUTO_SOURCE_CACHE;
        
        info!("🔄 Updating AUTO source cache...");
        let start_time = std::time::Instant::now();
        
        // Get all available pairs from all exchanges
        let mut all_pairs: HashSet<Pair> = HashSet::new();
        
        // First, get pairs from exchange configs
        for source in crate::exchange_config::get_active_exchanges() {
            if let Ok(pairs) = crate::exchange_config::get_cached_pairs_if_enabled(source) {
                all_pairs.extend(pairs);
            }
        }
        
        // IMPORTANT: Also include all pairs from the hub (both active and inactive)
        // The hub.keys() only returns actively streaming pairs
        // We need to check the prices cache which contains all pairs ever seen
        let hub = crate::SurgeHub::global();
        
        // Get all pairs from the hub's price cache (includes inactive pairs)
        for (source, pair) in hub.all_known_pairs() {
            // Skip weighted/auto sources and exchange-specific pairs
            if source != Source::Weighted && source != Source::Auto && !pair.base.contains(':') {
                all_pairs.insert(pair);
            }
        }
        
        // Group pairs by base currency
        let mut base_to_pairs: HashMap<String, Vec<Pair>> = HashMap::new();
        for pair in all_pairs {
            base_to_pairs.entry(pair.base.clone()).or_default().push(pair);
        }
        
        let mut updated_count = 0;
        
        // For each base currency, find the best source
        for (base, pairs) in base_to_pairs {
            // Group by quote currency and collect metrics for normalization
            let mut quote_groups: HashMap<String, Vec<(Source, Pair, f64, f64, f64, f64)>> = HashMap::new();
            
            for pair in pairs {
                let volume_fetcher = crate::volume_cache::VolumeFetcher::global();
                
                // First pass: collect all metrics to find max values
                let mut all_metrics = Vec::new();
                
                for source in crate::exchange_config::get_active_exchanges() {
                    // Skip weighted/auto
                    if source == Source::Weighted || source == Source::Auto {
                        continue;
                    }
                    
                    // Check if exchange has this pair
                    if let Ok(cached_pairs) = crate::exchange_config::get_cached_pairs_if_enabled(source) {
                        if let Some(actual_pair) = cached_pairs.iter().find(|p| **p == pair).cloned() {
                            // Get symbol format
                            let symbol = match source {
                                Source::Binance => actual_pair.as_binance_str(),
                                Source::Bybit => actual_pair.as_bybit_str(),
                                Source::Okx => actual_pair.as_okx_str(),
                                Source::Coinbase => actual_pair.as_coinbase_str().replace("/", "-"),
                                Source::Bitget => actual_pair.as_bitget_str(),
                                _ => continue,
                            };
                            
                            let msg_rate = MESSAGE_RATE_CACHE.get_average_rate(source, &actual_pair);
                            let spread = volume_fetcher.get_spread(source, &symbol).unwrap_or(100.0);
                            let volume = volume_fetcher.get_volume_sync(source, &actual_pair);
                            let (bid_size, ask_size) = volume_fetcher.get_market_depth(source, &symbol)
                                .unwrap_or((0.0, 0.0));
                            let depth = bid_size + ask_size;
                            
                            all_metrics.push((source, actual_pair, msg_rate, spread, volume, depth));
                        }
                    }
                }
                
                // Find max/min values for normalization
                let max_rate = all_metrics.iter().map(|(_, _, r, _, _, _)| *r).fold(0.0, f64::max);
                let min_spread = all_metrics.iter().map(|(_, _, _, s, _, _)| *s).filter(|s| *s > 0.0).fold(100.0, f64::min);
                let max_volume = all_metrics.iter().map(|(_, _, _, _, v, _)| *v).fold(0.0, f64::max);
                let max_depth = all_metrics.iter().map(|(_, _, _, _, _, d)| *d).fold(0.0, f64::max);
                
                let max_values = (max_rate, min_spread, max_volume, max_depth);
                
                // Second pass: calculate scores with normalized values
                for (source, actual_pair, msg_rate, spread, volume, depth) in all_metrics {
                    let score = self.calculate_auto_score(source, &actual_pair, &max_values);
                    // Always include pairs, even with 0 score, to ensure USD pairs are created
                    // Score of 0 just means inactive, but we still want to list it
                    quote_groups.entry(pair.quote.clone())
                        .or_default()
                        .push((source, actual_pair, msg_rate, spread, volume, depth));
                }
            }
            
            // For USD and USD-equivalents, combine all options and select the single best
            // This matches how WEIGHTED works - finding the best across all USD equivalents
            let usd_quotes = vec!["USD", "USDT", "USDC", "FDUSD"];
            let mut is_usd_group = false;
            let mut all_usd_sources = Vec::new();
            
            // Collect all USD-equivalent sources
            for quote in &usd_quotes {
                if let Some(sources) = quote_groups.remove(*quote) {
                    is_usd_group = true;
                    all_usd_sources.extend(sources);
                }
            }
            
            // If we have USD equivalents, find the single best across all
            if is_usd_group && !all_usd_sources.is_empty() {
                // Removed verbose comparison logging
                
                // Calculate max values across ALL USD equivalents
                let max_rate = all_usd_sources.iter().map(|(_, _, r, _, _, _)| *r).fold(0.0, f64::max);
                let min_spread = all_usd_sources.iter().map(|(_, _, _, s, _, _)| *s).filter(|s| *s > 0.0).fold(100.0, f64::min);
                let max_volume = all_usd_sources.iter().map(|(_, _, _, _, v, _)| *v).fold(0.0, f64::max);
                let max_depth = all_usd_sources.iter().map(|(_, _, _, _, _, d)| *d).fold(0.0, f64::max);
                let max_values = (max_rate, min_spread, max_volume, max_depth);
                
                // Removed normalization logging
                
                // Calculate scores for all USD equivalents
                let mut scored_sources: Vec<(Source, Pair, f64)> = all_usd_sources
                    .into_iter()
                    .map(|(source, pair, _, _, _, _)| {
                        let score = self.calculate_auto_score(source, &pair, &max_values);
                        (source, pair, score)
                    })
                    .collect();
                
                scored_sources.sort_by(|a, b| b.2.partial_cmp(&a.2).unwrap_or(std::cmp::Ordering::Equal));

                // Debug logging for BTC Binance pairs
                if base == "BTC" {
                    for (source, pair, score) in &scored_sources {
                        if *source == Source::Binance && (pair.quote == "FDUSD" || pair.quote == "USDT") {
                            let volume_fetcher = crate::volume_cache::VolumeFetcher::global();
                            let avg_msg_rate = MESSAGE_RATE_CACHE.get_average_rate(*source, pair);
                            let volume = volume_fetcher.get_volume_sync(*source, pair);
                            let symbol = pair.as_binance_str();
                            let spread = volume_fetcher.get_spread(*source, &symbol).unwrap_or(0.0);

                            info!(
                                "[DEBUG SCORING] BTC/{} on Binance | Score: {:.3} | Rate: {:.1} msg/s | Volume: ${:.0} | Spread: {:.4}%",
                                pair.quote,
                                score,
                                avg_msg_rate,
                                volume,
                                spread
                            );
                        }
                    }
                }

                if let Some((best_source, best_pair, best_score)) = scored_sources.first() {
                    // Store under the canonical USD pair
                    let canonical_pair = Pair {
                        base: base.clone(),
                        quote: "USD".to_string(),
                    };

                    // Skip Pyth stablecoins PRIMARY update - they're permanently locked to Pyth via bootstraps
                    // But we still need to calculate secondaries for them!
                    const PYTH_STABLECOINS: &[&str] = &["PYUSD", "USDS", "USDG"];
                    let is_pyth_stablecoin = PYTH_STABLECOINS.contains(&base.as_str());

                    if is_pyth_stablecoin {
                        debug!("🔒 {}/USD primary locked to Pyth, but calculating CEX secondary...", base);
                        // Don't continue - we want to calculate secondary from CEX exchanges
                    }

                    // Find secondary source - MUST be different exchange AND can be any USD stablecoin
                    let mut secondary: Option<(Source, Pair)> = None;

                    // For Pyth stablecoins, find best CEX source (exclude Pyth/Titan)
                    // For regular pairs, find different exchange than primary
                    if is_pyth_stablecoin {
                        // Find best CEX exchange (any that's not Pyth/Titan)
                        secondary = scored_sources.iter()
                            .find(|(s, _, _)| *s != Source::Pyth && *s != Source::Titan)
                            .map(|(s, p, _)| (*s, p.clone()));
                    } else {
                        // Normal secondary logic for non-Pyth pairs
                        // First try to find same stablecoin on different exchange (best case)
                        secondary = scored_sources.iter()
                            .find(|(s, p, _)| *s != *best_source && p.quote == best_pair.quote)
                            .map(|(s, p, _)| (*s, p.clone()));

                        // If no same stablecoin found, find ANY USD-equivalent on different exchange
                        // This handles cases like BTC/FDUSD on Binance needing BTC/USDT on OKX for validation
                        if secondary.is_none() {
                            secondary = scored_sources.iter()
                                .find(|(s, _, _)| *s != *best_source)
                                .map(|(s, p, _)| (*s, p.clone()));
                        }
                    }

                    // Log cross-stablecoin secondaries for debugging
                    if let Some((sec_source, sec_pair)) = &secondary {
                        if is_pyth_stablecoin {
                            info!("[DEBUG LOG] AUTO: Pyth stablecoin {}/USD using CEX secondary: {} on {}",
                                base, sec_pair.as_str(), sec_source.as_str());
                        } else if (base == "BTC" || base == "ETH" || base == "SOL") && sec_pair.quote != best_pair.quote {
                            info!("[DEBUG LOG] AUTO: Cross-stablecoin secondary for {}/USD: {} on {} (primary) vs {} on {} (secondary)",
                                base,
                                best_pair.as_str(), best_source.as_str(),
                                sec_pair.as_str(), sec_source.as_str()
                            );
                        }
                    } else if base == "BTC" || base == "ETH" || base == "SOL" || is_pyth_stablecoin {
                        warn!("⚠️ AUTO: No secondary source found for {}/USD with primary {} on {}",
                            base, best_pair.as_str(), best_source.as_str());
                    }

                    // For Pyth stablecoins, preserve Pyth as primary but update secondary
                    // For regular pairs, use calculated best source
                    let entry = if is_pyth_stablecoin {
                        crate::hub::WeightedSourceEntry {
                            source: Source::Pyth,  // Keep Pyth as primary!
                            actual_pair: canonical_pair.clone(),  // Use PYUSD/USD not PYUSD/USDT
                            secondary_source: secondary.as_ref().map(|(s, _)| *s),
                            secondary_pair: secondary.map(|(_, p)| p),
                            calculated_at: get_timestamp_ms(),
                        }
                    } else {
                        crate::hub::WeightedSourceEntry {
                            source: *best_source,
                            actual_pair: best_pair.clone(),
                            secondary_source: secondary.as_ref().map(|(s, _)| *s),
                            secondary_pair: secondary.map(|(_, p)| p),
                            calculated_at: get_timestamp_ms(),
                        }
                    };

                    AUTO_SOURCE_CACHE.insert(canonical_pair.clone(), entry.clone());
                    updated_count += 1;

                    // Only log BTC/USD winner with its metrics
                    if canonical_pair.base == "BTC" {
                        let volume_fetcher = crate::volume_cache::VolumeFetcher::global();
                        let avg_msg_rate = MESSAGE_RATE_CACHE.get_average_rate(*best_source, best_pair);
                        let volume = volume_fetcher.get_volume_sync(*best_source, best_pair);
                        let symbol = match *best_source {
                            Source::Binance => best_pair.as_binance_str(),
                            Source::Bybit => best_pair.as_bybit_str(),
                            Source::Okx => best_pair.as_okx_str(),
                            Source::Coinbase => best_pair.as_coinbase_str().replace("/", "-"),
                            Source::Bitget => best_pair.as_bitget_str(),
                            _ => String::new(),
                        };
                        let spread = volume_fetcher.get_spread(*best_source, &symbol).unwrap_or(0.0);

                        info!(
                            "[DEBUG LOG] BTC/USD AUTO winner: {} on {} | Score: {:.3} | Rate: {:.1} msg/s | Volume: ${:.0} | Spread: {:.4}%",
                            best_pair.as_str(),
                            best_source.as_str(),
                            best_score,
                            avg_msg_rate,
                            volume,
                            spread
                        );
                        // Log secondary source if it exists
                        if let (Some(sec_source), Some(sec_pair)) = (&entry.secondary_source, &entry.secondary_pair) {
                            let sec_avg_msg_rate = MESSAGE_RATE_CACHE.get_average_rate(*sec_source, sec_pair);
                            let sec_volume = volume_fetcher.get_volume_sync(*sec_source, sec_pair);
                            let sec_symbol = match *sec_source {
                                Source::Binance => sec_pair.as_binance_str(),
                                Source::Bybit => sec_pair.as_bybit_str(),
                                Source::Okx => sec_pair.as_okx_str(),
                                Source::Coinbase => sec_pair.as_coinbase_str().replace("/", "-"),
                                Source::Bitget => sec_pair.as_bitget_str(),
                                _ => String::new(),
                            };
                            let sec_spread = volume_fetcher.get_spread(*sec_source, &sec_symbol).unwrap_or(0.0);

                            info!(
                                "[DEBUG LOG] BTC/USD AUTO secondary: {} on {} | Rate: {:.1} msg/s | Volume: ${:.0} | Spread: {:.4}%",
                                sec_pair.as_str(),
                                sec_source.as_str(),
                                sec_avg_msg_rate,
                                sec_volume,
                                sec_spread
                            );
                        }
                    }
                }
            }
            
            // Handle non-USD quotes separately (EUR, GBP, etc.)
            for (quote, sources) in quote_groups {
                // Calculate max values for this specific quote group
                let max_rate = sources.iter().map(|(_, _, r, _, _, _)| *r).fold(0.0, f64::max);
                let min_spread = sources.iter().map(|(_, _, _, s, _, _)| *s).filter(|s| *s > 0.0).fold(100.0, f64::min);
                let max_volume = sources.iter().map(|(_, _, _, _, v, _)| *v).fold(0.0, f64::max);
                let max_depth = sources.iter().map(|(_, _, _, _, _, d)| *d).fold(0.0, f64::max);
                let max_values = (max_rate, min_spread, max_volume, max_depth);
                
                // Calculate scores and sort
                let mut scored_sources: Vec<(Source, Pair, f64)> = sources
                    .into_iter()
                    .map(|(source, pair, _, _, _, _)| {
                        let score = self.calculate_auto_score(source, &pair, &max_values);
                        (source, pair, score)
                    })
                    .collect();
                
                scored_sources.sort_by(|a, b| b.2.partial_cmp(&a.2).unwrap_or(std::cmp::Ordering::Equal));
                
                // Removed ranking logging
                
                if let Some((best_source, best_pair, _best_score)) = scored_sources.first() {
                    let canonical_pair = Pair {
                        base: base.clone(),
                        quote: quote.clone(),
                    };
                    
                    // Find secondary source (different exchange)
                    let secondary = scored_sources.iter()
                        .find(|(s, _, _)| *s != *best_source)
                        .map(|(s, p, _)| (*s, p.clone()));
                    
                    let entry = crate::hub::WeightedSourceEntry {
                        source: *best_source,
                        actual_pair: best_pair.clone(),
                        secondary_source: secondary.as_ref().map(|(s, _)| *s),
                        secondary_pair: secondary.map(|(_, p)| p),
                        calculated_at: get_timestamp_ms(),
                    };
                    
                    AUTO_SOURCE_CACHE.insert(canonical_pair.clone(), entry);
                    updated_count += 1;
                    
                    // Removed non-USD logging
                }
            }
        }
        
        // Add ALL exchange-specific pairs to cache for variance checking
        debug!("Adding all exchange-specific pairs with secondaries...");
        
        // Build a map of all available pairs across all exchanges
        let mut pair_to_exchanges: HashMap<Pair, Vec<(Source, f64)>> = HashMap::new();
        let mut fdusd_pair_count = 0;
        
        for source in crate::exchange_config::get_active_exchanges() {
            if source == Source::Weighted || source == Source::Auto {
                continue;
            }
            
            // Get all pairs for this exchange
            if let Ok(cached_pairs) = crate::exchange_config::get_cached_pairs_if_enabled(source) {
                for pair in cached_pairs {
                    // Get volume/score for ranking
                    let volume = crate::volume_cache::VolumeFetcher::global()
                        .get_volume_sync(source, &pair);
                    
                    // Count FDUSD pairs for debugging
                    if pair.quote == "FDUSD" {
                        fdusd_pair_count += 1;
                        if pair.base == "BTC" || pair.base == "ETH" {
                            debug!("Found {}/{} on {} with volume: {:.2}", 
                                pair.base, pair.quote, source.as_str(), volume);
                        }
                    }
                    
                    pair_to_exchanges
                        .entry(pair.clone())
                        .or_default()
                        .push((source, volume));
                }
            }
        }
        
        debug!("Processing {} unique pairs across all exchanges", pair_to_exchanges.len());
        
        // For each pair, sort exchanges by volume and create entries
        for (pair, mut exchanges) in pair_to_exchanges {
            // Sort by volume descending
            exchanges.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
            
            // Create an entry for each exchange that has this pair
            for i in 0..exchanges.len() {
                let (primary_source, primary_volume) = exchanges[i];
                
                // Find the best secondary from a different exchange
                let mut secondary = exchanges.iter()
                    .find(|(s, _)| *s != primary_source)
                    .map(|(s, _)| (*s, pair.clone()));
                
                // If no exact match found and this is a stablecoin pair, 
                // look for equivalent stablecoin pairs on other exchanges
                if secondary.is_none() && is_usd_equivalent(&pair.quote) {
                    // Collect all USD-equivalent pairs from other exchanges with their AUTO scores
                    let mut equiv_candidates: Vec<(Source, Pair, f64)> = Vec::new();
                    
                    // Get normalization values for scoring
                    let volume_fetcher = crate::volume_cache::VolumeFetcher::global();
                    
                    for alt_source in crate::exchange_config::get_active_exchanges() {
                        if alt_source == primary_source || alt_source == Source::Weighted || alt_source == Source::Auto {
                            continue;
                        }
                        
                        // Check all USD stablecoins for this base
                        for stable in USD_STABLECOINS {
                            let equiv_pair = Pair {
                                base: pair.base.clone(),
                                quote: stable.to_string(),
                            };
                            
                            // Check if this exchange has this equivalent pair
                            if let Ok(cached_pairs) = crate::exchange_config::get_cached_pairs_if_enabled(alt_source) {
                                if cached_pairs.iter().any(|p| *p == equiv_pair) {
                                    // Use volume as simple score for now (could use full AUTO scoring)
                                    let volume = volume_fetcher.get_volume_sync(alt_source, &equiv_pair);
                                    if volume > 0.0 {
                                        equiv_candidates.push((alt_source, equiv_pair.clone(), volume));
                                    }
                                }
                            }
                        }
                    }
                    
                    // Sort by score (volume) descending and pick the best
                    equiv_candidates.sort_by(|a, b| b.2.partial_cmp(&a.2).unwrap_or(std::cmp::Ordering::Equal));
                    
                    if let Some((source, equiv_pair, score)) = equiv_candidates.first() {
                        secondary = Some((*source, equiv_pair.clone()));
                        
                        // Log important cross-stablecoin secondaries
                        if pair.base == "BTC" || pair.base == "ETH" || pair.base == "SOL" {
                            debug!("Cross-stablecoin secondary for {}/{} on {}: {}/{} on {} (volume: {:.2})", 
                                pair.base, pair.quote, primary_source.as_str(),
                                equiv_pair.base, equiv_pair.quote, source.as_str(),
                                score);
                        }
                    } else if pair.base == "BTC" || pair.base == "ETH" || pair.base == "SOL" {
                        warn!("⚠️ No USD-equivalent secondary found for {}/{} on {}", 
                            pair.base, pair.quote, primary_source.as_str());
                    }
                }
                
                // Create a unique cache key for this exchange/pair combo
                // Format: "Exchange:BASE/QUOTE" in the base field
                let cache_key = Pair {
                    base: format!("{}:{}", primary_source.as_str(), pair.base),
                    quote: pair.quote.clone(),
                };
                
                let entry = crate::hub::WeightedSourceEntry {
                    source: primary_source,
                    actual_pair: pair.clone(),  // The exact pair requested
                    secondary_source: secondary.as_ref().map(|(s, _)| *s),
                    secondary_pair: secondary.as_ref().map(|(_, p)| p.clone()),
                    calculated_at: get_timestamp_ms(),
                };
                
                // Log exchange-specific entries with secondaries for important pairs
                if (pair.base == "BTC" || pair.base == "ETH") && pair.quote == "FDUSD" {
                    if let Some((sec_source, sec_pair)) = &secondary {
                        debug!("Added exchange-specific entry: {}:{}/{} → secondary {}/{} on {}", 
                            primary_source.as_str(), pair.base, pair.quote,
                            sec_pair.base, sec_pair.quote, sec_source.as_str());
                    } else {
                        debug!("No secondary for exchange-specific: {}:{}/{}", 
                            primary_source.as_str(), pair.base, pair.quote);
                    }
                }
                
                AUTO_SOURCE_CACHE.insert(cache_key.clone(), entry);
                updated_count += 1;
            }
        }
        
        info!(
            "✅ Updated {} AUTO sources (canonical + exchange-specific) in {:?}",
            updated_count,
            start_time.elapsed()
        );
        
        // Debug: Check if we have the specific Binance:BTC/FDUSD entry
        if cfg!(test) {
            let test_key = Pair {
                base: format!("Binance:BTC"),
                quote: "FDUSD".to_string(),
            };
            if let Some(entry) = AUTO_SOURCE_CACHE.get(&test_key) {
                debug!("Found Binance:BTC/FDUSD in cache - Primary: {} on {}, Secondary: {:?}",
                    entry.actual_pair.as_str(), entry.source.as_str(),
                    entry.secondary_pair.as_ref().map(|p| format!("{} on {:?}", p.as_str(), entry.secondary_source.unwrap()))
                );
            } else {
                debug!("Binance:BTC/FDUSD NOT found in AUTO_SOURCE_CACHE");
            }
            debug!("Total FDUSD pairs found: {}", fdusd_pair_count);
        }
        
        Ok(())
    }

    /// Initialize the AUTO cache with volume-only scoring (synchronous on startup)
    pub async fn initialize_cache() -> Result<()> {
        let calculator = AutoSourceCalculator::new_with_mode(true);
        
        info!("📊 Performing initial volume-only AUTO source cache initialization...");
        calculator.update_all_auto_sources().await?;
        info!("✅ Initial AUTO cache populated successfully");
        
        Ok(())
    }
    
    /// Start the background task that updates AUTO sources periodically
    pub fn start_background_updates() {
        crate::runtime_separation::spawn_on_ingestion_named(
            "auto-source-updater",
            async move {
                use crate::hub::AUTO_SOURCE_CACHE;

                // Bootstrap critical pairs immediately (before exchanges even connect)
                // This ensures BTC/USD gets the best streaming feed from the start
                info!("🚀 Bootstrapping AUTO cache with optimal streaming pairs...");

                // BTC/USD -> BTC/FDUSD on Binance (best streaming characteristics)
                let btc_usd_pair = Pair {
                    base: "BTC".to_string(),
                    quote: "USD".to_string(),
                };
                let btc_fdusd_pair = Pair {
                    base: "BTC".to_string(),
                    quote: "FDUSD".to_string(),
                };

                // Set bootstrap entry with BTC/FDUSD as primary (expires in 5 minutes = 300000ms)
                let bootstrap_entry = crate::hub::WeightedSourceEntry {
                    source: Source::Binance,
                    actual_pair: btc_fdusd_pair.clone(),
                    secondary_source: Some(Source::Okx),
                    secondary_pair: Some(Pair {
                        base: "BTC".to_string(),
                        quote: "USDT".to_string(),
                    }),
                    calculated_at: get_timestamp_ms(),
                };

                AUTO_SOURCE_CACHE.insert(btc_usd_pair.clone(), bootstrap_entry);
                info!("✅ Bootstrap: BTC/USD → BTC/FDUSD on Binance (valid for 5 minutes)");

                // Bootstrap Pyth stablecoins - force Pyth as primary source
                // PYUSD/USD
                let pyusd_pair = Pair {
                    base: "PYUSD".to_string(),
                    quote: "USD".to_string(),
                };
                let pyusd_bootstrap = crate::hub::WeightedSourceEntry {
                    source: Source::Pyth,
                    actual_pair: pyusd_pair.clone(),
                    secondary_source: None,  // No secondary for now
                    secondary_pair: None,
                    calculated_at: get_timestamp_ms(),
                };
                AUTO_SOURCE_CACHE.insert(pyusd_pair.clone(), pyusd_bootstrap);
                info!("✅ Bootstrap: PYUSD/USD → Pyth PYUSD/USD (forced primary)");

                // USDS/USD
                let usds_pair = Pair {
                    base: "USDS".to_string(),
                    quote: "USD".to_string(),
                };
                let usds_bootstrap = crate::hub::WeightedSourceEntry {
                    source: Source::Pyth,
                    actual_pair: usds_pair.clone(),
                    secondary_source: None,
                    secondary_pair: None,
                    calculated_at: get_timestamp_ms(),
                };
                AUTO_SOURCE_CACHE.insert(usds_pair.clone(), usds_bootstrap);
                info!("✅ Bootstrap: USDS/USD → Pyth USDS/USD (forced primary)");

                // USDG/USD
                let usdg_pair = Pair {
                    base: "USDG".to_string(),
                    quote: "USD".to_string(),
                };
                let usdg_bootstrap = crate::hub::WeightedSourceEntry {
                    source: Source::Pyth,
                    actual_pair: usdg_pair.clone(),
                    secondary_source: None,
                    secondary_pair: None,
                    calculated_at: get_timestamp_ms(),
                };
                AUTO_SOURCE_CACHE.insert(usdg_pair.clone(), usdg_bootstrap);
                info!("✅ Bootstrap: USDG/USD → Pyth USDG/USD (forced primary)");

                // Start with volume-only mode
                let mut calculator = AutoSourceCalculator::new_with_mode(true);

                info!("🚀 Starting AUTO source updater background task");
                // Wait 10 seconds for exchanges to connect and populate volume data
                info!("⏳ Waiting 10 seconds for exchanges to connect and populate data...");
                tokio::time::sleep(Duration::from_secs(10)).await;

                // Do initial volume-only update after exchanges are ready
                info!("📊 Performing initial volume-only AUTO source cache initialization...");
                if let Err(e) = calculator.update_all_auto_sources().await {
                    warn!("Failed initial volume-only AUTO source update: {}", e);
                } else {
                    // Mark AUTO cache as ready for subscribeAll
                    AUTO_CACHE_READY.store(true, Ordering::Relaxed);
                    info!("✅ AUTO cache ready for subscribeAll (initial population complete)");
                }

                // Re-apply bootstrap for BTC/FDUSD after volume-only update (which may have changed it to USDT)
                info!("🔄 Re-applying BTC/FDUSD bootstrap after volume-only update...");
                let btc_fdusd_bootstrap = crate::hub::WeightedSourceEntry {
                    source: Source::Binance,
                    actual_pair: btc_fdusd_pair.clone(),
                    secondary_source: Some(Source::Okx),
                    secondary_pair: Some(Pair {
                        base: "BTC".to_string(),
                        quote: "USDT".to_string(),
                    }),
                    calculated_at: get_timestamp_ms(),
                };
                AUTO_SOURCE_CACHE.insert(btc_usd_pair.clone(), btc_fdusd_bootstrap);
                info!("✅ Bootstrap re-applied: BTC/USD → BTC/FDUSD on Binance");

                // Don't re-apply Pyth bootstraps - let AUTO calculation handle it with secondaries
                info!("✅ Pyth stablecoins (PYUSD/USDS/USDG) will get CEX secondaries from AUTO calculation");

                // Wait 2 minutes before switching to full AUTO scoring
                tokio::time::sleep(Duration::from_secs(120)).await;

                // Switch to full AUTO scoring mode
                calculator.is_volume_only_mode = false;
                info!("🔄 Switching to full AUTO scoring mode (message rate + spread + volume + depth)");

                // First full AUTO update (2 min)
                info!("📊 Performing first full AUTO source cache update...");
                if let Err(e) = calculator.update_all_auto_sources().await {
                    warn!("Failed first full AUTO source update: {}", e);
                }

                // Re-apply bootstrap one more time to ensure FDUSD is used during early startup
                info!("🔄 Final BTC/FDUSD bootstrap re-application...");
                let btc_fdusd_final = crate::hub::WeightedSourceEntry {
                    source: Source::Binance,
                    actual_pair: btc_fdusd_pair.clone(),
                    secondary_source: Some(Source::Okx),
                    secondary_pair: Some(Pair {
                        base: "BTC".to_string(),
                        quote: "USDT".to_string(),
                    }),
                    calculated_at: get_timestamp_ms(),
                };
                AUTO_SOURCE_CACHE.insert(btc_usd_pair.clone(), btc_fdusd_final);
                info!("✅ Final bootstrap: BTC/USD → BTC/FDUSD on Binance (AUTO scoring will take over at 4min)");
                info!("✅ Pyth stablecoins (PYUSD/USDS/USDG) now have CEX secondaries from AUTO calculation");

                // Wait another 2 minutes
                tokio::time::sleep(Duration::from_secs(120)).await;
                
                // Second full AUTO update (4 min)
                info!("📊 Performing second full AUTO source cache update...");
                if let Err(e) = calculator.update_all_auto_sources().await {
                    warn!("Failed second full AUTO source update: {}", e);
                }
                
                // Then update every 10 minutes
                let mut interval = tokio::time::interval(Duration::from_secs(AUTO_SOURCE_UPDATE_INTERVAL_SECS));
                interval.tick().await; // Consume the first tick
                
                loop {
                    interval.tick().await;

                    info!("🔄 Performing AUTO source cache update (every 10 minutes)...");
                    if let Err(e) = calculator.update_all_auto_sources().await {
                        warn!("Failed AUTO source update: {}", e);
                    }

                    // No need to re-apply Pyth bootstraps - AUTO calculation preserves Pyth primary + calculates CEX secondaries
                    debug!("✅ Pyth stablecoins (PYUSD/USDS/USDG) maintained with Pyth primary + CEX secondaries");
                }
            }
        );
    }
}
