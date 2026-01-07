use anyhow::{anyhow, Result};
use dashmap::DashMap;
use once_cell::sync::Lazy;
use reqwest::Client;
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::{warn, debug};

use crate::{Source, Pair};

/// Global volume cache shared across all instances
pub static VOLUME_CACHE: Lazy<VolumeFetcher> = Lazy::new(VolumeFetcher::new);

/// Volume data with timestamp
#[derive(Clone, Debug)]
struct CachedVolumeData {
    volumes: HashMap<String, f64>,
    trade_counts: HashMap<String, u64>,  // NEW: 24hr trade counts
    spreads: HashMap<String, f64>,       // NEW: bid-ask spread percentages
    bid_sizes: HashMap<String, f64>,     // NEW: total bid volume at best price
    ask_sizes: HashMap<String, f64>,     // NEW: total ask volume at best price
    /// Top 2 sources by volume for each pair (base/quote)
    /// Key is "BASE/QUOTE", value is [(Source1, volume1), (Source2, volume2)]
    _top_sources_by_pair: HashMap<String, Vec<(Source, f64)>>,
    cached_at: u64,
}

/// Handles volume fetching and caching for all exchanges
#[derive(Clone)]
pub struct VolumeFetcher {
    /// Cache for each exchange's volume data
    cache: Arc<DashMap<Source, CachedVolumeData>>,
    /// HTTP client for API requests
    client: Client,
}

impl VolumeFetcher {
    fn new() -> Self {
        // Create HTTP client with User-Agent header
        let client = Client::builder()
            .user_agent("switchboard-surge/1.0")
            .build()
            .expect("Failed to create HTTP client");
            
        let fetcher = Self {
            cache: Arc::new(DashMap::new()),
            client,
        };

        // Spawn initial volume fetch immediately
        {
            let volume_fetcher = fetcher.clone();
            crate::runtime_separation::spawn_on_ingestion_named(
                "volume-cache-initial-fetch",
                async move {
                tracing::info!("Fetching initial volume data for all exchanges...");
                for source in crate::exchange_config::get_active_exchanges() {
                    if let Err(e) = volume_fetcher.refresh_cache(source).await {
                        tracing::error!("Failed to fetch initial {} volumes: {}", source.as_str(), e);
                    }
                }
                tracing::info!("Initial volume data fetch complete");
            });
        }

        fetcher
    }

    /// Get the global volume fetcher instance
    pub fn global() -> &'static VolumeFetcher {
        &VOLUME_CACHE
    }
    
    /// Get spread percentage for a symbol on an exchange
    pub fn get_spread(&self, source: Source, symbol: &str) -> Option<f64> {
        self.cache.get(&source).and_then(|data| {
            data.spreads.get(symbol).copied()
        })
    }
    
    /// Get market depth (bid/ask sizes) for a symbol on an exchange
    pub fn get_market_depth(&self, source: Source, symbol: &str) -> Option<(f64, f64)> {
        self.cache.get(&source).and_then(|data| {
            let bid_size = data.bid_sizes.get(symbol).copied()?;
            let ask_size = data.ask_sizes.get(symbol).copied()?;
            Some((bid_size, ask_size))
        })
    }
    
    /// Get volume synchronously from cache (no async/await)
    pub fn get_volume_sync(&self, source: Source, symbol: &Pair) -> f64 {
        // Convert symbol to exchange format
        let symbol_str = match source {
            Source::Binance => symbol.as_binance_str(),
            Source::Bybit => symbol.as_bybit_str(),
            Source::Okx => symbol.as_okx_str(),
            Source::Coinbase => symbol.as_coinbase_str().replace("/", "-"),
            Source::Bitget => symbol.as_bitget_str(),
            Source::Gate => symbol.as_gate_str(),
            Source::Pyth => symbol.as_pyth_str(),
            Source::Titan => symbol.as_titan_str(),
            Source::Weighted | Source::Auto => return 0.0,
        };
        
        self.cache
            .get(&source)
            .and_then(|entry| entry.volumes.get(&symbol_str).copied())
            .unwrap_or(0.0)
    }
    
    /// Get the secondary source for validation (second highest volume, different exchange)
    /// Returns None if no secondary source is available
    pub fn get_secondary_source(&self, pair: &Pair, primary_source: Source) -> Option<(Source, Pair, f64)> {
        // Only log for tokens we care about debugging
        if pair.base == "BTC" || pair.base == "ETH" || pair.base == "APT" || pair.base == "BNB" {
            debug!("🔍 VOLUME_CACHE: Finding secondary source for {} (primary: {})", 
                pair.as_str(), primary_source.as_str());
        }
        
        let mut volumes: Vec<(Source, Pair, f64)> = Vec::new();
        
        // For USD pairs, search for equivalent quotes (USDT, USDC, FDUSD, etc.)
        let quotes_to_check = if crate::weighted_source::is_usd_equivalent(&pair.quote) {
            crate::weighted_source::get_equivalent_quotes("USD")
        } else {
            vec![pair.quote.clone()]
        };
        
        // Check all exchanges for equivalent pairs
        for source in crate::exchange_config::get_active_exchanges() {
            // Skip weighted/auto source and primary source
            if source == Source::Weighted || source == Source::Auto || source == primary_source {
                continue;
            }
            
            // Try all possible quote currencies for this base
            for quote in &quotes_to_check {
                let test_pair = crate::Pair {
                    base: pair.base.clone(),
                    quote: quote.clone(),
                };
                
                // Get the appropriate symbol format for this exchange
                let symbol_str = match source {
                    Source::Binance => test_pair.as_binance_str(),
                    Source::Bybit => test_pair.as_bybit_str(),
                    Source::Okx => test_pair.as_okx_str(),
                    Source::Coinbase => test_pair.as_coinbase_str().replace("/", "-"),
                    Source::Bitget => test_pair.as_bitget_str(),
                    Source::Gate => test_pair.as_gate_str(),
                    Source::Pyth => test_pair.as_pyth_str(),
                    Source::Titan => test_pair.as_titan_str(),
                    Source::Weighted | Source::Auto => continue,
                };
                
                // Reduce log verbosity - only log for major tokens
                if pair.base == "BTC" || pair.base == "ETH" || pair.base == "APT" || pair.base == "BNB" {
                    debug!("🔍 VOLUME_CACHE: Checking {} for symbol: {}", source.as_str(), symbol_str);
                }
                
                // Get volume from cache
                if let Some(cached_data) = self.cache.get(&source) {
                    if let Some(&volume) = cached_data.volumes.get(&symbol_str) {
                        if volume > 0.0 {
                            if pair.base == "BTC" || pair.base == "ETH" || pair.base == "APT" || pair.base == "BNB" {
                                debug!("✅ VOLUME_CACHE: Found volume for {} on {}: {:.2}", 
                                    symbol_str, source.as_str(), volume);
                            }
                            volumes.push((source, test_pair, volume));
                            break; // Found a pair on this exchange, don't check other quotes
                        }
                    }
                }
            }
        }
        
        if pair.base == "BTC" || pair.base == "ETH" || pair.base == "APT" || pair.base == "BNB" {
            debug!("🔍 VOLUME_CACHE: Found {} exchanges with volume data for {}", volumes.len(), pair.as_str());
        }
        
        // Sort by volume descending
        volumes.sort_by(|a, b| b.2.partial_cmp(&a.2).unwrap_or(std::cmp::Ordering::Equal));
        
        // Log sorted volumes only for major tokens
        if pair.base == "BTC" || pair.base == "ETH" || pair.base == "APT" || pair.base == "BNB" {
            for (i, (src, _, vol)) in volumes.iter().enumerate() {
                debug!("📊 VOLUME_CACHE: Rank {}: {} with volume {:.2}", i+1, src.as_str(), vol);
            }
        }
        
        // If primary source is the highest volume, return the second
        if let Some(highest) = volumes.first() {
            if highest.0 == primary_source {
                // Primary is highest volume, return second if available
                if let Some(second) = volumes.get(1) {
                    if pair.base == "BTC" || pair.base == "ETH" || pair.base == "APT" || pair.base == "BNB" {
                        debug!("✅ VOLUME_CACHE: Primary {} is highest volume, returning second: {} (vol: {:.2})", 
                            primary_source.as_str(), second.0.as_str(), second.2);
                    }
                    Some(second.clone())
                } else {
                    if pair.base == "BTC" || pair.base == "ETH" || pair.base == "APT" || pair.base == "BNB" {
                        debug!("⚠️ VOLUME_CACHE: Primary {} is highest volume but no second option available", 
                            primary_source.as_str());
                    }
                    None
                }
            } else {
                // Primary is not highest, return the highest for validation
                if pair.base == "BTC" || pair.base == "ETH" || pair.base == "APT" || pair.base == "BNB" {
                    debug!("✅ VOLUME_CACHE: Primary {} is not highest, returning highest: {} (vol: {:.2})", 
                        primary_source.as_str(), highest.0.as_str(), highest.2);
                }
                Some(highest.clone())
            }
        } else {
            debug!("⚠️ VOLUME_CACHE: No volume data available for any exchange for {}", pair.as_str());
            None
        }
    }

    /// Get 24h trade count for a specific pair on a specific exchange
    /// Returns 0 if trade count data is not available
    pub async fn get_trade_count(&self, source: Source, symbol: &Pair) -> u64 {
        let now = get_timestamp_ms();

        // Check if we need to refresh the cache (2 minutes TTL)
        let needs_refresh = self.cache
            .get(&source)
            .map(|entry| now - entry.cached_at > 120000)
            .unwrap_or(true);

        if needs_refresh {
            // Try to refresh but don't fail if it doesn't work
            if let Err(e) = self.refresh_cache(source).await {
                debug!("Failed to refresh {} cache: {}", source.as_str(), e);
            }
        }

        // Get the appropriate symbol format for this exchange
        let symbol_str = match source {
            Source::Binance => symbol.as_binance_str(),
            Source::Bybit => symbol.as_bybit_str(),
            Source::Okx => symbol.as_okx_str(),
            Source::Coinbase => symbol.as_coinbase_str().replace("/", "-"),
            Source::Bitget => symbol.as_bitget_str(),
            Source::Gate => symbol.as_gate_str(),
            Source::Pyth => symbol.as_pyth_str(),
            Source::Titan => symbol.as_titan_str(),
            Source::Weighted | Source::Auto => {
                // Weighted/Auto sources don't have their own trade counts
                return 0;
            }
        };

        self.cache
            .get(&source)
            .and_then(|entry| entry.trade_counts.get(&symbol_str).copied())
            .unwrap_or(0)
    }

    /// Get 24h volume for a specific pair on a specific exchange
    /// Returns 0.0 if volume data is not available
    pub async fn get_volume(&self, source: Source, symbol: &Pair) -> f64 {
        // Add BNB-specific logging
        if symbol.base == "BNB" {
            debug!("🔍 BNB VOLUME REQUEST: {} on {}", symbol.as_str(), source.as_str());
        }
        
        let now = get_timestamp_ms();

        // Check if we need to refresh the cache (2 minutes TTL)
        let needs_refresh = self.cache
            .get(&source)
            .map(|entry| now - entry.cached_at > 120000)
            .unwrap_or(true);

        if needs_refresh {
            // Try to refresh but don't fail if it doesn't work
            if let Err(e) = self.refresh_cache(source).await {
                tracing::warn!("Failed to refresh volume cache for {:?}: {}", source, e);
                // Continue with stale data or return 0
            }
        }

        // Get from cache
        let symbol_str = match source {
            Source::Binance => symbol.as_binance_str(),
            Source::Bybit => symbol.as_bybit_str(),
            Source::Okx => symbol.as_okx_str(),
            Source::Coinbase => symbol.as_coinbase_str().replace("/", "-"), // Coinbase uses BTC-USD format
            Source::Bitget => symbol.as_bitget_str(),
            Source::Gate => symbol.as_gate_str(),
            Source::Pyth => symbol.as_pyth_str(),
            Source::Titan => symbol.as_titan_str(),
            Source::Weighted | Source::Auto => {
                // Weighted/Auto sources don't have their own volume
                return 0.0;
            }
        };

        let result = self.cache
            .get(&source)
            .and_then(|entry| entry.volumes.get(&symbol_str).copied())
            .unwrap_or(0.0); // Return 0 volume if not found instead of erroring
            
        if symbol.base == "BNB" {
            debug!("📊 BNB VOLUME RESULT: {} = {:.2} (symbol_str: {})", 
                symbol.as_str(), result, symbol_str);
        }
        
        result
    }

    /// Refresh volume cache for a specific exchange
    async fn refresh_cache(&self, source: Source) -> Result<()> {
        // Try to fetch volumes, trade counts, spreads and depths with a timeout
        let (volumes, trade_counts, spreads, bid_sizes, ask_sizes) = match source {
            Source::Binance => match tokio::time::timeout(
                Duration::from_secs(5),
                self.fetch_binance_volumes()
            ).await {
                Ok(Ok((vols, counts, spreads, bid_sizes, ask_sizes))) => (vols, counts, spreads, bid_sizes, ask_sizes),
                Ok(Err(e)) => {
                    tracing::warn!("Failed to fetch volumes from Binance: {}", e);
                    (HashMap::new(), HashMap::new(), HashMap::new(), HashMap::new(), HashMap::new())
                }
                Err(_) => {
                    tracing::warn!("Timeout fetching volumes from Binance");
                    (HashMap::new(), HashMap::new(), HashMap::new(), HashMap::new(), HashMap::new())
                }
            },
            Source::Bybit => match tokio::time::timeout(
                Duration::from_secs(5),
                self.fetch_bybit_volumes()
            ).await {
                Ok(Ok((vols, counts, spreads, bid_sizes, ask_sizes))) => (vols, counts, spreads, bid_sizes, ask_sizes),
                Ok(Err(e)) => {
                    tracing::warn!("Failed to fetch volumes from Bybit: {}", e);
                    (HashMap::new(), HashMap::new(), HashMap::new(), HashMap::new(), HashMap::new())
                }
                Err(_) => {
                    tracing::warn!("Timeout fetching volumes from Bybit");
                    (HashMap::new(), HashMap::new(), HashMap::new(), HashMap::new(), HashMap::new())
                }
            },
            Source::Okx => match tokio::time::timeout(
                Duration::from_secs(5),
                self.fetch_okx_volumes()
            ).await {
                Ok(Ok((vols, counts, spreads, bid_sizes, ask_sizes))) => (vols, counts, spreads, bid_sizes, ask_sizes),
                Ok(Err(e)) => {
                    tracing::warn!("Failed to fetch volumes from OKX: {}", e);
                    (HashMap::new(), HashMap::new(), HashMap::new(), HashMap::new(), HashMap::new())
                }
                Err(_) => {
                    tracing::warn!("Timeout fetching volumes from OKX");
                    (HashMap::new(), HashMap::new(), HashMap::new(), HashMap::new(), HashMap::new())
                }
            },
            Source::Coinbase => match tokio::time::timeout(
                Duration::from_secs(5),
                self.fetch_coinbase_volumes()
            ).await {
                Ok(Ok((vols, counts, spreads, bid_sizes, ask_sizes))) => (vols, counts, spreads, bid_sizes, ask_sizes),
                Ok(Err(e)) => {
                    tracing::warn!("Failed to fetch volumes from Coinbase: {}", e);
                    (HashMap::new(), HashMap::new(), HashMap::new(), HashMap::new(), HashMap::new())
                }
                Err(_) => {
                    tracing::warn!("Timeout fetching volumes from Coinbase");
                    (HashMap::new(), HashMap::new(), HashMap::new(), HashMap::new(), HashMap::new())
                }
            },
            Source::Bitget => match tokio::time::timeout(
                Duration::from_secs(5),
                self.fetch_bitget_volumes()
            ).await {
                Ok(Ok((vols, counts, spreads, bid_sizes, ask_sizes))) => (vols, counts, spreads, bid_sizes, ask_sizes),
                Ok(Err(e)) => {
                    tracing::warn!("Failed to fetch volumes from Bitget: {}", e);
                    (HashMap::new(), HashMap::new(), HashMap::new(), HashMap::new(), HashMap::new())
                }
                Err(_) => {
                    tracing::warn!("Timeout fetching volumes from Bitget");
                    (HashMap::new(), HashMap::new(), HashMap::new(), HashMap::new(), HashMap::new())
                }
            },
            Source::Gate => match tokio::time::timeout(
                Duration::from_secs(5),
                self.fetch_gate_volumes()
            ).await {
                Ok(Ok((vols, counts, spreads, bid_sizes, ask_sizes))) => (vols, counts, spreads, bid_sizes, ask_sizes),
                Ok(Err(e)) => {
                    tracing::warn!("Failed to fetch volumes from Gate.io: {}", e);
                    (HashMap::new(), HashMap::new(), HashMap::new(), HashMap::new(), HashMap::new())
                }
                Err(_) => {
                    tracing::warn!("Timeout fetching volumes from Gate.io");
                    (HashMap::new(), HashMap::new(), HashMap::new(), HashMap::new(), HashMap::new())
                }
            },
            Source::Pyth => {
                // Pyth is an oracle service, not a traditional exchange with volume data
                return Ok(());
            }
            Source::Titan => {
                // Titan is a DEX aggregator, not a traditional exchange with volume data
                return Ok(());
            }
            Source::Weighted | Source::Auto => {
                // Weighted/Auto sources don't have their own volumes
                return Ok(());
            }
        };

        let cached_data = CachedVolumeData {
            volumes,
            trade_counts,
            spreads,
            bid_sizes,
            ask_sizes,
            _top_sources_by_pair: HashMap::new(), // Not used, kept for compatibility
            cached_at: get_timestamp_ms(),
        };

        self.cache.insert(source, cached_data);
        Ok(())
    }

    /// Fetch all volumes, trade counts, spreads and market depth from Binance
    pub async fn fetch_binance_volumes(&self) -> Result<(HashMap<String, f64>, HashMap<String, u64>, HashMap<String, f64>, HashMap<String, f64>, HashMap<String, f64>)> {
        let response = self.client
            .get("https://api.binance.com/api/v3/ticker/24hr")
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(anyhow!("Binance API error: {}", response.status()));
        }

        #[derive(Deserialize)]
        struct Ticker24hr {
            symbol: String,
            volume: String,
            #[serde(rename = "quoteVolume")]
            quote_volume: String,
            count: u64,  // NEW: 24hr trade count
            #[serde(rename = "bidPrice")]
            bid_price: String,
            #[serde(rename = "bidQty")]
            bid_qty: String,
            #[serde(rename = "askPrice")]
            ask_price: String,
            #[serde(rename = "askQty")]
            ask_qty: String,
        }

        let tickers: Vec<Ticker24hr> = response.json().await?;
        let mut volume_map = HashMap::new();
        let mut count_map = HashMap::new();
        let mut spread_map = HashMap::new();
        let mut bid_size_map = HashMap::new();
        let mut ask_size_map = HashMap::new();

        for ticker in tickers {
            if let Ok(quote_volume) = ticker.quote_volume.parse::<f64>() {
                // Add BNB volume logging
                if ticker.symbol.starts_with("BNB") {
                    debug!("📊 BNB VOLUME: {} = {:.2} (base: {}, quote: {:.2})", 
                        ticker.symbol, quote_volume, ticker.volume, quote_volume);
                }
                volume_map.insert(ticker.symbol.clone(), quote_volume);
                count_map.insert(ticker.symbol.clone(), ticker.count);
                
                // Calculate spread percentage
                if let (Ok(bid), Ok(ask)) = (ticker.bid_price.parse::<f64>(), ticker.ask_price.parse::<f64>()) {
                    if bid > 0.0 && ask > 0.0 {
                        let spread_pct = ((ask - bid) / ask) * 100.0;
                        spread_map.insert(ticker.symbol.clone(), spread_pct);
                    }
                }
                
                // Store bid/ask sizes
                if let Ok(bid_qty) = ticker.bid_qty.parse::<f64>() {
                    bid_size_map.insert(ticker.symbol.clone(), bid_qty);
                }
                if let Ok(ask_qty) = ticker.ask_qty.parse::<f64>() {
                    ask_size_map.insert(ticker.symbol.clone(), ask_qty);
                }
            } else if ticker.symbol.starts_with("BNB") {
                warn!("❌ BNB VOLUME PARSE FAIL: {} quote_volume: '{}'", ticker.symbol, ticker.quote_volume);
            }
        }

        Ok((volume_map, count_map, spread_map, bid_size_map, ask_size_map))
    }

    /// Fetch all volumes, spreads and market depth from Bybit
    pub async fn fetch_bybit_volumes(&self) -> Result<(HashMap<String, f64>, HashMap<String, u64>, HashMap<String, f64>, HashMap<String, f64>, HashMap<String, f64>)> {
        let response = self.client
            .get("https://api.bybit.com/v5/market/tickers?category=spot")
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(anyhow!("Bybit API error: {}", response.status()));
        }

        let data = response.json::<Value>().await?;
        let tickers = data["result"]["list"]
            .as_array()
            .ok_or_else(|| anyhow!("missing result.list"))?;

        let mut volume_map = HashMap::new();
        let mut spread_map = HashMap::new();
        let mut bid_size_map = HashMap::new();
        let mut ask_size_map = HashMap::new();
        
        for ticker in tickers {
            // Use turnover24h (quote volume in USDT) instead of volume24h (base volume)
            // This matches what Binance uses (quoteVolume) for fair comparison
            if let (Some(symbol), Some(turnover_str)) = (
                ticker["symbol"].as_str(),
                ticker["turnover24h"].as_str()  // Changed from volume24h to turnover24h
            ) {
                if let Ok(volume) = turnover_str.parse::<f64>() {
                    // Log for DOGE pairs to show the fix
                    if symbol.contains("DOGE") && !symbol.contains("BABY") {
                        debug!("📊 Bybit Volume for {}: {:.2} USDT (using turnover24h)", symbol, volume);
                    }
                    volume_map.insert(symbol.to_string(), volume);
                }
                
                // Calculate spread percentage
                if let (Some(bid_str), Some(ask_str)) = (
                    ticker["bid1Price"].as_str(),
                    ticker["ask1Price"].as_str()
                ) {
                    if let (Ok(bid), Ok(ask)) = (bid_str.parse::<f64>(), ask_str.parse::<f64>()) {
                        if bid > 0.0 && ask > 0.0 {
                            let spread_pct = ((ask - bid) / ask) * 100.0;
                            spread_map.insert(symbol.to_string(), spread_pct);
                        }
                    }
                }
                
                // Store bid/ask sizes
                if let Some(bid_size_str) = ticker["bid1Size"].as_str() {
                    if let Ok(bid_size) = bid_size_str.parse::<f64>() {
                        bid_size_map.insert(symbol.to_string(), bid_size);
                    }
                }
                if let Some(ask_size_str) = ticker["ask1Size"].as_str() {
                    if let Ok(ask_size) = ask_size_str.parse::<f64>() {
                        ask_size_map.insert(symbol.to_string(), ask_size);
                    }
                }
            }
        }

        Ok((volume_map, HashMap::new(), spread_map, bid_size_map, ask_size_map))  // Bybit doesn't provide trade counts
    }

    /// Fetch all volumes, spreads and market depth from OKX
    pub async fn fetch_okx_volumes(&self) -> Result<(HashMap<String, f64>, HashMap<String, u64>, HashMap<String, f64>, HashMap<String, f64>, HashMap<String, f64>)> {
        let response = self.client
            .get("https://www.okx.com/api/v5/market/tickers?instType=SPOT")
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(anyhow!("OKX API error: {}", response.status()));
        }

        let data = response.json::<Value>().await?;
        let tickers = data["data"]
            .as_array()
            .ok_or_else(|| anyhow!("missing data array"))?;

        let mut volume_map = HashMap::new();
        let mut spread_map = HashMap::new();
        let mut bid_size_map = HashMap::new();
        let mut ask_size_map = HashMap::new();
        
        for ticker in tickers {
            // Use volCcy24h (quote volume in USD/USDT) instead of vol24h (base volume)
            // This matches what Binance uses (quoteVolume) for fair comparison
            if let (Some(inst_id), Some(vol_ccy_str)) = (
                ticker["instId"].as_str(),
                ticker["volCcy24h"].as_str()  // Changed from vol24h to volCcy24h
            ) {
                if let Ok(volume) = vol_ccy_str.parse::<f64>() {
                    // Log for DOGE pairs to show the fix
                    if inst_id.contains("DOGE") && !inst_id.contains("BABY") && !inst_id.contains("POLY") && !inst_id.contains("AI") {
                        debug!("📊 OKX Volume for {}: {:.2} USDT (using volCcy24h)", inst_id, volume);
                    }
                    volume_map.insert(inst_id.to_string(), volume);
                }
                
                // Calculate spread percentage
                if let (Some(bid_str), Some(ask_str)) = (
                    ticker["bidPx"].as_str(),
                    ticker["askPx"].as_str()
                ) {
                    if let (Ok(bid), Ok(ask)) = (bid_str.parse::<f64>(), ask_str.parse::<f64>()) {
                        if bid > 0.0 && ask > 0.0 {
                            let spread_pct = ((ask - bid) / ask) * 100.0;
                            spread_map.insert(inst_id.to_string(), spread_pct);
                        }
                    }
                }
                
                // Store bid/ask sizes
                if let Some(bid_size_str) = ticker["bidSz"].as_str() {
                    if let Ok(bid_size) = bid_size_str.parse::<f64>() {
                        bid_size_map.insert(inst_id.to_string(), bid_size);
                    }
                }
                if let Some(ask_size_str) = ticker["askSz"].as_str() {
                    if let Ok(ask_size) = ask_size_str.parse::<f64>() {
                        ask_size_map.insert(inst_id.to_string(), ask_size);
                    }
                }
            }
        }

        Ok((volume_map, HashMap::new(), spread_map, bid_size_map, ask_size_map))  // OKX doesn't provide trade counts
    }

    /// Fetch all volumes, spreads and market depth from Coinbase
    pub async fn fetch_coinbase_volumes(&self) -> Result<(HashMap<String, f64>, HashMap<String, u64>, HashMap<String, f64>, HashMap<String, f64>, HashMap<String, f64>)> {
        let response = self.client
            .get("https://api.exchange.coinbase.com/products/volume-summary")
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_else(|_| "Failed to read body".to_string());
            tracing::error!("Coinbase volume API error: {} - Body: {}", status, body);
            return Err(anyhow!("Coinbase API error: {} - {}", status, body));
        }

        #[derive(Deserialize)]
        struct VolumeItem {
            id: String,
            #[serde(rename = "spot_volume_24hour")]
            spot_volume_24h: Option<String>,
            #[serde(rename = "rfq_volume_24hour")]
            rfq_volume_24h: Option<String>,
            #[serde(rename = "conversion_volume_24hour")]
            conversion_volume_24h: Option<String>,
        }

        let items: Vec<VolumeItem> = response.json().await?;
        let mut volume_map = HashMap::new();

        for item in items {
            // Combine all volume types (spot + rfq + conversion)
            let mut total_volume = 0.0;
            
            if let Some(vol_str) = &item.spot_volume_24h {
                if let Ok(vol) = vol_str.parse::<f64>() {
                    total_volume += vol;
                }
            }
            
            if let Some(vol_str) = &item.rfq_volume_24h {
                if let Ok(vol) = vol_str.parse::<f64>() {
                    total_volume += vol;
                }
            }
            
            if let Some(vol_str) = &item.conversion_volume_24h {
                if let Ok(vol) = vol_str.parse::<f64>() {
                    total_volume += vol;
                }
            }
            
            // Only include if there's any volume
            if total_volume > 0.0 {
                volume_map.insert(item.id, total_volume);
            }
        }

        // Coinbase volume endpoint doesn't provide bid/ask data
        // We'd need to use the order book endpoint for each pair (too many API calls)
        Ok((volume_map, HashMap::new(), HashMap::new(), HashMap::new(), HashMap::new()))
    }

    /// Fetch all volumes, spreads and market depth from Bitget
    pub async fn fetch_bitget_volumes(&self) -> Result<(HashMap<String, f64>, HashMap<String, u64>, HashMap<String, f64>, HashMap<String, f64>, HashMap<String, f64>)> {
        let response = self.client
            .get("https://api.bitget.com/api/v2/spot/market/tickers")
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(anyhow!("Bitget API error: {}", response.status()));
        }

        #[derive(Deserialize)]
        struct BitgetResponse {
            code: String,
            msg: String,
            data: Vec<BitgetTicker>,
        }

        #[derive(Deserialize)]
        struct BitgetTicker {
            symbol: String,
            #[serde(rename = "usdtVolume")]
            usdt_volume: String,
            #[serde(rename = "bidPr")]
            bid_price: String,
            #[serde(rename = "askPr")]
            ask_price: String,
            #[serde(rename = "bidSz")]
            bid_size: Option<String>,  // Can be null for some symbols
            #[serde(rename = "askSz")]
            ask_size: Option<String>,  // Can be null for some symbols
        }

        let data: BitgetResponse = response.json().await?;
        
        if data.code != "00000" {
            return Err(anyhow!("Bitget API error: {} - {}", data.code, data.msg));
        }

        let mut volume_map = HashMap::new();
        let mut spread_map = HashMap::new();
        let mut bid_size_map = HashMap::new();
        let mut ask_size_map = HashMap::new();
        
        for ticker in data.data {
            if let Ok(volume) = ticker.usdt_volume.parse::<f64>() {
                volume_map.insert(ticker.symbol.clone(), volume);
            }
            
            // Calculate spread percentage
            if let (Ok(bid), Ok(ask)) = (ticker.bid_price.parse::<f64>(), ticker.ask_price.parse::<f64>()) {
                if bid > 0.0 && ask > 0.0 {
                    let spread_pct = ((ask - bid) / ask) * 100.0;
                    spread_map.insert(ticker.symbol.clone(), spread_pct);
                }
            }
            
            // Store bid/ask sizes (if present)
            if let Some(bid_size_str) = ticker.bid_size {
                if let Ok(bid_size) = bid_size_str.parse::<f64>() {
                    bid_size_map.insert(ticker.symbol.clone(), bid_size);
                }
            }
            if let Some(ask_size_str) = ticker.ask_size {
                if let Ok(ask_size) = ask_size_str.parse::<f64>() {
                    ask_size_map.insert(ticker.symbol.clone(), ask_size);
                }
            }
        }

        Ok((volume_map, HashMap::new(), spread_map, bid_size_map, ask_size_map))  // Bitget doesn't provide trade counts  // Bitget doesn't provide trade counts
    }

    /// Fetch all volumes, spreads and market depth from Gate.io
    pub async fn fetch_gate_volumes(&self) -> Result<(HashMap<String, f64>, HashMap<String, u64>, HashMap<String, f64>, HashMap<String, f64>, HashMap<String, f64>)> {
        let response = self.client
            .get("https://api.gateio.ws/api/v4/spot/tickers")
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(anyhow!("Gate.io API error: {}", response.status()));
        }

        #[derive(Deserialize)]
        struct GateTicker {
            currency_pair: String,
            #[serde(default)]
            quote_volume: String,
            #[serde(default)]
            lowest_ask: String,
            #[serde(default)]
            highest_bid: String,
        }

        let tickers: Vec<GateTicker> = response.json().await?;
        let mut volume_map = HashMap::new();
        let mut spread_map = HashMap::new();
        let mut bid_size_map = HashMap::new();
        let mut ask_size_map = HashMap::new();

        for ticker in tickers {
            // Volume
            if let Ok(volume) = ticker.quote_volume.parse::<f64>() {
                volume_map.insert(ticker.currency_pair.clone(), volume);
            }

            // Calculate spread percentage
            if let (Ok(bid), Ok(ask)) = (ticker.highest_bid.parse::<f64>(), ticker.lowest_ask.parse::<f64>()) {
                if bid > 0.0 && ask > 0.0 {
                    let spread_pct = ((ask - bid) / ask) * 100.0;
                    spread_map.insert(ticker.currency_pair.clone(), spread_pct);
                }
            }

            // Gate.io tickers don't include bid/ask sizes directly
            // We'd need the order book endpoint for depth data
        }

        Ok((volume_map, HashMap::new(), spread_map, bid_size_map, ask_size_map))  // Gate.io doesn't provide trade counts in ticker endpoint
    }
}

/// Get current timestamp in milliseconds
fn get_timestamp_ms() -> u64 {
    crate::clock_sync::get_corrected_timestamp_ms()
}