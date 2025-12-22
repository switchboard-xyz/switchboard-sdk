use anyhow::{anyhow, Result};
use dashmap::DashMap;
use once_cell::sync::Lazy;
use reqwest::Client;
use rust_decimal::Decimal;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tokio::time::interval;
use tracing::{debug, error, info, warn};

use crate::{hub::SurgeHub, Pair, Source, Tick};
use crate::exchanges::pyth::PYTH_PRICE_FEEDS;

/// Global REST manager instance
static REST_MANAGER: Lazy<RestManager> = Lazy::new(RestManager::new);

/// REST polling interval (2 seconds)
const REST_POLL_INTERVAL: Duration = Duration::from_secs(2);

/// Backoff duration for hard rate limits (418 errors)
const RATE_LIMIT_BACKOFF_SECS: u64 = 5; // 5 seconds

/// REST manager for symbols not in top 200 by volume
#[derive(Debug)]
pub struct RestManager {
    /// HTTP client for API requests
    client: Client,
    /// Symbols to poll via REST (per exchange)
    rest_symbols: Arc<RwLock<DashMap<Source, HashSet<String>>>>,
    /// Track rate limit status per exchange
    rate_limited_until: Arc<RwLock<DashMap<Source, Instant>>>,
}

#[derive(Deserialize, Debug)]
struct BinanceTickerPrice {
    symbol: String,
    price: String,
}

#[derive(Deserialize, Debug)]
struct BinanceServerTime {
    #[serde(rename = "serverTime")]
    server_time: u64,
}

#[derive(Deserialize, Debug)]
struct BybitTickerResponse {
    #[serde(rename = "retCode")]
    ret_code: i32,
    result: BybitTickerResult,
    time: u64,
}

#[derive(Deserialize, Debug)]
struct BybitTickerResult {
    list: Vec<BybitTicker>,
}

#[derive(Deserialize, Debug)]
struct BybitTicker {
    symbol: String,
    #[serde(rename = "lastPrice")]
    last_price: String,
}

#[derive(Deserialize, Debug)]
struct OkxTickerResponse {
    code: String,
    data: Vec<OkxTicker>,
}

#[derive(Deserialize, Debug)]
struct OkxTicker {
    #[serde(rename = "instId")]
    inst_id: String,
    last: String,
    #[serde(rename = "ts")]
    timestamp: String,
}

#[derive(Deserialize, Debug)]
struct PythPriceFeed {
    id: String,
    price: PythPrice,
}

#[derive(Deserialize, Debug)]
struct PythPrice {
    price: String,
    conf: String,
    expo: i32,
    publish_time: u64,
}

impl RestManager {
    fn new() -> Self {
        Self {
            client: Client::builder()
                .timeout(Duration::from_secs(15))
                .build()
                .expect("Failed to create HTTP client"),
            rest_symbols: Arc::new(RwLock::new(DashMap::new())),
            rate_limited_until: Arc::new(RwLock::new(DashMap::new())),
        }
    }

    /// Get the global REST manager instance
    pub fn global() -> &'static RestManager {
        &REST_MANAGER
    }

    /// Set the symbols to poll via REST for a specific exchange
    pub async fn set_rest_symbols(&self, source: Source, symbols: HashSet<String>) {
        let rest_symbols = self.rest_symbols.write().await;
        if symbols.is_empty() {
            rest_symbols.remove(&source);
        } else {
            rest_symbols.insert(source, symbols);
        }
        
        let symbol_count = rest_symbols.get(&source).map(|s| s.value().len()).unwrap_or(0);
        
        info!("REST Manager: Set {} symbols for {} to poll via REST", 
            symbol_count, source.as_str());
        
        // Warn if setting up a large number of symbols
        if symbol_count > 100 {
            warn!("REST Manager: Large number of symbols ({}) for {} - may cause rate limiting", 
                symbol_count, source.as_str());
        }
    }
    
    /// Add symbols to REST polling (used by connection health monitor)
    pub async fn add_symbols_temporarily(&self, source: Source, symbols: &HashSet<String>) {
        let rest_symbols = self.rest_symbols.read().await;
        if let Some(mut entry) = rest_symbols.get_mut(&source) {
            entry.value_mut().extend(symbols.iter().cloned());
            info!("REST Manager: Added {} temporary symbols for {} (total: {})", 
                symbols.len(), source.as_str(), entry.value().len());
        };
    }
    
    /// Remove symbols from REST polling (when WebSocket reconnects)
    pub async fn remove_symbols(&self, source: Source, symbols: &HashSet<String>) {
        let rest_symbols = self.rest_symbols.read().await;
        if let Some(mut entry) = rest_symbols.get_mut(&source) {
            for symbol in symbols {
                entry.value_mut().remove(symbol);
            }
            info!("REST Manager: Removed {} symbols for {} (remaining: {})", 
                symbols.len(), source.as_str(), entry.value().len());
        };
    }

    /// Start the REST polling loop
    pub async fn start_polling(&self) {
        let mut poll_interval = interval(REST_POLL_INTERVAL);
        let mut first_poll = true;
        
        loop {
            poll_interval.tick().await;
            
            if first_poll {
                info!("🏁 REST Manager: Starting to poll prices every {}ms", REST_POLL_INTERVAL.as_millis());
                first_poll = false;
            }
            
            // Note: tokio::time::interval already enforces REST_POLL_INTERVAL timing
            
            // Poll each exchange
            let rest_symbols = self.rest_symbols.read().await;
            for entry in rest_symbols.iter() {
                let source = *entry.key();
                let symbols = entry.value().clone();
                
                if !symbols.is_empty() {
                    // Check if this exchange is rate limited
                    {
                        let rate_limited = self.rate_limited_until.read().await;
                        if let Some(until) = rate_limited.get(&source) {
                            if Instant::now() < *until.value() {
                                debug!("REST: Skipping {} - rate limited for {:?} more", 
                                    source.as_str(), 
                                    until.value().duration_since(Instant::now())
                                );
                                continue;
                            }
                        };
                    }
                    
                    match self.poll_exchange(source, &symbols).await {
                        Ok(_) => {
                            // Clear any rate limit status on success
                            self.rate_limited_until.write().await.remove(&source);
                        },
                        Err(e) => {
                            let error_str = e.to_string();
                            error!("REST poll failed for {}: {}", source.as_str(), e);
                            
                            // Handle different error types
                            if error_str.contains("418") {
                                // Hard rate limit - back off for 5 minutes
                                let until = Instant::now() + Duration::from_secs(RATE_LIMIT_BACKOFF_SECS);
                                self.rate_limited_until.write().await.insert(source, until);
                                error!("REST: {} returned 418 (hard rate limit) - backing off for {} seconds", 
                                    source.as_str(), RATE_LIMIT_BACKOFF_SECS);
                            } else if error_str.contains("403") || error_str.contains("429") {
                                // Soft rate limit - back off for 5 seconds
                                let until = Instant::now() + Duration::from_secs(5);
                                self.rate_limited_until.write().await.insert(source, until);
                                warn!("REST: {} rate limited - backing off for 5 seconds", source.as_str());
                            }
                        }
                    }
                }
            }
        }
    }
    
    /// Poll prices for a specific exchange
    async fn poll_exchange(&self, source: Source, symbols: &HashSet<String>) -> Result<()> {
        match source {
            Source::Binance => self.poll_binance(symbols).await,
            Source::Bybit => self.poll_bybit(symbols).await,
            Source::Okx => self.poll_okx(symbols).await,
            Source::Coinbase => self.poll_coinbase(symbols).await,
            Source::Bitget => self.poll_bitget(symbols).await,
            Source::Pyth => self.poll_pyth(symbols).await,
            Source::Titan => {
                // Titan uses its own polling mechanism
                Ok(())
            },
            Source::Weighted => {
                // Weighted sources don't have REST endpoints
                Ok(())
            },
             Source::Auto => {
                // Auto sources don't have REST endpoints
                Ok(())
            }
        }
    }
    
    /// Poll Binance REST API for all symbols at once
    async fn poll_binance(&self, symbols: &HashSet<String>) -> Result<()> {
        debug!("REST: Polling Binance for {} symbols", symbols.len());
        
        // Fetch all ticker prices and server time in parallel
        let (price_response, time_response) = tokio::try_join!(
            self.client
                .get("https://api.binance.com/api/v3/ticker/price")
                .send(),
            self.client
                .get("https://api.binance.com/api/v3/time")
                .send()
        )?;
        
        if !price_response.status().is_success() {
            return Err(anyhow!("Binance price API error: {}", price_response.status()));
        }
        
        if !time_response.status().is_success() {
            return Err(anyhow!("Binance time API error: {}", time_response.status()));
        }
        
        let price_data: Vec<BinanceTickerPrice> = price_response.json().await?;
        let time_data: BinanceServerTime = time_response.json().await?;
        
        // Symbols are already in Binance format (e.g., "BTCUSDT")
        let binance_symbols = symbols.clone();
        
        // Get the symbol-to-pair mapping from cached exchange info
        let binance_mapping = crate::exchanges::binance::get_cached_pairs()
            .unwrap_or_else(|_| {
                error!("Binance exchange info not cached! REST manager may miss symbols.");
                Vec::new()
            });
        
        // Convert to HashMap for O(1) lookup
        let symbol_to_pair: HashMap<String, Pair> = binance_mapping
            .into_iter()
            .map(|pair| (pair.as_binance_str(), pair))
            .collect();
        
        // Update cache
        let hub = SurgeHub::global();
        let received_at = crate::clock_sync::get_corrected_timestamp_ms();
        let mut updated_count = 0;
        let mut unmapped_count = 0;
        let mut skipped_websocket = 0;
        
        for ticker in &price_data {
            if binance_symbols.contains(&ticker.symbol) {
                // Look up in our mapping
                if let Some(pair) = symbol_to_pair.get(&ticker.symbol) {
                    // Use exact same parsing as WebSocket ingestion
                    if let Ok(price) = ticker.price.parse::<Decimal>() {
                        let tick = Tick {
                            price,
                            event_ts: time_data.server_time,  // Binance server time (authoritative)
                            seen_at: received_at,              // Our local time
                            verified_at: received_at,          // Same as seen_at initially
                        };
                        
                        // Handle WebSocket vs REST logic
                        if hub.is_websocket_active(Source::Binance, pair) {
                            // WebSocket is active - only update verified_at if conditions met
                            if let Ok(Some(existing)) = hub.latest(Source::Binance, pair) {
                                // Only update verified_at if:
                                // 1. Price matches (data validity confirmed)
                                // 2. REST timestamp is more recent than cached verified_at
                                if existing.price == price && received_at > existing.verified_at {
                                    hub.update_verified_at(Source::Binance, pair, received_at);
                                    debug!("REST updated verified_at for WS symbol {} ({} -> {})",
                                           pair.as_str(), existing.verified_at, received_at);
                                }
                            }
                            skipped_websocket += 1;
                        } else {
                            // No WebSocket - REST fully manages this symbol
                            hub.update(Source::Binance, pair.clone(), tick).await;
                            updated_count += 1;
                        }
                    }
                } else {
                    unmapped_count += 1;
                    if ticker.symbol.starts_with("BNB") {
                        warn!("❌ BNB REST NO MAPPING: {} not found in symbol_to_pair", ticker.symbol);
                    }
                }
            }
        }
        
        if unmapped_count > 0 {
            static LOGGED: Lazy<AtomicBool> = Lazy::new(|| AtomicBool::new(false));
            
            if !LOGGED.load(Ordering::Relaxed) {
                warn!("REST: {} Binance symbols have no mapping in exchange info", unmapped_count);
                LOGGED.store(true, Ordering::Relaxed);
            }
        }
        Ok(())
    }
    
    /// Poll Bybit REST API for all symbols at once
    async fn poll_bybit(&self, symbols: &HashSet<String>) -> Result<()> {
        debug!("REST: Polling Bybit for {} symbols", symbols.len());
        
        // Bybit public API endpoint for spot tickers
        let response = self.client
            .get("https://api.bybit.com/v5/market/tickers?category=spot")
            .send()
            .await?;
        
        if !response.status().is_success() {
            return Err(anyhow!("Bybit API error: {}", response.status()));
        }
        
        let ticker_data: BybitTickerResponse = response.json().await?;
        
        if ticker_data.ret_code != 0 {
            return Err(anyhow!("Bybit API returned error code: {}", ticker_data.ret_code));
        }
        
        // Symbols are in Bybit format (e.g., "BTCUSDT")
        let bybit_symbols = symbols.clone();
        
        // Get the symbol-to-pair mapping from cached exchange info
        let bybit_mapping = crate::exchanges::bybit::get_cached_pairs()
            .unwrap_or_else(|_| {
                error!("Bybit exchange info not cached! REST manager may miss symbols.");
                Vec::new()
            });
        
        // Convert to HashMap for O(1) lookup
        let symbol_to_pair: HashMap<String, Pair> = bybit_mapping
            .into_iter()
            .map(|pair| (pair.as_bybit_str(), pair))
            .collect();
        
        // Update cache
        let hub = SurgeHub::global();
        let received_at = crate::clock_sync::get_corrected_timestamp_ms();
        let mut updated_count = 0;
        let mut skipped_websocket = 0;
        
        for ticker in ticker_data.result.list {
            if bybit_symbols.contains(&ticker.symbol) {
                // Look up in our mapping
                if let Some(pair) = symbol_to_pair.get(&ticker.symbol) {
                    // Use exact same parsing as WebSocket ingestion
                    if let Ok(price) = ticker.last_price.parse::<Decimal>() {
                        let tick = Tick {
                            price,
                            event_ts: ticker_data.time,  // Bybit server time
                            seen_at: received_at,         // Our local time
                            verified_at: received_at,     // Same as seen_at initially
                        };
                        
                        // Handle WebSocket vs REST logic
                        if hub.is_websocket_active(Source::Bybit, pair) {
                            // WebSocket is active - only update verified_at if conditions met
                            if let Ok(Some(existing)) = hub.latest(Source::Bybit, pair) {
                                // Only update verified_at if:
                                // 1. Price matches (data validity confirmed)
                                // 2. REST timestamp is more recent than cached verified_at
                                if existing.price == price && received_at > existing.verified_at {
                                    hub.update_verified_at(Source::Bybit, pair, received_at);
                                    debug!("REST updated verified_at for WS symbol {} ({} -> {})",
                                           pair.as_str(), existing.verified_at, received_at);
                                }
                            }
                            skipped_websocket += 1;
                        } else {
                            // No WebSocket - REST fully manages this symbol
                            hub.update(Source::Bybit, pair.clone(), tick).await;
                            updated_count += 1;
                        }
                    }
                }
            }
        }
        
        debug!("REST: Bybit updated {} prices (skipped {} WebSocket-active)", 
            updated_count, skipped_websocket);
        
        Ok(())
    }
    
    /// Poll OKX REST API for all symbols at once
    async fn poll_okx(&self, symbols: &HashSet<String>) -> Result<()> {
        debug!("REST: Polling OKX for {} symbols", symbols.len());
        
        // OKX public API endpoint for tickers
        let response = self.client
            .get("https://www.okx.com/api/v5/market/tickers?instType=SPOT")
            .send()
            .await?;
        
        if !response.status().is_success() {
            return Err(anyhow!("OKX API error: {}", response.status()));
        }
        
        let ticker_data: OkxTickerResponse = response.json().await?;
        
        if ticker_data.code != "0" {
            return Err(anyhow!("OKX API returned error code: {}", ticker_data.code));
        }
        
        // Symbols are in OKX format (e.g., "BTC-USDT")
        let okx_symbols = symbols.clone();
        
        // Get the symbol-to-pair mapping from cached exchange info
        let okx_mapping = crate::exchanges::okx::get_cached_pairs()
            .unwrap_or_else(|_| {
                error!("OKX exchange info not cached! REST manager may miss symbols.");
                Vec::new()
            });
        
        // Convert to HashMap for O(1) lookup
        let symbol_to_pair: HashMap<String, Pair> = okx_mapping
            .into_iter()
            .map(|pair| (pair.as_okx_str(), pair))
            .collect();
        
        // Update cache
        let hub = SurgeHub::global();
        let received_at = crate::clock_sync::get_corrected_timestamp_ms();
        let mut updated_count = 0;
        let mut skipped_websocket = 0;
        
        for ticker in ticker_data.data {
            if okx_symbols.contains(&ticker.inst_id) {
                // Look up in our mapping
                if let Some(pair) = symbol_to_pair.get(&ticker.inst_id) {
                    // Use exact same parsing as WebSocket ingestion
                    if let Ok(price) = ticker.last.parse::<Decimal>() {
                        // Parse timestamp from string
                        let event_ts = ticker.timestamp.parse::<u64>().unwrap_or_else(|_| {
                            warn!("Failed to parse OKX timestamp: {}", ticker.timestamp);
                            received_at
                        });
                        
                        let tick = Tick {
                            price,
                            event_ts,
                            seen_at: received_at,
                            verified_at: received_at,
                        };
                        
                        // Handle WebSocket vs REST logic
                        if hub.is_websocket_active(Source::Okx, pair) {
                            // WebSocket is active - only update verified_at if conditions met
                            if let Ok(Some(existing)) = hub.latest(Source::Okx, pair) {
                                // Only update verified_at if:
                                // 1. Price matches (data validity confirmed)
                                // 2. REST timestamp is more recent than cached verified_at
                                if existing.price == price && received_at > existing.verified_at {
                                    hub.update_verified_at(Source::Okx, pair, received_at);
                                    debug!("REST updated verified_at for WS symbol {} ({} -> {})",
                                           pair.as_str(), existing.verified_at, received_at);
                                }
                            }
                            skipped_websocket += 1;
                        } else {
                            // No WebSocket - REST fully manages this symbol
                            hub.update(Source::Okx, pair.clone(), tick).await;
                            updated_count += 1;
                        }
                    }
                }
            }
        }
        
        debug!("REST: OKX updated {} prices (skipped {} WebSocket-active)", 
            updated_count, skipped_websocket);
        
        Ok(())
    }
    
    /// Poll Coinbase REST API for all symbols at once
    async fn poll_coinbase(&self, _symbols: &HashSet<String>) -> Result<()> {
        // TODO: Implement Coinbase REST polling
        Ok(())
    }
    
    /// Poll Bitget REST API for all symbols at once
    async fn poll_bitget(&self, symbols: &HashSet<String>) -> Result<()> {
        debug!("REST: Polling Bitget for {} symbols", symbols.len());
        
        // Bitget public API endpoint for spot tickers
        let response = self.client
            .get("https://api.bitget.com/api/v2/spot/market/tickers")
            .send()
            .await?;
        
        if !response.status().is_success() {
            return Err(anyhow!("Bitget API error: {}", response.status()));
        }
        
        #[derive(Deserialize)]
        struct BitgetTickerResponse {
            code: String,
            msg: String,
            data: Vec<BitgetTicker>,
        }
        
        #[derive(Deserialize)]
        struct BitgetTicker {
            symbol: String,
            #[serde(rename = "lastPr")]
            last_price: String,
            ts: String,
        }
        
        let ticker_data: BitgetTickerResponse = response.json().await?;
        
        if ticker_data.code != "00000" {
            return Err(anyhow!("Bitget API returned error: {} - {}", ticker_data.code, ticker_data.msg));
        }
        
        // Symbols are in Bitget format (e.g., "BTCUSDT")
        let bitget_symbols = symbols.clone();
        
        // Get the symbol-to-pair mapping from cached exchange info
        let bitget_mapping = crate::exchanges::bitget::get_cached_pairs()
            .unwrap_or_else(|_| {
                error!("Bitget exchange info not cached! REST manager may miss symbols.");
                Vec::new()
            });
        
        // Convert to HashMap for O(1) lookup
        let symbol_to_pair: HashMap<String, Pair> = bitget_mapping
            .into_iter()
            .map(|pair| (pair.as_bitget_str(), pair))
            .collect();
        
        // Update cache
        let hub = crate::hub::SurgeHub::global();
        let received_at = crate::clock_sync::get_corrected_timestamp_ms();
        let mut updated_count = 0;
        let mut skipped_websocket = 0;
        
        for ticker in ticker_data.data {
            if bitget_symbols.contains(&ticker.symbol) {
                // Look up in our mapping
                if let Some(pair) = symbol_to_pair.get(&ticker.symbol) {
                    // Use exact same parsing as WebSocket ingestion
                    if let Ok(price) = ticker.last_price.parse::<Decimal>() {
                        if let Ok(timestamp) = ticker.ts.parse::<u64>() {
                            let tick = Tick {
                                price,
                                event_ts: timestamp,
                                seen_at: received_at,
                                verified_at: received_at,
                            };
                            
                            // Handle WebSocket vs REST logic
                            if hub.is_websocket_active(Source::Bitget, pair) {
                                // WebSocket is active - only update verified_at if conditions met
                                if let Ok(Some(existing)) = hub.latest(Source::Bitget, pair) {
                                    // Only update verified_at if:
                                    // 1. Price matches (data validity confirmed)
                                    // 2. REST timestamp is more recent than cached verified_at
                                    if existing.price == price && received_at > existing.verified_at {
                                        hub.update_verified_at(Source::Bitget, pair, received_at);
                                        debug!("REST updated verified_at for WS symbol {} ({} -> {})",
                                               pair.as_str(), existing.verified_at, received_at);
                                    }
                                }
                                skipped_websocket += 1;
                            } else {
                                // No WebSocket - REST fully manages this symbol
                                hub.update(Source::Bitget, pair.clone(), tick).await;
                                updated_count += 1;
                            }
                        }
                    }
                }
            }
        }
        
        debug!("REST: Bitget updated {} prices (skipped {} WebSocket-active)",
            updated_count, skipped_websocket);

        Ok(())
    }

    /// Poll Pyth REST API for price feeds
    async fn poll_pyth(&self, symbols: &HashSet<String>) -> Result<()> {
        debug!("REST: Polling Pyth for {} feed IDs", symbols.len());

        if symbols.is_empty() {
            return Ok(());
        }

        // Build query string with all feed IDs
        // Format: https://hermes.pyth.network/api/latest_price_feeds?ids[]=FEED_ID1&ids[]=FEED_ID2
        let mut url = String::from("https://hermes.pyth.network/api/latest_price_feeds");
        let mut first = true;

        for feed_id in symbols {
            if first {
                url.push_str("?ids[]=");
                first = false;
            } else {
                url.push_str("&ids[]=");
            }
            url.push_str(feed_id);
        }

        let response = self.client
            .get(&url)
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(anyhow!("Pyth API error: {}", response.status()));
        }

        let price_feeds: Vec<PythPriceFeed> = response.json().await?;

        let hub = SurgeHub::global();
        let received_at = crate::clock_sync::get_corrected_timestamp_ms();
        let mut updated_count = 0;
        let mut skipped_websocket = 0;

        for feed in price_feeds {
            // Find the pair for this feed ID from PYTH_PRICE_FEEDS
            let pair_opt = PYTH_PRICE_FEEDS.iter()
                .find(|entry| entry.value().1 == feed.id)
                .map(|entry| entry.value().0.clone());

            if let Some(pair) = pair_opt {
                // Parse price from string with exponent
                if let Ok(price_raw) = feed.price.price.parse::<i64>() {
                    // Apply exponent: price = price_raw * 10^expo
                    // For expo=-8: price = price_raw / 100000000
                    let price = if feed.price.expo < 0 {
                        Decimal::from(price_raw) / Decimal::from(10i64.pow(feed.price.expo.unsigned_abs()))
                    } else {
                        Decimal::from(price_raw) * Decimal::from(10i64.pow(feed.price.expo as u32))
                    };

                    // Convert Unix timestamp (seconds) to milliseconds
                    let timestamp_ms = feed.price.publish_time * 1000;

                    let tick = Tick {
                        price,
                        event_ts: timestamp_ms,
                        seen_at: received_at,
                        verified_at: received_at,
                    };

                    // Handle WebSocket vs REST logic
                    if hub.is_websocket_active(Source::Pyth, &pair) {
                        // WebSocket is active - only update verified_at if conditions met
                        if let Ok(Some(existing)) = hub.latest(Source::Pyth, &pair) {
                            // Only update verified_at if:
                            // 1. Price matches (data validity confirmed)
                            // 2. REST timestamp is more recent than cached verified_at
                            if existing.price == price && received_at > existing.verified_at {
                                hub.update_verified_at(Source::Pyth, &pair, received_at);
                                debug!("REST updated verified_at for WS feed {} ({} -> {})",
                                       pair.as_str(), existing.verified_at, received_at);
                            }
                        }
                        skipped_websocket += 1;
                    } else {
                        // No WebSocket - REST fully manages this feed
                        hub.update(Source::Pyth, pair.clone(), tick).await;
                        updated_count += 1;

                        // Log Pyth price updates for visibility
                        debug!("🟣 REST Pyth: Updated {} = ${} (via REST API, feed_id: {})",
                            pair.as_str(), price, &feed.id[..8]);
                    }
                }
            } else {
                debug!("Pyth feed ID {} not found in PYTH_PRICE_FEEDS", feed.id);
            }
        }

        debug!("REST: Pyth updated {} prices (skipped {} WebSocket-active)",
            updated_count, skipped_websocket);

        Ok(())
    }
}

/// Start the global REST manager
pub async fn start_rest_manager() {
    info!("🚀 Starting REST manager for non-WebSocket symbols");
    RestManager::global().start_polling().await;
}