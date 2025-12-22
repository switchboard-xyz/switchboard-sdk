use crate::{exchange_config::get_allowed_quotes, pair::Pair, traits::TickerStream, types::{Source, Ticker}, exchanges::connection_state::ConnectionState};
use anyhow::{anyhow, Result};
use dashmap::DashMap;
use futures::Stream;
use once_cell::sync::Lazy;
use serde::Deserialize;
use std::collections::HashSet;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::{mpsc, Mutex};
use tracing::{error, info, warn, debug};


// Global cache for Binance exchange info
static BINANCE_EXCHANGE_INFO: Lazy<Arc<DashMap<String, Pair>>> = Lazy::new(|| Arc::new(DashMap::new()));

// Top symbols count for WebSocket (rest use REST API)
const TOP_SYMBOLS_COUNT: usize = 220;

/// Get all cached Binance pairs
pub fn get_cached_pairs() -> Result<Vec<Pair>> {
    if BINANCE_EXCHANGE_INFO.is_empty() {
        return Err(anyhow!("Exchange info not fetched yet"));
    }
    
    let pairs: Vec<Pair> = BINANCE_EXCHANGE_INFO
        .iter()
        .map(|entry| entry.value().clone())
        .collect();
    
    Ok(pairs)
}

use super::binance_connection::{ConnectionPair, PRIORITY_CONNECTION_SYMBOLS, HIGH_VOLUME_CONNECTION_SYMBOLS, REGULAR_CONNECTION_SYMBOLS};

pub struct BinanceStream {
    connection_pairs: Vec<Arc<Mutex<ConnectionPair>>>,
    rx: mpsc::UnboundedReceiver<Result<Ticker>>,
    tx: mpsc::UnboundedSender<Vec<Ticker>>,
    symbol_to_pair_cache: Arc<DashMap<String, Pair>>,
    connection_state: ConnectionState,
}

impl BinanceStream {
    pub async fn new() -> Result<Self> {
        let (tx, rx) = mpsc::unbounded_channel();
        
        // Create internal tx/rx for aggregating from multiple connections
        let (internal_tx, mut internal_rx) = mpsc::unbounded_channel::<Vec<Ticker>>();
        
        // Spawn aggregator task
        let external_tx = tx.clone();
        let symbol_to_pair_cache = BINANCE_EXCHANGE_INFO.clone();
        let symbol_to_pair_cache_for_spawn = symbol_to_pair_cache.clone();

        crate::runtime_separation::spawn_on_ingestion_named("binance-aggregator", async move {
            while let Some(tickers) = internal_rx.recv().await {
                for ticker in tickers {
                    // Process each ticker
                    let _ = external_tx.send(Ok(ticker.clone()));
                    if let Err(e) = process_ticker(&symbol_to_pair_cache_for_spawn, ticker).await {
                        error!("Failed to process ticker: {:?}", e);
                    }
                }
            }
        });
        
        let mut stream = Self {
            connection_pairs: Vec::new(),
            rx,
            tx: internal_tx,
            symbol_to_pair_cache,
            connection_state: ConnectionState::new(),
        };
        stream.connect().await?;
        Ok(stream)
    }
    
    /// Pre-fetch exchange info without connecting WebSocket
    pub async fn prefetch_exchange_info() -> Result<()> {
        if BINANCE_EXCHANGE_INFO.is_empty() {
            let _ = Self::fetch_and_cache_exchange_info().await?;
        }
        Ok(())
    }
    
    /// Refresh exchange info to discover newly listed tokens
    /// Returns the number of new symbols added
    pub async fn refresh_exchange_info() -> Result<usize> {
        Self::fetch_and_cache_exchange_info().await
    }

    async fn connect(&mut self) -> Result<()> {
        info!("Creating Binance WebSocket connections");
        
        // Get top symbols by volume AND all symbols
        let (websocket_symbols, _) = Self::select_top_symbols().await?;
        
        // Initially, REST should poll ALL symbols
        let all_symbols = Self::get_all_symbols().await?;
        let all_symbols_set: HashSet<String> = all_symbols.into_iter().collect();
        
        info!("📊 Binance startup: REST polling {} symbols initially, WebSocket will handle {} high-volume symbols", 
            all_symbols_set.len(), websocket_symbols.len());
        
        // Set up REST polling for ALL symbols initially
        let rest_manager = crate::rest_manager::RestManager::global();
        rest_manager.set_rest_symbols(Source::Binance, all_symbols_set).await;
        
        // Priority tickers
        let priority_tickers = vec![
            "BTCUSDT", "BTCFDUSD", "SOLUSDT", "SOLFDUSD",
            "BNBUSDT", "BNBFDUSD", "ETHUSDT", "ETHFDUSD",
            "XRPUSDT", "XRPFDUSD"
        ];
        
        let mut symbols_used = 0;
        
        // Connection 1: Priority symbols (up to PRIORITY_CONNECTION_SYMBOLS)
        let mut priority_symbols = HashSet::new();
        for ticker in &priority_tickers {
            if websocket_symbols.contains(&ticker.to_string()) && priority_symbols.len() < PRIORITY_CONNECTION_SYMBOLS {
                priority_symbols.insert(ticker.to_string());
                symbols_used += 1;
            }
        }
        
        if !priority_symbols.is_empty() {
            // DEBUG: Check if BTC/FDUSD is in priority
            if priority_symbols.contains("BTCFDUSD") {
                warn!("🔍 BTC/FDUSD assigned to Priority connection");
            }
            
            let pair = Arc::new(Mutex::new(ConnectionPair::new(
                "Priority".to_string(),
                priority_symbols.clone(),
                self.symbol_to_pair_cache.clone(),
                self.tx.clone(),
            )));
            self.connection_pairs.push(pair);

            // Register in global connection metadata cache
            crate::hub::CONNECTION_METADATA_CACHE.insert(
                (Source::Binance, "Priority".to_string()),
                crate::hub::ConnectionMetadata {
                    symbols: priority_symbols.iter().cloned().collect(),
                    is_connected: false,
                    last_updated: crate::clock_sync::get_corrected_timestamp_ms(),
                }
            );

            info!("Created Priority connection with {} symbols", symbols_used);
        }
        
        // Connection 2: High-volume symbols (next HIGH_VOLUME_CONNECTION_SYMBOLS)
        let mut high_volume_symbols = HashSet::new();
        let mut count = 0;
        for symbol in &websocket_symbols {
            if !priority_tickers.contains(&symbol.as_str()) && count < HIGH_VOLUME_CONNECTION_SYMBOLS {
                high_volume_symbols.insert(symbol.clone());
                count += 1;
            }
        }
        
        if !high_volume_symbols.is_empty() {
            // DEBUG: Check if BTC/FDUSD is in high volume
            if high_volume_symbols.contains("BTCFDUSD") {
                warn!("🔍 BTC/FDUSD assigned to HighVolume connection");
            }
            
            let pair = Arc::new(Mutex::new(ConnectionPair::new(
                "HighVolume".to_string(),
                high_volume_symbols.clone(),
                self.symbol_to_pair_cache.clone(),
                self.tx.clone(),
            )));
            self.connection_pairs.push(pair);

            // Register in global connection metadata cache
            crate::hub::CONNECTION_METADATA_CACHE.insert(
                (Source::Binance, "HighVolume".to_string()),
                crate::hub::ConnectionMetadata {
                    symbols: high_volume_symbols.iter().cloned().collect(),
                    is_connected: false,
                    last_updated: crate::clock_sync::get_corrected_timestamp_ms(),
                }
            );

            info!("Created HighVolume connection with {} symbols", high_volume_symbols.len());
        }
        
        // Connection 3 & 4: Regular symbols (split into two connections for stability)
        let mut regular1_symbols = HashSet::new();
        let mut regular2_symbols = HashSet::new();
        count = 0;
        for symbol in &websocket_symbols {
            if !priority_symbols.contains(symbol) && !high_volume_symbols.contains(symbol) {
                if count < REGULAR_CONNECTION_SYMBOLS {
                    regular1_symbols.insert(symbol.clone());
                } else if count < REGULAR_CONNECTION_SYMBOLS * 2 {
                    regular2_symbols.insert(symbol.clone());
                }
                count += 1;
                if count >= REGULAR_CONNECTION_SYMBOLS * 2 {
                    break;
                }
            }
        }
        
        if !regular1_symbols.is_empty() {
            // DEBUG: Check if BTC/FDUSD is in regular1
            if regular1_symbols.contains("BTCFDUSD") {
                warn!("🔍 BTC/FDUSD assigned to Regular1 connection");
            }
            
            let pair = Arc::new(Mutex::new(ConnectionPair::new(
                "Regular1".to_string(),
                regular1_symbols.clone(),
                self.symbol_to_pair_cache.clone(),
                self.tx.clone(),
            )));
            self.connection_pairs.push(pair);

            // Register in global connection metadata cache
            crate::hub::CONNECTION_METADATA_CACHE.insert(
                (Source::Binance, "Regular1".to_string()),
                crate::hub::ConnectionMetadata {
                    symbols: regular1_symbols.iter().cloned().collect(),
                    is_connected: false,
                    last_updated: crate::clock_sync::get_corrected_timestamp_ms(),
                }
            );

            info!("Created Regular1 connection with {} symbols", regular1_symbols.len());
        }
        
        if !regular2_symbols.is_empty() {
            // DEBUG: Check if BTC/FDUSD is in regular2
            if regular2_symbols.contains("BTCFDUSD") {
                warn!("🔍 BTC/FDUSD assigned to Regular2 connection");
            }
            
            let pair = Arc::new(Mutex::new(ConnectionPair::new(
                "Regular2".to_string(),
                regular2_symbols.clone(),
                self.symbol_to_pair_cache.clone(),
                self.tx.clone(),
            )));
            self.connection_pairs.push(pair);

            // Register in global connection metadata cache
            crate::hub::CONNECTION_METADATA_CACHE.insert(
                (Source::Binance, "Regular2".to_string()),
                crate::hub::ConnectionMetadata {
                    symbols: regular2_symbols.iter().cloned().collect(),
                    is_connected: false,
                    last_updated: crate::clock_sync::get_corrected_timestamp_ms(),
                }
            );

            info!("Created Regular2 connection with {} symbols", regular2_symbols.len());
        }
        
        // Start all connections gradually
        info!("Starting {} connection pairs with gradual initialization", self.connection_pairs.len());
        for (i, pair) in self.connection_pairs.iter().enumerate() {
            // Clone the Arc to avoid holding the lock during start()
            let pair_clone = pair.clone();
            
            // Start the connection pair
            {
                let mut conn = pair_clone.lock().await;
                if let Err(e) = conn.start().await {
                    error!("Failed to start connection pair {}: {}", i, e);
                }
            }
            
            // Delay between starting each connection pair (1 second)
            if i < self.connection_pairs.len() - 1 {
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            }
        }
        
        // Start connection state monitor
        self.start_connection_monitor();
        
        Ok(())
    }
    
    
    /// Get all symbols from exchange info
    async fn get_all_symbols() -> Result<Vec<String>> {
        // Ensure exchange info is cached
        if BINANCE_EXCHANGE_INFO.is_empty() {
            Self::fetch_and_cache_exchange_info().await?;
        }
        
        let all_symbols: Vec<String> = BINANCE_EXCHANGE_INFO.iter()
            .map(|entry| entry.key().clone())
            .collect();
            
        Ok(all_symbols)
    }
    
    /// Select top symbols by volume - simplified approach
    async fn select_top_symbols() -> Result<(Vec<String>, HashSet<String>)> {
        // Priority tickers always get WebSocket
        let priority_tickers = vec![
            "BTCUSDT", "BTCFDUSD", "SOLUSDT", "SOLFDUSD",
            "BNBUSDT", "BNBFDUSD", "ETHUSDT", "ETHFDUSD",
            "XRPUSDT", "XRPFDUSD", "DOGEUSDT", "DOGEFDUSD",
            "ADAUSDT", "ADAFDUSD", "SUIUSDT", "SUIFDUSD",
            "FDUSDUSDT", "TUSDUSDT", "BTCUSDC", "USDCUSDT"
        ];

        // Ensure exchange info is cached (only fetch if empty)
        if BINANCE_EXCHANGE_INFO.is_empty() {
            Self::fetch_and_cache_exchange_info().await?;
        }
        
        // Use ALL cached symbols - they're already filtered by allowed quotes
        let all_symbols: Vec<String> = BINANCE_EXCHANGE_INFO.iter()
            .map(|entry| entry.key().clone())
            .collect();
        
        info!("📊 Using {} symbols from exchange info (all have allowed quotes)", all_symbols.len());

        // Get 24hr volume data to sort by volume
        let volume_fetcher = crate::volume_cache::VolumeFetcher::global();
        let (volumes, _, _, _, _) = volume_fetcher.fetch_binance_volumes().await?;

        // Create volume-sorted list
        let mut symbol_volumes: Vec<(String, f64)> = Vec::new();
        
        for symbol in &all_symbols {
            let volume = volumes.get(symbol).copied().unwrap_or(0.0);
            symbol_volumes.push((symbol.clone(), volume));
        }

        // Sort by volume (highest first)
        symbol_volumes.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        
        // Log BNB pairs' rankings in the volume-sorted list
        for (rank, (symbol, volume)) in symbol_volumes.iter().enumerate() {
            if symbol.starts_with("BNB") {
                debug!("📊 BNB RANK: #{} - {} (volume: {:.2})", rank + 1, symbol, volume);
            }
        }

        // Select WebSocket symbols: priority first, then by volume
        let mut websocket_symbols = Vec::new();
        let mut used = HashSet::new();

        // Add priority tickers first (only if they exist in our symbols)
        for ticker in priority_tickers {
            if all_symbols.contains(&ticker.to_string()) {
                websocket_symbols.push(ticker.to_string());
                used.insert(ticker.to_string());
                debug!("🔥 Priority ticker added to slot {}: {}", websocket_symbols.len(), ticker);
            } else {
                warn!("⚠️ Priority ticker {} not found in cached symbols", ticker);
            }
        }
        
        info!("✅ Priority tickers processed: {} added, {} total slots used", 
            websocket_symbols.len(), websocket_symbols.len());

        // Add top symbols by volume until we reach WebSocket limit
        for (symbol, volume) in symbol_volumes {
            if used.contains(&symbol) {
                // Symbol already added as priority ticker
                if symbol.starts_with("BNB") {
                    debug!("✅ BNB PRIORITY: {} already added as priority ticker (volume: {:.2})", 
                        symbol, volume);
                }
            } else if websocket_symbols.len() < TOP_SYMBOLS_COUNT {
                websocket_symbols.push(symbol.clone());
                used.insert(symbol.clone());
                // Add BNB-specific aggressive logging
                if symbol.starts_with("BNB") {
                    debug!("🚀 BNB VOLUME: {} volume: {:.2} - ADDING to WebSocket slot {} (not priority)", 
                        symbol, volume, websocket_symbols.len());
                } else if volume > 0.0 {
                    debug!("📈 High volume symbol added: {} (volume: {})", symbol, volume);
                }
            } else if symbol.starts_with("BNB") {
                warn!("❌ BNB REJECTED: {} volume: {:.2} - WebSocket full: {}/{}", 
                    symbol, volume, websocket_symbols.len(), TOP_SYMBOLS_COUNT);
            }
        }

        // Everything else goes to REST API
        let rest_symbols: HashSet<String> = all_symbols
            .into_iter()
            .filter(|s| !used.contains(s))
            .collect();

        info!("Selected {} symbols for WebSocket, {} for REST API", 
            websocket_symbols.len(), rest_symbols.len());

        Ok((websocket_symbols, rest_symbols))
    }
    
    async fn fetch_and_cache_exchange_info() -> Result<usize> {
        info!("🔄 Fetching fresh Binance exchange info");
        
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()?;
            
        let response = client
            .get("https://api.binance.com/api/v3/exchangeInfo")
            .send()
            .await?;
            
        #[derive(Debug, Deserialize)]
        struct ExchangeInfo {
            symbols: Vec<SymbolInfo>,
        }
        
        #[derive(Debug, Deserialize)]
        struct SymbolInfo {
            symbol: String,
            status: String,
            #[serde(rename = "baseAsset")]
            base_asset: String,
            #[serde(rename = "quoteAsset")]
            quote_asset: String,
        }
        
        let info: ExchangeInfo = response.json().await?;
        let allowed_quotes = get_allowed_quotes(Source::Binance);
        let is_initial_fetch = BINANCE_EXCHANGE_INFO.is_empty();
        
        if is_initial_fetch {
            info!("📝 Allowed quote currencies: {:?}", allowed_quotes);
        }
        
        // Cache ALL trading symbols with allowed quotes - keep it simple
        let mut new_symbols = 0;
        let mut total_count = 0;
        let mut btcfdusd_found = false;
        let mut total_symbols = 0;
        
        for symbol_info in info.symbols {
            total_symbols += 1;
            
            if symbol_info.status == "TRADING" {
                let pair = Pair {
                    base: symbol_info.base_asset,
                    quote: symbol_info.quote_asset,
                };
                
                // Include if quote is in our allowed list 
                if allowed_quotes.contains(&pair.quote.as_str()) {
                    total_count += 1;
                    
                    // Only insert if not already present (additive only)
                    if !BINANCE_EXCHANGE_INFO.contains_key(&symbol_info.symbol) {
                        BINANCE_EXCHANGE_INFO.insert(symbol_info.symbol.clone(), pair);
                        new_symbols += 1;
                        
                        // Log new discoveries after initial fetch
                        if !is_initial_fetch {
                            info!("🆕 Discovered new Binance listing: {}", symbol_info.symbol);
                        }
                    }
                    
                    if symbol_info.symbol == "BTCFDUSD" {
                        btcfdusd_found = true;
                        if is_initial_fetch {
                            info!("✅ Found BTCFDUSD: BTC/FDUSD (cached)");
                        }
                    }
                }
            }
        }
        
        if is_initial_fetch {
            info!("📊 Processed {} total symbols from Binance", total_symbols);
            info!("✅ Cached {} trading pairs with allowed quotes", new_symbols);
            
            if !btcfdusd_found {
                warn!("❌ BTCFDUSD not found - checking if FDUSD is in allowed quotes");
            }
        } else if new_symbols > 0 {
            info!("✅ Added {} new Binance symbols to cache (total: {})", new_symbols, total_count);
        } else {
            debug!("No new Binance symbols discovered (total: {})", total_count);
        }
        
        Ok(new_symbols)
    }
    
    /// Start a background task to monitor connection states
    fn start_connection_monitor(&self) {
        let connection_pairs = self.connection_pairs.clone();
        let connection_state = self.connection_state.clone();
        
        crate::runtime_separation::spawn_on_ingestion_named("binance-connection-monitor", async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(5));
            
            loop {
                interval.tick().await;
                
                let mut any_connected = false;
                
                // Check each connection pair
                for conn_pair in &connection_pairs {
                    if let Ok(conn) = conn_pair.try_lock() {
                        // Check connections without blocking
                        // This is just a quick check - actual connection state is maintained by the connections
                        if !conn.symbols.is_empty() {
                            any_connected = true;
                            break;
                        }
                    }
                }
                
                // Update the cached connection state
                connection_state.set_connected(any_connected);
            }
        });
    }
}


async fn process_ticker(_symbol_to_pair_cache: &Arc<DashMap<String, Pair>>, ticker: Ticker) -> Result<()> {

    // The ticker.symbol is already in "BTC/USDT" format from binance_connection
    // We need to parse it to get the Pair
    if let Ok(pair) = Pair::from_task(&ticker.symbol) {
        // Create a Tick (ticker.price is already a Decimal)
        let now = crate::clock_sync::get_corrected_timestamp_ms();
        let tick = crate::types::Tick {
            price: ticker.price,
            event_ts: ticker.timestamp,
            seen_at: now,
            verified_at: now,
        };
        
        // Store in the global SurgeHub cache (MESSAGE_COUNTER is now handled in hub.update)
        let hub = crate::hub::SurgeHub::global();
        hub.update(Source::Binance, pair.clone(), tick).await;
        
    } else {
        warn!("❌ Failed to parse ticker symbol: {}", ticker.symbol);
    }

    Ok(())
}

impl TickerStream for BinanceStream {
    fn listen(&mut self, _pair: Pair) -> Result<()> {
        // In multi-connection setup, subscriptions are handled during connection setup
        // This is a no-op for now but could be used for dynamic subscription
        Ok(())
    }

    fn unlisten(&mut self, _pair: &Pair) -> Result<()> {
        // In multi-connection setup, unsubscriptions would need to find the right connection
        // This is a no-op for now
        Ok(())
    }

    fn subscriptions(&self) -> Vec<Pair> {
        // Return all pairs from all connections
        let mut all_pairs = Vec::new();
        for conn_pair in &self.connection_pairs {
            if let Ok(conn) = conn_pair.try_lock() {
                for symbol in &conn.symbols {
                    if let Some(pair) = self.symbol_to_pair_cache.get(symbol) {
                        all_pairs.push(pair.value().clone());
                    }
                }
            }
        }
        all_pairs
    }

    fn is_connected(&self) -> bool {
        // Return cached connection state (non-blocking)
        self.connection_state.is_connected()
    }
}

impl BinanceStream {
    /// Get WebSocket connection health for monitoring
    pub async fn get_websocket_connection_health(&self) -> Vec<crate::types::WebSocketConnectionHealth> {
        let mut health = Vec::new();
        let hub = crate::hub::SurgeHub::global();

        for conn_pair in &self.connection_pairs {
            let conn_pair = conn_pair.lock().await;
            let conn = conn_pair.connection.read().await;

            // Use existing methods for connection metrics
            let msg_rate = conn.get_message_rate();
            let latency = conn.get_latency_ms();
            let is_connected = conn.is_connected();
            let in_grace = conn.is_in_grace_period();

            // Count how many symbols are actually on WebSocket vs REST
            let mut websocket_active_count = 0;
            let mut rest_fallback_symbols = Vec::new();

            // Get all cached Binance pairs for lookup
            let all_pairs = get_cached_pairs().unwrap_or_default();

            for symbol_str in &conn_pair.symbols {
                // Find the corresponding Pair for this symbol
                if let Some(pair) = all_pairs.iter().find(|p| p.as_binance_str() == *symbol_str) {
                    if hub.is_websocket_active(Source::Binance, pair) {
                        websocket_active_count += 1;
                    } else {
                        rest_fallback_symbols.push(symbol_str.clone());
                    }
                }
            }

            let total_symbols = conn_pair.symbols.len();
            let rest_count = rest_fallback_symbols.len();

            // Determine overall connection status
            let status = if !is_connected {
                "disconnected".to_string()
            } else if in_grace {
                "initializing".to_string()
            } else if websocket_active_count == 0 && total_symbols > 0 {
                "all_rest_fallback".to_string()
            } else if rest_count > 0 {
                "partial_rest_fallback".to_string()
            } else if msg_rate < 0.05 {
                "degraded".to_string()
            } else {
                "healthy".to_string()
            };

            health.push(crate::types::WebSocketConnectionHealth {
                exchange: "binance".to_string(),
                connection_type: conn_pair.id.clone(),
                total_symbols,
                websocket_active_symbols: websocket_active_count,
                rest_fallback_symbols: rest_count,
                msg_per_sec: msg_rate,
                latency_ms: latency,
                status,
                rest_symbols: if rest_count > 0 { Some(rest_fallback_symbols) } else { None },
            });
        }

        health
    }
}

impl Stream for BinanceStream {
    type Item = Result<Ticker>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}