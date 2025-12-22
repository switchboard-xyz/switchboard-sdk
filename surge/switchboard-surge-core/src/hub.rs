use crate::{
    exchanges::{BinanceStream, BybitStream, CoinbaseStream, OkxStream, BitgetStream, PythStream},
    pair::Pair,
    traits::TickerStream,
    types::{MapKey, PxEntry, Source, Tick},
    message_rate::MESSAGE_COUNTER,
    bip_correction::{should_apply_bip_correction, BipCorrectedReceiver, apply_bip_correction},
};
use anyhow::{anyhow, Result};
use dashmap::DashMap;
use once_cell::sync::{OnceCell, Lazy};
use std::sync::Arc;
use std::collections::{HashMap, HashSet};
use tokio::sync::watch;
use tracing::{debug, error, info, warn};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use crate::weighted_source::is_usd_equivalent;

/// Maximum allowed price variance between exchanges as coefficient of variation (40 bips = 0.4%)
const MAX_PRICE_VARIANCE_CV_PERCENT: f64 = 0.4;

/// Global SurgeHub instance
static SURGE_HUB: OnceCell<Arc<SurgeHub>> = OnceCell::new();

/// Connection metadata for real-time health monitoring
/// NOTE: msg_per_sec is calculated from MESSAGE_RATE_CACHE when building health response
#[derive(Clone, Debug)]
pub struct ConnectionMetadata {
    pub symbols: Vec<String>,
    pub is_connected: bool,
    pub last_updated: u64,
}

/// Global registry for WebSocket connection metadata (updated by connections in real-time)
/// Key: (Source, ConnectionID) -> Metadata
/// Lifecycle: Registered on creation, updated every 10-60s, removed on drop/disconnect
pub static CONNECTION_METADATA_CACHE: Lazy<Arc<DashMap<(Source, String), ConnectionMetadata>>> =
    Lazy::new(|| Arc::new(DashMap::new()));

/// Cache entry for pre-calculated WEIGHTED source selection
#[derive(Clone, Debug)]
pub struct WeightedSourceEntry {
    /// The exchange with highest volume/rate for this pair (primary)
    pub source: Source,
    /// The actual pair on the exchange that has the highest volume
    /// (e.g., BTC/FDUSD when looking for BTC/USD)
    pub actual_pair: Pair,
    /// Secondary source for validation (different exchange, second highest volume)
    pub secondary_source: Option<Source>,
    /// The actual pair on the secondary exchange
    pub secondary_pair: Option<Pair>,
    /// Timestamp when this entry was calculated (ms)
    pub calculated_at: u64,
}

impl WeightedSourceEntry {
    /// Check if this entry has expired (12 hours)
    pub fn is_expired(&self) -> bool {
        let current_time = crate::clock_sync::get_corrected_timestamp_ms();
        let age_ms = current_time.saturating_sub(self.calculated_at);
        age_ms > (12 * 60 * 60 * 1000) // 12 hours in milliseconds
    }
}

/// Global cache for pre-calculated WEIGHTED source selections
/// Maps Pair -> (Source with highest volume, timestamp)
/// TTL: 2 minutes (refreshed every 2 minutes)
pub static WEIGHTED_SOURCE_CACHE: Lazy<Arc<DashMap<Pair, WeightedSourceEntry>>> =
    Lazy::new(|| Arc::new(DashMap::new()));

/// Global cache for pre-calculated AUTO source selections
/// Maps Pair -> (Source with best score, timestamp)
/// TTL: 10 minutes (refreshed every 10 minutes)
pub static AUTO_SOURCE_CACHE: Lazy<Arc<DashMap<Pair, WeightedSourceEntry>>> =
    Lazy::new(|| Arc::new(DashMap::new()));

/// Main hub for managing exchange connections and price feeds
#[derive(Clone)]
pub struct SurgeHub {
    /// Price cache: (Source, Pair) -> (Tick, watch::Sender)
    prices: Arc<DashMap<MapKey, PxEntry>>,

    /// Active exchange streams
    binance: Option<Arc<tokio::sync::Mutex<BinanceStream>>>,
    bybit: Option<Arc<tokio::sync::Mutex<BybitStream>>>,
    okx: Option<Arc<tokio::sync::Mutex<OkxStream>>>,
    coinbase: Option<Arc<tokio::sync::Mutex<CoinbaseStream>>>,
    bitget: Option<Arc<tokio::sync::Mutex<BitgetStream>>>,
    pyth: Option<Arc<tokio::sync::Mutex<PythStream>>>,
    // titan: Option<Arc<tokio::sync::Mutex<TitanStream>>>, // Disabled

    /// WebSocket active pairs per exchange (Source, Pair) -> ()
    /// This tracks which pairs are actively being updated via WebSocket
    websocket_active_pairs: Arc<DashMap<(Source, Pair), ()>>,
}

impl Default for SurgeHub {
    fn default() -> Self {
        Self::new()
    }
}

impl SurgeHub {
    /// Get or create the global SurgeHub instance
    pub fn global() -> Arc<Self> {
        SURGE_HUB.get_or_init(|| Arc::new(Self::new())).clone()
    }
    
    /// Set the global SurgeHub instance (used during initialization)
    pub fn set_global(hub: Arc<Self>) {
        let _ = SURGE_HUB.set(hub);
    }
    
    /// Initialize the global hub with all exchanges (must be called before using global())
    pub async fn initialize_global() -> Result<()> {
        // Create a new hub with all exchanges
        let mut hub = Self::new();
        
        // Start all exchanges
        info!("Starting all exchanges for global hub...");
        hub.start_all_exchanges().await?;
        
        // Pre-populate all pairs into hub.prices
        info!("Pre-populating all pairs into hub.prices...");
        if let Err(e) = hub.pre_populate_all_pairs().await {
            error!("Failed to pre-populate hub prices: {}", e);
        }
        
        // Pre-compute feed metadata for all pairs
        info!("Pre-computing feed metadata for all pairs...");
        if let Err(e) = crate::feed_metadata::precompute_all_feed_metadata().await {
            error!("Failed to pre-compute feed metadata: {}", e);
        }

        // Start BIP correction monitor (it will handle its own subscriptions)
        info!("Starting FDUSD BIP correction monitor...");
        crate::bip_correction::start_bip_correction_monitor().await;
        
        // Try to set it as the global instance
        match SURGE_HUB.set(Arc::new(hub)) {
            Ok(_) => {
                info!("Global SurgeHub initialized with all exchanges");
                Ok(())
            }
            Err(_) => {
                // Already initialized
                info!("Global SurgeHub was already initialized");
                Ok(())
            }
        }
    }
    

    /// Create a new SurgeHub
    pub fn new() -> Self {
        // Start clock synchronization task (only once per process)
        crate::clock_sync::start_clock_sync_task();
        
        Self {
            prices: Arc::new(DashMap::new()),
            binance: None,
            bybit: None,
            okx: None,
            coinbase: None,
            bitget: None,
            pyth: None,
            // titan: None, // Disabled
            websocket_active_pairs: Arc::new(DashMap::new()),
        }
    }
    
    /// Pre-populate price cache with all available pairs
    async fn pre_populate_all_pairs(&self) -> Result<()> {
        info!("🚀 PRE-POPULATE: Starting to pre-populate hub.prices with all exchange pairs");
        let mut total_pairs = 0;
        
        // Binance
        if let Ok(pairs) = crate::exchanges::binance::get_cached_pairs() {
            let count = pairs.len();
            for pair in pairs {
                let key = (Source::Binance, pair.clone());
                self.prices.entry(key).or_insert_with(|| {
                    let tick = Tick::default();
                    let (tx, _) = watch::channel(tick);
                    (tick, tx)
                });
            }
            info!("✅ PRE-POPULATE: Added {} Binance pairs", count);
            total_pairs += count;
        } else {
            warn!("⚠️ PRE-POPULATE: Failed to get Binance cached pairs");
        }
        
        // Bybit
        if let Ok(pairs) = crate::exchanges::bybit::get_cached_pairs() {
            let count = pairs.len();
            for pair in pairs {
                let key = (Source::Bybit, pair.clone());
                self.prices.entry(key).or_insert_with(|| {
                    let tick = Tick::default();
                    let (tx, _) = watch::channel(tick);
                    (tick, tx)
                });
            }
            info!("✅ PRE-POPULATE: Added {} Bybit pairs", count);
            total_pairs += count;
        } else {
            warn!("⚠️ PRE-POPULATE: Failed to get Bybit cached pairs");
        }
        
        // OKX
        if let Ok(pairs) = crate::exchanges::okx::get_cached_pairs() {
            let count = pairs.len();
            for pair in pairs {
                let key = (Source::Okx, pair.clone());
                self.prices.entry(key).or_insert_with(|| {
                    let tick = Tick::default();
                    let (tx, _) = watch::channel(tick);
                    (tick, tx)
                });
            }
            info!("✅ PRE-POPULATE: Added {} OKX pairs", count);
            total_pairs += count;
        } else {
            warn!("⚠️ PRE-POPULATE: Failed to get OKX cached pairs");
        }
        
        // Coinbase
        if let Ok(pairs) = crate::exchanges::coinbase::get_cached_pairs() {
            let count = pairs.len();
            for pair in pairs {
                let key = (Source::Coinbase, pair.clone());
                self.prices.entry(key).or_insert_with(|| {
                    let tick = Tick::default();
                    let (tx, _) = watch::channel(tick);
                    (tick, tx)
                });
            }
            info!("✅ PRE-POPULATE: Added {} Coinbase pairs", count);
            total_pairs += count;
        } else {
            warn!("⚠️ PRE-POPULATE: Failed to get Coinbase cached pairs");
        }
        
        // Bitget
        if let Ok(pairs) = crate::exchanges::bitget::get_cached_pairs() {
            let count = pairs.len();
            for pair in pairs {
                let key = (Source::Bitget, pair.clone());
                self.prices.entry(key).or_insert_with(|| {
                    let tick = Tick::default();
                    let (tx, _) = watch::channel(tick);
                    (tick, tx)
                });
            }
            info!("✅ PRE-POPULATE: Added {} Bitget pairs", count);
            total_pairs += count;
        } else {
            warn!("⚠️ PRE-POPULATE: Failed to get Bitget cached pairs");
        }
        
        info!("✅ PRE-POPULATE: Complete! Total {} pairs in hub.prices", total_pairs);
        
        // Log some sample pairs for verification
        let sample_count = 5;
        let mut samples = Vec::new();
        for (i, entry) in self.prices.iter().enumerate() {
            if i >= sample_count { break; }
            let ((source, pair), _) = entry.pair();
            samples.push(format!("{}: {}", source.as_str(), pair.as_str()));
        }
        info!("📋 PRE-POPULATE: Sample pairs: {:?}", samples);
        
        Ok(())
    }

    /// Ensure exchange loop is running for the given source
    pub async fn ensure_loop(&mut self, source: Source) -> Result<()> {
        match source {
            Source::Weighted => {
                // Weighted sources don't have their own loops
                return Err(anyhow!("Cannot ensure loop for weighted source"));
            }
            Source::Auto => {
                // Auto sources don't have their own loops
                return Err(anyhow!("Cannot ensure loop for auto source"));
            }
            Source::Binance => {
                if self.binance.is_none() {
                    info!("Starting Binance stream");
                    let stream = BinanceStream::new().await?;
                    self.binance = Some(Arc::new(tokio::sync::Mutex::new(stream)));
                    self.spawn_exchange_loop(source).await?;
                }
            }
            Source::Bybit => {
                if self.bybit.is_none() {
                    info!("Starting Bybit stream");
                    let stream = BybitStream::new().await?;
                    self.bybit = Some(Arc::new(tokio::sync::Mutex::new(stream)));
                    self.spawn_exchange_loop(source).await?;
                }
            }
            Source::Okx => {
                if self.okx.is_none() {
                    info!("Starting OKX stream");
                    let stream = OkxStream::new().await?;
                    self.okx = Some(Arc::new(tokio::sync::Mutex::new(stream)));
                    self.spawn_exchange_loop(source).await?;
                }
            }
            Source::Coinbase => {
                if self.coinbase.is_none() {
                    info!("Starting Coinbase stream");
                    let stream = CoinbaseStream::new().await?;
                    self.coinbase = Some(Arc::new(tokio::sync::Mutex::new(stream)));
                    self.spawn_exchange_loop(source).await?;
                }
            }
            Source::Bitget => {
                if self.bitget.is_none() {
                    info!("Starting Bitget stream");
                    let stream = BitgetStream::new().await?;
                    self.bitget = Some(Arc::new(tokio::sync::Mutex::new(stream)));
                    self.spawn_exchange_loop(source).await?;
                }
            }
            Source::Pyth => {
                if self.pyth.is_none() {
                    info!("Starting Pyth stream");
                    let stream = PythStream::new().await?;
                    self.pyth = Some(Arc::new(tokio::sync::Mutex::new(stream)));
                    self.spawn_exchange_loop(source).await?;
                }
            }
            Source::Titan => {
                // Disabled - Titan module kept for future use
                info!("Titan exchange is disabled");
            }
        }
        Ok(())
    }

    /// Spawn the processing loop for an exchange
    async fn spawn_exchange_loop(&self, source: Source) -> Result<()> {
        let prices = self.prices.clone();
        
        match source {
            Source::Binance => {
                if let Some(stream) = &self.binance {
                    info!("Spawning Binance exchange processing task");
                    let stream = stream.clone();
                    crate::runtime_separation::spawn_on_ingestion_named(
                        "binance-processor",
                        async move {
                        info!("Binance processing task started");
                        // Don't hold the lock for the entire loop
                        loop {
                            let result = {
                                let mut stream_guard = stream.lock().await;
                                use futures::StreamExt;
                                stream_guard.next().await
                            };
                            
                            match result {
                                Some(Ok(ticker)) => {
                                    debug!("Processing ticker: {} @ {}", ticker.symbol, ticker.price);
                                    if let Ok(pair) = Pair::from_task(&ticker.symbol) {
                                        let key = (source, pair.clone());
                                        let now = crate::clock_sync::get_corrected_timestamp_ms();
                                        let tick = Tick {
                                            price: ticker.price,
                                            event_ts: ticker.timestamp,
                                            seen_at: now,
                                            verified_at: now,
                                        };
                                        
                                        debug!("Updating price cache for {} {}", source.as_str(), pair.as_str());
                                        prices.entry(key)
                                            .and_modify(|(t, tx)| {
                                                *t = tick;
                                                let _ = tx.send(tick);
                                            })
                                            .or_insert_with(|| {
                                                let (tx, _) = watch::channel(tick);
                                                (tick, tx)
                                            });
                                        debug!("Updated price cache for {} {}", source.as_str(), pair.as_str());
                                    } else {
                                        warn!("Failed to parse Binance symbol: {}", ticker.symbol);
                                    }
                                }
                                Some(Err(e)) => {
                                    warn!("Binance ticker error: {}", e);
                                }
                                None => {
                                    warn!("Binance stream ended");
                                    break;
                                }
                            }
                        }
                        warn!("Binance exchange processing loop ended!");
                    });
                }
            }
            Source::Bybit => {
                if let Some(stream) = &self.bybit {
                    info!("Spawning Bybit exchange processing task");
                    let stream = stream.clone();
                    crate::runtime_separation::spawn_on_ingestion_named(
                        "bybit-processor",
                        async move {
                        info!("Bybit processing task started");
                        // Start the read loop for Bybit
                        {
                            let mut stream_guard = stream.lock().await;
                            stream_guard.start_read_loop();
                        }
                        
                        // Don't hold the lock for the entire loop
                        loop {
                            let result = {
                                let mut stream_guard = stream.lock().await;
                                use futures::StreamExt;
                                stream_guard.next().await
                            };
                            
                            match result {
                                Some(Ok(ticker)) => {
                                    debug!("Processing Bybit ticker: {} @ {}", ticker.symbol, ticker.price);
                                    if let Ok(pair) = Pair::from_task(&ticker.symbol) {
                                        let key = (source, pair.clone());
                                        let now = crate::clock_sync::get_corrected_timestamp_ms();
                                        let tick = Tick {
                                            price: ticker.price,
                                            event_ts: ticker.timestamp,
                                            seen_at: now,
                                            verified_at: now,
                                        };
                                        
                                        debug!("Updating price cache for {} {}", source.as_str(), pair.as_str());
                                        prices.entry(key)
                                            .and_modify(|(t, tx)| {
                                                *t = tick;
                                                let _ = tx.send(tick);
                                            })
                                            .or_insert_with(|| {
                                                let (tx, _) = watch::channel(tick);
                                                (tick, tx)
                                            });
                                        debug!("Updated price cache for {} {}", source.as_str(), pair.as_str());
                                    } else {
                                        warn!("Failed to parse Bybit symbol: {}", ticker.symbol);
                                    }
                                }
                                Some(Err(e)) => {
                                    warn!("Bybit ticker error: {}", e);
                                }
                                None => {
                                    warn!("Bybit stream ended");
                                    break;
                                }
                            }
                        }
                        warn!("Bybit exchange processing loop ended!");
                    });
                }
            }
            Source::Okx => {
                if let Some(stream) = &self.okx {
                    let stream = stream.clone();
                    crate::runtime_separation::spawn_on_ingestion_named(
                        "okx-processor",
                        async move {
                        let mut stream_guard = stream.lock().await;
                        use futures::StreamExt;
                        while let Some(result) = stream_guard.next().await {
                            match result {
                                Ok(ticker) => {
                                    if let Ok(pair) = Pair::from_task(&ticker.symbol) {
                                        let key = (source, pair.clone());
                                        let now = crate::clock_sync::get_corrected_timestamp_ms();
                                        let tick = Tick {
                                            price: ticker.price,
                                            event_ts: ticker.timestamp,
                                            seen_at: now,
                                            verified_at: now,
                                        };
                                        
                                        prices.entry(key)
                                            .and_modify(|(t, tx)| {
                                                *t = tick;
                                                let _ = tx.send(tick);
                                            })
                                            .or_insert_with(|| {
                                                let (tx, _) = watch::channel(tick);
                                                (tick, tx)
                                            });
                                    }
                                }
                                Err(e) => {
                                    warn!("OKX ticker error: {}", e);
                                }
                            }
                        }
                    });
                }
            }
            Source::Coinbase => {
                if let Some(stream) = &self.coinbase {
                    let stream = stream.clone();
                    crate::runtime_separation::spawn_on_ingestion_named(
                        "coinbase-processor",
                        async move {
                        let mut stream_guard = stream.lock().await;
                        use futures::StreamExt;
                        while let Some(result) = stream_guard.next().await {
                            match result {
                                Ok(ticker) => {
                                    if let Ok(pair) = Pair::from_task(&ticker.symbol) {
                                        let key = (source, pair.clone());
                                        let now = crate::clock_sync::get_corrected_timestamp_ms();
                                        let tick = Tick {
                                            price: ticker.price,
                                            event_ts: ticker.timestamp,
                                            seen_at: now,
                                            verified_at: now,
                                        };
                                        
                                        prices.entry(key)
                                            .and_modify(|(t, tx)| {
                                                *t = tick;
                                                let _ = tx.send(tick);
                                            })
                                            .or_insert_with(|| {
                                                let (tx, _) = watch::channel(tick);
                                                (tick, tx)
                                            });
                                    }
                                }
                                Err(e) => {
                                    warn!("Coinbase ticker error: {}", e);
                                }
                            }
                        }
                    });
                }
            }
            Source::Bitget => {
                if let Some(stream) = &self.bitget {
                    info!("Spawning Bitget exchange processing task");
                    let stream = stream.clone();
                    crate::runtime_separation::spawn_on_ingestion_named(
                        "bitget-processor",
                        async move {
                        info!("Bitget processing task started");
                        loop {
                            let result = {
                                let mut stream_guard = stream.lock().await;
                                use futures::StreamExt;
                                stream_guard.next().await
                            };
                            
                            match result {
                                Some(Ok(ticker)) => {
                                    debug!("Processing ticker: {} @ {}", ticker.symbol, ticker.price);
                                    if let Ok(pair) = Pair::from_task(&ticker.symbol) {
                                        let key = (source, pair.clone());
                                        let now = crate::clock_sync::get_corrected_timestamp_ms();
                                        let tick = Tick {
                                            price: ticker.price,
                                            event_ts: ticker.timestamp,
                                            seen_at: now,
                                            verified_at: now,
                                        };
                                        
                                        debug!("Updating price cache for {} {}", source.as_str(), pair.as_str());
                                        prices.entry(key)
                                            .and_modify(|(t, tx)| {
                                                *t = tick;
                                                let _ = tx.send(tick);
                                            })
                                            .or_insert_with(|| {
                                                let (tx, _) = watch::channel(tick);
                                                (tick, tx)
                                            });
                                        debug!("Updated price cache for {} {}", source.as_str(), pair.as_str());
                                    } else {
                                        warn!("Failed to parse Bitget symbol: {}", ticker.symbol);
                                    }
                                }
                                Some(Err(e)) => {
                                    warn!("Bitget ticker error: {}", e);
                                }
                                None => {
                                    warn!("Bitget stream ended");
                                    break;
                                }
                            }
                        }
                        warn!("Bitget exchange processing loop ended!");
                    });
                }
            }
            _ => {
                // Weighted sources are handled via watch_price
                return Err(anyhow!("Cannot spawn exchange loop for weighted/auto source"));
            }
        }
        
        Ok(())
    }

    /// Get a watch receiver for a price feed with variance checking
    /// Returns a receiver that only emits prices that pass variance validation
    /// For AUTO/Weighted sources and exchange sources with configured secondaries
    pub fn watch_price_checked(&self, source: Source, pair: &Pair) -> Option<watch::Receiver<Tick>> {
        // Get the regular watch receiver first
        let rx = self.watch_price(source, pair)?;
        
        // For now, return the regular receiver
        // TODO: Implement filtered receiver that validates variance before emitting
        // This would require spawning a task that:
        // 1. Watches the original receiver
        // 2. Runs variance check on each tick
        // 3. Only forwards validated ticks to a new channel
        Some(rx)
    }
    
    /// Get a watch receiver for a price feed (for crossbar mode - read only)
    /// This doesn't modify exchange subscriptions, just watches the existing cache
    pub fn watch_price(&self, source: Source, pair: &Pair) -> Option<watch::Receiver<Tick>> {
        if source == Source::Weighted || source == Source::Auto {
            // Both WEIGHTED and AUTO now use the AUTO score-based selection
            let calculator = crate::weighted_source::AutoSourceCalculator::new();
            
            // Try to get from cache synchronously (no blocking!)
            if let Some((actual_source, actual_pair)) = calculator.get_from_cache_sync(pair) {
                if source == Source::Weighted {
                    info!(
                        "🎯 WEIGHTED SOURCE (using AUTO scoring): {} (requested) → {} on {} (actual feed used)",
                        pair.as_str(),
                        actual_pair.as_str(),
                        actual_source.as_str()
                    );
                    info!(
                        "   ↳ Note: WEIGHTED now uses AUTO scoring algorithm for better price discovery"
                    );
                } else {
                    info!(
                        "🎯 AUTO SOURCE: {} (requested) → {} on {} (actual feed used)",
                        pair.as_str(),
                        actual_pair.as_str(),
                        actual_source.as_str()
                    );
                }
                
                // Get the raw price stream
                let key = (actual_source, actual_pair.clone());
                if let Some(entry) = self.prices.get(&key) {
                    let raw_rx = entry.1.subscribe();

                    // Check if BIP correction should be applied
                    if should_apply_bip_correction(source, &pair.quote, &actual_pair.quote) {
                        let current_bips = if actual_pair.quote.to_uppercase() == "FDUSD" {
                            crate::bip_correction::FDUSD_BIP_CORRECTION_CACHE.load(std::sync::atomic::Ordering::Relaxed) as f64 / 100.0
                        } else {
                            0.0
                        };

                        info!(
                            "💱 Setting up BIP correction for {}/{} → USD (current: {:+} bips for {}, type: {})",
                            actual_pair.base,
                            actual_pair.quote,
                            current_bips,
                            actual_pair.quote,
                            if actual_pair.quote.to_uppercase() == "FDUSD" { "DYNAMIC" } else { "STATIC" }
                        );

                        // Create a corrected receiver wrapper that auto-cleans up
                        let corrected = BipCorrectedReceiver::new(raw_rx, &actual_pair.quote);
                        return Some(corrected.into());
                    }

                    return Some(raw_rx);
                }
                None
            } else {
                let source_name = if source == Source::Auto { "AUTO" } else { "WEIGHTED (via AUTO)" };
                warn!("⚠️ {} source not in cache for {}, unable to stream price", source_name, pair.as_str());
                None
            }
        } else {
            let key = (source, pair.clone());
            self.prices.get(&key).map(|entry| entry.1.subscribe())
        }
    }
    
    /// Get weighted sources for multiple pairs efficiently (for subscribeAll)
    /// Returns a map of original pair -> (resolved source, resolved pair)
    pub async fn batch_resolve_weighted_sources(
        &self, 
        pairs: Vec<Pair>
    ) -> HashMap<Pair, (Source, Pair)> {
        let calculator = crate::weighted_source::WeightedSourceCalculator::new();
        let mut results = HashMap::new();
        
        for pair in pairs {
            if let Ok((source, resolved_pair)) = calculator.get_weighted_source(&pair).await {
                results.insert(pair, (source, resolved_pair));
            }
        }
        
        results
    }
    
    /// Get watch receivers for pairs with optional quote filter (for subscribeAll)
    pub fn get_filtered_pairs(&self, quote_filter: Option<&str>) -> Vec<(Source, Pair)> {
        self.prices
            .iter()
            .filter_map(|entry| {
                let ((source, pair), _) = entry.pair();
                
                // Apply quote filter if specified
                if let Some(filter) = quote_filter {
                    if !pair.quote.eq_ignore_ascii_case(filter) {
                        return None;
                    }
                }
                
                Some((*source, pair.clone()))
            })
            .collect()
    }
    
    /// Subscribe to a price feed
    pub async fn subscribe(&self, source: Source, pair: Pair) -> Result<watch::Receiver<Tick>> {
        // Handle both WEIGHTED and AUTO with the AUTO flow (score-based selection)
        if source == Source::Weighted || source == Source::Auto {
            return self.subscribe_auto(pair).await;
        }
        
        // Use the internal method for regular sources
        self.subscribe_to_exchange(source, pair).await
    }

    /// Unsubscribe from a price feed
    pub async fn unsubscribe(&self, source: Source, pair: &Pair) -> Result<()> {
        match source {
            Source::Binance => {
                if let Some(stream) = &self.binance {
                    let mut stream_guard = stream.lock().await;
                    stream_guard.unlisten(pair)?;
                }
            }
            Source::Bybit => {
                if let Some(stream) = &self.bybit {
                    let mut stream_guard = stream.lock().await;
                    stream_guard.unlisten(pair)?;
                }
            }
            Source::Okx => {
                if let Some(stream) = &self.okx {
                    let mut stream_guard = stream.lock().await;
                    stream_guard.unlisten(pair)?;
                }
            }
            Source::Coinbase => {
                if let Some(stream) = &self.coinbase {
                    let mut stream_guard = stream.lock().await;
                    stream_guard.unlisten(pair)?;
                }
            }
            Source::Bitget => {
                if let Some(stream) = &self.bitget {
                    let mut stream_guard = stream.lock().await;
                    stream_guard.unlisten(pair)?;
                }
            }
            Source::Pyth => {
                if let Some(stream) = &self.pyth {
                    let mut stream_guard = stream.lock().await;
                    stream_guard.unlisten(pair)?;
                }
            }
            Source::Titan => {
                // Disabled - Titan module kept for future use
            }
            Source::Weighted => {
                // For WEIGHTED, find the actual source being used
                let (actual_source, actual_pair) = crate::weighted_source::WeightedSourceCalculator::new()
                    .get_from_cache_sync(pair)
                    .ok_or_else(|| anyhow!("No weighted source found for {}", pair.as_str()))?;
                
                // Unsubscribe from the actual exchange (avoid recursion by handling directly)
                match actual_source {
                    Source::Binance if self.binance.is_some() => {
                        let mut stream_guard = self.binance.as_ref().unwrap().lock().await;
                        stream_guard.unlisten(&actual_pair)?;
                    }
                    Source::Bybit if self.bybit.is_some() => {
                        let mut stream_guard = self.bybit.as_ref().unwrap().lock().await;
                        stream_guard.unlisten(&actual_pair)?;
                    }
                    Source::Okx if self.okx.is_some() => {
                        let mut stream_guard = self.okx.as_ref().unwrap().lock().await;
                        stream_guard.unlisten(&actual_pair)?;
                    }
                    Source::Coinbase if self.coinbase.is_some() => {
                        let mut stream_guard = self.coinbase.as_ref().unwrap().lock().await;
                        stream_guard.unlisten(&actual_pair)?;
                    }
                    Source::Bitget if self.bitget.is_some() => {
                        let mut stream_guard = self.bitget.as_ref().unwrap().lock().await;
                        stream_guard.unlisten(&actual_pair)?;
                    }
                    Source::Pyth if self.pyth.is_some() => {
                        let mut stream_guard = self.pyth.as_ref().unwrap().lock().await;
                        stream_guard.unlisten(&actual_pair)?;
                    }
                    Source::Titan => {
                        // Disabled - Titan module kept for future use
                    }
                    _ => return Err(anyhow!("Exchange not available for weighted source")),
                }
                
                // Remove from cache
                let key = (actual_source, actual_pair);
                self.prices.remove(&key);
                return Ok(());
            }
            Source::Auto => {
                // For AUTO, find the actual source being used
                let entry = AUTO_SOURCE_CACHE
                    .get(pair)
                    .or_else(|| {
                        // Check USD equivalent if applicable
                        if crate::weighted_source::is_usd_equivalent(&pair.quote) && pair.quote != "USD" {
                            let usd_pair = Pair {
                                base: pair.base.clone(),
                                quote: "USD".to_string(),
                            };
                            AUTO_SOURCE_CACHE.get(&usd_pair)
                        } else {
                            None
                        }
                    })
                    .ok_or_else(|| anyhow!("No AUTO source found for {}", pair.as_str()))?;
                
                let actual_source = entry.source;
                let actual_pair = entry.actual_pair.clone();
                
                // Unsubscribe from the actual exchange (avoid recursion by handling directly)
                match actual_source {
                    Source::Binance if self.binance.is_some() => {
                        let mut stream_guard = self.binance.as_ref().unwrap().lock().await;
                        stream_guard.unlisten(&actual_pair)?;
                    }
                    Source::Bybit if self.bybit.is_some() => {
                        let mut stream_guard = self.bybit.as_ref().unwrap().lock().await;
                        stream_guard.unlisten(&actual_pair)?;
                    }
                    Source::Okx if self.okx.is_some() => {
                        let mut stream_guard = self.okx.as_ref().unwrap().lock().await;
                        stream_guard.unlisten(&actual_pair)?;
                    }
                    Source::Coinbase if self.coinbase.is_some() => {
                        let mut stream_guard = self.coinbase.as_ref().unwrap().lock().await;
                        stream_guard.unlisten(&actual_pair)?;
                    }
                    Source::Bitget if self.bitget.is_some() => {
                        let mut stream_guard = self.bitget.as_ref().unwrap().lock().await;
                        stream_guard.unlisten(&actual_pair)?;
                    }
                    Source::Pyth if self.pyth.is_some() => {
                        let mut stream_guard = self.pyth.as_ref().unwrap().lock().await;
                        stream_guard.unlisten(&actual_pair)?;
                    }
                    Source::Titan => {
                        // Disabled - Titan module kept for future use
                    }
                    _ => return Err(anyhow!("Exchange not available for AUTO source")),
                }
                
                // Remove from cache
                let key = (actual_source, actual_pair);
                self.prices.remove(&key);
                return Ok(());
            }
        }
        
        // Remove from cache if no more subscribers
        let key = (source, pair.clone());
        self.prices.remove(&key);
        
        Ok(())
    }

    /// Get all available price keys
    pub fn keys(&self) -> Vec<MapKey> {
        // Always return all possible keys from exchange info, not just active ones
        let mut keys = Vec::new();
        
        // Get pairs from active exchanges only
        for source in crate::exchange_config::get_active_exchanges() {
            if let Ok(pairs) = crate::exchange_config::get_cached_pairs_if_enabled(source) {
                for pair in pairs {
                    keys.push((source, pair));
                }
            }
        }
        
        keys
    }
    
    /// Get all pairs that have ever been in the price cache (including inactive ones)
    pub fn all_known_pairs(&self) -> Vec<MapKey> {
        self.prices.iter()
            .map(|entry| entry.key().clone())
            .collect()
    }

    /// Get current price for a feed
    pub fn get_price(&self, source: Source, pair: &Pair) -> Option<Tick> {
        let key = (source, pair.clone());
        self.prices.get(&key).map(|entry| entry.0)
    }
    
    /// Update a price (used by REST manager and WebSocket handlers)
    pub async fn update(&self, source: Source, pair: Pair, tick: Tick) {
        let key = (source, pair.clone());

        self.prices
            .entry(key)
            .and_modify(|(t, tx)| {
                // Only process if this tick is newer
                if tick.event_ts >= t.event_ts {
                    if tick.price != t.price {
                        // Price changed - full update
                        MESSAGE_COUNTER.increment(source, pair.clone());
                        *t = tick;
                        let _ = tx.send(tick);
                    } else if tick.verified_at > t.verified_at {
                        // Price unchanged but verified_at is newer - update verified_at
                        t.verified_at = tick.verified_at;
                        let _ = tx.send(*t);  // Still notify watchers of freshness
                    }
                }
            })
            .or_insert_with(|| {
                // First time seeing this pair - increment counter
                MESSAGE_COUNTER.increment(source, pair.clone());
                let (tx, _) = watch::channel(tick);
                (tick, tx)
            });
    }
    
    /// Update only the verified_at timestamp (used by REST manager for freshness confirmation)
    pub fn update_verified_at(&self, source: Source, pair: &Pair, verified_at: u64) {
        let key = (source, pair.clone());
        if let Some(mut entry) = self.prices.get_mut(&key) {
            if verified_at > entry.0.verified_at {
                entry.0.verified_at = verified_at;
                // Notify watchers of freshness update
                let _ = entry.1.send(entry.0);
            }
        }
    }

    /// Get the latest price for a symbol (alias for get_price)
    pub fn latest(&self, source: Source, pair: &Pair) -> Result<Option<Tick>> {
        Ok(self.get_price(source, pair))
    }
    
    /// Check if a price has acceptable variance against secondary source
    /// Returns Ok(()) if variance is within threshold or no secondary configured
    /// Returns Err if variance exceeds 1% threshold
    pub fn check_variance(&self, source: Source, pair: &Pair, primary_price: Decimal) -> Result<()> {
        // Determine cache key based on source type
        let cache_key = if source == Source::Auto || source == Source::Weighted {
            // For AUTO/Weighted, use canonical pair
            debug!("Using canonical pair {} for variance check", pair.as_str());
            pair.clone()
        } else {
            // For direct exchange requests, use exchange-specific key
            let key = Pair {
                base: format!("{}:{}", source.as_str(), pair.base),
                quote: pair.quote.clone(),
            };
            debug!("Using exchange-specific key {}:{}/{} for variance check", 
                  source.as_str(), pair.base, pair.quote);
            key
        };
        
        // Get the cache entry which contains secondary source
        if let Some(entry) = AUTO_SOURCE_CACHE.get(&cache_key) {
            // Check if we have a secondary source for variance checking
            if let (Some(secondary_source), Some(secondary_pair)) = 
                (entry.secondary_source, entry.secondary_pair.as_ref()) {
                
                // Get secondary price
                if let Some(secondary_tick) = self.get_price(secondary_source, secondary_pair) {
                    let now = crate::clock_sync::get_corrected_timestamp_ms();
                    let age_ms = now.saturating_sub(secondary_tick.seen_at);
                    
                    if age_ms < 60_000 { // Less than 60 seconds old
                        // Use prices as-is for variance comparison
                        // Primary is already BIP-corrected from the stream, secondary is raw
                        let primary_f64 = primary_price.to_f64().unwrap_or(0.0);
                        let secondary_f64 = secondary_tick.price.to_f64().unwrap_or(0.0);

                        if primary_f64 > 0.0 && secondary_f64 > 0.0 {
                            // Calculate statistical variance using coefficient of variation
                            let mean = (primary_f64 + secondary_f64) / 2.0;
                            let variance = ((primary_f64 - mean).powi(2) + (secondary_f64 - mean).powi(2)) / 2.0;
                            let std_dev = variance.sqrt();
                            let coefficient_of_variation = (std_dev / mean) * 100.0;

                            if coefficient_of_variation > MAX_PRICE_VARIANCE_CV_PERCENT {
                                return Err(anyhow::anyhow!(
                                    "Price variance exceeded: {:.3}% coefficient of variation (threshold: {:.1}%). Primary: {} = ${:.2}, Secondary: {} on {} = ${:.2}, Mean: ${:.2}, StdDev: {:.2}",
                                    coefficient_of_variation, MAX_PRICE_VARIANCE_CV_PERCENT,
                                    pair.as_str(), primary_f64,
                                    secondary_pair.as_str(), secondary_source.as_str(), secondary_f64,
                                    mean, std_dev
                                ));
                            }
                            debug!("Variance {:.3}% coefficient of variation within acceptable {:.1}% threshold", coefficient_of_variation, MAX_PRICE_VARIANCE_CV_PERCENT);
                        }
                    }
                }
            }
        }
        
        // No secondary configured or variance check passed
        Ok(())
    }
    
    /// Get the latest price with variance check against second-best AUTO source
    pub fn latest_checked(&self, source: Source, pair: &Pair) -> Result<Option<Tick>> {
        debug!("latest_checked: Checking {} on {:?}", pair.as_str(), source);
        
        // Determine cache key based on source type
        let cache_key = if source == Source::Auto || source == Source::Weighted {
            // For AUTO/Weighted, use canonical pair
            debug!("Using canonical pair {} for AUTO/Weighted lookup", pair.as_str());
            pair.clone()
        } else {
            // For direct exchange requests, use exchange-specific key
            let key = Pair {
                base: format!("{}:{}", source.as_str(), pair.base),
                quote: pair.quote.clone(),
            };
            debug!("Using exchange-specific key {}:{}/{}", 
                  source.as_str(), pair.base, pair.quote);
            key
        };
        
        // Get the cache entry which contains both primary and secondary
        if let Some(entry) = AUTO_SOURCE_CACHE.get(&cache_key) {
            debug!("Cache entry found: primary={} on {:?}, secondary={:?}", 
                  entry.actual_pair.as_str(), entry.source, 
                  entry.secondary_pair.as_ref().map(|p| format!("{} on {:?}", p.as_str(), entry.secondary_source.unwrap())));
            
            // Get the primary price
            let mut primary_tick = match self.get_price(entry.source, &entry.actual_pair) {
                Some(tick) => {
                    debug!("Primary price: {} on {:?} = ${}",
                        entry.actual_pair.as_str(), entry.source, tick.price);
                    tick
                },
                None => return Err(anyhow::anyhow!(
                    "Price unavailable: {} on {} has no current price data",
                    entry.actual_pair.as_str(), entry.source.as_str()
                )),
            };

            // Apply BIP correction BEFORE variance check so we compare USD-equivalent prices
            // Only apply if user requested USD but got FDUSD (not if user specifically requested FDUSD)
            let original_primary_price = primary_tick.price;
            if should_apply_bip_correction(source, &pair.quote, &entry.actual_pair.quote) {
                let corrected_price = apply_bip_correction(original_primary_price, &entry.actual_pair.quote);
                primary_tick.price = corrected_price;

                debug!(
                    "💱 Task-runner BIP correction applied (before variance check): {} {} ${} → USD ${} (user requested {}, AUTO resolved to {}/{})",
                    entry.actual_pair.base,
                    entry.actual_pair.quote,
                    original_primary_price,
                    corrected_price,
                    pair.quote,
                    entry.actual_pair.base,
                    entry.actual_pair.quote
                );
            } else if pair.quote.to_uppercase() == entry.actual_pair.quote.to_uppercase() {
                debug!(
                    "No BIP correction: user specifically requested {} price ({})",
                    entry.actual_pair.quote,
                    entry.actual_pair.as_str()
                );
            }

            // Check if we have a secondary source for variance checking
            if let (Some(secondary_source), Some(secondary_pair)) = 
                (entry.secondary_source, entry.secondary_pair.as_ref()) {
                
                debug!("Secondary found: {} on {:?}", 
                      secondary_pair.as_str(), secondary_source);
                
                // Get secondary price and check variance
                if let Some(mut secondary_tick) = self.get_price(secondary_source, secondary_pair) {
                    debug!("Secondary price: {} on {:?} = ${}",
                          secondary_pair.as_str(), secondary_source, secondary_tick.price);

                    // Apply BIP correction to secondary price if needed
                    // Only apply if user requested USD and secondary is FDUSD
                    let original_secondary_price = secondary_tick.price;
                    if pair.quote.to_uppercase() == "USD" && secondary_pair.quote.to_uppercase() == "FDUSD" {
                        let corrected_secondary_price = apply_bip_correction(original_secondary_price, &secondary_pair.quote);
                        secondary_tick.price = corrected_secondary_price;

                        debug!(
                            "💱 Secondary BIP correction applied: {} {} ${} → USD ${} (user requested USD, secondary is FDUSD)",
                            secondary_pair.base,
                            secondary_pair.quote,
                            original_secondary_price,
                            corrected_secondary_price
                        );
                    } else {
                        debug!(
                            "No secondary BIP correction: user requested {}, secondary is {} (only correct FDUSD→USD when user wants USD)",
                            pair.quote,
                            secondary_pair.quote
                        );
                    }

                    let now = crate::clock_sync::get_corrected_timestamp_ms();
                    let age_ms = now.saturating_sub(secondary_tick.verified_at);

                    if age_ms < 60_000 { // Less than 60 seconds old (uses verified_at for REST freshness)
                        let primary_price = primary_tick.price.to_f64().unwrap_or(0.0);
                        let secondary_price = secondary_tick.price.to_f64().unwrap_or(0.0);
                        
                        if primary_price > 0.0 && secondary_price > 0.0 {
                            // Calculate statistical variance using coefficient of variation
                            let mean = (primary_price + secondary_price) / 2.0;
                            let variance = ((primary_price - mean).powi(2) + (secondary_price - mean).powi(2)) / 2.0;
                            let std_dev = variance.sqrt();
                            let coefficient_of_variation = (std_dev / mean) * 100.0;

                            debug!("Variance calculation: primary=${:.2} vs secondary=${:.2} = {:.3}% coefficient of variation",
                                  primary_price, secondary_price, coefficient_of_variation);

                            if coefficient_of_variation > MAX_PRICE_VARIANCE_CV_PERCENT {
                                warn!("VARIANCE FAILED {}: {} {} ${} vs {} {} ${} = {:.3}% CV (max {:.1}%)",
                                    pair.as_str(),
                                    entry.source.as_str(), entry.actual_pair.as_str(), primary_price,
                                    secondary_source.as_str(), secondary_pair.as_str(), secondary_price,
                                    coefficient_of_variation, MAX_PRICE_VARIANCE_CV_PERCENT);
                                // Determine which pair strings to use in error message
                                let primary_pair_str = if source == Source::Auto || source == Source::Weighted {
                                    entry.actual_pair.as_str()
                                } else {
                                    pair.as_str()
                                };

                                return Err(anyhow::anyhow!(
                                    "Price variance exceeded: {:.3}% coefficient of variation (threshold: {:.1}%). Primary: {} on {} = ${:.2}, Secondary: {} on {} = ${:.2}, Mean: ${:.2}, StdDev: {:.2}",
                                    coefficient_of_variation, MAX_PRICE_VARIANCE_CV_PERCENT,
                                    primary_pair_str, source.as_str(), primary_price,
                                    secondary_pair.as_str(), secondary_source.as_str(), secondary_price,
                                    mean, std_dev
                                ));
                            } else {
                                info!("VARIANCE OK {}: {} {} ${} vs {} {} ${} = {:.3}% CV",
                                    pair.as_str(),
                                    entry.source.as_str(), entry.actual_pair.as_str(), primary_price,
                                    secondary_source.as_str(), secondary_pair.as_str(), secondary_price,
                                    coefficient_of_variation);
                            }
                        }
                    } else {
                        info!("⚠️ NO VARIANCE CHECK: Secondary price too old ({}ms > 60s)", age_ms);
                    }
                } else {
                    info!("⚠️ NO VARIANCE CHECK: Secondary price unavailable for {} on {}",
                          secondary_pair.as_str(), secondary_source.as_str());
                }
            } else {
                info!("⚠️ NO VARIANCE CHECK: No secondary configured");
            }

            // Return the primary tick (BIP correction already applied above before variance check)
            Ok(Some(primary_tick))
        } else {
            // No cache entry found
            if source == Source::Auto || source == Source::Weighted {
                // AUTO/Weighted requires cache entry
                Err(anyhow::anyhow!(
                    "AUTO source unavailable: {} has not been resolved in the cache. The AUTO scoring system may still be initializing", 
                    pair.as_str()
                ))
            } else {
                // Direct exchange request - just get the price without variance checking
                match self.get_price(source, pair) {
                    Some(mut tick) => {
                        debug!("Direct price (no variance check): {} on {:?} = ${}",
                            pair.as_str(), source, tick.price);

                        // For direct exchange requests, BIP correction is not typically needed
                        // since they specify exact exchange/pair. But log for completeness.
                        debug!("Direct exchange request: {} on {:?} - no BIP correction applied",
                            pair.as_str(), source);

                        Ok(Some(tick))
                    },
                    None => Err(anyhow::anyhow!(
                        "Price unavailable: {} on {} exchange has no current price data",
                        pair.as_str(), source.as_str()
                    ))
                }
            }
        }
    }
    
    /// Get the latest price for a weighted feed with secondary validation
    /// Checks if a secondary source (by volume) has price within 1% variance
    /// Returns error if secondary source exists but variance > 1%
    /// Continues with warning if no secondary source configured
    pub fn latest_weighted(&self, pair: &Pair) -> Result<Option<Tick>> {
        debug!("🔍 LATEST_WEIGHTED: Starting validation for {}", pair.as_str());
        
        // Get the weighted source entry from cache
        if let Some(entry) = crate::hub::WEIGHTED_SOURCE_CACHE.get(pair) {
            debug!("✅ LATEST_WEIGHTED: Found cache entry for {} → primary: {} on {}", 
                pair.as_str(), entry.actual_pair.as_str(), entry.source.as_str());
            
            // Get primary price
            if let Some(primary_tick) = self.get_price(entry.source, &entry.actual_pair) {
                debug!("✅ LATEST_WEIGHTED: Primary price found: {} = {:.4} (age: {}ms)", 
                    entry.actual_pair.as_str(), primary_tick.price, 
                    crate::clock_sync::get_corrected_timestamp_ms().saturating_sub(primary_tick.seen_at));
                
                // Check secondary source if available
                if let (Some(secondary_source), Some(secondary_pair)) = 
                    (entry.secondary_source, entry.secondary_pair.as_ref()) {

                    debug!("🔍 LATEST_WEIGHTED: Checking secondary source: {} on {}", 
                        secondary_pair.as_str(), secondary_source.as_str());
                    
                    // Get secondary price if it exists and is fresh (< 60 seconds old)
                    if let Some(secondary_tick) = self.get_price(secondary_source, secondary_pair) {
                        let now = crate::clock_sync::get_corrected_timestamp_ms();
                        let age_ms = now.saturating_sub(secondary_tick.seen_at);
                        
                        debug!("✅ LATEST_WEIGHTED: Secondary price found: {} = {:.4} (age: {}ms)", 
                            secondary_pair.as_str(), secondary_tick.price, age_ms);
                        
                        if age_ms < 60_000 { // Less than 60 seconds old
                            // Calculate variance
                            let primary_price = primary_tick.price.to_f64().unwrap_or(0.0);
                            let secondary_price = secondary_tick.price.to_f64().unwrap_or(0.0);
                            
                            if primary_price > 0.0 && secondary_price > 0.0 {
                                // Calculate statistical variance using coefficient of variation
                                let mean = (primary_price + secondary_price) / 2.0;
                                let variance = ((primary_price - mean).powi(2) + (secondary_price - mean).powi(2)) / 2.0;
                                let std_dev = variance.sqrt();
                                let coefficient_of_variation = (std_dev / mean) * 100.0;

                                debug!("🔍 LATEST_WEIGHTED: Price variance calculation: {:.4} vs {:.4} = {:.3}% coefficient of variation",
                                    primary_price, secondary_price, coefficient_of_variation);

                                if coefficient_of_variation > MAX_PRICE_VARIANCE_CV_PERCENT {
                                    // ERROR: Secondary source exists but variance is out of bounds
                                    return Err(anyhow::anyhow!(
                                        "WEIGHTED PRICE VARIANCE ERROR: {}/{} has {:.3}% coefficient of variation between {} ({:.4}) and {} ({:.4}) - exceeds {:.1}% threshold, Mean: {:.4}, StdDev: {:.4}",
                                        pair.base, pair.quote, coefficient_of_variation, MAX_PRICE_VARIANCE_CV_PERCENT,
                                        entry.source.as_str(), primary_price,
                                        secondary_source.as_str(), secondary_price,
                                        mean, std_dev
                                    ));
                                } else {
                                    debug!(
                                        "✅ WEIGHTED PRICE VALIDATED: {}/{} variance {:.3}% coefficient of variation between {} and {}",
                                        pair.base, pair.quote, coefficient_of_variation,
                                        entry.source.as_str(), secondary_source.as_str()
                                    );
                                }
                            } else {
                                warn!("⚠️ LATEST_WEIGHTED: Invalid prices - primary: {:.4}, secondary: {:.4}", 
                                    primary_price, secondary_price);
                            }
                        } else {
                            warn!("⚠️ LATEST_WEIGHTED: Secondary price too old: {}ms > 60000ms", age_ms);
                        }
                    } else {
                        warn!("⚠️ LATEST_WEIGHTED_2: No secondary price available for {} on {}", 
                            secondary_pair.as_str(), secondary_source.as_str());
                    }
                } else {
                    // WARNING: No secondary source configured - continue but warn
                    warn!("⚠️ LATEST_WEIGHTED_2: No secondary source configured for {}", pair.as_str());
                }
                
                // Return primary price if validation passed or no secondary source
                debug!("✅ LATEST_WEIGHTED: Returning primary price {:.4} for {}", 
                    primary_tick.price, pair.as_str());
                return Ok(Some(primary_tick));
            } else {
                debug!("❌ LATEST_WEIGHTED: No primary price available for {} on {}", 
                    entry.actual_pair.as_str(), entry.source.as_str());
            }
        } else {
            debug!("❌ LATEST_WEIGHTED: No cache entry found for {}", pair.as_str());
        }
        
        // Fallback to regular latest if weighted source not found
        debug!("🔄 LATEST_WEIGHTED: Falling back to regular latest() for {}", pair.as_str());
        self.latest(Source::Weighted, pair)
    }
    
    /// Subscribe to a weighted price feed
    /// This will automatically select the exchange with the highest volume for the given pair
    pub async fn subscribe_weighted(&self, pair: Pair) -> Result<watch::Receiver<Tick>> {
        let calculator = crate::weighted_source::WeightedSourceCalculator::new();
        let (source, actual_pair) = calculator.get_weighted_source(&pair).await?;
        
        info!(
            "🎯 WEIGHTED: {} → {} on {}",
            pair.as_str(),
            actual_pair.as_str(),
            source.as_str()
        );
        
        // Subscribe to the actual source/pair combination (non-recursively)
        self.subscribe_to_exchange(source, actual_pair).await
    }
    
    /// Subscribe to an AUTO price feed
    /// This uses the pre-calculated AUTO scores from the cache
    /// Note: Both WEIGHTED and AUTO sources use this method
    pub async fn subscribe_auto(&self, pair: Pair) -> Result<watch::Receiver<Tick>> {
        // Get from AUTO cache
        let entry = AUTO_SOURCE_CACHE
            .get(&pair)
            .or_else(|| {
                // If pair has USD-equivalent quote, check USD cache
                if crate::weighted_source::is_usd_equivalent(&pair.quote) && pair.quote != "USD" {
                    let usd_pair = Pair {
                        base: pair.base.clone(),
                        quote: "USD".to_string(),
                    };
                    AUTO_SOURCE_CACHE.get(&usd_pair)
                } else {
                    None
                }
            })
            .ok_or_else(|| anyhow!("No AUTO source available for {} (cache may still be initializing)", pair.as_str()))?;
        
        let source = entry.source;
        let actual_pair = entry.actual_pair.clone();
        
        info!(
            "🎯 AUTO SUBSCRIPTION: {} → {} on {} (using AUTO score-based selection)",
            pair.as_str(),
            actual_pair.as_str(),
            source.as_str()
        );
        info!(
            "   ↳ This selection was made based on: 50% message rate, 25% spread quality, 15% volume, 10% market depth"
        );
        
        // Subscribe to the actual source/pair combination (non-recursively)
        self.subscribe_to_exchange(source, actual_pair).await
    }
    
    /// Internal method to subscribe to an exchange without recursion
    async fn subscribe_to_exchange(&self, source: Source, pair: Pair) -> Result<watch::Receiver<Tick>> {
        // Check if trying to subscribe to virtual sources
        if matches!(source, Source::Weighted | Source::Auto) {
            return Err(anyhow!("Cannot subscribe to {} source directly",
                match source { Source::Weighted => "weighted", Source::Auto => "auto", _ => "unknown" }));
        }

        // Check if exchange is in the list of active exchanges
        let active_exchanges = crate::exchange_config::get_active_exchanges();
        if !active_exchanges.contains(&source) {
            return Err(anyhow!("{} exchange is not active in configuration", source.as_str()));
        }

        // Verify the exchange is initialized (only check if it's in active exchanges)
        let exchange_initialized = match source {
            s if active_exchanges.contains(&s) => match s {
                Source::Binance => self.binance.is_some(),
                Source::Bybit => self.bybit.is_some(),
                Source::Okx => self.okx.is_some(),
                Source::Coinbase => self.coinbase.is_some(),
                Source::Bitget => self.bitget.is_some(),
                Source::Pyth => self.pyth.is_some(),
                Source::Titan => false, // Disabled
                Source::Weighted | Source::Auto => unreachable!(), // Already handled above
            },
            _ => false, // Not in active exchanges
        };

        if !exchange_initialized {
            return Err(anyhow!("{} exchange not initialized (but is active in config)", source.as_str()));
        }
        
        // Subscribe to the pair on the exchange (only for active exchanges)
        match source {
            s if active_exchanges.contains(&s) => match s {
                Source::Binance => {
                    if let Some(stream) = &self.binance {
                        let mut stream_guard = stream.lock().await;
                        stream_guard.listen(pair.clone())?;
                    }
                }
                Source::Bybit => {
                    if let Some(stream) = &self.bybit {
                        let mut stream_guard = stream.lock().await;
                        stream_guard.listen(pair.clone())?;
                    }
                }
                Source::Okx => {
                    if let Some(stream) = &self.okx {
                        let mut stream_guard = stream.lock().await;
                        stream_guard.listen(pair.clone())?;
                    }
                }
                Source::Coinbase => {
                    if let Some(stream) = &self.coinbase {
                        let mut stream_guard = stream.lock().await;
                        stream_guard.listen(pair.clone())?;
                    }
                }
                Source::Bitget => {
                    if let Some(stream) = &self.bitget {
                        let mut stream_guard = stream.lock().await;
                        stream_guard.listen(pair.clone())?;
                    }
                }
                Source::Pyth => {
                    if let Some(stream) = &self.pyth {
                        let mut stream_guard = stream.lock().await;
                        stream_guard.listen(pair.clone())?;
                    }
                }
                Source::Titan => {
                    // Disabled - Titan module kept for future use
                }
                Source::Weighted | Source::Auto => unreachable!(), // Already handled above
            },
            _ => return Err(anyhow!("{} exchange not found in active exchanges", source.as_str())),
        }
        
        // Get or create the watch channel
        let key = (source, pair);
        let rx = self.prices
            .get(&key)
            .map(|entry| entry.1.subscribe())
            .unwrap_or_else(|| {
                let tick = Tick::default();
                let (tx, rx) = watch::channel(tick);
                self.prices.insert(key, (tick, tx));
                rx
            });
        
        Ok(rx)
    }
    
    /// Get the current weighted price for a pair
    /// Returns the price from the exchange with highest volume
    pub async fn get_weighted_price(&self, pair: &Pair) -> Result<Option<Tick>> {
        let calculator = crate::weighted_source::WeightedSourceCalculator::new();
        let (source, actual_pair) = calculator.get_weighted_source(pair).await?;
        
        Ok(self.get_price(source, &actual_pair))
    }
    
    /// Start all exchanges and subscribe to all available trading pairs
    /// This is used for crossbar mode where we want all feeds active
    pub async fn start_all_exchanges(&mut self) -> Result<()> {
        info!("Starting all exchange connections for crossbar mode");
        
        // Start all exchanges concurrently
        info!("🚀 Starting all exchanges CONCURRENTLY");
        
        // We need a way to share the hub state across tasks
        // For now, let's at least start them without waiting for completion
        let _prices = self.prices.clone();
        
        // Spawn Binance
        crate::runtime_separation::spawn_on_ingestion_named(
            "binance-startup",
            async move {
                info!("🔵 Starting Binance exchange...");
                match BinanceStream::new().await {
                    Ok(_stream) => {
                        info!("✅ Binance stream created successfully - internal processing will handle updates");
                    }
                    Err(e) => error!("❌ Failed to start Binance: {}", e),
                }
            }
        );
        
        // Spawn Bybit
        crate::runtime_separation::spawn_on_ingestion_named(
            "bybit-startup",
            async move {
                info!("🟢 Starting Bybit exchange...");
                match BybitStream::new().await {
                    Ok(mut stream) => {
                        info!("✅ Bybit stream created successfully - internal processing will handle updates");
                        // Start the read loop - this is needed for Bybit
                        stream.start_read_loop();
                    }
                    Err(e) => error!("❌ Failed to start Bybit: {}", e),
                }
            }
        );
        
        // Spawn OKX
        crate::runtime_separation::spawn_on_ingestion_named(
            "okx-startup",
            async move {
                info!("🟡 Starting OKX exchange...");
                match OkxStream::new().await {
                    Ok(_stream) => {
                        info!("✅ OKX stream created successfully - internal processing will handle updates");
                    }
                    Err(e) => error!("❌ Failed to start OKX: {}", e),
                }
            }
        );
        
        // Spawn Bitget
        crate::runtime_separation::spawn_on_ingestion_named(
            "bitget-startup",
            async move {
                info!("🟠 Starting Bitget exchange...");
                match BitgetStream::new().await {
                    Ok(_stream) => {
                        info!("✅ Bitget stream created successfully - internal processing will handle updates");
                    }
                    Err(e) => error!("❌ Failed to start Bitget: {}", e),
                }
            }
        );

        // Spawn Pyth
        crate::runtime_separation::spawn_on_ingestion_named(
            "pyth-startup",
            async move {
                info!("🟣 Starting Pyth exchange...");
                match PythStream::new().await {
                    Ok(_stream) => {
                        info!("✅ Pyth stream created successfully - internal processing will handle updates");
                    }
                    Err(e) => error!("❌ Failed to start Pyth: {}", e),
                }
            }
        );

        // Spawn Coinbase
        // {
        //     let prices = prices.clone();
        //     crate::runtime_separation::spawn_on_ingestion_named(
        //         "coinbase-parallel-startup",
        //         async move {
        //             info!("🟣 Starting Coinbase exchange in parallel...");
        //         match CoinbaseStream::new().await {
        //             Ok(stream) => {
        //                 info!("✅ Coinbase stream created successfully");
        //                 let stream = Arc::new(tokio::sync::Mutex::new(stream));
                        
        //                 // Spawn the processing loop
        //                 let stream_clone = stream.clone();
        //                 let prices_clone = prices.clone();
        //                 crate::runtime_separation::spawn_on_ingestion_named(
        //                     "coinbase-parallel-processor",
        //                     async move {
        //                         info!("🟣 Coinbase processing task started");
        //                     loop {
        //                         let result = {
        //                             let mut stream_guard = stream_clone.lock().await;
        //                             use futures::StreamExt;
        //                             stream_guard.next().await
        //                         };
                                
        //                         match result {
        //                             Some(Ok(ticker)) => {
        //                                 if let Ok(pair) = Pair::from_task(&ticker.symbol) {
        //                                     let key = (Source::Coinbase, pair.clone());
        //                                     let tick = Tick {
        //                                         price: ticker.price,
        //                                         event_ts: ticker.timestamp,
        //                                         seen_at: crate::clock_sync::get_corrected_timestamp_ms(),
        //                                     };
                                            
        //                                     prices_clone.entry(key)
        //                                         .and_modify(|(t, tx)| {
        //                                             *t = tick;
        //                                             let _ = tx.send(tick);
        //                                         })
        //                                         .or_insert_with(|| {
        //                                             let (tx, _) = watch::channel(tick);
        //                                             (tick, tx)
        //                                         });
        //                                 }
        //                             }
        //                             Some(Err(e)) => {
        //                                 warn!("Coinbase ticker error: {}", e);
        //                             }
        //                             None => {
        //                                 warn!("Coinbase stream ended");
        //                                 break;
        //                             }
        //                         }
        //                     }
        //                 });
        //             }
        //             Err(e) => error!("❌ Failed to start Coinbase: {}", e),
        //         }
        //     });
        // }
        
        // Don't store the streams in hub fields since they're managed by the spawned tasks
        info!("🚀 All exchange initialization tasks spawned");
        
        // Also spawn REST manager and other background services concurrently
        crate::runtime_separation::spawn_on_ingestion_named(
            "rest-manager-parallel",
            async {
                info!("Starting REST manager for lower volume symbols");
            crate::rest_manager::start_rest_manager().await;
        });
        
        // Start weighted source calculator background updates
        // DISABLED - Using AUTO only for now
        // crate::runtime_separation::spawn_on_ingestion_named(
        //     "weighted-source-calculator",
        //     async {
        //         info!("Starting weighted source calculator");
        //     crate::weighted_source::WeightedSourceCalculator::start_background_updates();
        //     
        //     // Do initial calculation after a short delay to let exchanges populate
        //     tokio::time::sleep(Duration::from_secs(10)).await;
        //     let calculator = crate::weighted_source::WeightedSourceCalculator::new();
        //     if let Err(e) = calculator.update_all_weighted_sources().await {
        //         warn!("Failed initial weighted source calculation: {}", e);
        //     }
        // });
        
        info!("All exchange initialization tasks started");
        Ok(())
    }
    
    /// Set WebSocket pairs for a specific exchange
    /// This replaces all existing pairs for the exchange
    pub fn set_websocket_pairs(&self, source: Source, pairs: &[Pair]) {
        let before_count = self.websocket_active_pairs.len();
        
        // Clear old entries for this source
        self.websocket_active_pairs.retain(|(s, _), _| *s != source);
        
        // Add new pairs
        for pair in pairs {
            self.websocket_active_pairs.insert((source, pair.clone()), ());
            // Debug log for BTC/USDT
            if source == Source::Binance && pair.base == "BTC" && pair.quote == "USDT" {
                info!("📡 DEBUG: Registered BTC/USDT for Binance. Pair: {:?}", pair);
            }
        }
        
        let after_count = self.websocket_active_pairs.len();
        info!("📡 WEBSOCKET-PAIRS: Updated {} pairs for {} (total tracked: {} → {})", 
            pairs.len(), source.as_str(), before_count, after_count);
    }
    
    /// Check if a pair is actively tracked by WebSocket
    pub fn is_websocket_active(&self, source: Source, pair: &Pair) -> bool {
        self.websocket_active_pairs.contains_key(&(source, pair.clone()))
    }
    
    /// Clear all WebSocket pairs for a specific exchange
    pub fn clear_websocket_pairs(&self, source: Source) {
        let before_count = self.websocket_active_pairs.len();
        self.websocket_active_pairs.retain(|(s, _), _| *s != source);
        let after_count = self.websocket_active_pairs.len();
        
        info!("📡 WEBSOCKET-PAIRS: Cleared {} pairs for {} (total tracked: {} → {})", 
            before_count - after_count, source.as_str(), before_count, after_count);
    }
    
    /// Get all WebSocket pairs for an exchange
    pub fn get_websocket_pairs(&self, source: Source) -> Vec<Pair> {
        self.websocket_active_pairs
            .iter()
            .filter(|entry| entry.key().0 == source)
            .map(|entry| entry.key().1.clone())
            .collect()
    }
    
    /// Register WebSocket pairs for an exchange (accumulative - adds to existing pairs)
    pub fn register_websocket_pairs(&self, source: Source, pairs: &[Pair]) {
        let source_before = self.get_websocket_pairs(source).len();
        
        let mut added = 0;
        let mut duplicates = 0;
        
        // Add new pairs without clearing existing ones
        for pair in pairs {
            let key = (source, pair.clone());
            if self.websocket_active_pairs.contains_key(&key) {
                duplicates += 1;
            } else {
                self.websocket_active_pairs.insert(key, ());
                added += 1;
            }
        }
        
        let source_after = self.get_websocket_pairs(source).len();
        
        info!("📡 WS-REGISTER: {} added {} pairs ({} duplicates, total: {} → {})", 
            source.as_str(), added, duplicates, source_before, source_after);
    }
    
    /// Remove WebSocket pairs for an exchange (used during failover)
    pub fn remove_websocket_pairs(&self, source: Source, pairs: &[Pair]) {
        let before_count = self.get_websocket_pairs(source).len();
        let mut removed = 0;
        
        for pair in pairs {
            let key = (source, pair.clone());
            if self.websocket_active_pairs.remove(&key).is_some() {
                removed += 1;
            }
        }
        
        let after_count = self.get_websocket_pairs(source).len();
        
        info!("📡 WS-REMOVE: {} removed {} pairs (total: {} → {})",
            source.as_str(), removed, before_count, after_count);
    }

    /// Get WebSocket health summary with connection-level details from MESSAGE_RATE_CACHE
    pub fn get_websocket_health_summary(&self) -> std::collections::HashMap<String, crate::types::ExchangeWebSocketHealth> {
        use std::collections::HashMap;
        use crate::message_rate::MESSAGE_RATE_CACHE;

        let mut health = HashMap::new();

        for source in crate::exchange_config::get_active_exchanges() {
            let websocket_pairs = self.get_websocket_pairs(source);
            let websocket_count = websocket_pairs.len();

            let all_pairs: Vec<_> = self.keys()
                .into_iter()
                .filter(|(s, _)| *s == source)
                .collect();
            let total_count = all_pairs.len();
            let rest_count = total_count.saturating_sub(websocket_count);

            // Build connection-level health from MESSAGE_RATE_CACHE
            let connections = self.build_connection_health_from_rates(source);

            health.insert(
                source.as_str().to_lowercase(),
                crate::types::ExchangeWebSocketHealth {
                    total_websocket_symbols: websocket_count,
                    total_rest_symbols: rest_count,
                    connections,
                }
            );
        }

        health
    }

    /// Build connection health by reading from CONNECTION_METADATA_CACHE (populated by streams)
    fn build_connection_health_from_rates(&self, source: Source) -> Vec<crate::types::WebSocketConnectionHealth> {
        use crate::message_rate::MESSAGE_RATE_CACHE;
        let mut connections = Vec::new();

        // Check what's in the cache for debugging
        let cache_count = CONNECTION_METADATA_CACHE.iter()
            .filter(|e| e.key().0 == source)
            .count();

        // Read from CONNECTION_METADATA_CACHE (has actual connection IDs and symbols from streams!)
        for entry in CONNECTION_METADATA_CACHE.iter() {
            let ((src, conn_id), metadata) = entry.pair();
            if *src != source { continue; }

            // Count how many symbols are on WebSocket vs REST
            let mut ws_count = 0;
            let mut rest_count = 0;
            let mut rest_symbols = Vec::new();

            // Calculate average msg_per_sec for symbols on this connection from MESSAGE_RATE_CACHE
            let mut total_rate = 0.0;
            let mut rate_count = 0;

            // Get all cached pairs for symbol lookup
            let cached_pairs = match source {
                Source::Binance => crate::exchanges::binance::get_cached_pairs().ok(),
                Source::Okx => crate::exchanges::okx::get_cached_pairs().ok(),
                Source::Bybit => crate::exchanges::bybit::get_cached_pairs().ok(),
                Source::Bitget => crate::exchanges::bitget::get_cached_pairs().ok(),
                Source::Pyth => crate::exchanges::pyth::get_cached_pairs().ok(),
                _ => None,
            };

            if let Some(pairs) = cached_pairs {
                for symbol_str in &metadata.symbols {
                    // Find the Pair for this symbol
                    let pair_opt = match source {
                        Source::Binance => pairs.iter().find(|p| p.as_binance_str() == *symbol_str),
                        Source::Okx => pairs.iter().find(|p| p.as_okx_str() == *symbol_str),
                        Source::Bybit => pairs.iter().find(|p| p.as_bybit_str() == *symbol_str),
                        Source::Bitget => pairs.iter().find(|p| p.as_bitget_str() == *symbol_str),
                        Source::Pyth => pairs.iter().find(|p| p.as_pyth_str() == *symbol_str),
                        _ => None,
                    };

                    if let Some(pair) = pair_opt {
                        // Calculate msg_per_sec from MESSAGE_RATE_CACHE (price changes only)
                        let rate = MESSAGE_RATE_CACHE.get_rate(source, pair);
                        if rate > 0.0 {
                            debug!("🔍 {}: {} has rate {:.2}", source.as_str(), pair.as_str(), rate);
                        }
                        total_rate += rate;
                        rate_count += 1;

                        if self.is_websocket_active(source, pair) {
                            ws_count += 1;
                        } else {
                            rest_count += 1;
                            rest_symbols.push(symbol_str.clone());
                        }
                    }
                }
            }

            // Calculate total msg_per_sec across all symbols on this connection
            let total_msg_per_sec = total_rate;  // Sum of all symbol rates = total connection throughput

            // Determine status based on connection state and WebSocket coverage
            let total = metadata.symbols.len();
            let ws_percentage = if total > 0 {
                (ws_count as f64 / total as f64) * 100.0
            } else {
                0.0
            };

            let status = if !metadata.is_connected {
                "disconnected"
            } else if ws_percentage < 50.0 {
                "partially_connected"  // Less than 50% of symbols on WebSocket
            } else {
                "connected"  // 50% or more symbols on WebSocket
            };

            connections.push(crate::types::WebSocketConnectionHealth {
                exchange: source.as_str().to_lowercase(),
                connection_type: conn_id.clone(), // Real ID: "Priority", "HighVolume", "Regular1", "Regular2"
                total_symbols: metadata.symbols.len(), // All symbols, not just ones with messages!
                websocket_active_symbols: ws_count,
                rest_fallback_symbols: rest_count,
                msg_per_sec: total_msg_per_sec, // Total throughput from MESSAGE_RATE_CACHE (price changes only)!
                latency_ms: 0, // Not available from MESSAGE_RATE_CACHE
                status: status.to_string(),
                rest_symbols: if !rest_symbols.is_empty() { Some(rest_symbols) } else { None },
            });
        }
        connections
    }

    /// Get priority pairs for a specific exchange
    fn get_priority_pairs_for_exchange(&self, source: Source) -> Vec<Pair> {
        match source {
            Source::Binance => vec![
                Pair::from_task("BTC/USDT").unwrap(), Pair::from_task("BTC/FDUSD").unwrap(),
                Pair::from_task("ETH/USDT").unwrap(), Pair::from_task("ETH/FDUSD").unwrap(),
                Pair::from_task("SOL/USDT").unwrap(), Pair::from_task("BNB/USDT").unwrap(),
            ],
            Source::Okx | Source::Bybit | Source::Bitget => vec![
                Pair::from_task("BTC/USDT").unwrap(), Pair::from_task("ETH/USDT").unwrap(),
                Pair::from_task("SOL/USDT").unwrap(),
            ],
            _ => Vec::new(),
        }
    }

    /// Get priority pairs WebSocket status
    /// If custom_pairs provided, adds those to the default priority list
    pub fn get_priority_pairs_status(&self, custom_pairs: Option<Vec<String>>) -> crate::types::PriorityPairsStatus {
        use std::collections::HashMap;

        // Default priority pairs
        let mut priority_pairs = vec![
            Pair::from_task("BTC/USDT").unwrap(),
            Pair::from_task("BTC/FDUSD").unwrap(),
            Pair::from_task("ETH/USDT").unwrap(),
            Pair::from_task("ETH/FDUSD").unwrap(),
            Pair::from_task("SOL/USDT").unwrap(),
            Pair::from_task("BNB/USDT").unwrap(),
            Pair::from_task("BNB/FDUSD").unwrap(),
            Pair::from_task("XRP/USDT").unwrap(),
            Pair::from_task("DOGE/USDT").unwrap(),
        ];

        // Store mapping of pair to specified exchange (for custom pairs with exchange specified)
        let mut specified_exchanges: HashMap<Pair, Source> = HashMap::new();

        // Add custom pairs if provided (limit to 10 additional)
        // Format: "pair" or "exchange:pair" (e.g., "BTC/USDT" or "Binance:BTC/USDT")
        if let Some(custom) = custom_pairs {
            let mut added_count = 0;
            let mut custom_pairs_with_exchange: Vec<(Option<Source>, Pair)> = Vec::new();

            for pair_str in custom {
                if added_count >= 10 {
                    warn!("Custom pairs limit reached (10), ignoring remaining pairs");
                    break;
                }

                // Parse format: "exchange:pair" or just "pair"
                let (specified_exchange, pair_part) = if pair_str.contains(':') {
                    let parts: Vec<&str> = pair_str.splitn(2, ':').collect();
                    let exch = match parts[0].to_lowercase().as_str() {
                        "binance" => Some(Source::Binance),
                        "okx" => Some(Source::Okx),
                        "bybit" => Some(Source::Bybit),
                        "bitget" => Some(Source::Bitget),
                        "coinbase" => Some(Source::Coinbase),
                        _ => {
                            warn!("Unknown exchange '{}', ignoring", parts[0]);
                            None
                        }
                    };
                    (exch, parts[1])
                } else {
                    (None, pair_str.as_str())
                };

                if let Ok(mut pair) = Pair::from_task(pair_part) {
                    // If quote is USD, resolve via AUTO cache to get actual pair
                    if pair.quote == "USD" {
                        use crate::hub::AUTO_SOURCE_CACHE;
                        if let Some(entry) = AUTO_SOURCE_CACHE.get(&pair) {
                            // Use the resolved pair from AUTO cache (unless exchange was specified)
                            if specified_exchange.is_none() {
                                pair = entry.actual_pair.clone();
                                debug!("Resolved {}/USD via AUTO to {} on {}", pair.base, pair.as_str(), entry.source.as_str());
                            } else {
                                warn!("Cannot specify exchange for USD pairs, use specific stablecoin (USDT/FDUSD)");
                                continue;
                            }
                        } else {
                            // AUTO cache doesn't have this USD pair yet
                            warn!("AUTO cache doesn't have {}/USD, skipping", pair.base);
                            continue;
                        }
                    }

                    // Check if equivalent already in list (only if exchange NOT specified)
                    let has_equivalent = if specified_exchange.is_none() {
                        priority_pairs.iter().any(|p| {
                            p.base == pair.base && (
                                p.quote == pair.quote ||
                                (is_usd_equivalent(&p.quote) && is_usd_equivalent(&pair.quote))
                            )
                        })
                    } else {
                        // If exchange specified, allow even if equivalent exists
                        // (e.g., can request both Binance:BTC/USDT and OKX:BTC/USDT)
                        false
                    };

                    if !has_equivalent {
                        custom_pairs_with_exchange.push((specified_exchange, pair));
                        added_count += 1;
                    }
                }
            }

            if added_count > 0 {
                info!("Added {} custom pairs to priority status check", added_count);
            }

            // Store exchange mappings and add pairs
            for (specified_exch, pair) in custom_pairs_with_exchange {
                if let Some(exch) = specified_exch {
                    specified_exchanges.insert(pair.clone(), exch);
                }
                priority_pairs.push(pair);
            }
        }

        let mut websocket_active = 0;
        let mut rest_fallback = 0;
        let mut pairs_status = HashMap::new();

        for pair in &priority_pairs {
            use crate::message_rate::MESSAGE_RATE_CACHE;

            let mut found_exchange: Option<Source> = None;
            let mut is_on_websocket = false;

            // Check if exchange was specified for this pair
            if let Some(specified_exch) = specified_exchanges.get(pair) {
                // Use the specified exchange (check WebSocket or REST)
                if self.is_websocket_active(*specified_exch, &pair) {
                    is_on_websocket = true;
                    found_exchange = Some(*specified_exch);
                } else {
                    // Not on WebSocket, but check if it exists on this exchange at all (REST)
                    let exists_on_exchange = self.keys().iter().any(|(s, p)| *s == *specified_exch && p == pair);
                    if exists_on_exchange {
                        found_exchange = Some(*specified_exch);
                        is_on_websocket = false;
                    }
                }
            } else {
                // Find which exchange has this pair on WebSocket - prefer Binance for BTC pairs
                let exchanges_to_check: Vec<Source> = if pair.base == "BTC" {
                    // Check Binance first for BTC pairs
                    let mut exch = vec![Source::Binance];
                    exch.extend(crate::exchange_config::get_active_exchanges().into_iter().filter(|s| *s != Source::Binance));
                    exch
                } else {
                    crate::exchange_config::get_active_exchanges()
                };

                for source in exchanges_to_check {
                    if self.is_websocket_active(source, &pair) {
                        is_on_websocket = true;
                        found_exchange = Some(source);
                        break;
                    }
                }
            }

            if is_on_websocket {
                websocket_active += 1;
                let exchange = found_exchange.unwrap();
                let msg_rate = MESSAGE_RATE_CACHE.get_rate(exchange, pair);

                pairs_status.insert(
                    pair.as_str().to_string(),
                    crate::types::PriorityPairDetail {
                        status: "ws_active".to_string(),  // Successfully on WebSocket
                        exchange: Some(exchange.as_str().to_string()),
                        msg_per_sec: msg_rate,
                    }
                );
            } else {
                // Not on WebSocket - check if it should be (is it in a WebSocket connection's symbol list?)
                let mut should_be_ws = false;
                let mut found_exchange_for_rest: Option<Source> = None;

                // Check if this pair is in CONNECTION_METADATA_CACHE (means it's assigned to a WebSocket connection)
                for entry in CONNECTION_METADATA_CACHE.iter() {
                    let ((source, _conn_id), metadata) = entry.pair();

                    // Check if this pair's symbol is in the connection's symbols list
                    let symbol_str = match source {
                        Source::Binance => pair.as_binance_str(),
                        Source::Okx => pair.as_okx_str(),
                        Source::Bybit => pair.as_bybit_str(),
                        Source::Bitget => pair.as_bitget_str(),
                        Source::Pyth => pair.as_pyth_str(),
                        _ => String::new(),
                    };

                    if metadata.symbols.contains(&symbol_str) {
                        should_be_ws = true;
                        found_exchange_for_rest = Some(*source);
                        break;
                    }
                }

                // Find which exchange has this pair (even if on REST)
                let pair_exchange = found_exchange_for_rest.or_else(|| {
                    self.keys().iter()
                        .find(|(_, p)| p == pair)
                        .map(|(s, _)| *s)
                });

                if let Some(exch) = pair_exchange {
                    rest_fallback += 1;
                    let status = if should_be_ws {
                        "ws_rest_failover"  // Should be on WS but fell back to REST
                    } else {
                        "rest"  // Never was on WebSocket (REST-only symbol)
                    };

                    // Get message rate even for REST symbols (they still update via REST API)
                    let msg_rate = MESSAGE_RATE_CACHE.get_rate(exch, pair);

                    pairs_status.insert(
                        pair.as_str().to_string(),
                        crate::types::PriorityPairDetail {
                            status: status.to_string(),
                            exchange: Some(exch.as_str().to_string()),
                            msg_per_sec: msg_rate,
                        }
                    );
                }
            }
        }

        crate::types::PriorityPairsStatus {
            total_priority_pairs: priority_pairs.len(),
            websocket_active,
            rest_fallback,
            pairs: pairs_status,
        }
    }

    /// Build requested feeds status - resolves and tracks each requested feed
    /// This is separate from priority_pairs_status to maintain original request names
    pub fn build_requested_feeds_status(&self, requested_pairs: &[String]) -> std::collections::HashMap<String, crate::types::RequestedFeedDetail> {
        use crate::message_rate::MESSAGE_RATE_CACHE;
        use std::collections::HashMap;

        let mut results = HashMap::new();

        for pair_str in requested_pairs {
            // Parse: "exchange:symbol" or just "symbol"
            let (specified_exch, symbol_part) = if pair_str.contains(':') {
                let parts: Vec<&str> = pair_str.splitn(2, ':').collect();
                let exch = match parts[0].to_lowercase().as_str() {
                    "binance" => Some(Source::Binance),
                    "okx" => Some(Source::Okx),
                    "bybit" => Some(Source::Bybit),
                    "bitget" => Some(Source::Bitget),
                    _ => None,
                };
                (exch, parts[1])
            } else {
                (None, pair_str.as_str())
            };

            // Parse the pair
            let Ok(mut pair) = Pair::from_task(symbol_part) else { continue; };
            let original_symbol = symbol_part.to_string();

            // Resolve if USD quote - also capture secondary info
            let (resolved_pair, resolved_source, secondary_info) = if pair.quote == "USD" {
                use crate::hub::AUTO_SOURCE_CACHE;
                if let Some(entry) = AUTO_SOURCE_CACHE.get(&pair) {
                    // Capture secondary source/pair if available
                    let secondary = if let (Some(sec_source), Some(sec_pair)) = (entry.secondary_source, entry.secondary_pair.clone()) {
                        Some((sec_source, sec_pair))
                    } else {
                        None
                    };
                    (entry.actual_pair.clone(), entry.source, secondary)
                } else {
                    continue; // Skip if AUTO cache doesn't have it yet
                }
            } else if let Some(exch) = specified_exch {
                // Exchange specified, use it (no secondary for explicit exchange requests)
                (pair.clone(), exch, None)
            } else {
                // Find best exchange (prefer Binance for BTC) - no secondary for non-USD quotes
                let exchange = if pair.base == "BTC" {
                    // Check Binance first
                    if self.is_websocket_active(Source::Binance, &pair) {
                        Source::Binance
                    } else {
                        // Find any exchange with this pair
                        crate::exchange_config::get_active_exchanges()
                            .into_iter()
                            .find(|s| self.is_websocket_active(*s, &pair))
                            .unwrap_or(Source::Binance)
                    }
                } else {
                    crate::exchange_config::get_active_exchanges()
                        .into_iter()
                        .find(|s| self.is_websocket_active(*s, &pair))
                        .unwrap_or(Source::Binance)
                };
                (pair.clone(), exchange, None)
            };

            // Get status and message rate
            let is_ws = self.is_websocket_active(resolved_source, &resolved_pair);
            let msg_rate = MESSAGE_RATE_CACHE.get_rate(resolved_source, &resolved_pair);

            // Check if should be on WS (is it in a WS connection?)
            let should_be_ws = CONNECTION_METADATA_CACHE.iter().any(|entry| {
                let ((source, _), metadata) = entry.pair();
                if *source != resolved_source {
                    return false;
                }

                let symbol_str = match resolved_source {
                    Source::Binance => resolved_pair.as_binance_str(),
                    Source::Okx => resolved_pair.as_okx_str(),
                    Source::Bybit => resolved_pair.as_bybit_str(),
                    Source::Bitget => resolved_pair.as_bitget_str(),
                    Source::Pyth => resolved_pair.as_pyth_str(),
                    _ => String::new(),
                };

                metadata.symbols.contains(&symbol_str)
            });

            let status = if is_ws {
                "ws_active"
            } else if should_be_ws {
                "ws_rest_failover"
            } else {
                "rest"
            };

            // Get secondary info if available
            let (secondary_exchange, secondary_pair_str, secondary_msg_rate) = if let Some((sec_source, sec_pair)) = secondary_info {
                let sec_rate = MESSAGE_RATE_CACHE.get_rate(sec_source, &sec_pair);
                (
                    Some(sec_source.as_str().to_string()),
                    Some(sec_pair.as_str().to_string()),
                    Some(sec_rate)
                )
            } else {
                (None, None, None)
            };

            results.insert(
                pair_str.clone(),  // Use original request as key!
                crate::types::RequestedFeedDetail {
                    original_request: original_symbol,
                    resolved_to: resolved_pair.as_str().to_string(),
                    exchange: resolved_source.as_str().to_string(),
                    msg_per_sec: msg_rate,
                    status: status.to_string(),
                    secondary_exchange,
                    secondary_pair: secondary_pair_str,
                    secondary_msg_per_sec: secondary_msg_rate,
                }
            );
        }

        results
    }
}

/// Get all available surge sources
pub fn all_surge_sources() -> Vec<Source> {
    crate::exchange_config::get_active_exchanges()
}

/// Get all available surge feeds including WEIGHTED sources and USD equivalents
pub async fn get_all_surge_feeds(
    symbol_filter: Option<String>,
    exchange_filter: Option<String>,
) -> Result<crate::schema::SurgeFeedsResponse> {
    use crate::schema::{SurgeFeed, SurgeFeedInfo, SurgeFeedsResponse};
    use crate::feed_metadata::get_or_create_feed_metadata;
    use std::collections::HashMap;

    let hub = SurgeHub::global();
    let mut symbol_feeds: HashMap<Pair, Vec<SurgeFeedInfo>> = HashMap::new();

    // Get all exchange feeds from the hub
    let all_keys = hub.keys();

    for (source, pair) in all_keys {
        let source_name = source.as_str().to_uppercase();

        // Get the actual cryptographic feed_id from metadata
        let feed_id = match get_or_create_feed_metadata(&pair, Some(source)) {
            Ok(metadata) => metadata.feed_id,
            Err(e) => {
                tracing::warn!("Failed to get metadata for {}/{}: {}, using fallback", pair, source_name, e);
                format!("feed_{}_{}", pair.as_str(), source_name)
            }
        };

        let feed_info = SurgeFeedInfo {
            source: source_name,
            feed_id,
        };

        symbol_feeds
            .entry(pair.clone())
            .or_default()
            .push(feed_info);
    }

    // Add WEIGHTED source for symbols with multiple exchanges
    let symbols_with_multiple: Vec<Pair> = symbol_feeds
        .iter()
        .filter(|(_, feeds)| feeds.len() > 1)
        .map(|(symbol, _)| symbol.clone())
        .collect();

    for symbol in symbols_with_multiple {
        // For WEIGHTED, get the metadata for WEIGHTED source (None means WEIGHTED)
        let feed_id = match get_or_create_feed_metadata(&symbol, None) {
            Ok(metadata) => metadata.feed_id,
            Err(e) => {
                tracing::warn!("Failed to get WEIGHTED metadata for {}: {}, using fallback", symbol, e);
                format!("feed_{}_WEIGHTED", symbol.as_str())
            }
        };
        
        symbol_feeds.get_mut(&symbol).unwrap().push(SurgeFeedInfo {
            source: "WEIGHTED".to_string(),
            feed_id,
        });
    }

    // Add WEIGHTED source for symbols with only 1 exchange
    let symbols_with_single: Vec<Pair> = symbol_feeds
        .iter()
        .filter(|(_, feeds)| feeds.len() == 1)
        .map(|(symbol, _)| symbol.clone())
        .collect();

    for symbol in symbols_with_single {
        // For single source, WEIGHTED still has its own feed_id (None means WEIGHTED)
        let feed_id = match get_or_create_feed_metadata(&symbol, None) {
            Ok(metadata) => metadata.feed_id,
            Err(e) => {
                tracing::warn!("Failed to get WEIGHTED metadata for single source {}: {}, using fallback", symbol, e);
                format!("feed_{}_WEIGHTED", symbol.as_str())
            }
        };
        
        symbol_feeds.get_mut(&symbol).unwrap().push(SurgeFeedInfo {
            source: "WEIGHTED".to_string(),
            feed_id,
        });
    }

    // Add ALL USD pairs from WEIGHTED_SOURCE_CACHE
    // These are synthetic USD pairs created from USD-equivalent stablecoins
    for entry in WEIGHTED_SOURCE_CACHE.iter() {
        let pair = entry.key();
        
        // Skip exchange-specific pairs (format: "Exchange:BASE/QUOTE")
        if pair.base.contains(':') {
            continue;
        }
        
        // Only process USD pairs that aren't already in symbol_feeds
        if pair.quote == "USD" && !symbol_feeds.contains_key(pair) {
            // For WEIGHTED USD pairs, get the metadata
            let feed_id = match get_or_create_feed_metadata(pair, None) {
                Ok(metadata) => metadata.feed_id,
                Err(e) => {
                    tracing::warn!("Failed to get WEIGHTED USD metadata for {}: {}, using fallback", pair, e);
                    format!("feed_{}_WEIGHTED", pair.as_str())
                }
            };
            
            symbol_feeds
                .entry(pair.clone())
                .or_default()
                .push(SurgeFeedInfo {
                    source: "WEIGHTED".to_string(),
                    feed_id,
                });
        }
    }

    // Add ALL USD pairs from AUTO_SOURCE_CACHE
    // These are the best single-exchange USD pairs selected by AUTO algorithm
    for entry in AUTO_SOURCE_CACHE.iter() {
        let pair = entry.key();
        
        // Skip exchange-specific pairs (format: "Exchange:BASE/QUOTE")
        if pair.base.contains(':') {
            continue;
        }
        
        // Add all USD pairs from AUTO cache
        if pair.quote == "USD" {
            // Check if we need to add AUTO feed for this pair
            let needs_auto = !symbol_feeds
                .get(pair)
                .map(|feeds| feeds.iter().any(|f| f.source == "AUTO"))
                .unwrap_or(false);
                
            if needs_auto {
                let feed_id = match get_or_create_feed_metadata(pair, Some(Source::Auto)) {
                    Ok(metadata) => metadata.feed_id,
                    Err(e) => {
                        tracing::warn!("Failed to get AUTO USD metadata for {}: {}, using fallback", pair, e);
                        format!("feed_{}_AUTO", pair.as_str())
                    }
                };
                
                symbol_feeds
                    .entry(pair.clone())
                    .or_default()
                    .push(SurgeFeedInfo {
                        source: "AUTO".to_string(),
                        feed_id,
                    });
            }
        }
    }

    // Add AUTO feeds for ALL pairs in AUTO_SOURCE_CACHE (including non-USD pairs)
    // This ensures we have AUTO for all pairs that AUTO algorithm has processed
    for entry in AUTO_SOURCE_CACHE.iter() {
        let pair = entry.key();
        
        // Skip exchange-specific pairs (format: "Exchange:BASE/QUOTE")
        if pair.base.contains(':') {
            continue;
        }
        
        // Skip USD pairs as we already handled them above
        if pair.quote == "USD" {
            continue;
        }
        
        // Check if we need to add AUTO feed for this pair
        let needs_auto = !symbol_feeds
            .get(pair)
            .map(|feeds| feeds.iter().any(|f| f.source == "AUTO"))
            .unwrap_or(false);
            
        if needs_auto {
            let feed_id = match get_or_create_feed_metadata(pair, Some(Source::Auto)) {
                Ok(metadata) => metadata.feed_id,
                Err(e) => {
                    tracing::warn!("Failed to get AUTO metadata for {}: {}, using fallback", pair, e);
                    format!("feed_{}_AUTO", pair.as_str())
                }
            };
            
            symbol_feeds
                .entry(pair.clone())
                .or_default()
                .push(SurgeFeedInfo {
                    source: "AUTO".to_string(),
                    feed_id,
                });
        }
    }
    
    // Also add AUTO for any exchange pairs not yet in AUTO_SOURCE_CACHE
    // This handles newly added pairs before AUTO cache updates
    let mut pairs_needing_auto: Vec<Pair> = Vec::new();
    
    for (pair, feed_infos) in &symbol_feeds {
        // Skip if already has AUTO
        if feed_infos.iter().any(|f| f.source == "AUTO") {
            continue;
        }
        
        // If has at least one real exchange feed (not just WEIGHTED)
        if feed_infos.iter().any(|f| f.source != "WEIGHTED" && f.source != "AUTO") {
            pairs_needing_auto.push(pair.clone());
        }
    }
    
    for pair in pairs_needing_auto {
        let feed_id = match get_or_create_feed_metadata(&pair, Some(Source::Auto)) {
            Ok(metadata) => metadata.feed_id,
            Err(e) => {
                tracing::warn!("Failed to get AUTO metadata for {}: {}, using fallback", pair, e);
                format!("feed_{}_AUTO", pair.as_str())
            }
        };
        
        symbol_feeds
            .entry(pair)
            .or_default()
            .push(SurgeFeedInfo {
                source: "AUTO".to_string(),
                feed_id,
            });
    }

    // Ensure all USD pairs with AUTO also have WEIGHTED
    // Since WEIGHTED uses AUTO cache under the hood, this is just for API consistency
    let mut usd_pairs_with_auto: Vec<Pair> = Vec::new();
    for (pair, feed_infos) in &symbol_feeds {
        if pair.quote == "USD" && feed_infos.iter().any(|f| f.source == "AUTO") {
            // Check if it already has WEIGHTED
            if !feed_infos.iter().any(|f| f.source == "WEIGHTED") {
                usd_pairs_with_auto.push(pair.clone());
            }
        }
    }
    
    // Add WEIGHTED feeds for USD pairs that have AUTO but no WEIGHTED
    for pair in usd_pairs_with_auto {
        let feed_id = match get_or_create_feed_metadata(&pair, None) {  // None = WEIGHTED
            Ok(metadata) => metadata.feed_id,
            Err(e) => {
                tracing::warn!("Failed to get WEIGHTED metadata for {}: {}, using fallback", pair, e);
                format!("feed_{}_WEIGHTED", pair.as_str())
            }
        };
        
        symbol_feeds
            .entry(pair)
            .or_default()
            .push(SurgeFeedInfo {
                source: "WEIGHTED".to_string(),
                feed_id,
            });
    }
    
    // Apply filters
    let mut feeds = Vec::new();
    for (pair, feed_infos) in symbol_feeds {
        // Apply symbol filter if provided
        if let Some(ref symbol_filter) = symbol_filter {
            let symbol_str = pair.as_str();
            if !symbol_str.to_lowercase().contains(&symbol_filter.to_lowercase()) {
                continue;
            }
        }

        // Apply exchange filter if provided
        let filtered_infos: Vec<SurgeFeedInfo> = if let Some(ref exchange_filter) = exchange_filter {
            let exchange_upper = exchange_filter.to_uppercase();
            feed_infos
                .into_iter()
                .filter(|info| info.source == exchange_upper)
                .collect()
        } else {
            feed_infos
        };

        if !filtered_infos.is_empty() {
            feeds.push(SurgeFeed {
                symbol: pair,
                feeds: filtered_infos,
            });
        }
    }

    // Sort by symbol for consistent output
    feeds.sort_by(|a, b| a.symbol.cmp(&b.symbol));

    let total = feeds.iter().map(|f| f.feeds.len()).sum();
    let message = if feeds.is_empty() && symbol_filter.is_some() {
        Some("No match for query".to_string())
    } else {
        None
    };

    Ok(SurgeFeedsResponse {
        total,
        message,
        data: feeds,
    })
}