use crate::{exchange_config::get_allowed_quotes, exchanges::connection_state::ConnectionState, pair::Pair, traits::TickerStream, types::{Source, Ticker}};
use anyhow::{anyhow, Result};
use dashmap::DashMap;
use fastwebsockets::{FragmentCollector, Frame, OpCode, WebSocket, Role};
use futures::Stream;
use http_body_util::Empty;
use hyper::body::Bytes;
use hyper::header::{CONNECTION, UPGRADE};
use hyper::upgrade::Upgraded;
use hyper::{Request, Uri};
use hyper_tls::HttpsConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::{TokioExecutor, TokioIo};
use once_cell::sync::Lazy;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU32, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::{mpsc, Mutex, RwLock};
use tracing::{debug, error, info, warn};
use crate::exchanges::binance_connection::MIN_HEALTHY_MESSAGE_RATE;
use crate::exchanges::binance_connection::STARTUP_GRACE_PERIOD_MS;
// Global cache for Bybit exchange info
static BYBIT_EXCHANGE_INFO: Lazy<Arc<DashMap<String, Pair>>> = Lazy::new(|| Arc::new(DashMap::new()));

// Configuration constants
const TOP_SYMBOLS_COUNT: usize = 150;
const BYBIT_WS_ENDPOINT: &str = "wss://stream.bybit.com/v5/public/spot";
const SUBSCRIPTION_BATCH_SIZE: usize = 10; // Bybit allows 10 symbols per subscription
const SUBSCRIPTION_BATCH_DELAY_MS: u64 = 500;
const CONNECTION_DELAY_SECS: u64 = 1; // Reduced for faster startup
const HEALTH_CHECK_INTERVAL_SECS: u64 = 5; // More frequent health checks
const MAX_HEALTHY_LATENCY_MS: u64 = 10000; // 10 seconds - time since last message threshold  

type BybitWebSocket = FragmentCollector<TokioIo<Upgraded>>;

#[derive(Debug, Deserialize)]
struct BybitMessage {
    topic: String,
    ts: u64,
    #[serde(rename = "type")]
    msg_type: String,
    data: serde_json::Value,
}

#[derive(Debug, Deserialize)]
struct BybitTickerData {
    symbol: String,
    #[serde(rename = "lastPrice")]
    last_price: String,
}

#[derive(Debug, Serialize)]
struct BybitSubscribe {
    op: String,
    args: Vec<String>,
}

/// Get all cached Bybit pairs
pub fn get_cached_pairs() -> Result<Vec<Pair>> {
    if BYBIT_EXCHANGE_INFO.is_empty() {
        return Err(anyhow!("Exchange info not fetched yet"));
    }
    
    let pairs: Vec<Pair> = BYBIT_EXCHANGE_INFO
        .iter()
        .map(|entry| entry.value().clone())
        .collect();
    
    Ok(pairs)
}

/// A single WebSocket connection to Bybit
pub struct BybitConnection {
    id: String,
    ws: Option<Arc<Mutex<BybitWebSocket>>>,
    symbols: HashSet<String>,
    symbol_to_pair: Arc<DashMap<String, Pair>>,
    tx: mpsc::UnboundedSender<Vec<Ticker>>,
    is_connected: Arc<AtomicBool>,
    last_message_time: Arc<AtomicU64>,
    subscribed_symbols: Arc<RwLock<HashSet<String>>>,
    // Message rate tracking
    message_count: Arc<AtomicU64>,
    last_rate_log_time: Arc<AtomicU64>,
    last_rate_value: Arc<AtomicU32>, // Store rate as fixed point (x100)
    // Track when subscription completed for startup grace period
    subscription_completed_time: Arc<AtomicU64>,
    
    // Track if read loop is running to prevent duplicates
    read_loop_running: Arc<AtomicBool>,
    
    // Handle to ping task for cleanup
    ping_task_handle: Option<tokio::task::JoinHandle<()>>,
    
    // Notify monitor to check immediately
    monitor_trigger: Arc<tokio::sync::Notify>,
    
    // Track symbol confirmations (symbol -> message count)
    symbol_confirmations: Arc<DashMap<String, u32>>,
}

impl BybitConnection {
    pub fn new(
        id: String,
        symbols: HashSet<String>,
        symbol_to_pair: Arc<DashMap<String, Pair>>,
        tx: mpsc::UnboundedSender<Vec<Ticker>>,
    ) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
            
        Self {
            id,
            ws: None,
            symbols,
            symbol_to_pair,
            tx,
            is_connected: Arc::new(AtomicBool::new(false)),
            last_message_time: Arc::new(AtomicU64::new(0)),
            subscribed_symbols: Arc::new(RwLock::new(HashSet::new())),
            message_count: Arc::new(AtomicU64::new(0)),
            last_rate_log_time: Arc::new(AtomicU64::new(now)),
            last_rate_value: Arc::new(AtomicU32::new(0)),
            subscription_completed_time: Arc::new(AtomicU64::new(0)),
            read_loop_running: Arc::new(AtomicBool::new(false)),
            ping_task_handle: None,
            monitor_trigger: Arc::new(tokio::sync::Notify::new()),
            symbol_confirmations: Arc::new(DashMap::new()),
        }
    }

    pub async fn connect(&mut self) -> Result<()> {
        info!("[{}] Connecting to Bybit WebSocket", self.id);
        
        // Create HTTPS client
        let https = HttpsConnector::new();
        let client = Client::builder(TokioExecutor::new())
            .build::<_, Empty<Bytes>>(https);
        
        // Parse URI
        let uri: Uri = BYBIT_WS_ENDPOINT.replace("wss://", "https://").parse()?;
        
        // Create WebSocket upgrade request
        let req = Request::builder()
            .method("GET")
            .uri(&uri)
            .header("Host", "stream.bybit.com")
            .header(UPGRADE, "websocket")
            .header(CONNECTION, "upgrade")
            .header("Sec-WebSocket-Key", fastwebsockets::handshake::generate_key())
            .header("Sec-WebSocket-Version", "13")
            .body(Empty::<Bytes>::new())?;
            
        // Send request and get upgraded connection
        let res = client.request(req).await?;
        
        if res.status() != hyper::StatusCode::SWITCHING_PROTOCOLS {
            return Err(anyhow!("WebSocket handshake failed: {}", res.status()));
        }
        
        // Get the upgraded connection
        let upgraded = hyper::upgrade::on(res).await?;
        let ws = WebSocket::after_handshake(TokioIo::new(upgraded), Role::Client);
        let ws = FragmentCollector::new(ws);
        
        self.is_connected.store(true, Ordering::Release);
        self.last_message_time.store(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            Ordering::Release
        );

        // Update global connection metadata cache to mark as connected
        if let Some(mut entry) = crate::hub::CONNECTION_METADATA_CACHE.get_mut(&(crate::types::Source::Bybit, self.id.clone())) {
            entry.is_connected = true;
            entry.last_updated = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
        }
        
        let ws = Arc::new(Mutex::new(ws));
        self.ws = Some(ws.clone());
        
        // Don't start the read loop here - it will be started after subscription
        
        info!("[{}] Connected successfully", self.id);
        Ok(())
    }

    fn start_ping_task(&mut self) {
        if let Some(ws) = &self.ws {
            let ws = ws.clone();
            let connection_id = self.id.clone();
            let is_connected = self.is_connected.clone();
            
            let handle = crate::runtime_separation::spawn_on_ingestion_named(
                &format!("bybit-ping-{}", connection_id),
                async move {
                    // Wait a bit before starting ping to ensure connection is stable
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    
                    // Bybit documentation recommends sending ping every 20 seconds
                    let mut interval = tokio::time::interval(Duration::from_secs(20));
                    
                    loop {
                        interval.tick().await;
                        
                        // Check if still connected
                        if !is_connected.load(Ordering::Acquire) {
                            warn!("[{}] Ping task stopping - connection closed", connection_id);
                            break;
                        }
                        
                        // Bybit expects JSON format: {"op": "ping"}
                        // Can optionally add req_id: {"req_id": "1", "op": "ping"}
                        let ping_msg = r#"{"op":"ping"}"#;
                        
                        let mut ws_guard = ws.lock().await;
                        if let Err(e) = ws_guard.write_frame(Frame::text(ping_msg.as_bytes().into())).await {
                            error!("[{}] Failed to send ping: {}", connection_id, e);
                            // Mark connection as disconnected
                            is_connected.store(false, Ordering::Release);
                            break;
                        }
                        drop(ws_guard);
                        debug!("[{}] Sent ping to Bybit", connection_id);
                    }
                    
                    debug!("[{}] Ping task ended", connection_id);
                }
            );
            
            self.ping_task_handle = Some(handle);
        }
    }
    
    pub async fn subscribe_gradually(&mut self) -> Result<()> {
        debug!("[{}] Starting gradual subscription process", self.id);
        let ws = self.ws.as_ref().ok_or_else(|| anyhow!("Not connected"))?;
        
        // Get what's already confirmed as working in websocket_active_pairs
        let hub = crate::hub::SurgeHub::global();
        let already_active = hub.get_websocket_pairs(Source::Bybit);
        let active_symbols: HashSet<String> = already_active.iter()
            .map(|pair| pair.as_bybit_str())
            .collect();
        
        // Figure out what needs subscription (symbols not yet in websocket_active)
        let outstanding_symbols: Vec<String> = self.symbols.iter()
            .filter(|symbol| !active_symbols.contains(*symbol))
            .cloned()
            .collect();
        
        if outstanding_symbols.is_empty() {
            info!("[{}] ✅ All {} symbols already active in WebSocket, nothing to subscribe", 
                self.id, self.symbols.len());
            return Ok(());
        }
        
        info!("[{}] 📡 Will subscribe to {} outstanding symbols (out of {} total)", 
            self.id, outstanding_symbols.len(), self.symbols.len());
        
        // Subscribe to symbols in batches
        {
            let mut subscribed = self.subscribed_symbols.write().await;
            
            for (i, chunk) in outstanding_symbols.chunks(SUBSCRIPTION_BATCH_SIZE).enumerate() {
                let mut topics = Vec::new();
                
                for symbol in chunk {
                    if !subscribed.contains(symbol) {
                        topics.push(format!("tickers.{symbol}"));
                        subscribed.insert(symbol.clone());
                    }
                }
                
                if !topics.is_empty() {
                    let subscribe_msg = BybitSubscribe {
                        op: "subscribe".to_string(),
                        args: topics,
                    };
                    
                    if let Ok(msg) = serde_json::to_string(&subscribe_msg) {
                        debug!("[{}] Sending subscription message: {}", self.id, msg);
                        let mut ws_guard = ws.lock().await;
                        ws_guard.write_frame(Frame::text(msg.as_bytes().into())).await?;
                        drop(ws_guard);
                        
                        info!("[{}] 📤 Subscribed to batch {}/{} ({} symbols, total subscribed: {})", 
                            self.id, 
                            i + 1, 
                            (outstanding_symbols.len() + SUBSCRIPTION_BATCH_SIZE - 1) / SUBSCRIPTION_BATCH_SIZE,
                            chunk.len(),
                            subscribed.len()
                        );
                    }
                    
                    // Delay between batches to avoid rate limits
                    if i < outstanding_symbols.len() / SUBSCRIPTION_BATCH_SIZE {
                        tokio::time::sleep(Duration::from_millis(SUBSCRIPTION_BATCH_DELAY_MS)).await;
                    }
                }
            }
        } // Drop the write lock here
        
        info!("[{}] Subscribed to {} symbols gradually", self.id, outstanding_symbols.len());
        
        // Count how many of THIS connection's symbols were already active
        let already_active_count = self.symbols.iter()
            .filter(|s| active_symbols.contains(*s))
            .count();
        let outstanding_count = outstanding_symbols.len();
        
        // Wait for actual prices to confirm NEW subscriptions are working (before starting read loop)
        let confirmed = self.confirm_prices(outstanding_symbols.iter().cloned().collect()).await;
        
        // Register newly confirmed symbols as WebSocket-active
        let all_pairs = get_cached_pairs().unwrap_or_default();
        if !confirmed.is_empty() {
            let pairs_to_register: Vec<Pair> = confirmed.iter()
                .filter_map(|symbol| {
                    all_pairs.iter()
                        .find(|pair| pair.as_bybit_str() == *symbol)
                        .cloned()
                })
                .collect();
            
            if !pairs_to_register.is_empty() {
                hub.register_websocket_pairs(Source::Bybit, &pairs_to_register);
                info!("[{}] ✅ Registered {} newly confirmed symbols as WebSocket-active", 
                    self.id, pairs_to_register.len());
            }
        }
        
        // Calculate total active symbols for this connection
        let total_active = already_active_count + confirmed.len();
        
        if total_active > 0 {
            // Connection has value - either new symbols added or existing ones working
            if confirmed.is_empty() && outstanding_count > 0 {
                // Couldn't add new ones but have existing active
                info!("[{}] Could not confirm {} new symbols, but {}/{} symbols remain active", 
                    self.id, outstanding_count, total_active, self.symbols.len());
            } else if !confirmed.is_empty() {
                // Successfully added new symbols  
                info!("[{}] ✅ Added {} new symbols. Total active: {}/{}", 
                    self.id, confirmed.len(), total_active, self.symbols.len());
            } else {
                // All symbols were already active
                info!("[{}] ✅ All {} symbols already active", self.id, total_active);
            }
            
            // Mark subscription as successful since we have active symbols
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            self.subscription_completed_time.store(now, Ordering::Release);
            
            Ok(()) // Connection is useful!
        } else {
            // No symbols working at all - connection is useless
            error!("[{}] ❌ No symbols confirmed! Connection has 0 active feeds.", self.id);
            Err(anyhow!("No symbols confirmed - connection has no active feeds"))
        }
    }
    
    /// Wait for actual price confirmations from subscribed symbols
    /// Returns only the symbols that have received at least 3 price updates
    async fn confirm_prices(&mut self, subscribed: HashSet<String>) -> HashSet<String> {
        info!("[{}] ⏳ Waiting for price confirmations from {} symbols...", self.id, subscribed.len());
        
        // Initialize confirmation tracking for all subscribed symbols BEFORE starting read loop
        self.symbol_confirmations.clear();
        for symbol in &subscribed {
            self.symbol_confirmations.insert(symbol.clone(), 0);
            debug!("[{}] Tracking confirmations for symbol: {}", self.id, symbol);
        }
        
        // Small delay to ensure the map is fully populated before read loop processes messages
        tokio::time::sleep(Duration::from_millis(100)).await;
        
        // Start the read loop which will track confirmations
        info!("[{}] Starting read loop to process ticker messages", self.id);
        self.start_read_loop();
        
        // Wait for confirmations with timeout
        let start = std::time::Instant::now();
        let timeout = Duration::from_secs(45);
        
        // Poll for confirmation completion
        while start.elapsed() < timeout {
            tokio::time::sleep(Duration::from_millis(100)).await;
            
            // Check if all symbols have been confirmed
            let mut all_confirmed = true;
            let mut confirmed_count = 0;
            
            for entry in self.symbol_confirmations.iter() {
                if *entry.value() >= 3 {
                    confirmed_count += 1;
                } else {
                    all_confirmed = false;
                }
            }
            
            // If we've confirmed enough symbols, mark as complete
            if all_confirmed && confirmed_count > 0 {
                info!("[{}] ✅ All {} symbols confirmed with price data!", self.id, confirmed_count);
                break;
            }
        }
        
        // Collect confirmed symbols
        let mut confirmed = HashSet::new();
        for entry in self.symbol_confirmations.iter() {
            let (symbol, count) = entry.pair();
            if *count >= 3 {
                confirmed.insert(symbol.clone());
            }
        }
        
        let failed: Vec<String> = subscribed.iter()
            .filter(|s| !confirmed.contains(*s))
            .cloned()
            .collect();
            
        if !failed.is_empty() {
            warn!("[{}] ⚠️ {} symbols did NOT receive 3 price updates: {:?}", 
                self.id, failed.len(), failed);
        }
        
        info!("[{}] ✅ {} out of {} symbols confirmed with price data", 
            self.id, confirmed.len(), subscribed.len());
            
        confirmed
    }

    pub fn is_connected(&self) -> bool {
        self.is_connected.load(Ordering::Acquire)
    }
    

    pub fn get_latency_ms(&self) -> u64 {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let last = self.last_message_time.load(Ordering::Acquire);
        now.saturating_sub(last)
    }
    
    /// Get current message rate (messages per second)
    pub fn get_message_rate(&self) -> f64 {
        let rate_fixed = self.last_rate_value.load(Ordering::Acquire);
        rate_fixed as f64 / 100.0
    }
    
    /// Check if connection is in startup grace period
    pub fn is_in_grace_period(&self) -> bool {
        let subscription_time = self.subscription_completed_time.load(Ordering::Acquire);
        if subscription_time == 0 {
            return true; // Still in grace period if not yet subscribed
        }
        
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
            
        now - subscription_time < STARTUP_GRACE_PERIOD_MS
    }

    pub async fn cleanup(&mut self) {
        debug!("[{}] Cleaning up connection", self.id);

        // Mark as disconnected
        self.is_connected.store(false, Ordering::Release);

        // Update cache to mark as disconnected
        if let Some(mut entry) = crate::hub::CONNECTION_METADATA_CACHE.get_mut(&(crate::types::Source::Bybit, self.id.clone())) {
            entry.is_connected = false;
        }
        
        // Stop ping task if running
        if let Some(handle) = self.ping_task_handle.take() {
            handle.abort();
            debug!("[{}] Aborted ping task", self.id);
        }
        
        // Clear WebSocket
        self.ws = None;
        
        // Mark read loop as not running (it should exit on its own)
        self.read_loop_running.store(false, Ordering::Release);
        
        // Clear subscribed symbols
        self.subscribed_symbols.write().await.clear();
        
        // Clear confirmations
        self.symbol_confirmations.clear();
        
        debug!("[{}] Cleanup complete", self.id);
    }

    pub fn start_read_loop(&mut self) {
        // Check if read loop is already running
        if self.read_loop_running.load(Ordering::Acquire) {
            warn!("[{}] Read loop already running, skipping start", self.id);
            return;
        }
        
        if let Some(ws) = &self.ws {
            // Mark read loop as running
            self.read_loop_running.store(true, Ordering::Release);
            
            let ws = ws.clone();
            let tx = self.tx.clone();
            let is_connected = self.is_connected.clone();
            let last_message_time = self.last_message_time.clone();
            let symbol_to_pair = self.symbol_to_pair.clone();
            let connection_id = self.id.clone();
            let message_count = self.message_count.clone();
            let last_rate_log_time = self.last_rate_log_time.clone();
            let last_rate_value = self.last_rate_value.clone();
            let read_loop_running = self.read_loop_running.clone();
            let monitor_trigger = self.monitor_trigger.clone();
            let symbol_confirmations = self.symbol_confirmations.clone();
            
            // Start ping task after read loop is set up
            self.start_ping_task();
            
            crate::runtime_separation::spawn_on_ingestion_named(
                &format!("bybit-reader-{}", connection_id),
                async move {
                    info!("[{}] Starting read loop task", connection_id);
                    if let Err(e) = Self::read_loop(
                        connection_id.clone(),
                        ws,
                        tx,
                        is_connected.clone(),
                        last_message_time.clone(),
                        symbol_to_pair,
                        message_count,
                        last_rate_log_time,
                        last_rate_value,
                        symbol_confirmations,
                        monitor_trigger.clone(),
                    ).await {
                        error!("[{}] Bybit read loop error: {}", connection_id, e);
                        is_connected.store(false, Ordering::Release);

                        // Update cache to mark as disconnected
                        if let Some(mut entry) = crate::hub::CONNECTION_METADATA_CACHE.get_mut(&(crate::types::Source::Bybit, connection_id.clone())) {
                            entry.is_connected = false;
                        }

                        // Notify monitor to check immediately
                        monitor_trigger.notify_one();

                        // Reset last_message_time to prevent stale latency calculations
                        last_message_time.store(
                            std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap()
                                .as_millis() as u64,
                            Ordering::Release
                        );
                    }
                    info!("[{}] Read loop task ended", connection_id);
                    // Mark read loop as not running
                    read_loop_running.store(false, Ordering::Release);
                });
        }
    }

    async fn read_loop(
        connection_id: String,
        ws: Arc<Mutex<BybitWebSocket>>,
        tx: mpsc::UnboundedSender<Vec<Ticker>>,
        is_connected: Arc<AtomicBool>,
        last_message_time: Arc<AtomicU64>,
        symbol_to_pair: Arc<DashMap<String, Pair>>,
        message_count: Arc<AtomicU64>,
        last_rate_log_time: Arc<AtomicU64>,
        last_rate_value: Arc<AtomicU32>,
        symbol_confirmations: Arc<DashMap<String, u32>>,
        monitor_trigger: Arc<tokio::sync::Notify>,
    ) -> Result<()> {
        loop {
            let frame = {
                let mut ws_guard = ws.lock().await;
                match ws_guard.read_frame().await {
                    Ok(frame) => frame,
                    Err(e) => {
                        error!("[{}] Failed to read frame: {}", connection_id, e);
                        is_connected.store(false, Ordering::Release);

                        // Update cache to mark as disconnected
                        if let Some(mut entry) = crate::hub::CONNECTION_METADATA_CACHE.get_mut(&(crate::types::Source::Bybit, connection_id.clone())) {
                            entry.is_connected = false;
                        }

                        // Reset last_message_time to prevent stale latency calculations
                        last_message_time.store(
                            std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap()
                                .as_millis() as u64,
                            Ordering::Release
                        );
                        return Err(anyhow!("WebSocket error: {}", e));
                    }
                }
            };
            
            // Update last message time
            last_message_time.store(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64,
                Ordering::Release
            );
            
            match frame.opcode {
                OpCode::Text => {
                    let text = String::from_utf8(frame.payload.to_vec())?;
                    
                    // Handle pong response from Bybit
                    // Expected format: {"success": true, "ret_msg": "pong", "conn_id": "...", "op": "ping"}
                    if text.contains(r#""ret_msg":"pong"#) && text.contains(r#""op":"ping"#) {
                        debug!("[{}] Received pong from Bybit", connection_id);
                        continue;
                    }
                    
                    // Log first few messages for debugging
                    static MSG_COUNT: AtomicU64 = AtomicU64::new(0);
                    let count = MSG_COUNT.fetch_add(1, Ordering::Relaxed);
                    if count < 5 {
                        debug!("[{}] Bybit message {}: {}", connection_id, count, text);
                    }
                    
                    if let Ok(msg) = serde_json::from_str::<BybitMessage>(&text) {
                        if msg.topic.starts_with("tickers.") && msg.msg_type == "snapshot" {
                            // Increment message count for rate tracking
                            message_count.fetch_add(1, Ordering::Relaxed);
                            
                            // Check if it's time to calculate/log the rate
                            let now = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap()
                                .as_millis() as u64;
                            let last_log = last_rate_log_time.load(Ordering::Acquire);
                            
                            // Calculate rate every 10 seconds for monitoring
                            if now - last_log >= 10_000 { // 10 seconds
                                let count = message_count.load(Ordering::Acquire);
                                let elapsed_ms = now - last_log;
                                let rate = (count as f64 * 1000.0) / elapsed_ms as f64;
                                
                                // Store rate as fixed point (x100) for atomic storage
                                let rate_fixed = (rate * 100.0) as u32;
                                last_rate_value.store(rate_fixed, Ordering::Release);

                                // Log every 600 seconds
                                if now - last_log >= 600_000 {
                                    message_count.store(0, Ordering::Release); // Reset for next period
                                    
                                    if rate > 1.0 || count > 10 {
                                        info!("[{}] 📊 Raw Message rate: {:.1} msg/sec ({} messages in {:.1}s)", 
                                            connection_id, rate, count, elapsed_ms as f64 / 1000.0);
                                    }
                                    
                                    last_rate_log_time.store(now, Ordering::Release);
                                }
                            }
                            
                            if let Ok(ticker_data) = serde_json::from_value::<BybitTickerData>(msg.data.clone()) {
                                debug!("[{}] Bybit ticker: symbol={}, price={}", 
                                    connection_id, ticker_data.symbol, ticker_data.last_price);
                                
                                // Track confirmations for this symbol
                                let mut confirm_count = symbol_confirmations
                                    .entry(ticker_data.symbol.clone())
                                    .or_insert(0);
                                *confirm_count += 1;
                                
                                if *confirm_count == 3 {
                                    debug!("[{}] Symbol {} confirmed with 3+ messages", 
                                        connection_id, ticker_data.symbol);
                                }
                                
                                // Look up the pair from our cache
                                if let Some(pair) = symbol_to_pair.get(&ticker_data.symbol) {
                                    if let Ok(price) = ticker_data.last_price.parse::<Decimal>() {
                                        let tick = Ticker {
                                            symbol: pair.as_str(),
                                            price,
                                            timestamp: msg.ts,
                                        };
                                        if tx.send(vec![tick]).is_err() {
                                            error!("[{}] Failed to send Bybit ticker", connection_id);
                                        } else {
                                            debug!("[{}] Sent Bybit ticker: {} @ {}", 
                                                connection_id, pair.as_str(), price);
                                        }
                                    } else {
                                        warn!("[{}] Failed to parse Bybit price: {}", 
                                            connection_id, ticker_data.last_price);
                                    }
                                } else {
                                    warn!("[{}] Bybit symbol not found in cache: {}", 
                                        connection_id, ticker_data.symbol);
                                }
                            } else if count < 10 {
                                warn!("[{}] Failed to parse Bybit ticker data: {:?}", 
                                    connection_id, 
                                    msg.data);
                            }
                        }
                    } else if count < 10 {
                        warn!("[{}] Failed to parse Bybit message: {}", connection_id, text);
                    }
                }
                OpCode::Close => {
                    info!("[{}] WebSocket closed", connection_id);
                    is_connected.store(false, Ordering::Release);

                    // Update cache to mark as disconnected
                    if let Some(mut entry) = crate::hub::CONNECTION_METADATA_CACHE.get_mut(&(crate::types::Source::Bybit, connection_id.clone())) {
                        entry.is_connected = false;
                    }

                    // Notify monitor immediately for instant failover
                    monitor_trigger.notify_one();

                    // Reset last_message_time to prevent stale latency calculations
                    last_message_time.store(
                        std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_millis() as u64,
                        Ordering::Release
                    );
                    break;
                }
                OpCode::Ping => {
                    let mut ws_guard = ws.lock().await;
                    ws_guard.write_frame(Frame::pong(frame.payload)).await?;
                }
                _ => {}
            }
        }
        Ok(())
    }
}

/// Connection pair with primary/secondary redundancy
/// Single connection with health monitoring
pub struct BybitConnectionPair {
    pub id: String,
    pub connection: Arc<RwLock<BybitConnection>>,
    pub symbols: HashSet<String>,
    monitoring_task: Option<tokio::task::JoinHandle<()>>,
}

impl BybitConnectionPair {
    pub fn new(
        id: String,
        symbols: HashSet<String>,
        symbol_to_pair: Arc<DashMap<String, Pair>>,
        tx: mpsc::UnboundedSender<Vec<Ticker>>,
    ) -> Self {
        let connection = BybitConnection::new(
            id.clone(),
            symbols.clone(),
            symbol_to_pair,
            tx,
        );
        
        Self {
            id,
            connection: Arc::new(RwLock::new(connection)),
            symbols,
            monitoring_task: None,
        }
    }

    pub async fn start(&mut self) -> Result<()> {
        info!("[{}] Starting connection", self.id);
        
        // Connect and subscribe
        {
            let mut conn = self.connection.write().await;
            if let Err(e) = conn.connect().await {
                error!("[{}] Connection failed: {}", self.id, e);
                return Err(e);
            }
            
            info!("[{}] Connected, starting subscription", self.id);
            if let Err(e) = conn.subscribe_gradually().await {
                error!("[{}] Subscription failed: {}", self.id, e);
                return Err(e);
            }
            
            info!("[{}] Subscription completed", self.id);
            // Note: read_loop is already started in subscribe_gradually
        }
        
        // Start monitoring task
        let connection = self.connection.clone();
        let connection_id = self.id.clone();
        let symbols = self.symbols.clone();
        
        let monitoring_task = crate::runtime_separation::spawn_on_ingestion_named(
            &format!("bybit-monitor-{connection_id}"),
            async move {
                Self::monitor_connection(
                    connection_id,
                    connection,
                    symbols,
                ).await;
            });
        
        self.monitoring_task = Some(monitoring_task);
        
        Ok(())
    }

    async fn monitor_connection(
        connection_id: String,
        connection: Arc<RwLock<BybitConnection>>,
        symbols: HashSet<String>,
    ) {
        let mut check_interval = tokio::time::interval(Duration::from_secs(HEALTH_CHECK_INTERVAL_SECS));
        let mut symbols_in_rest_failover = false;
        // Initialize with current time to prevent immediate reconnection
        let mut last_reconnect_attempt: Option<std::time::Instant> = Some(std::time::Instant::now());
        const RECONNECT_INTERVAL_SECS: u64 = 300; // 5 minutes between reconnect attempts
        
        // Get the monitor trigger for immediate notifications
        let monitor_trigger = {
            let conn = connection.read().await;
            conn.monitor_trigger.clone()
        };
        
        // Spawn retry task for unconfirmed symbols
        let connection_retry = connection.clone();
        let conn_id_retry = connection_id.clone();
        let symbols_retry = symbols.clone();
        
        crate::runtime_separation::spawn_on_ingestion_named(
            &format!("bybit-retry-{}", conn_id_retry),
            async move {
                // Wait before first retry
                tokio::time::sleep(Duration::from_secs(300)).await;
                
                loop {
                    tokio::time::sleep(Duration::from_secs(300)).await; // Every 5 minutes
                    
                    // Check if connected
                    let is_connected = connection_retry.read().await.is_connected();
                    if !is_connected {
                        debug!("[{}] Skipping retry - connection not active", conn_id_retry);
                        continue;
                    }
                    
                    // Get subscribed symbols
                    let subscribed = connection_retry.read().await.subscribed_symbols.read().await.clone();
                    
                    // Get websocket-active symbols from hub
                    let hub = crate::hub::SurgeHub::global();
                    
                    let active_symbols: HashSet<String> = symbols_retry.iter()
                        .filter(|symbol| {
                            if let Some(pair_entry) = BYBIT_EXCHANGE_INFO.get(*symbol) {
                                hub.is_websocket_active(crate::Source::Bybit, &pair_entry.value().clone())
                            } else {
                                false
                            }
                        })
                        .cloned()
                        .collect();
                    
                    // Find unconfirmed symbols (subscribed but not websocket-active)
                    let unconfirmed: Vec<String> = subscribed.difference(&active_symbols)
                        .cloned()
                        .collect();
                    
                    if !unconfirmed.is_empty() {
                        info!("[{}] 🔄 Retrying {} unconfirmed symbols: {:?}", 
                            conn_id_retry, unconfirmed.len(), unconfirmed);
                        
                        // Try to subscribe and confirm again
                        let mut conn = connection_retry.write().await;
                        if let Err(e) = conn.subscribe_gradually().await {
                            warn!("[{}] Failed to retry subscriptions: {}", conn_id_retry, e);
                        }
                    }
                }
            },
        );
        
        loop {
            tokio::select! {
                _ = check_interval.tick() => {
                    // Regular interval check
                }
                _ = monitor_trigger.notified() => {
                    info!("[{}] Monitor triggered immediately due to connection failure", connection_id);
                }
            }
            
            let conn = connection.read().await;
            let is_connected = conn.is_connected();
            let latency_ms = conn.get_latency_ms();
            let in_grace_period = conn.is_in_grace_period();
            let message_rate = conn.get_message_rate();
            drop(conn); // Release read lock
            
            // Check health conditions (skip during grace period)
            if is_connected && !in_grace_period {
                let unhealthy = 
                    latency_ms > MAX_HEALTHY_LATENCY_MS || 
                    message_rate < MIN_HEALTHY_MESSAGE_RATE;
                
                if unhealthy && !symbols_in_rest_failover {
                    warn!("[{}] 🚨 Connection unhealthy! Latency: {}ms, Rate: {:.1} msg/sec. Moving {} symbols to REST API", 
                        connection_id, latency_ms, message_rate, symbols.len());
                    
                    // Close the unhealthy connection
                    {
                        let mut conn = connection.write().await;
                        conn.is_connected.store(false, Ordering::Release);
                        // Reset last_message_time to prevent stale latency calculations
                        conn.last_message_time.store(
                            std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap()
                                .as_millis() as u64,
                            Ordering::Release
                        );
                        conn.ws = None;
                        conn.subscribed_symbols.write().await.clear();
                    }
                    
                    // Clear these symbols from WebSocket tracking
                    let pairs_to_clear: Vec<crate::Pair> = symbols.iter()
                        .filter_map(|symbol| {
                            BYBIT_EXCHANGE_INFO.get(symbol)
                                .map(|entry| entry.value().clone())
                        })
                        .collect();
                    
                    if !pairs_to_clear.is_empty() {
                        let hub = crate::hub::SurgeHub::global();
                        hub.remove_websocket_pairs(crate::Source::Bybit, &pairs_to_clear);
                        info!("[{}] 📡 Removed {} symbols from WebSocket tracking for REST fallback", 
                            connection_id, pairs_to_clear.len());
                    }
                    
                    symbols_in_rest_failover = true;
                }
            } else if !is_connected && !symbols_in_rest_failover {
                // Connection is down
                error!("[{}] 🚨 Connection DOWN! Moving {} symbols to REST API", 
                    connection_id, symbols.len());
                
                // Clear these symbols from WebSocket tracking
                let pairs_to_clear: Vec<crate::Pair> = symbols.iter()
                    .filter_map(|symbol| {
                        BYBIT_EXCHANGE_INFO.get(symbol)
                            .map(|entry| entry.value().clone())
                    })
                    .collect();
                
                if !pairs_to_clear.is_empty() {
                    let hub = crate::hub::SurgeHub::global();
                    hub.remove_websocket_pairs(crate::Source::Bybit, &pairs_to_clear);
                    info!("[{}] 📡 Removed {} symbols from WebSocket tracking for REST fallback", 
                            connection_id, pairs_to_clear.len());
                }
                
                symbols_in_rest_failover = true;
            }
            
            // Attempt reconnection if in failover mode and enough time has passed
            if symbols_in_rest_failover {
                let should_reconnect = match last_reconnect_attempt {
                    None => true,
                    Some(last_attempt) => last_attempt.elapsed().as_secs() >= RECONNECT_INTERVAL_SECS,
                };
                
                if should_reconnect {
                    info!("[{}] 🔄 Attempting to reconnect WebSocket connection...", connection_id);
                    last_reconnect_attempt = Some(std::time::Instant::now());
                    
                    // Try to reconnect
                    let mut conn = connection.write().await;
                    
                    // Clean up old connection state (stops ping task, clears read loop flag, etc.)
                    conn.cleanup().await;
                    
                    // Reset the connection state
                    conn.symbols = symbols.clone();
                    
                    // Try to reconnect using the existing connect method
                    match conn.connect().await {
                        Ok(_) => {
                            info!("[{}] ✅ WebSocket reconnected successfully", connection_id);
                            
                            // Try to subscribe to symbols
                            match conn.subscribe_gradually().await {
                                Ok(_) => {
                                    info!("[{}] ✅ Successfully re-subscribed to symbols after reconnection", connection_id);
                                    // Note: read_loop is already started in subscribe_gradually
                                    
                                    symbols_in_rest_failover = false; // Reset the failover flag
                                }
                                Err(e) => {
                                    // subscribe_gradually only returns Err when 0 symbols work
                                    error!("[{}] ❌ Failed to re-subscribe after reconnection: {}", connection_id, e);
                                    // No symbols working - connection will be marked as unhealthy in the next check
                                    // Will retry reconnection after delay
                                }
                            }
                        }
                        Err(e) => {
                            warn!("[{}] ❌ Failed to reconnect WebSocket: {}", connection_id, e);
                            // Will retry again after RECONNECT_INTERVAL_SECS
                        }
                    }
                }
            }
            
            // Log status periodically
            if check_interval.period().as_secs() % 30 == 0 {
                if is_connected {
                    debug!("[{}] Status: {} ({}ms latency, {:.1} msg/sec){}",
                        connection_id,
                        if is_connected { "UP" } else { "DOWN" },
                        latency_ms,
                        message_rate,
                        if in_grace_period { " (grace period)" } else { "" }
                    );
                }
            }
        }
    }
}

pub struct BybitStream {
    connection_pairs: Vec<Arc<Mutex<BybitConnectionPair>>>,
    rx: mpsc::UnboundedReceiver<Result<Ticker>>,
    tx: mpsc::UnboundedSender<Vec<Ticker>>,
    symbol_to_pair_cache: Arc<DashMap<String, Pair>>,
    internal_rx: Option<mpsc::UnboundedReceiver<Vec<Ticker>>>,
    external_tx: Option<mpsc::UnboundedSender<Result<Ticker>>>,
    connection_state: ConnectionState,
}

impl BybitStream {
    pub async fn new() -> Result<Self> {
        let (tx, rx) = mpsc::unbounded_channel();
        
        // Create internal tx/rx for aggregating from multiple connections
        let (internal_tx, mut internal_rx) = mpsc::unbounded_channel::<Vec<Ticker>>();
        
        // Spawn aggregator task (same as Binance)
        let external_tx = tx.clone();
        let symbol_to_pair_cache = BYBIT_EXCHANGE_INFO.clone();
        let symbol_to_pair_cache_for_spawn = symbol_to_pair_cache.clone();

        crate::runtime_separation::spawn_on_ingestion_named(
            "bybit-aggregator",
            async move {
                info!("🟢 Starting Bybit aggregator task");
                let mut count = 0u64;
                while let Some(tickers) = internal_rx.recv().await {
                    for ticker in tickers {
                        count += 1;
                        if count <= 5 {
                            info!("🟢 Bybit aggregator forwarding ticker #{}: {} @ {}", 
                                count, ticker.symbol, ticker.price);
                        }
                        // Process each ticker and update cache
                        let _ = external_tx.send(Ok(ticker.clone()));
                        if let Err(e) = process_ticker(&symbol_to_pair_cache_for_spawn, ticker).await {
                            error!("Failed to process ticker: {:?}", e);
                        }
                    }
                }
            }
        );
        
        let mut stream = Self {
            connection_pairs: Vec::new(),
            rx,
            tx: internal_tx,
            symbol_to_pair_cache: BYBIT_EXCHANGE_INFO.clone(),
            internal_rx: None,
            external_tx: None,
            connection_state: ConnectionState::new(),
        };
        stream.connect().await?;
        Ok(stream)
    }
    
    /// Start the main read loop that aggregates from all connections
    pub fn start_read_loop(&mut self) {
        if let (Some(mut internal_rx), Some(external_tx)) = (self.internal_rx.take(), self.external_tx.take()) {
            info!("🟢 Starting Bybit aggregator read loop");
            crate::runtime_separation::spawn_on_ingestion_named(
                "bybit-read-loop",
                async move {
                    let mut count = 0u64;
                while let Some(tickers) = internal_rx.recv().await {
                    for ticker in tickers {
                        count += 1;
                        if count <= 5 {
                            info!("🟢 Bybit aggregator forwarding ticker #{}: {} @ {}", 
                                count, ticker.symbol, ticker.price);
                        }
                        let _ = external_tx.send(Ok(ticker));
                    }
                }
                warn!("🟢 Bybit aggregator loop ended");
            });
        } else {
            warn!("🟢 Bybit read loop already started or not initialized");
        }
    }
    
    /// Pre-fetch exchange info without connecting WebSocket
    pub async fn prefetch_exchange_info() -> Result<()> {
        if BYBIT_EXCHANGE_INFO.is_empty() {
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
        info!("Creating Bybit WebSocket connections");
        
        // Get top symbols by volume AND all symbols
        let (websocket_symbols, _) = Self::select_top_symbols().await?;
        
        // Initially, REST should poll ALL symbols
        let all_symbols = Self::get_all_symbols().await?;
        let all_symbols_set: HashSet<String> = all_symbols.into_iter().collect();
        
        info!("📊 Bybit startup: REST polling {} symbols initially, WebSocket will handle {} high-volume symbols", 
            all_symbols_set.len(), websocket_symbols.len());
        
        // Set up REST polling for ALL symbols initially
        let rest_manager = crate::rest_manager::RestManager::global();
        rest_manager.set_rest_symbols(Source::Bybit, all_symbols_set).await;
        
        // Create connection pairs
        let symbols_per_connection = 50; // Reasonable batch size for Bybit
        let symbol_chunks: Vec<_> = websocket_symbols.chunks(symbols_per_connection)
            .map(|chunk| chunk.iter().cloned().collect::<HashSet<String>>())
            .collect();
        
        for (i, symbols) in symbol_chunks.into_iter().enumerate() {
            let conn_id = format!("Bybit-{}", i + 1);
            let pair = Arc::new(Mutex::new(BybitConnectionPair::new(
                conn_id.clone(),
                symbols.clone(),
                self.symbol_to_pair_cache.clone(),
                self.tx.clone(),
            )));
            self.connection_pairs.push(pair);

            // Register in global connection metadata cache
            crate::hub::CONNECTION_METADATA_CACHE.insert(
                (Source::Bybit, conn_id),
                crate::hub::ConnectionMetadata {
                    symbols: symbols.iter().cloned().collect(),
                    is_connected: false,
                    last_updated: crate::clock_sync::get_corrected_timestamp_ms(),
                }
            );
        }
        
        // Start all connections with delays
        info!("Starting {} Bybit connection pairs", self.connection_pairs.len());
        for (i, pair) in self.connection_pairs.iter().enumerate() {
            let mut conn = pair.lock().await;
            if let Err(e) = conn.start().await {
                error!("Failed to start Bybit connection pair {}: {}", i, e);
            }
            
            // Delay between starting each pair
            if i < self.connection_pairs.len() - 1 {
                tokio::time::sleep(Duration::from_secs(CONNECTION_DELAY_SECS)).await;
            }
        }
        
        // Start connection state monitor
        self.start_connection_monitor();
        
        Ok(())
    }
    
    /// Get all symbols from exchange info
    async fn get_all_symbols() -> Result<Vec<String>> {
        // Ensure exchange info is cached
        if BYBIT_EXCHANGE_INFO.is_empty() {
            let _ = Self::fetch_and_cache_exchange_info().await?;
        }
        
        let all_symbols: Vec<String> = BYBIT_EXCHANGE_INFO.iter()
            .map(|entry| entry.key().clone())
            .collect();
            
        Ok(all_symbols)
    }
    
    /// Select top symbols by volume
    async fn select_top_symbols() -> Result<(Vec<String>, HashSet<String>)> {
        // Priority tickers
        let priority_tickers = vec![
            "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", 
            "DOGEUSDT", "ADAUSDT", "SUIUSDT", "AVAXUSDT", "LINKUSDT"
        ];

        // Ensure exchange info is cached
        if BYBIT_EXCHANGE_INFO.is_empty() {
            let _ = Self::fetch_and_cache_exchange_info().await?;
        }
        
        // Use ALL cached symbols - they're already filtered by allowed quotes
        let all_symbols: Vec<String> = BYBIT_EXCHANGE_INFO.iter()
            .map(|entry| entry.key().clone())
            .collect();
        
        info!("📊 Using {} symbols from Bybit exchange info", all_symbols.len());

        // Get 24hr volume data to sort by volume
        let volume_fetcher = crate::volume_cache::VolumeFetcher::global();
        let (volumes, _, _, _, _) = volume_fetcher.fetch_bybit_volumes().await?;

        // Create volume-sorted list
        let mut symbol_volumes: Vec<(String, f64)> = Vec::new();
        
        for symbol in &all_symbols {
            let volume = volumes.get(symbol).copied().unwrap_or(0.0);
            symbol_volumes.push((symbol.clone(), volume));
        }

        // Sort by volume (highest first)
        symbol_volumes.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        // Select WebSocket symbols: priority first, then by volume
        let mut websocket_symbols = Vec::new();
        let mut used = HashSet::new();

        // Add priority tickers first
        for ticker in priority_tickers {
            if all_symbols.contains(&ticker.to_string()) {
                websocket_symbols.push(ticker.to_string());
                used.insert(ticker.to_string());
                debug!("🔥 Priority ticker added: {}", ticker);
            }
        }

        // Add top symbols by volume until we reach limit
        for (symbol, volume) in symbol_volumes {
            if !used.contains(&symbol) && websocket_symbols.len() < TOP_SYMBOLS_COUNT {
                websocket_symbols.push(symbol.clone());
                used.insert(symbol.clone());
                if volume > 0.0 {
                    debug!("📈 High volume symbol added: {} (volume: {})", symbol, volume);
                }
            }
        }

        // Everything else goes to REST API
        let rest_symbols: HashSet<String> = all_symbols
            .into_iter()
            .filter(|s| !used.contains(s))
            .collect();

        Ok((websocket_symbols, rest_symbols))
    }
    
    /// Start a background task to monitor connection states
    fn start_connection_monitor(&self) {
        let connection_pairs = self.connection_pairs.clone();
        let connection_state = self.connection_state.clone();
        
        crate::runtime_separation::spawn_on_ingestion_named(
            "bybit-connection-monitor",
            async move {
                let mut interval = tokio::time::interval(Duration::from_secs(5));
            
            loop {
                interval.tick().await;
                
                let mut any_connected = false;
                
                // Check each connection pair
                for conn_pair in &connection_pairs {
                    if let Ok(conn) = conn_pair.try_lock() {
                        // Check connections without blocking
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
    
    async fn fetch_and_cache_exchange_info() -> Result<usize> {
        info!("🔄 Fetching fresh Bybit exchange info");
        
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()?;
            
        let response = client
            .get("https://api.bybit.com/v5/market/instruments-info")
            .query(&[("category", "spot")])
            .send()
            .await?;
            
        #[derive(Debug, Deserialize)]
        struct InstrumentsResponse {
            result: InstrumentsResult,
        }
        
        #[derive(Debug, Deserialize)]
        struct InstrumentsResult {
            list: Vec<InstrumentInfo>,
        }

        #[derive(Debug, Deserialize)]
        struct InstrumentInfo {
            symbol: String,
            status: String,
            #[serde(rename = "baseCoin")]
            base_coin: String,
            #[serde(rename = "quoteCoin")]
            quote_coin: String,
        }
        
        let info: InstrumentsResponse = response.json().await?;
        let allowed_quotes = get_allowed_quotes(Source::Bybit);
        let is_initial_fetch = BYBIT_EXCHANGE_INFO.is_empty();
        
        if is_initial_fetch {
            info!("📝 Allowed quote currencies for Bybit: {:?}", allowed_quotes);
        }
        
        // Cache ALL trading symbols with allowed quotes
        let mut new_symbols = 0;
        let mut total_count = 0;
        let mut total_symbols = 0;
        
        for instrument in info.result.list {
            total_symbols += 1;
            
            if instrument.status == "Trading" {
                let pair = Pair {
                    base: instrument.base_coin,
                    quote: instrument.quote_coin,
                };
                
                // Include if quote is in our allowed list
                if allowed_quotes.contains(&pair.quote.as_str()) {
                    total_count += 1;
                    
                    // Only insert if not already present (additive only)
                    if !BYBIT_EXCHANGE_INFO.contains_key(&instrument.symbol) {
                        BYBIT_EXCHANGE_INFO.insert(instrument.symbol.clone(), pair);
                        new_symbols += 1;
                        
                        // Log new discoveries after initial fetch
                        if !is_initial_fetch {
                            info!("🆕 Discovered new Bybit listing: {}", instrument.symbol);
                        }
                    }
                }
            }
        }
        
        if is_initial_fetch {
            info!("📊 Processed {} total symbols from Bybit", total_symbols);
            info!("✅ Cached {} trading pairs with allowed quotes", new_symbols);
        } else if new_symbols > 0 {
            info!("✅ Added {} new Bybit symbols to cache (total: {})", new_symbols, total_count);
        } else {
            debug!("No new Bybit symbols discovered (total: {})", total_count);
        }
        
        Ok(new_symbols)
    }

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

            // Get all cached Bybit pairs for lookup
            for symbol_str in &conn_pair.symbols {
                // Find the corresponding Pair in the cache
                if let Some(pair_entry) = BYBIT_EXCHANGE_INFO.get(symbol_str) {
                    let pair = pair_entry.value();
                    if hub.is_websocket_active(Source::Bybit, pair) {
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
                exchange: "bybit".to_string(),
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


impl TickerStream for BybitStream {
    fn listen(&mut self, _pair: Pair) -> Result<()> {
        // In multi-connection setup, subscriptions are handled during connection setup
        Ok(())
    }

    fn unlisten(&mut self, _pair: &Pair) -> Result<()> {
        // In multi-connection setup, unsubscriptions would need to find the right connection
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

async fn process_ticker(_symbol_to_pair_cache: &Arc<DashMap<String, Pair>>, ticker: Ticker) -> Result<()> {
    // The ticker.symbol is already in "BTC/USDT" format from BybitConnection
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
        
        // Store in the global SurgeHub cache
        let hub = crate::hub::SurgeHub::global();
        hub.update(Source::Bybit, pair.clone(), tick).await;
        
    } else {
        warn!("❌ Failed to parse ticker symbol: {}", ticker.symbol);
    }

    Ok(())
}

impl Stream for BybitStream {
    type Item = Result<Ticker>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}