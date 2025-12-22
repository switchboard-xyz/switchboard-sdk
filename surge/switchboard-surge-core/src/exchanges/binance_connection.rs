use anyhow::{anyhow, Result};
use dashmap::DashMap;
use fastwebsockets::{FragmentCollector, Frame, OpCode, WebSocket, Role};
use http_body_util::Empty;
use hyper::body::Bytes;
use hyper::header::{CONNECTION, UPGRADE};
use hyper::upgrade::Upgraded;
use hyper::{Request, Uri};
use hyper_tls::HttpsConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::{TokioExecutor, TokioIo};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex, RwLock};
use tracing::{debug, error, info, warn};

use crate::{Pair, Ticker};

// ===== WEBSOCKET URLS =====
/// Binance WebSocket URLs
const BINANCE_WS_URLS: &[&str] = &[
    "wss://stream.binance.com:443/ws",
    "wss://stream.binance.com:9443/ws",
];

// ===== CONNECTION LIMITS (configurable) =====
/// Number of priority symbols in the first connection
pub const PRIORITY_CONNECTION_SYMBOLS: usize = 10;

/// Number of high-volume symbols in the second connection  
pub const HIGH_VOLUME_CONNECTION_SYMBOLS: usize = 10;

/// Maximum symbols per regular connection (split into two for better stability)
pub const REGULAR_CONNECTION_SYMBOLS: usize = 90;

/// Subscription batch size per iteration
pub const SUBSCRIPTION_BATCH_SIZE: usize = 20;

/// Delay between subscription batches (milliseconds)
pub const SUBSCRIPTION_BATCH_DELAY_MS: u64 = 200;

/// Minimum message rate to consider connection healthy (messages per second)
pub const MIN_HEALTHY_MESSAGE_RATE: f64 = 1.0;

/// Connection health check interval (seconds)
pub const HEALTH_CHECK_INTERVAL_SECS: u64 = 5;

/// Maximum latency before considering connection unhealthy (milliseconds)
pub const MAX_HEALTHY_LATENCY_MS: u64 = 15000;

/// Startup grace period after subscription (milliseconds)
/// During this time, connections won't be considered unhealthy
pub const STARTUP_GRACE_PERIOD_MS: u64 = 45000; // 45 seconds

// // TEST FLAG: Force disconnect after this many seconds for testing reconnection
// // Set TEST_FORCE_DISCONNECT_AFTER_SECS env var to enable (e.g., "60" for 60 seconds)
// lazy_static::lazy_static! {
//     static ref TEST_FORCE_DISCONNECT_AFTER_SECS: Option<u64> = {
//         std::env::var("TEST_FORCE_DISCONNECT_AFTER_SECS")
//             .ok()
//             .and_then(|s| s.parse::<u64>().ok())
//     };
// }

type BinanceWebSocket = FragmentCollector<TokioIo<Upgraded>>;

#[derive(Debug, Deserialize)]
struct BinanceMessage {
    #[allow(dead_code)]
    stream: Option<String>,
}

#[derive(Debug, Deserialize)]
struct BinanceAggTrade {
    #[serde(rename = "e")]
    event_type: String,
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "p")]
    price: String,
    #[serde(rename = "T")]
    trade_time: u64,
}

#[derive(Debug, Serialize)]
struct BinanceSubscribe {
    method: String,
    params: Vec<String>,
    id: u64,
}

/// A single WebSocket connection to Binance
pub struct BinanceConnection {
    id: String,
    ws: Option<Arc<Mutex<BinanceWebSocket>>>,
    symbols: HashSet<String>,
    symbol_to_pair: Arc<DashMap<String, Pair>>,
    tx: mpsc::UnboundedSender<Vec<Ticker>>,
    is_connected: Arc<AtomicBool>,
    last_message_time: Arc<AtomicU64>,
    subscribed_symbols: Arc<RwLock<HashSet<String>>>,
    url_index: usize, // For rotating between URLs
    // Message rate tracking for Regular connections
    message_count: Arc<AtomicU64>,
    last_rate_log_time: Arc<AtomicU64>,
    last_rate_value: Arc<AtomicU32>, // Store rate as fixed point (x100)
    // Track when subscription completed for startup grace period
    subscription_completed_time: Arc<AtomicU64>,
    // Handle to ping task for keepalive
    ping_task_handle: Option<tokio::task::JoinHandle<()>>,
    // Track if read loop is running to prevent duplicates (matching other exchanges)
    read_loop_running: Arc<AtomicBool>,
    // Notify monitor to check immediately for instant failover
    monitor_trigger: Arc<tokio::sync::Notify>,
}

impl BinanceConnection {
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
            url_index: 0, // Start with first URL
            message_count: Arc::new(AtomicU64::new(0)),
            last_rate_log_time: Arc::new(AtomicU64::new(now)),
            last_rate_value: Arc::new(AtomicU32::new(0)),
            subscription_completed_time: Arc::new(AtomicU64::new(0)),
            ping_task_handle: None,
            read_loop_running: Arc::new(AtomicBool::new(false)),
            monitor_trigger: Arc::new(tokio::sync::Notify::new()),
        }
    }

    pub async fn connect(&mut self) -> Result<()> {
        info!("[{}] Connecting to Binance WebSocket", self.id);
        
        // Create HTTPS client
        let https = HttpsConnector::new();
        let client = Client::builder(TokioExecutor::new())
            .build::<_, Empty<Bytes>>(https);
        
        // Use URL rotation between the two Binance WebSocket URLs
        let ws_url = BINANCE_WS_URLS[self.url_index % BINANCE_WS_URLS.len()];
        info!("[{}] Using WebSocket URL: {}", self.id, ws_url);
        
        // Parse URI
        let uri: Uri = ws_url.replace("wss://", "https://").parse()?;
        
        // Extract host from URL for proper header
        let host = if ws_url.contains(":443") {
            "stream.binance.com:443"
        } else {
            "stream.binance.com:9443"
        };
        
        // Create WebSocket upgrade request
        let req = Request::builder()
            .method("GET")
            .uri(&uri)
            .header("Host", host)
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
        let mut ws = WebSocket::after_handshake(TokioIo::new(upgraded), Role::Client);
        // Enable auto pong to handle pings automatically
        ws.set_auto_pong(true);
        ws.set_auto_close(true);
        info!("[{}] WebSocket auto_pong set to TRUE - pings handled automatically", self.id);
        let ws = FragmentCollector::new(ws);
        
        self.is_connected.store(true, Ordering::Release);

        // Update global connection metadata cache to mark as connected
        if let Some(mut entry) = crate::hub::CONNECTION_METADATA_CACHE.get_mut(&(crate::types::Source::Binance, self.id.clone())) {
            entry.is_connected = true;
            entry.last_updated = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
        }

        self.last_message_time.store(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            Ordering::Release
        );
        
        let ws = Arc::new(Mutex::new(ws));
        self.ws = Some(ws.clone());
        
        // Start the ping task after connection is established
        self.start_ping_task();
        
        // Don't start the read loop here - it will be started after subscription
        
        info!("[{}] Connected successfully", self.id);
        Ok(())
    }

    pub async fn subscribe_gradually(&mut self) -> Result<()> {
        debug!("[{}] Starting gradual subscription process", self.id);
        let ws = self.ws.as_ref().ok_or_else(|| anyhow!("Not connected"))?;
        
        // Get what's already confirmed as working in websocket_active_pairs
        let hub = crate::hub::SurgeHub::global();
        let already_active = hub.get_websocket_pairs(crate::Source::Binance);
        let active_symbols: HashSet<String> = already_active.iter()
            .map(|pair| pair.as_binance_str())
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
        
        debug!("[{}] Getting write lock for subscribed symbols", self.id);
        let mut subscribed = self.subscribed_symbols.write().await;
        debug!("[{}] Got write lock, preparing to subscribe", self.id);
        
        for (i, chunk) in outstanding_symbols.chunks(SUBSCRIPTION_BATCH_SIZE).enumerate() {
            let mut streams = Vec::new();
            
            for symbol in chunk {
                if !subscribed.contains(symbol) {
                    streams.push(format!("{}@aggTrade", symbol.to_lowercase()));
                    subscribed.insert(symbol.clone());
                }
            }
            
            if !streams.is_empty() {
                let subscribe_msg = BinanceSubscribe {
                    method: "SUBSCRIBE".to_string(),
                    params: streams,
                    id: (i + 1) as u64,
                };
                
                if let Ok(msg) = serde_json::to_string(&subscribe_msg) {
                    debug!("[{}] Sending subscription for batch {} ({} symbols)", self.id, i + 1, chunk.len());
                    let mut ws_guard = ws.lock().await;
                    debug!("[{}] Got WebSocket lock, writing frame", self.id);
                    ws_guard.write_frame(Frame::text(msg.as_bytes().into())).await?;
                    drop(ws_guard);

                    info!("[{}] 📤 Subscribed to batch {}/{} ({} symbols, total subscribed: {})", 
                        self.id, 
                        i + 1, 
                        (outstanding_symbols.len() + SUBSCRIPTION_BATCH_SIZE - 1) / SUBSCRIPTION_BATCH_SIZE,
                        chunk.len(),
                        subscribed.len()
                    );
                    // Don't register pairs yet - wait until subscription is confirmed
                }
                
                // Delay between batches to avoid rate limits
                if i < outstanding_symbols.len() / SUBSCRIPTION_BATCH_SIZE {
                    tokio::time::sleep(Duration::from_millis(SUBSCRIPTION_BATCH_DELAY_MS)).await;
                }
            } else {
                debug!("[{}] Skipping batch {} - all symbols already subscribed", self.id, i + 1);
            }
        }
        
        info!("[{}] Subscribed to {} symbols gradually", self.id, outstanding_symbols.len());
        
        // Count how many of THIS connection's symbols were already active
        let already_active_count = self.symbols.iter()
            .filter(|s| active_symbols.contains(*s))
            .count();
        let outstanding_count = outstanding_symbols.len();
        
        // Wait for actual prices to confirm NEW subscriptions are working
        let confirmed = self.confirm_prices(outstanding_symbols.iter().cloned().collect()).await;
        
        // Register newly confirmed symbols as WebSocket-active
        let all_pairs = crate::exchanges::binance::get_cached_pairs().unwrap_or_default();
        if !confirmed.is_empty() {
            let pairs_to_register: Vec<crate::Pair> = confirmed.iter()
                .filter_map(|symbol| {
                    all_pairs.iter()
                        .find(|pair| pair.as_binance_str() == *symbol)
                        .cloned()
                })
                .collect();
            
            if !pairs_to_register.is_empty() {
                let hub = crate::hub::SurgeHub::global();
                hub.register_websocket_pairs(crate::Source::Binance, &pairs_to_register);
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
    async fn confirm_prices(&self, subscribed: HashSet<String>) -> HashSet<String> {
        use dashmap::DashMap;
        use std::sync::Arc;
        
        info!("[{}] ⏳ Waiting for price confirmations from {} symbols...", self.id, subscribed.len());
        
        // Track message count per symbol
        let message_counts: Arc<DashMap<String, u32>> = Arc::new(DashMap::new());
        for symbol in &subscribed {
            message_counts.insert(symbol.clone(), 0);
        }
        
        // Clone for the monitoring task
        let counts_clone = message_counts.clone();
        let ws = self.ws.as_ref().expect("WebSocket not connected").clone();
        let connection_id = self.id.clone();
        
        // Start a task to count incoming messages
        let counter_handle = tokio::spawn(async move {
            let mut confirmed = HashSet::new();
            let start = std::time::Instant::now();
            
            loop {
                // Check if we have enough confirmations or timeout
                if start.elapsed() > Duration::from_secs(45) {
                    warn!("[{}] Timeout waiting for price confirmations after 30s", connection_id);
                    break;
                }
                
                // Check each symbol's count
                let mut all_confirmed = true;
                for entry in counts_clone.iter() {
                    let (symbol, count) = entry.pair();
                    if *count >= 3 {
                        confirmed.insert(symbol.clone());
                    } else {
                        all_confirmed = false;
                    }
                }
                
                // If all symbols have 3+ messages, we're done
                if all_confirmed {
                    info!("[{}] ✅ All {} symbols confirmed with price data!", connection_id, confirmed.len());
                    break;
                }
                
                // Read a frame and check if it's for our symbols
                let frame = {
                    let mut ws_guard = ws.lock().await;
                    match tokio::time::timeout(Duration::from_millis(100), ws_guard.read_frame()).await {
                        Ok(Ok(frame)) => frame,
                        _ => continue,
                    }
                };
                
                if let OpCode::Text = frame.opcode {
                    if let Ok(text) = String::from_utf8(frame.payload.to_vec()) {
                        // Try to parse as aggTrade message
                        if let Ok(trade) = serde_json::from_str::<BinanceAggTrade>(&text) {
                            if trade.event_type == "aggTrade" {
                                if let Some(mut count) = counts_clone.get_mut(&trade.symbol) {
                                    *count += 1;
                                    if *count == 2 {
                                        debug!("[{}] Symbol {} confirmed with 3 prices", connection_id, trade.symbol);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            
            confirmed
        });
        
        // Wait for the counter task to complete
        match counter_handle.await {
            Ok(confirmed) => {
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
            Err(e) => {
                error!("[{}] Failed to confirm prices: {}", self.id, e);
                HashSet::new()
            }
        }
    }

    pub fn is_connected(&self) -> bool {
        self.is_connected.load(Ordering::Acquire)
    }
    
    pub async fn cleanup(&mut self) {
        debug!("[{}] Cleaning up connection", self.id);
        
        // IMPORTANT: Remove our symbols from websocket_active so REST picks them up
        let hub = crate::hub::SurgeHub::global();
        let all_pairs = crate::exchanges::binance::get_cached_pairs().unwrap_or_default();
        
        let pairs_to_remove: Vec<crate::Pair> = self.symbols.iter()
            .filter_map(|symbol| {
                all_pairs.iter()
                    .find(|p| p.as_binance_str() == *symbol)
                    .cloned()
            })
            .collect();
        
        if !pairs_to_remove.is_empty() {
            hub.remove_websocket_pairs(crate::Source::Binance, &pairs_to_remove);
            info!("[{}] 🔄 Removed {} symbols from WebSocket tracking → REST will handle", 
                self.id, pairs_to_remove.len());
        }
        
        // Mark as disconnected
        self.is_connected.store(false, Ordering::Release);
        
        // Stop ping task if running
        if let Some(handle) = self.ping_task_handle.take() {
            handle.abort();
            debug!("[{}] Aborted ping task", self.id);
        }
        
        // Mark read loop as stopped
        self.read_loop_running.store(false, Ordering::Release);
        
        // Close WebSocket if still open
        if let Some(ws) = &self.ws {
            let mut ws_guard = ws.lock().await;
            let _ = ws_guard.write_frame(Frame::close(1000, b"cleanup")).await;
            drop(ws_guard);
        }
        
        // Clear the WebSocket reference
        self.ws = None;
        
        // Clear subscribed symbols
        self.subscribed_symbols.write().await.clear();
        
        debug!("[{}] Cleanup complete", self.id);
    }
    
    fn start_ping_task(&mut self) {
        if let Some(ws) = &self.ws {
            let ws = ws.clone();
            let is_connected = self.is_connected.clone();
            let connection_id = self.id.clone();
            
            let handle = crate::runtime_separation::spawn_on_ingestion_named(
                &format!("binance-ping-{}", connection_id),
                async move {
                    // Track connection start time for 24-hour reconnect
                    let connection_start = std::time::Instant::now();
                    const BINANCE_24H_LIMIT: Duration = Duration::from_secs(23 * 3600 + 50 * 60); // 23h50m to reconnect before 24h
                    
                    // Wait a bit before starting ping to ensure connection is stable
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    
                    // According to Binance docs:
                    // - Server sends ping every 20 seconds
                    // - We must respond with pong within 1 minute
                    // - We can also send unsolicited pongs to keep connection alive
                    // Send unsolicited pong every 30 seconds as keepalive
                    let mut interval = tokio::time::interval(Duration::from_secs(30));
                    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                    
                    loop {
                        interval.tick().await;
                        
                        // Check if still connected
                        if !is_connected.load(Ordering::Acquire) {
                            debug!("[{}] Ping task stopping - connection closed", connection_id);
                            break;
                        }
                        
                        // Check if approaching 24-hour limit
                        if connection_start.elapsed() > BINANCE_24H_LIMIT {
                            info!("[{}] Approaching 24-hour connection limit, triggering reconnect", connection_id);
                            is_connected.store(false, Ordering::Release);

                            // Update cache to mark as disconnected
                            if let Some(mut entry) = crate::hub::CONNECTION_METADATA_CACHE.get_mut(&(crate::types::Source::Binance, connection_id.clone())) {
                                entry.is_connected = false;
                            }

                            break;
                        }
                        
                        // Send unsolicited pong frame as keepalive (empty payload as recommended)
                        let mut ws_guard = ws.lock().await;
                        if let Err(e) = ws_guard.write_frame(Frame::pong(vec![].into())).await {
                            error!("[{}] Failed to send keepalive pong: {}", connection_id, e);
                            // Mark connection as disconnected
                            is_connected.store(false, Ordering::Release);

                            // Update cache to mark as disconnected
                            if let Some(mut entry) = crate::hub::CONNECTION_METADATA_CACHE.get_mut(&(crate::types::Source::Binance, connection_id.clone())) {
                                entry.is_connected = false;
                            }

                            break;
                        }
                        drop(ws_guard);
                        debug!("[{}] Sent keepalive PONG to Binance (unsolicited)", connection_id);
                    }
                    
                    debug!("[{}] Ping task ended", connection_id);
                }
            );
            
            self.ping_task_handle = Some(handle);
        }
    }

    pub fn get_latency_ms(&self) -> u64 {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let last = self.last_message_time.load(Ordering::Acquire);
        now.saturating_sub(last)
    }
    
    /// Check if the connection has active subscriptions
    pub async fn has_active_subscriptions(&self) -> bool {
        let subscribed = self.subscribed_symbols.read().await;
        !subscribed.is_empty()
    }
    
    /// Get the number of subscribed symbols
    pub async fn get_subscribed_count(&self) -> usize {
        let subscribed = self.subscribed_symbols.read().await;
        subscribed.len()
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
    
    pub fn start_read_loop(&mut self) {
        // Check if read loop is already running (matching other exchanges)
        if self.read_loop_running.load(Ordering::Acquire) {
            warn!("[{}] Read loop already running, skipping start", self.id);
            return;
        }
        
        if let Some(ws) = &self.ws {
            let ws = ws.clone();
            let tx = self.tx.clone();
            let is_connected = self.is_connected.clone();
            let last_message_time = self.last_message_time.clone();
            let symbol_to_pair = self.symbol_to_pair.clone();
            let connection_id = self.id.clone();
            let subscribed_symbols = self.subscribed_symbols.clone();
            let message_count = self.message_count.clone();
            let last_rate_log_time = self.last_rate_log_time.clone();
            let last_rate_value = self.last_rate_value.clone();
            let read_loop_running = self.read_loop_running.clone();
            let monitor_trigger = self.monitor_trigger.clone();
            
            // Mark read loop as running
            self.read_loop_running.store(true, Ordering::Release);
            
            crate::runtime_separation::spawn_on_ingestion_named(
                &format!("binance-reader-{connection_id}"),
                async move {
                    info!("[{}] Starting read loop task", connection_id);
                if let Err(e) = Self::read_loop(
                    connection_id.clone(),
                    ws,
                    tx,
                    is_connected.clone(),
                    last_message_time.clone(),
                    symbol_to_pair,
                    subscribed_symbols.clone(),
                    message_count,
                    last_rate_log_time,
                    last_rate_value,
                    monitor_trigger.clone(),
                ).await {
                    error!("[{}] Binance read loop error: {}", connection_id, e);
                    is_connected.store(false, Ordering::Release);
                    read_loop_running.store(false, Ordering::Release);
                    
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
                    // Keep subscribed symbols for reconnection retry
                }
                info!("[{}] Read loop task ended", connection_id);
                read_loop_running.store(false, Ordering::Release);
            });
        }
    }

    async fn read_loop(
        connection_id: String,
        ws: Arc<Mutex<BinanceWebSocket>>,
        tx: mpsc::UnboundedSender<Vec<Ticker>>,
        is_connected: Arc<AtomicBool>,
        last_message_time: Arc<AtomicU64>,
        symbol_to_pair: Arc<DashMap<String, Pair>>,
        _subscribed_symbols: Arc<RwLock<HashSet<String>>>,
        message_count: Arc<AtomicU64>,
        last_rate_log_time: Arc<AtomicU64>,
        last_rate_value: Arc<AtomicU32>,
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
                        if let Some(mut entry) = crate::hub::CONNECTION_METADATA_CACHE.get_mut(&(crate::types::Source::Binance, connection_id.clone())) {
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
                        // Keep subscribed symbols for reconnection retry
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
                    
                    // Log first few messages for debugging
                    static MSG_COUNT: AtomicU64 = AtomicU64::new(0);
                    let count = MSG_COUNT.fetch_add(1, Ordering::Relaxed);
                    if count < 5 {
                        debug!("[{}] Received message {}: {}", connection_id, count, text);
                    }
                    
                    // Try to parse as aggTrade message
                    if let Ok(trade) = serde_json::from_str::<BinanceAggTrade>(&text) {
                        if trade.event_type == "aggTrade" {
                            // Increment message count for all connections
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

                                // Log every 600 seconds (only for Regular connections to reduce log spam)
                                if now - last_log >= 600_000 {
                                    message_count.store(0, Ordering::Release); // Reset for next period
                                    
                                    // Only log if we have meaningful data and it's a Regular connection
                                    if connection_id.contains("Regular") && (rate > 1.0 || count > 10) {
                                        info!("[{}] 📊 Raw Message rate: {:.1} msg/sec ({} messages in {:.1}s)", 
                                            connection_id, rate, count, elapsed_ms as f64 / 1000.0);
                                    }
                                    
                                    last_rate_log_time.store(now, Ordering::Release);
                                }
                            }
                            
                            // Look up the pair from our cache
                            if let Some(pair) = symbol_to_pair.get(&trade.symbol) {
                                if let Ok(price) = trade.price.parse::<Decimal>() {
                                    // // DEBUG: Track BTC/FDUSD specifically
                                    // if trade.symbol == "BTCFDUSD" {
                                    //     info!("[{}] 🔍 BTC/FDUSD WebSocket update: {} @ {}", 
                                    //         connection_id, pair.as_str(), price);
                                    // }
                                    
                                    let tick = Ticker {
                                        symbol: pair.as_str(),
                                        price,
                                        timestamp: trade.trade_time,
                                    };
                                    if tx.send(vec![tick]).is_err() {
                                        error!("[{}] Failed to send ticker to channel", connection_id);
                                    } else {
                                        debug!("[{}] Sent ticker: {} @ {}", connection_id, pair.as_str(), price);
                                    }
                                }
                            } else {
                                debug!("Pair not found in cache: {}", trade.symbol);
                            }
                        }
                    } else if text.contains("\"result\":") || text.contains("\"error\"") {
                        // Subscription response or error - ignore
                        debug!("[{}] Subscription response: {}", connection_id, text);
                    } else {
                        // Unknown message format
                        if count < 10 {
                            warn!("[{}] Unknown message format: {}", connection_id, text);
                        }
                    }
                }
                OpCode::Close => {
                    info!("[{}] WebSocket closed", connection_id);
                    is_connected.store(false, Ordering::Release);
                    
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
                    // Keep subscribed symbols for reconnection retry
                    break;
                }
                OpCode::Ping => {
                    // This should not happen since we have set_auto_pong(true)
                    // But if it does, handle it manually as a fallback
                    info!("[{}] Received PING from Binance (unexpected with auto_pong=true), sending PONG response", connection_id);
                    let mut ws_guard = ws.lock().await;
                    ws_guard.write_frame(Frame::pong(frame.payload)).await?;
                    drop(ws_guard);
                    info!("[{}] PONG sent successfully to Binance (manual fallback)", connection_id);
                }
                _ => {}
            }
        }
        Ok(())
    }
}

/// Single connection with health monitoring
pub struct ConnectionPair {
    pub id: String,
    pub connection: Arc<RwLock<BinanceConnection>>,
    pub symbols: HashSet<String>,
    monitoring_task: Option<tokio::task::JoinHandle<()>>,
}

impl ConnectionPair {
    pub fn new(
        id: String,
        symbols: HashSet<String>,
        symbol_to_pair: Arc<DashMap<String, Pair>>,
        tx: mpsc::UnboundedSender<Vec<Ticker>>,
    ) -> Self {
        let connection = BinanceConnection::new(
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
            conn.start_read_loop();
        }
        
        // Start monitoring task
        let connection = self.connection.clone();
        let connection_id = self.id.clone();
        let symbols = self.symbols.clone();
        
        let monitoring_task = crate::runtime_separation::spawn_on_ingestion_named(
            &format!("binance-monitor-{connection_id}"),
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
        connection: Arc<RwLock<BinanceConnection>>,
        symbols: HashSet<String>,
    ) {
        let mut check_interval = tokio::time::interval(Duration::from_secs(HEALTH_CHECK_INTERVAL_SECS));
        let mut symbols_in_rest_failover = false;
        // Initialize with current time to prevent immediate reconnection
        let mut last_reconnect_attempt: Option<std::time::Instant> = Some(std::time::Instant::now());
        const RECONNECT_INTERVAL_SECS: u64 = 30; // 30 seconds between reconnect attempts
        
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
            &format!("binance-retry-{}", conn_id_retry),
            async move {
                // Wait before first retry
                tokio::time::sleep(Duration::from_secs(200)).await;
                
                loop {
                    tokio::time::sleep(Duration::from_secs(500)).await; // Every 5 minutes
                    
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
                    let all_pairs = crate::exchanges::binance::get_cached_pairs().unwrap_or_default();
                    
                    let active_symbols: HashSet<String> = symbols_retry.iter()
                        .filter(|symbol| {
                            if let Some(pair) = all_pairs.iter()
                                .find(|p| p.as_binance_str() == **symbol) {
                                hub.is_websocket_active(crate::Source::Binance, &pair.clone())
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
                        // Check if ALL symbols are unconfirmed (connection might be dead)
                        if unconfirmed.len() == subscribed.len() && !subscribed.is_empty() {
                            // All subscribed symbols are inactive - connection is probably dead!
                            error!("[{}] ⚠️ ALL {} symbols are inactive! Connection likely dead, triggering cleanup", 
                                conn_id_retry, subscribed.len());
                            
                            // Mark connection as disconnected to trigger reconnection
                            let mut conn = connection_retry.write().await;
                            conn.is_connected.store(false, Ordering::Release);
                            conn.cleanup().await;
                            drop(conn);
                            // Monitor will detect disconnection and reconnect
                            continue;
                        }
                        
                        // Just some symbols need retry
                        info!("[{}] 🔄 Retrying {} unconfirmed symbols out of {}", 
                            conn_id_retry, unconfirmed.len(), subscribed.len());
                        
                        let mut conn = connection_retry.write().await;
                        match conn.subscribe_gradually().await {
                            Ok(_) => {
                                debug!("[{}] Retry processed successfully", conn_id_retry);
                            }
                            Err(e) => {
                                // This should only happen if connection went from some active to 0 active
                                error!("[{}] Connection degraded to 0 active symbols: {}", conn_id_retry, e);
                                conn.is_connected.store(false, Ordering::Release);
                                conn.cleanup().await;
                                drop(conn);
                                // Monitor will detect and reconnect
                            }
                        }
                    }
                }
            },
        );
        
        // // Track connection start time for test disconnect
        // let mut connection_start_time = std::time::Instant::now();
        // let mut test_disconnect_triggered = false;
        
        let mut iteration_count = 0u64;

        loop {
            iteration_count += 1;

            tokio::select! {
                _ = check_interval.tick() => {
                    // Regular interval check
                }
                _ = monitor_trigger.notified() => {
                    info!("[{}] Monitor triggered immediately due to connection failure", connection_id);
                }
            }
            
            // // TEST: Force disconnect after specified time (only once per connection)
            // if let Some(disconnect_after) = *TEST_FORCE_DISCONNECT_AFTER_SECS {
            //     if !test_disconnect_triggered && connection_start_time.elapsed().as_secs() >= disconnect_after {
            //         if !symbols_in_rest_failover {
            //             test_disconnect_triggered = true;
            //             warn!("[{}] 🧪 TEST: Forcing disconnect after {} seconds", connection_id, disconnect_after);
            //             let mut conn = connection.write().await;
                        
            //             // Actually close the WebSocket connection
            //             if let Some(ws) = &conn.ws {
            //                 let mut ws_guard = ws.lock().await;
            //                 // Send close frame to properly disconnect
            //                 let _ = ws_guard.write_frame(fastwebsockets::Frame::close(1000, b"Test disconnect")).await;
            //                 drop(ws_guard);
            //             }
                        
            //             conn.is_connected.store(false, Ordering::Release);
            //             conn.ws = None;
            //             conn.subscribed_symbols.write().await.clear();
            //             drop(conn);
            //             // Continue to let normal reconnection logic handle it
            //         }
            //     }
            // }
            
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
                    
                    // DEBUG: Check if BTC/FDUSD is in this connection
                    // if symbols.contains("BTCFDUSD") {
                    //     warn!("[{}] 🔍 BTC/FDUSD being moved to REST API due to unhealthy connection", connection_id);
                    // }
                    
                    // Close the unhealthy connection
                    {
                        let mut conn = connection.write().await;
                        // Use cleanup method like other exchanges
                        conn.cleanup().await;
                    }
                    
                    // Clear these symbols from WebSocket tracking
                    let all_pairs = crate::exchanges::binance::get_cached_pairs().unwrap_or_default();
                    let pairs_to_clear: Vec<crate::Pair> = symbols.iter()
                        .filter_map(|symbol| {
                            all_pairs.iter()
                                .find(|pair| pair.as_binance_str() == *symbol)
                                .cloned()
                        })
                        .collect();
                    
                    if !pairs_to_clear.is_empty() {
                        match std::panic::catch_unwind(|| {
                            let hub = crate::hub::SurgeHub::global();
                            hub.remove_websocket_pairs(crate::Source::Binance, &pairs_to_clear);
                        }) {
                            Ok(_) => {
                                info!("[{}] 📡 Removed {} symbols from WebSocket tracking for REST fallback",
                                    connection_id, pairs_to_clear.len());
                            }
                            Err(_) => {
                                error!("[{}] ❌ Failed to remove symbols from WebSocket tracking", connection_id);
                            }
                        }
                    }
                    
                    symbols_in_rest_failover = true;
                }
            } else if !is_connected && !symbols_in_rest_failover {
                // Connection is down
                error!("[{}] 🚨 Connection DOWN! Moving {} symbols to REST API", 
                    connection_id, symbols.len());
                
                // DEBUG: Check if BTC/FDUSD is in this connection
                // if symbols.contains("BTCFDUSD") {
                //     warn!("[{}] 🔍 BTC/FDUSD being moved to REST API due to connection down", connection_id);
                // }
                
                // Clear these symbols from WebSocket tracking
                let all_pairs = crate::exchanges::binance::get_cached_pairs().unwrap_or_default();
                let pairs_to_clear: Vec<crate::Pair> = symbols.iter()
                    .filter_map(|symbol| {
                        all_pairs.iter()
                            .find(|pair| pair.as_binance_str() == *symbol)
                            .cloned()
                    })
                    .collect();
                
                if !pairs_to_clear.is_empty() {
                    match std::panic::catch_unwind(|| {
                        let hub = crate::hub::SurgeHub::global();
                        hub.remove_websocket_pairs(crate::Source::Binance, &pairs_to_clear);
                    }) {
                        Ok(_) => {
                            info!("[{}] 📡 Removed {} symbols from WebSocket tracking for REST fallback",
                                connection_id, pairs_to_clear.len());
                        }
                        Err(_) => {
                            error!("[{}] ❌ Failed to remove symbols from WebSocket tracking (connection down)", connection_id);
                        }
                    }
                }
                
                symbols_in_rest_failover = true;
                // Prevent immediate reconnection - wait for the configured interval
                last_reconnect_attempt = Some(std::time::Instant::now());
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
                                    // Start the read loop to process incoming messages
                                    conn.start_read_loop();
                                    info!("[{}] ✅ Read loop restarted after reconnection", connection_id);
                                    symbols_in_rest_failover = false; // Reset the failover flag    
                                    // Reset test disconnect timer for next test
                                    // connection_start_time = std::time::Instant::now();
                                    // test_disconnect_triggered = false;
                                }
                                Err(e) => {
                                    // subscribe_gradually only returns Err when 0 symbols work
                                    error!("[{}] ❌ Failed to re-subscribe after reconnection: {}", connection_id, e);
                                    // No symbols working - cleanup and retry fresh next time
                                    conn.cleanup().await;
                                    drop(conn); // Release the write lock
                                    error!("[{}] Cleaned up connection due to total subscription failure (0 symbols confirmed)", connection_id);
                                    // Will retry reconnection in 30 seconds
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
            
            // Log status periodically and monitor health
            if iteration_count % 100 == 0 { // Every 30 seconds (5 second intervals × 6)
                if is_connected {
                    debug!("[{}] Status: UP ({}ms latency, {:.1} msg/sec){}",
                        connection_id, latency_ms, message_rate,
                        if in_grace_period { " (grace period)" } else { "" }
                    );
                } else {
                    debug!("[{}] Status: DOWN (failover: {})",
                        connection_id, symbols_in_rest_failover);
                }

                // Log monitor health every 5 minutes
                if iteration_count % 600 == 0 {
                    info!("[{}] Monitor loop alive - iteration {}, failover: {}",
                        connection_id, iteration_count, symbols_in_rest_failover);
                }
            }

            // Defensive: Ensure loop continues
            if iteration_count % 1000 == 0 {
                debug!("[{}] Monitor completed {} iterations", connection_id, iteration_count);
            }

            // Catch-all to ensure we never accidentally break out of loop
            if iteration_count > u64::MAX - 1000 {
                // Reset counter to prevent overflow (after ~585 billion years at 5s intervals)
                iteration_count = 0;
                warn!("[{}] Iteration counter reset to prevent overflow", connection_id);
            }
        }
    }
}