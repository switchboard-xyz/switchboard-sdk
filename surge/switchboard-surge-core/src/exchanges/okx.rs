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
use std::sync::atomic::{AtomicBool, AtomicU64,  AtomicU32, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::{mpsc, Mutex, RwLock};
use tracing::{debug, error, info, warn};

// Global cache for OKX exchange info
static OKX_EXCHANGE_INFO: Lazy<Arc<DashMap<String, Pair>>> = Lazy::new(|| Arc::new(DashMap::new()));

// Configuration constants
const TOP_SYMBOLS_COUNT: usize = 150;
const OKX_WS_ENDPOINT: &str = "wss://ws.okx.com:8443/ws/v5/public";
const SUBSCRIPTION_BATCH_SIZE: usize = 100; // OKX allows up to 100 channels per subscription
const SUBSCRIPTION_BATCH_DELAY_MS: u64 = 500;
const CONNECTION_DELAY_SECS: u64 = 1; // Reduced for faster startup
const HEALTH_CHECK_INTERVAL_SECS: u64 = 5; // More frequent health checks
const MAX_HEALTHY_LATENCY_MS: u64 = 10000; // 10 seconds - time since last message threshold  

type OkxWebSocket = FragmentCollector<TokioIo<Upgraded>>;

#[derive(Debug, Deserialize)]
struct OkxMessage {
    arg: OkxArg,
    data: Vec<OkxTickerData>,
}

#[derive(Debug, Deserialize)]
struct OkxArg {
    channel: String,
}

#[derive(Debug, Deserialize)]
struct OkxTickerData {
    #[serde(rename = "instId")]
    inst_id: String,
    last: String,
    #[serde(rename = "ts")]
    timestamp: String,
}

#[derive(Debug, Serialize)]
struct OkxSubscribe {
    op: String,
    args: Vec<OkxSubscribeArg>,
}

#[derive(Debug, Serialize)]
struct OkxSubscribeArg {
    channel: String,
    #[serde(rename = "instId")]
    inst_id: String,
}

/// Get all cached OKX pairs
pub fn get_cached_pairs() -> Result<Vec<Pair>> {
    if OKX_EXCHANGE_INFO.is_empty() {
        return Err(anyhow!("Exchange info not fetched yet"));
    }
    
    let pairs: Vec<Pair> = OKX_EXCHANGE_INFO
        .iter()
        .map(|entry| entry.value().clone())
        .collect();
    
    Ok(pairs)
}

/// A single WebSocket connection to OKX
pub struct OkxConnection {
    id: String,
    ws: Option<Arc<Mutex<OkxWebSocket>>>,
    symbols: HashSet<String>,
    symbol_to_pair: Arc<DashMap<String, Pair>>,
    tx: mpsc::UnboundedSender<Vec<Ticker>>,
    is_connected: Arc<AtomicBool>,
    last_message_time: Arc<AtomicU64>,
    subscribed_symbols: Arc<RwLock<HashSet<String>>>,
    message_count: Arc<AtomicU64>,
    last_rate_log_time: Arc<AtomicU64>,
    last_rate_value: Arc<AtomicU32>,
    connection_start_time: Arc<AtomicU64>,
    
    // Track when subscription completed for startup grace period
    subscription_completed_time: Arc<AtomicU64>,
    
    // Track symbol confirmations (symbol -> message count)
    symbol_confirmations: Arc<DashMap<String, u32>>,
    
    // Track if read loop is running to prevent duplicates
    read_loop_running: Arc<AtomicBool>,
    
    // Handle to ping task for cleanup
    ping_task_handle: Option<tokio::task::JoinHandle<()>>,
    
    // Notify monitor to check immediately
    monitor_trigger: Arc<tokio::sync::Notify>,
}

impl OkxConnection {
    pub fn new(
        id: String,
        symbols: HashSet<String>,
        symbol_to_pair: Arc<DashMap<String, Pair>>,
        tx: mpsc::UnboundedSender<Vec<Ticker>>,
    ) -> Self {
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
            last_rate_log_time: Arc::new(AtomicU64::new(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64
            )),
            last_rate_value: Arc::new(AtomicU32::new(0)),
            connection_start_time: Arc::new(AtomicU64::new(0)),
            subscription_completed_time: Arc::new(AtomicU64::new(0)),
            symbol_confirmations: Arc::new(DashMap::new()),
            read_loop_running: Arc::new(AtomicBool::new(false)),
            ping_task_handle: None,
            monitor_trigger: Arc::new(tokio::sync::Notify::new()),
        }
    }

    pub async fn connect(&mut self) -> Result<()> {
        info!("[{}] Connecting to OKX WebSocket", self.id);
        
        // Create HTTPS client
        let https = HttpsConnector::new();
        let client = Client::builder(TokioExecutor::new())
            .build::<_, Empty<Bytes>>(https);
        
        // Parse URI
        let uri: Uri = OKX_WS_ENDPOINT.replace("wss://", "https://").parse()?;
        
        // Create WebSocket upgrade request
        let req = Request::builder()
            .method("GET")
            .uri(&uri)
            .header("Host", "ws.okx.com:8443")
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
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        self.last_message_time.store(now, Ordering::Release);
        self.connection_start_time.store(now, Ordering::Release);

        // Update global connection metadata cache to mark as connected
        if let Some(mut entry) = crate::hub::CONNECTION_METADATA_CACHE.get_mut(&(crate::types::Source::Okx, self.id.clone())) {
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
                &format!("okx-ping-{}", connection_id),
                async move {
                    // Wait a bit before starting ping to ensure connection is stable
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    
                    // OKX documentation says to send ping every N seconds where N < 30
                    // We'll use 20 seconds for a safe margin
                    let mut interval = tokio::time::interval(Duration::from_secs(20));
                    
                    loop {
                        interval.tick().await;
                        
                        // Check if still connected
                        if !is_connected.load(Ordering::Acquire) {
                            warn!("[{}] Ping task stopping - connection closed", connection_id);
                            break;
                        }
                        
                        // OKX expects plain string "ping"
                        let ping_msg = "ping";
                        
                        let mut ws_guard = ws.lock().await;
                        if let Err(e) = ws_guard.write_frame(Frame::text(ping_msg.as_bytes().into())).await {
                            error!("[{}] Failed to send ping: {}", connection_id, e);
                            // Mark connection as disconnected
                            is_connected.store(false, Ordering::Release);
                            break;
                        }
                        drop(ws_guard);
                        debug!("[{}] Sent ping to OKX", connection_id);
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
        let already_active = hub.get_websocket_pairs(Source::Okx);
        let active_symbols: HashSet<String> = already_active.iter()
            .map(|pair| pair.as_okx_str())
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
                let mut args = Vec::new();
                
                for symbol in chunk {
                    if !subscribed.contains(symbol) {
                        args.push(OkxSubscribeArg {
                            channel: "tickers".to_string(),
                            inst_id: symbol.clone(),
                        });
                        subscribed.insert(symbol.clone());
                    }
                }
                
                if !args.is_empty() {
                    let subscribe_msg = OkxSubscribe {
                        op: "subscribe".to_string(),
                        args,
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
                        .find(|pair| pair.as_okx_str() == *symbol)
                        .cloned()
                })
                .collect();
            
            if !pairs_to_register.is_empty() {
                hub.register_websocket_pairs(Source::Okx, &pairs_to_register);
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

    pub async fn cleanup(&mut self) {
        debug!("[{}] Cleaning up connection", self.id);
        
        // IMPORTANT: Remove our symbols from websocket_active so REST picks them up
        let hub = crate::hub::SurgeHub::global();
        let all_pairs = get_cached_pairs().unwrap_or_default();
        
        let pairs_to_remove: Vec<Pair> = self.symbols.iter()
            .filter_map(|symbol| {
                all_pairs.iter()
                    .find(|p| p.as_okx_str() == *symbol)
                    .cloned()
            })
            .collect();
        
        if !pairs_to_remove.is_empty() {
            hub.remove_websocket_pairs(Source::Okx, &pairs_to_remove);
            info!("[{}] 🔄 Removed {} symbols from WebSocket tracking → REST will handle", 
                self.id, pairs_to_remove.len());
        }
        
        // Mark as disconnected
        self.is_connected.store(false, Ordering::Release);

        // Update cache to mark as disconnected
        if let Some(mut entry) = crate::hub::CONNECTION_METADATA_CACHE.get_mut(&(crate::types::Source::Okx, self.id.clone())) {
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
            
        const STARTUP_GRACE_PERIOD_MS: u64 = 60_000; // 60 seconds grace period
        now - subscription_time < STARTUP_GRACE_PERIOD_MS
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
            let subscribed_symbols = self.subscribed_symbols.clone();
            let symbol_confirmations = self.symbol_confirmations.clone();
            let read_loop_running = self.read_loop_running.clone();
            let monitor_trigger = self.monitor_trigger.clone();
            
            // Start ping task after read loop is set up
            self.start_ping_task();
            
            crate::runtime_separation::spawn_on_ingestion_named(
                &format!("okx-reader-{}", connection_id),
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
                        subscribed_symbols,
                        symbol_confirmations,
                        monitor_trigger.clone(),
                    ).await {
                        error!("[{}] OKX read loop error: {}", connection_id, e);
                        is_connected.store(false, Ordering::Release);

                        // Update cache to mark as disconnected
                        if let Some(mut entry) = crate::hub::CONNECTION_METADATA_CACHE.get_mut(&(crate::types::Source::Okx, connection_id.clone())) {
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
        ws: Arc<Mutex<OkxWebSocket>>,
        tx: mpsc::UnboundedSender<Vec<Ticker>>,
        is_connected: Arc<AtomicBool>,
        last_message_time: Arc<AtomicU64>,
        symbol_to_pair: Arc<DashMap<String, Pair>>,
        message_count: Arc<AtomicU64>,
        last_rate_log_time: Arc<AtomicU64>,
        last_rate_value: Arc<AtomicU32>,
        _subscribed_symbols: Arc<RwLock<HashSet<String>>>,
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
                        if let Some(mut entry) = crate::hub::CONNECTION_METADATA_CACHE.get_mut(&(crate::types::Source::Okx, connection_id.clone())) {
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
                    
                    // Handle pong response
                    if text == "pong" {
                        debug!("[{}] Received pong from OKX", connection_id);
                        continue;
                    }
                    
                    // Log first few messages for debugging
                    static MSG_COUNT: AtomicU64 = AtomicU64::new(0);
                    let count = MSG_COUNT.fetch_add(1, Ordering::Relaxed);
                    if count < 5 {
                        debug!("[{}] OKX message {}: {}", connection_id, count, text);
                    }
                    
                    if let Ok(msg) = serde_json::from_str::<OkxMessage>(&text) {
                        if msg.arg.channel == "tickers" {
                            
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
                            
                            for ticker_data in msg.data {
                                // Track confirmations for this symbol
                                let mut confirm_count = symbol_confirmations
                                    .entry(ticker_data.inst_id.clone())
                                    .or_insert(0);
                                *confirm_count += 1;
                                
                                if *confirm_count == 3 {
                                    debug!("[{}] Symbol {} confirmed with 3+ messages", 
                                        connection_id, ticker_data.inst_id);
                                }
                                
                                // Look up the pair from our cache
                                if let Some(pair) = symbol_to_pair.get(&ticker_data.inst_id) {
                                    // Log APT-USDT ticker data received
                                    if ticker_data.inst_id == "APT-USDT" {
                                        debug!("📊 [{}] Received APT-USDT ticker: price={} ts={}", 
                                            connection_id, ticker_data.last, ticker_data.timestamp);
                                    }
                                    
                                    if let Ok(price) = ticker_data.last.parse::<Decimal>() {
                                        let timestamp = ticker_data.timestamp.parse::<u64>().unwrap_or(0);
                                        let tick = Ticker {
                                            symbol: pair.as_str(),
                                            price,
                                            timestamp,
                                        };
                                        let _ = tx.send(vec![tick]);
                                    }
                                }
                            }
                        }
                    }
                }
                OpCode::Close => {
                    info!("[{}] WebSocket closed", connection_id);
                    is_connected.store(false, Ordering::Release);

                    // Update cache to mark as disconnected
                    if let Some(mut entry) = crate::hub::CONNECTION_METADATA_CACHE.get_mut(&(crate::types::Source::Okx, connection_id.clone())) {
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

/// Single OKX connection with monitoring
pub struct OkxConnectionPair {
    pub id: String,
    pub connection: Arc<RwLock<OkxConnection>>,
    pub symbols: HashSet<String>,
    monitoring_task: Option<tokio::task::JoinHandle<()>>,
}

impl OkxConnectionPair {
    pub fn new(
        id: String,
        symbols: HashSet<String>,
        symbol_to_pair: Arc<DashMap<String, Pair>>,
        tx: mpsc::UnboundedSender<Vec<Ticker>>,
    ) -> Self {
        let connection = Arc::new(RwLock::new(OkxConnection::new(
            id.clone(),
            symbols.clone(),
            symbol_to_pair,
            tx,
        )));
        
        Self {
            id,
            connection,
            symbols,
            monitoring_task: None,
        }
    }

    pub async fn start(&mut self) -> Result<()> {
        info!("[{}] Starting OKX connection", self.id);
        
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
            &format!("okx-monitor-{connection_id}"),
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
        connection: Arc<RwLock<OkxConnection>>,
        symbols: HashSet<String>,
    ) {
        let mut check_interval = tokio::time::interval(Duration::from_secs(HEALTH_CHECK_INTERVAL_SECS));
        let mut symbols_in_rest_failover = false;
        const MIN_HEALTHY_MESSAGE_RATE: f64 = 5.0;  // Minimum 5 msg/sec to be considered healthy
        const RECONNECT_INTERVAL_SECS: u64 = 300; // 5 minutes between reconnect attempts
        let mut last_reconnect_attempt: Option<std::time::Instant> = Some(std::time::Instant::now());
        let mut tick_count = 0u64;
        
        // Get the monitor trigger for immediate notifications
        let monitor_trigger = {
            let conn = connection.read().await;
            conn.monitor_trigger.clone()
        };
        
        loop {
            tokio::select! {
                _ = check_interval.tick() => {
                    tick_count += 1;
                    // Regular interval check
                }
                _ = monitor_trigger.notified() => {
                    info!("[{}] Monitor triggered immediately due to connection failure", connection_id);
                }
            }
            
            let conn = connection.read().await;
            let is_connected = conn.is_connected();
            let latency_ms = conn.get_latency_ms();
            let message_rate = conn.get_message_rate();
            let in_grace_period = conn.is_in_grace_period();
            drop(conn); // Release lock early
            
            // Check if connection is unhealthy - skip grace period checks
            let unhealthy = if in_grace_period {
                !is_connected  // Only check connection status during grace period
            } else {
                !is_connected || 
                latency_ms > MAX_HEALTHY_LATENCY_MS || 
                message_rate < MIN_HEALTHY_MESSAGE_RATE
            };
            
            if unhealthy && !symbols_in_rest_failover && !in_grace_period {
                    error!("[{}] 🚨 CONNECTION UNHEALTHY! Connected: {}, Latency: {}ms, Rate: {:.1} msg/s. Moving {} symbols to REST API", 
                        connection_id, is_connected, latency_ms, message_rate, symbols.len());
                    
                    // Get pairs to remove from websocket cache
                    let pairs_to_remove: Vec<crate::Pair> = symbols.iter()
                        .filter_map(|symbol| {
                            OKX_EXCHANGE_INFO.get(symbol)
                                .map(|entry| entry.value().clone())
                        })
                        .collect();
                    
                    if !pairs_to_remove.is_empty() {
                        let hub = crate::hub::SurgeHub::global();
                        hub.remove_websocket_pairs(Source::Okx, &pairs_to_remove);
                        info!("[{}] 🔄 Removed {} pairs from WebSocket cache for REST failover", 
                            connection_id, pairs_to_remove.len());
                    }
                    
                    // REST already polls all symbols - just need to remove from websocket cache
                    symbols_in_rest_failover = true;
                }
            
            // Attempt reconnection if connection is unhealthy and enough time has passed
            if symbols_in_rest_failover {
                let should_reconnect = match last_reconnect_attempt {
                    None => true,
                    Some(last_attempt) => last_attempt.elapsed().as_secs() >= RECONNECT_INTERVAL_SECS,
                };
                
                if should_reconnect {
                    info!("[{}] 🔄 Attempting to reconnect OKX WebSocket...", connection_id);
                    last_reconnect_attempt = Some(std::time::Instant::now());
                    
                    // Try to reconnect
                    let mut conn = connection.write().await;
                    
                    // Clean up old connection state (stops ping task, clears read loop flag, etc.)
                    conn.cleanup().await;
                    
                    // Reset the connection state
                    conn.symbols = symbols.clone();
                    
                    match conn.connect().await {
                        Ok(_) => {
                            info!("[{}] ✅ WebSocket reconnected successfully", connection_id);
                            
                            match conn.subscribe_gradually().await {
                                Ok(_) => {
                                    info!("[{}] ✅ Successfully re-subscribed to symbols after reconnection", connection_id);
                                    // Note: read_loop is already started in subscribe_gradually
                                    
                                    symbols_in_rest_failover = false; // Reset the failover flag
                                }
                                Err(e) => {
                                    // subscribe_gradually only returns Err when 0 symbols work
                                    error!("[{}] ❌ Failed to re-subscribe after reconnection: {}", connection_id, e);
                                    // No symbols working - cleanup and retry fresh next time
                                    conn.cleanup().await;
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
            
            // Log status periodically (every 30 seconds)
            if tick_count % 6 == 0 {
                if in_grace_period {
                    info!("[{}] Status: {} ({}ms latency, {:.1} msg/s) - in grace period",
                        connection_id,
                        if is_connected { "UP" } else { "DOWN" },
                        latency_ms,
                        message_rate
                    );
                } else if is_connected {
                    debug!("[{}] Status: UP ({}ms latency, {:.1} msg/s)",
                        connection_id,
                        latency_ms,
                        message_rate
                    );
                }
            }
            
            // Check if connection came back online after failover
            if symbols_in_rest_failover && !unhealthy && !in_grace_period {
                    info!("[{}] ✅ WebSocket connection restored! Moving {} symbols back from REST", 
                        connection_id, symbols.len());
                    
                    // Re-register pairs as WebSocket-active
                    let pairs_to_register: Vec<crate::Pair> = symbols.iter()
                        .filter_map(|symbol| {
                            OKX_EXCHANGE_INFO.get(symbol)
                                .map(|entry| entry.value().clone())
                        })
                        .collect();
                    
                    if !pairs_to_register.is_empty() {
                        let hub = crate::hub::SurgeHub::global();
                        hub.register_websocket_pairs(Source::Okx, &pairs_to_register);
                        info!("[{}] 📡 Re-registered {} pairs as WebSocket-active", 
                            connection_id, pairs_to_register.len());
                    }
                    
                    symbols_in_rest_failover = false;
            }
        }
    }
}

pub struct OkxStream {
    connection_pairs: Vec<Arc<Mutex<OkxConnectionPair>>>,
    rx: mpsc::UnboundedReceiver<Result<Ticker>>,
    tx: mpsc::UnboundedSender<Vec<Ticker>>,
    symbol_to_pair_cache: Arc<DashMap<String, Pair>>,
    connection_state: ConnectionState,
}

impl OkxStream {
    pub async fn new() -> Result<Self> {
        let (tx, rx) = mpsc::unbounded_channel();
        
        // Create internal tx/rx for aggregating from multiple connections
        let (internal_tx, mut internal_rx) = mpsc::unbounded_channel::<Vec<Ticker>>();
        
        // Spawn aggregator task
        let _external_tx = tx.clone();
        let symbol_to_pair_cache = OKX_EXCHANGE_INFO.clone();
        let symbol_to_pair_cache_for_spawn = symbol_to_pair_cache.clone();

        crate::runtime_separation::spawn_on_ingestion_named(
            "okx-aggregator",
            async move {
                while let Some(tickers) = internal_rx.recv().await {
                    for ticker in tickers {
                        // Process each ticker and update cache
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
            symbol_to_pair_cache: OKX_EXCHANGE_INFO.clone(),
            connection_state: ConnectionState::new(),
        };
        stream.connect().await?;
        Ok(stream)
    }
    
    /// Pre-fetch exchange info without connecting WebSocket
    pub async fn prefetch_exchange_info() -> Result<()> {
        if OKX_EXCHANGE_INFO.is_empty() {
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
        info!("Creating OKX WebSocket connections");
        
        // Get top symbols by volume AND all symbols
        let (websocket_symbols, _) = Self::select_top_symbols().await?;
        
        // Initially, REST should poll ALL symbols
        let all_symbols = Self::get_all_symbols().await?;
        let all_symbols_set: HashSet<String> = all_symbols.into_iter().collect();
        
        info!("📊 OKX startup: REST polling {} symbols initially, WebSocket will handle {} high-volume symbols", 
            all_symbols_set.len(), websocket_symbols.len());
        
        // Set up REST polling for ALL symbols initially
        let rest_manager = crate::rest_manager::RestManager::global();
        rest_manager.set_rest_symbols(Source::Okx, all_symbols_set).await;
        
        // Create connection pairs
        let symbols_per_connection = 50; // Reasonable batch size for OKX
        let symbol_chunks: Vec<_> = websocket_symbols.chunks(symbols_per_connection)
            .map(|chunk| chunk.iter().cloned().collect::<HashSet<String>>())
            .collect();
        
        for (i, symbols) in symbol_chunks.into_iter().enumerate() {
            // Check if APT-USDT is in this chunk
            let has_apt = symbols.contains("APT-USDT");
            if has_apt {
                info!("🔍 APT-USDT found in OKX connection chunk {} (OKX-{})", i, i + 1);
                info!("   Chunk {} symbols: {:?}", i, symbols);
            }
            
            let conn_id = format!("OKX-{}", i + 1);
            let pair = Arc::new(Mutex::new(OkxConnectionPair::new(
                conn_id.clone(),
                symbols.clone(),
                self.symbol_to_pair_cache.clone(),
                self.tx.clone(),
            )));
            self.connection_pairs.push(pair);

            // Register in global connection metadata cache
            crate::hub::CONNECTION_METADATA_CACHE.insert(
                (Source::Okx, conn_id),
                crate::hub::ConnectionMetadata {
                    symbols: symbols.iter().cloned().collect(),
                    is_connected: false,
                    last_updated: crate::clock_sync::get_corrected_timestamp_ms(),
                }
            );
        }
        
        // Start all connections with delays
        info!("Starting {} OKX connection pairs", self.connection_pairs.len());
        for (i, pair) in self.connection_pairs.iter().enumerate() {
            let mut conn = pair.lock().await;
            if let Err(e) = conn.start().await {
                error!("Failed to start OKX connection pair {}: {}", i, e);
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
        if OKX_EXCHANGE_INFO.is_empty() {
            Self::fetch_and_cache_exchange_info().await?;
        }
        
        let all_symbols: Vec<String> = OKX_EXCHANGE_INFO.iter()
            .map(|entry| entry.key().clone())
            .collect();
            
        Ok(all_symbols)
    }
    
    /// Select top symbols by volume
    async fn select_top_symbols() -> Result<(Vec<String>, HashSet<String>)> {
        // Priority tickers
        let priority_tickers = vec![
            "BTC-USDT", "ETH-USDT", "SOL-USDT", "BNB-USDT", "XRP-USDT", 
            "DOGE-USDT", "ADA-USDT", "SUI-USDT", "AVAX-USDT", "LINK-USDT"
        ];

        // Ensure exchange info is cached
        if OKX_EXCHANGE_INFO.is_empty() {
            Self::fetch_and_cache_exchange_info().await?;
        }
        
        // Use ALL cached symbols - they're already filtered by allowed quotes
        let all_symbols: Vec<String> = OKX_EXCHANGE_INFO.iter()
            .map(|entry| entry.key().clone())
            .collect();
        
        info!("📊 Using {} symbols from OKX exchange info", all_symbols.len());

        // Get 24hr volume data to sort by volume
        let volume_fetcher = crate::volume_cache::VolumeFetcher::global();
        let (volumes, _, _, _, _) = volume_fetcher.fetch_okx_volumes().await?;

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
            "okx-connection-monitor",
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
        info!("🔄 Fetching fresh OKX exchange info");
        
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()?;
            
        let response = client
            .get("https://www.okx.com/api/v5/public/instruments")
            .query(&[("instType", "SPOT")])
            .send()
            .await?;
            
        #[derive(Debug, Deserialize)]
        struct InstrumentsResponse {
            code: String,
            msg: String,
            data: Vec<InstrumentInfo>,
        }
        
        #[derive(Debug, Deserialize)]
        struct InstrumentInfo {
            #[serde(rename = "instId")]
            inst_id: String,
            state: String,
            #[serde(rename = "baseCcy")]
            base_ccy: String,
            #[serde(rename = "quoteCcy")]
            quote_ccy: String,
        }
        
        let info: InstrumentsResponse = response.json().await?;
        
        if info.code != "0" {
            return Err(anyhow!("OKX API error: {}", info.msg));
        }
        
        let allowed_quotes = get_allowed_quotes(Source::Okx);
        let is_initial_fetch = OKX_EXCHANGE_INFO.is_empty();
        
        if is_initial_fetch {
            info!("📝 Allowed quote currencies for OKX: {:?}", allowed_quotes);
        }
        
        // Cache ALL trading symbols with allowed quotes
        let mut new_symbols = 0;
        let mut total_count = 0;
        let mut total_symbols = 0;
        
        for instrument in info.data {
            total_symbols += 1;
            
            if instrument.state == "live" {
                let pair = Pair {
                    base: instrument.base_ccy,
                    quote: instrument.quote_ccy,
                };
                
                // Include if quote is in our allowed list
                if allowed_quotes.contains(&pair.quote.as_str()) {
                    total_count += 1;
                    
                    // Only insert if not already present (additive only)
                    if !OKX_EXCHANGE_INFO.contains_key(&instrument.inst_id) {
                        OKX_EXCHANGE_INFO.insert(instrument.inst_id.clone(), pair);
                        new_symbols += 1;
                        
                        // Log new discoveries after initial fetch
                        if !is_initial_fetch {
                            info!("🆕 Discovered new OKX listing: {}", instrument.inst_id);
                        }
                    }
                }
            }
        }
        
        if is_initial_fetch {
            info!("📊 Processed {} total symbols from OKX", total_symbols);
            info!("✅ Cached {} trading pairs with allowed quotes", new_symbols);
        } else if new_symbols > 0 {
            info!("✅ Added {} new OKX symbols to cache (total: {})", new_symbols, total_count);
        } else {
            debug!("No new OKX symbols discovered (total: {})", total_count);
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

            // Get all cached OKX pairs for lookup
            let all_pairs = get_cached_pairs().unwrap_or_default();

            for symbol_str in &conn_pair.symbols {
                // Find the corresponding Pair for this symbol
                if let Some(pair) = all_pairs.iter().find(|p| p.as_okx_str() == *symbol_str) {
                    if hub.is_websocket_active(Source::Okx, pair) {
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
                exchange: "okx".to_string(),
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


impl TickerStream for OkxStream {
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
    // The ticker.symbol is already in "BTC/USDT" format from OkxConnection
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
        hub.update(Source::Okx, pair.clone(), tick).await;
        
    } else {
        warn!("❌ Failed to parse ticker symbol: {}", ticker.symbol);
    }

    Ok(())
}

impl Stream for OkxStream {
    type Item = Result<Ticker>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}