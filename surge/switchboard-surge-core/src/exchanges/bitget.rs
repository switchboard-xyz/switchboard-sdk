use crate::{
    exchange_config::get_allowed_quotes, 
    exchanges::connection_state::ConnectionState, 
    pair::Pair, 
    traits::TickerStream, 
    types::{Source, Ticker}
};
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
use std::collections::{HashSet, HashMap};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU32, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::{mpsc, Mutex, RwLock};
use tracing::{debug, error, info, warn};

// Global cache for Bitget exchange info
static BITGET_EXCHANGE_INFO: Lazy<Arc<DashMap<String, Pair>>> = Lazy::new(|| Arc::new(DashMap::new()));

// Configuration constants (Conservative with 3x safety margins)
const TOP_SYMBOLS_COUNT: usize = 120;  // Reduce to 120 for stability (3 connections * 40 symbols)
const SYMBOLS_PER_CONNECTION: usize = 40;  // Well under 50 recommended limit
const MAX_CONNECTIONS: usize = 3;  // Reduced from 5 to 3 for better stability
const BITGET_WS_ENDPOINT: &str = "wss://ws.bitget.com/v2/ws/public";

// Subscription limits with 3x safety margin
const SUBSCRIPTION_BATCH_SIZE: usize = 20;  // Small batches (3x safety from 10 msg/sec limit)
const SUBSCRIPTION_BATCH_DELAY_MS: u64 = 2000;  // 2 seconds between batches
const MAX_SUBSCRIPTIONS_PER_HOUR: u32 = 80;  // 3x safety from 240/hour limit

// Health monitoring constants (same as Binance)
const MIN_HEALTHY_MESSAGE_RATE: f64 = 1.0;  // Messages per second
const MAX_HEALTHY_LATENCY_MS: u64 = 15000;  // 15 seconds
const STARTUP_GRACE_PERIOD_MS: u64 = 60000;  // 60 seconds grace period
const HEALTH_CHECK_INTERVAL_SECS: u64 = 5;
const RECONNECT_INTERVAL_SECS: u64 = 30;  // 30 seconds between reconnect attempts

// Ping interval (30 seconds as per Bitget requirements)
const PING_INTERVAL_SECS: u64 = 30;

type BitgetWebSocket = FragmentCollector<TokioIo<Upgraded>>;

/// Get all cached Bitget pairs
pub fn get_cached_pairs() -> Result<Vec<Pair>> {
    if BITGET_EXCHANGE_INFO.is_empty() {
        return Err(anyhow!("Exchange info not fetched yet"));
    }
    
    let pairs: Vec<Pair> = BITGET_EXCHANGE_INFO
        .iter()
        .map(|entry| entry.value().clone())
        .collect();
    
    Ok(pairs)
}

// Bitget WebSocket message structures
#[derive(Debug, Serialize)]
struct BitgetSubscribe {
    op: String,  // "subscribe"
    args: Vec<BitgetSubscribeArg>,
}

#[derive(Debug, Serialize)]
struct BitgetSubscribeArg {
    #[serde(rename = "instType")]
    inst_type: String,  // "SPOT" for spot
    channel: String,    // "ticker"
    #[serde(rename = "instId")]
    inst_id: String,    // e.g., "BTCUSDT"
}

#[derive(Debug, Serialize)]
struct BitgetPing {
    op: String,  // "ping"
}

#[derive(Debug, Deserialize)]
struct BitgetMessage {
    arg: Option<BitgetArg>,
    action: Option<String>,
    data: Option<serde_json::Value>,
    event: Option<String>,
    code: Option<String>,
    msg: Option<String>,
    _ts: Option<serde_json::Value>,  // Changed to handle both string and numeric values
}

#[derive(Debug, Deserialize, Clone)]
struct BitgetArg {
    #[serde(rename = "instType")]
    inst_type: String,
    channel: String,
    #[serde(rename = "instId")]
    inst_id: String,
}

#[derive(Debug, Deserialize)]
struct BitgetTickerData {
    #[serde(rename = "instId")]
    inst_id: String,
    last: Option<String>,
    #[serde(rename = "lastPr")]
    last_pr: Option<String>,
    #[serde(rename = "askPr")]
    ask_pr: String,
    #[serde(rename = "bidPr")]
    bid_pr: String,
    #[serde(rename = "askSz")]
    ask_sz: String,
    #[serde(rename = "bidSz")]
    bid_sz: String,
    open24h: String,
    high24h: String,
    low24h: String,
    #[serde(rename = "change24h")]
    change_24h: String,
    #[serde(rename = "baseVolume")]
    base_volume: String,
    #[serde(rename = "quoteVolume")]
    quote_volume: String,
    #[serde(rename = "openUtc")]
    open_utc: String,
    ts: String,
}

/// A single WebSocket connection to Bitget
pub struct BitgetConnection {
    id: String,
    ws: Option<Arc<Mutex<BitgetWebSocket>>>,
    symbols: HashSet<String>,
    symbol_to_pair: Arc<DashMap<String, Pair>>,
    tx: mpsc::UnboundedSender<Vec<Ticker>>,
    is_connected: Arc<AtomicBool>,
    last_message_time: Arc<AtomicU64>,
    subscribed_symbols: Arc<RwLock<HashSet<String>>>,
    
    // Message rate tracking
    message_count: Arc<AtomicU64>,
    last_rate_log_time: Arc<AtomicU64>,
    last_rate_value: Arc<AtomicU32>,  // Store rate as fixed point (x100)
    
    // Track when subscription completed for startup grace period
    subscription_completed_time: Arc<AtomicU64>,
    
    // Rate limiting for subscriptions
    subscription_count: Arc<AtomicU32>,
    last_subscription_reset: Arc<AtomicU64>,
    
    // Track symbol confirmations
    symbol_confirmations: Arc<DashMap<String, u32>>,
    confirmation_complete: Arc<AtomicBool>,
    
    // Track if read loop is running to prevent duplicates
    read_loop_running: Arc<AtomicBool>,
    // Handle to ping task for cleanup
    ping_task_handle: Option<tokio::task::JoinHandle<()>>,
    // Notify monitor to check immediately
    monitor_trigger: Arc<tokio::sync::Notify>,
}

impl BitgetConnection {
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
            subscription_count: Arc::new(AtomicU32::new(0)),
            last_subscription_reset: Arc::new(AtomicU64::new(now)),
            symbol_confirmations: Arc::new(DashMap::new()),
            confirmation_complete: Arc::new(AtomicBool::new(false)),
            read_loop_running: Arc::new(AtomicBool::new(false)),
            ping_task_handle: None,
            monitor_trigger: Arc::new(tokio::sync::Notify::new()),
        }
    }

    pub async fn connect(&mut self) -> Result<()> {
        info!("[{}] Connecting to Bitget WebSocket", self.id);
        
        // Create HTTPS client
        let https = HttpsConnector::new();
        let client = Client::builder(TokioExecutor::new())
            .build::<_, Empty<Bytes>>(https);
        
        // Parse URI
        let uri: Uri = BITGET_WS_ENDPOINT.replace("wss://", "https://").parse()?;
        
        // Create WebSocket upgrade request
        let req = Request::builder()
            .method("GET")
            .uri(&uri)
            .header("Host", "ws.bitget.com")
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
        let mut ws = FragmentCollector::new(ws);
        
        // Read initial connection message if any
        match tokio::time::timeout(Duration::from_secs(2), ws.read_frame()).await {
            Ok(Ok(frame)) => {
                if let OpCode::Text = frame.opcode {
                    if let Ok(text) = String::from_utf8(frame.payload.to_vec()) {
                        info!("[{}] Initial connection message: {}", self.id, text);
                    }
                }
            }
            Ok(Err(e)) => {
                warn!("[{}] Error reading initial frame: {}", self.id, e);
            }
            Err(_) => {
                debug!("[{}] No initial message from server", self.id);
            }
        }
        
        self.is_connected.store(true, Ordering::Release);
        self.last_message_time.store(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            Ordering::Release
        );

        // Update global connection metadata cache to mark as connected
        if let Some(mut entry) = crate::hub::CONNECTION_METADATA_CACHE.get_mut(&(crate::types::Source::Bitget, self.id.clone())) {
            entry.is_connected = true;
            entry.last_updated = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
        }
        
        let ws = Arc::new(Mutex::new(ws));
        self.ws = Some(ws.clone());
        
        // Don't start ping task here - it will be started after read loop starts
        
        info!("[{}] Connected successfully", self.id);
        Ok(())
    }

    fn start_ping_task(&mut self) {
        if let Some(ws) = &self.ws {
            let ws = ws.clone();
            let connection_id = self.id.clone();
            let is_connected = self.is_connected.clone();
            
            let handle = crate::runtime_separation::spawn_on_ingestion_named(
                &format!("bitget-ping-{}", connection_id),
                async move {
                    // Wait a bit before starting ping to ensure connection is stable
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    
                    let mut interval = tokio::time::interval(Duration::from_secs(PING_INTERVAL_SECS));
                    
                    loop {
                        interval.tick().await;
                        
                        // Check if still connected
                        if !is_connected.load(Ordering::Acquire) {
                            debug!("[{}] Ping task stopping - connection closed", connection_id);
                            break;
                        }
                        
                        // Bitget expects plain string "ping", not JSON
                        let ping_msg = "ping";
                        
                        let mut ws_guard = ws.lock().await;
                        if let Err(e) = ws_guard.write_frame(Frame::text(ping_msg.as_bytes().into())).await {
                            error!("[{}] Failed to send ping: {}", connection_id, e);
                            // Mark connection as disconnected
                            is_connected.store(false, Ordering::Release);
                            break;
                        }
                        drop(ws_guard);
                        debug!("[{}] Sent ping string to Bitget", connection_id);
                    }
                    
                    debug!("[{}] Ping task ended", connection_id);
                }
            );
            
            self.ping_task_handle = Some(handle);
        }
    }

    fn can_subscribe(&self, count: usize) -> bool {
        let now = crate::clock_sync::get_corrected_timestamp_ms();
        let hour_ago = now - 3600000;
        
        // Reset counter if hour passed
        if self.last_subscription_reset.load(Ordering::Relaxed) < hour_ago {
            self.subscription_count.store(0, Ordering::Relaxed);
            self.last_subscription_reset.store(now, Ordering::Relaxed);
        }
        
        // Check if under limit (80 of 240 = 3x safety)
        let current = self.subscription_count.load(Ordering::Relaxed);
        current + count as u32 <= MAX_SUBSCRIPTIONS_PER_HOUR
    }

    pub async fn subscribe_gradually(&mut self) -> Result<()> {
        debug!("[{}] Starting gradual subscription process", self.id);
        let ws = self.ws.as_ref().ok_or_else(|| anyhow!("Not connected"))?;
        
        // Get what's already confirmed as working in websocket_active_pairs
        let hub = crate::hub::SurgeHub::global();
        let already_active = hub.get_websocket_pairs(Source::Bitget);
        let active_symbols: HashSet<String> = already_active.iter()
            .map(|pair| pair.as_bitget_str())
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
                // Check subscription rate limit
                if !self.can_subscribe(chunk.len()) {
                    warn!("[{}] Rate limit reached, waiting 60 seconds...", self.id);
                    tokio::time::sleep(Duration::from_secs(60)).await;
                }
                
                let mut args = Vec::new();
                
                for symbol in chunk {
                    if !subscribed.contains(symbol) {
                        args.push(BitgetSubscribeArg {
                            inst_type: "SPOT".to_string(),  // Spot
                            channel: "ticker".to_string(),
                            inst_id: symbol.clone(),
                        });
                        subscribed.insert(symbol.clone());
                    }
                }
                
                if !args.is_empty() {
                    let subscribe_msg = BitgetSubscribe {
                        op: "subscribe".to_string(),
                        args,
                    };
                    
                    if let Ok(msg) = serde_json::to_string(&subscribe_msg) {
                        debug!("[{}] Sending subscription message: {}", self.id, msg);
                        let mut ws_guard = ws.lock().await;
                        ws_guard.write_frame(Frame::text(msg.as_bytes().into())).await?;
                        drop(ws_guard);
                        
                        // Update subscription count
                        self.subscription_count.fetch_add(chunk.len() as u32, Ordering::Relaxed);

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
                        .find(|pair| pair.as_bitget_str() == *symbol)
                        .cloned()
                })
                .collect();
            
            if !pairs_to_register.is_empty() {
                hub.register_websocket_pairs(Source::Bitget, &pairs_to_register);
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
        self.confirmation_complete.store(false, Ordering::Release);
        
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
                self.confirmation_complete.store(true, Ordering::Release);
                break;
            }
            
            // Also break if confirmation was marked complete by read_loop
            if self.confirmation_complete.load(Ordering::Acquire) {
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
        info!("[{}] Cleaning up connection", self.id);

        // IMPORTANT: Remove our symbols from websocket_active so REST picks them up
        let hub = crate::hub::SurgeHub::global();
        let all_pairs = get_cached_pairs().unwrap_or_default();

        let pairs_to_remove: Vec<Pair> = self.symbols.iter()
            .filter_map(|symbol| {
                all_pairs.iter()
                    .find(|p| p.as_bitget_str() == *symbol)
                    .cloned()
            })
            .collect();

        if !pairs_to_remove.is_empty() {
            hub.remove_websocket_pairs(Source::Bitget, &pairs_to_remove);
            info!("[{}] 🔄 Removed {} symbols from WebSocket tracking → REST will handle",
                self.id, pairs_to_remove.len());
        }

        // Mark as disconnected
        self.is_connected.store(false, Ordering::Release);

        // Update cache to mark as disconnected
        if let Some(mut entry) = crate::hub::CONNECTION_METADATA_CACHE.get_mut(&(crate::types::Source::Bitget, self.id.clone())) {
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
        self.confirmation_complete.store(false, Ordering::Release);
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
    
    pub fn get_message_rate(&self) -> f64 {
        let rate_fixed = self.last_rate_value.load(Ordering::Acquire);
        rate_fixed as f64 / 100.0
    }
    
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
            let symbol_confirmations = self.symbol_confirmations.clone();
            let read_loop_running = self.read_loop_running.clone();
            let monitor_trigger = self.monitor_trigger.clone();
            
            // Start ping task after read loop is set up
            self.start_ping_task();
            
            crate::runtime_separation::spawn_on_ingestion_named(
                &format!("bitget-reader-{}", connection_id),
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
                        error!("[{}] Bitget read loop error: {}", connection_id, e);
                        is_connected.store(false, Ordering::Release);

                        // Update cache to mark as disconnected
                        if let Some(mut entry) = crate::hub::CONNECTION_METADATA_CACHE.get_mut(&(crate::types::Source::Bitget, connection_id.clone())) {
                            entry.is_connected = false;
                        }

                        // Notify monitor to check immediately
                        monitor_trigger.notify_one();
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
        ws: Arc<Mutex<BitgetWebSocket>>,
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
                        error!("[{}] Failed to read frame: {} (error details: {:?})", connection_id, e, e);
                        // Log additional context
                        let now = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_millis() as u64;
                        let last_msg = last_message_time.load(Ordering::Acquire);
                        let time_since_last = now - last_msg;
                        error!("[{}] Time since last message: {}ms", connection_id, time_since_last);
                        is_connected.store(false, Ordering::Release);

                        // Update cache to mark as disconnected
                        if let Some(mut entry) = crate::hub::CONNECTION_METADATA_CACHE.get_mut(&(crate::types::Source::Bitget, connection_id.clone())) {
                            entry.is_connected = false;
                        }

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
                    
                    // Check if it's a pong response (plain string "pong")
                    if text == "pong" {
                        debug!("[{}] Received pong from Bitget", connection_id);
                        continue;
                    }
                    
                    // Debug log first few messages
                    use std::sync::atomic::AtomicU64;
                    static MSG_COUNT: AtomicU64 = AtomicU64::new(0);
                    let count = MSG_COUNT.fetch_add(1, Ordering::Relaxed);
                    if count < 10 {
                        debug!("[{}] Received message {}: {}", connection_id, count, text);
                    }
                    
                    // Try to parse as Bitget message
                    if let Ok(msg) = serde_json::from_str::<BitgetMessage>(&text) {
                        // Debug: log what fields are present
                        if count < 20 {
                            debug!("[{}] Message has: action={:?}, event={:?}, arg={:?}, data={:?}", 
                                connection_id, 
                                msg.action.is_some(), 
                                msg.event.is_some(),
                                msg.arg.is_some(),
                                msg.data.is_some()
                            );
                        }
                        
                        // Check if this is a ticker data message (has action field like "snapshot" or "update")
                        if let Some(ref action) = msg.action {
                            debug!("[{}] Found action field: {}", connection_id, action);
                            if (action == "snapshot" || action == "update") && msg.arg.is_some() && msg.data.is_some() {
                                // This is ticker data, process it
                                if let (Some(arg), Some(data)) = (msg.arg.clone(), msg.data.clone()) {
                                    if arg.channel == "ticker" {
                                        debug!("[{}] Processing {} ticker data for symbol: {}", connection_id, action, arg.inst_id);
                                        // Increment message count
                                        message_count.fetch_add(1, Ordering::Relaxed);
                                        
                                        // Calculate message rate
                                        let now = std::time::SystemTime::now()
                                            .duration_since(std::time::UNIX_EPOCH)
                                            .unwrap()
                                            .as_millis() as u64;
                                        let last_log = last_rate_log_time.load(Ordering::Acquire);
                                        
                                        if now - last_log >= 10_000 { // 10 seconds
                                            let count = message_count.load(Ordering::Acquire);
                                            let elapsed_ms = now - last_log;
                                            let rate = (count as f64 * 1000.0) / elapsed_ms as f64;
                                            
                                            let rate_fixed = (rate * 100.0) as u32;
                                            last_rate_value.store(rate_fixed, Ordering::Release);

                                            if now - last_log >= 600_000 {
                                                message_count.store(0, Ordering::Release);
                                                if rate > 1.0 || count > 10 {
                                                    info!("[{}] 📊 Raw Message rate: {:.1} msg/sec ({} messages in {:.1}s)", 
                                                        connection_id, rate, count, elapsed_ms as f64 / 1000.0);
                                                }
                                                last_rate_log_time.store(now, Ordering::Release);
                                            }
                                        }
                                        
                                        // Parse ticker data - try array first, then single object
                                        let tickers = if let Ok(ticker_array) = serde_json::from_value::<Vec<BitgetTickerData>>(data.clone()) {
                                            ticker_array
                                        } else if let Ok(single_ticker) = serde_json::from_value::<BitgetTickerData>(data) {
                                            vec![single_ticker]
                                        } else {
                                            debug!("[{}] Failed to parse ticker data", connection_id);
                                            vec![]
                                        };
                                        
                                        for ticker in tickers {
                                            // Debug: Check what symbols are in the confirmation map
                                            if symbol_confirmations.is_empty() {
                                                error!("[{}] symbol_confirmations is EMPTY!", connection_id);
                                            } else {
                                                let tracked_count = symbol_confirmations.len();
                                                debug!("[{}] Tracking {} symbols in confirmation map", connection_id, tracked_count);
                                            }
                                            
                                            // Update confirmation count for this symbol
                                            if let Some(mut count) = symbol_confirmations.get_mut(&ticker.inst_id) {
                                                *count += 1;
                                                if *count == 1 {
                                                    debug!("[{}] First ticker update for symbol: {}", connection_id, ticker.inst_id);
                                                } else if *count == 3 {
                                                    debug!("[{}] Symbol {} confirmed with 3 ticker updates ✓", connection_id, ticker.inst_id);
                                                }
                                            } else {
                                                // Log if we receive a ticker for a symbol we're not tracking
                                                warn!("[{}] Received ticker for untracked symbol: {} (not in confirmation list)", connection_id, ticker.inst_id);
                                                // List what we ARE tracking
                                                let tracked: Vec<String> = symbol_confirmations.iter()
                                                    .take(5)
                                                    .map(|entry| entry.key().clone())
                                                    .collect();
                                                warn!("[{}] We are tracking symbols like: {:?}", connection_id, tracked);
                                            }
                                            
                                            // Look up the pair from our cache
                                            if let Some(pair) = symbol_to_pair.get(&ticker.inst_id) {
                                                // Get the last price (try both fields)
                                                let price_str = ticker.last.or(ticker.last_pr);
                                                
                                                if let Some(price_str) = price_str {
                                                    if let Ok(price) = price_str.parse::<Decimal>() {
                                                        if let Ok(timestamp) = ticker.ts.parse::<u64>() {
                                                            let tick = Ticker {
                                                                symbol: pair.as_str(),
                                                                price,
                                                                timestamp,
                                                            };
                                                            
                                                            if tx.send(vec![tick]).is_err() {
                                                                error!("[{}] Failed to send ticker to channel", connection_id);
                                                            }
                                                        }
                                                    }
                                                }
                                            } else {
                                                debug!("[{}] Symbol {} not found in cache", connection_id, ticker.inst_id);
                                            }
                                        }
                                    }
                                }
                            }
                        } else if let Some(ref event) = msg.event {
                            // Handle event messages (subscribe confirmations, errors, etc.)
                            if event == "pong" {
                                debug!("[{}] Received pong", connection_id);
                            } else if event == "subscribe" {
                                // Handle subscription confirmation
                                if let Some(ref code) = msg.code {
                                    if code == "00000" {
                                        info!("[{}] Subscription confirmed", connection_id);
                                    } else {
                                        error!("[{}] Subscription failed: {} - {}", connection_id, code, msg.msg.as_ref().unwrap_or(&"Unknown error".to_string()));
                                    }
                                }
                            } else if event == "error" {
                                error!("[{}] Bitget error event: {} - {}", connection_id, 
                                    msg.code.as_ref().unwrap_or(&"Unknown".to_string()),
                                    msg.msg.as_ref().unwrap_or(&"Unknown error".to_string()));
                            }
                        }
                    }
                }
                OpCode::Close => {
                    // Extract close code and reason if available
                    let payload = frame.payload.to_vec();
                    if payload.len() >= 2 {
                        let code = u16::from_be_bytes([payload[0], payload[1]]);
                        let reason = if payload.len() > 2 {
                            String::from_utf8_lossy(&payload[2..]).to_string()
                        } else {
                            "No reason provided".to_string()
                        };
                        error!("[{}] WebSocket closed with code {} and reason: {}", connection_id, code, reason);
                    } else {
                        error!("[{}] WebSocket closed with no close code", connection_id);
                    }
                    is_connected.store(false, Ordering::Release);

                    // Update cache to mark as disconnected
                    if let Some(mut entry) = crate::hub::CONNECTION_METADATA_CACHE.get_mut(&(crate::types::Source::Bitget, connection_id.clone())) {
                        entry.is_connected = false;
                    }

                    // Notify monitor immediately for instant failover
                    monitor_trigger.notify_one();

                    break;
                }
                OpCode::Ping => {
                    debug!("[{}] Received WebSocket Ping frame, sending Pong", connection_id);
                    let mut ws_guard = ws.lock().await;
                    ws_guard.write_frame(Frame::pong(frame.payload)).await?;
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
    pub connection: Arc<RwLock<BitgetConnection>>,
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
        let connection = BitgetConnection::new(
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
            // Note: read_loop is already started in subscribe_gradually -> confirm_prices
        }
        
        // Start monitoring task
        let connection = self.connection.clone();
        let connection_id = self.id.clone();
        let symbols = self.symbols.clone();
        
        let monitoring_task = crate::runtime_separation::spawn_on_ingestion_named(
            &format!("bitget-monitor-{}", connection_id),
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
        connection: Arc<RwLock<BitgetConnection>>,
        symbols: HashSet<String>,
    ) {
        let mut check_interval = tokio::time::interval(Duration::from_secs(HEALTH_CHECK_INTERVAL_SECS));
        let mut symbols_in_rest_failover = false;
        let mut last_reconnect_attempt: Option<std::time::Instant> = Some(std::time::Instant::now());
        let mut reconnect_delay_secs = RECONNECT_INTERVAL_SECS; // Start with 30 seconds
        
        // Get the monitor trigger for immediate notifications
        let monitor_trigger = {
            let conn = connection.read().await;
            conn.monitor_trigger.clone()
        };

        loop {
            // Wait for either regular interval or immediate trigger
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
            drop(conn);
            
            // Check health conditions (skip during grace period)
            if is_connected && !in_grace_period {
                let unhealthy = 
                    latency_ms > MAX_HEALTHY_LATENCY_MS || 
                    message_rate < MIN_HEALTHY_MESSAGE_RATE;
                
                if unhealthy && !symbols_in_rest_failover {
                    warn!("[{}] 🚨 Connection unhealthy! Latency: {}ms, Rate: {:.1} msg/sec. Moving {} symbols to REST API", 
                        connection_id, latency_ms, message_rate, symbols.len());
                    
                    // Clear these symbols from WebSocket tracking
                    let all_pairs = get_cached_pairs().unwrap_or_default();
                    let pairs_to_clear: Vec<Pair> = symbols.iter()
                        .filter_map(|symbol| {
                            all_pairs.iter()
                                .find(|pair| pair.as_bitget_str() == *symbol)
                                .cloned()
                        })
                        .collect();
                    
                    if !pairs_to_clear.is_empty() {
                        let hub = crate::hub::SurgeHub::global();
                        hub.remove_websocket_pairs(Source::Bitget, &pairs_to_clear);
                        info!("[{}] 📡 Removed {} symbols from WebSocket tracking for REST fallback", 
                            connection_id, pairs_to_clear.len());
                    }
                    
                    symbols_in_rest_failover = true;
                }
            } else if !is_connected && !symbols_in_rest_failover {
                error!("[{}] 🚨 Connection DOWN! Moving {} symbols to REST API", 
                    connection_id, symbols.len());
                
                // Clear these symbols from WebSocket tracking
                let all_pairs = get_cached_pairs().unwrap_or_default();
                let pairs_to_clear: Vec<Pair> = symbols.iter()
                    .filter_map(|symbol| {
                        all_pairs.iter()
                            .find(|pair| pair.as_bitget_str() == *symbol)
                            .cloned()
                    })
                    .collect();
                
                if !pairs_to_clear.is_empty() {
                    let hub = crate::hub::SurgeHub::global();
                    hub.remove_websocket_pairs(Source::Bitget, &pairs_to_clear);
                    info!("[{}] 📡 Removed {} symbols from WebSocket tracking for REST fallback", 
                        connection_id, pairs_to_clear.len());
                }
                
                symbols_in_rest_failover = true;
                last_reconnect_attempt = Some(std::time::Instant::now());
            }
            
            // Attempt reconnection if in failover mode
            if symbols_in_rest_failover {
                let should_reconnect = match last_reconnect_attempt {
                    None => true,
                    Some(last_attempt) => last_attempt.elapsed().as_secs() >= reconnect_delay_secs,
                };
                
                if should_reconnect {
                    info!("[{}] 🔄 Attempting to reconnect WebSocket connection (delay: {}s)...", 
                        connection_id, reconnect_delay_secs);
                    last_reconnect_attempt = Some(std::time::Instant::now());
                    
                    let mut conn = connection.write().await;
                    
                    // Clean up the old connection first
                    conn.cleanup().await;
                    
                    // Wait a bit after cleanup
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    
                    conn.symbols = symbols.clone();
                    
                    match conn.connect().await {
                        Ok(_) => {
                            info!("[{}] ✅ WebSocket reconnected successfully", connection_id);
                            
                            match conn.subscribe_gradually().await {
                                Ok(_) => {
                                    info!("[{}] ✅ Successfully re-subscribed to symbols after reconnection", connection_id);
                                    // Note: read_loop is already started in subscribe_gradually
                                    symbols_in_rest_failover = false;
                                    reconnect_delay_secs = RECONNECT_INTERVAL_SECS; // Reset delay on success
                                }
                                Err(e) => {
                                    // subscribe_gradually only returns Err when 0 symbols work
                                    error!("[{}] ❌ Failed to re-subscribe after reconnection: {}", connection_id, e);
                                    // No symbols working - cleanup and retry fresh next time
                                    conn.cleanup().await;
                                    // Increase delay with exponential backoff (max 5 minutes)
                                    reconnect_delay_secs = (reconnect_delay_secs * 2).min(300);
                                    // Will retry reconnection after delay
                                }
                            }
                        }
                        Err(e) => {
                            warn!("[{}] ❌ Failed to reconnect WebSocket: {}", connection_id, e);
                            // Increase delay with exponential backoff (max 5 minutes)
                            reconnect_delay_secs = (reconnect_delay_secs * 2).min(300);
                        }
                    }
                }
            }
        }
    }
}

pub struct BitgetStream {
    connections: Vec<Arc<Mutex<ConnectionPair>>>,
    rx: mpsc::UnboundedReceiver<Result<Ticker>>,
    tx: mpsc::UnboundedSender<Vec<Ticker>>,
    symbol_to_pair_cache: Arc<DashMap<String, Pair>>,
    connection_state: ConnectionState,
}

impl BitgetStream {
    pub async fn new() -> Result<Self> {
        let (tx, rx) = mpsc::unbounded_channel();
        
        // Create internal tx/rx for aggregating from multiple connections
        let (internal_tx, mut internal_rx) = mpsc::unbounded_channel::<Vec<Ticker>>();
        
        // Spawn aggregator task
        let external_tx = tx.clone();
        let symbol_to_pair_cache = BITGET_EXCHANGE_INFO.clone();
        let symbol_to_pair_cache_for_spawn = symbol_to_pair_cache.clone();

        crate::runtime_separation::spawn_on_ingestion_named("bitget-aggregator", async move {
            while let Some(tickers) = internal_rx.recv().await {
                for ticker in tickers {
                    let _ = external_tx.send(Ok(ticker.clone()));
                    if let Err(e) = process_ticker(&symbol_to_pair_cache_for_spawn, ticker).await {
                        error!("Failed to process ticker: {:?}", e);
                    }
                }
            }
        });
        
        let mut stream = Self {
            connections: Vec::new(),
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
        if BITGET_EXCHANGE_INFO.is_empty() {
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
        info!("Creating Bitget WebSocket connections");
        
        // Get top symbols by volume AND all symbols
        let (websocket_symbols, _) = Self::select_top_symbols().await?;
        
        // Initially, REST should poll ALL symbols
        let all_symbols = Self::get_all_symbols().await?;
        let all_symbols_set: HashSet<String> = all_symbols.into_iter().collect();
        
        info!("📊 Bitget startup: REST polling {} symbols initially, WebSocket will handle top {} high-volume symbols", 
            all_symbols_set.len(), websocket_symbols.len());
        
        // Set up REST polling for ALL symbols initially
        let rest_manager = crate::rest_manager::RestManager::global();
        rest_manager.set_rest_symbols(Source::Bitget, all_symbols_set).await;
        
        // Create connections for top 200 symbols (40 per connection, max 5 connections)
        let connection_count = (websocket_symbols.len() + SYMBOLS_PER_CONNECTION - 1) / SYMBOLS_PER_CONNECTION;
        let connection_count = connection_count.min(MAX_CONNECTIONS);
        
        for i in 0..connection_count {
            let start = i * SYMBOLS_PER_CONNECTION;
            let end = ((i + 1) * SYMBOLS_PER_CONNECTION).min(websocket_symbols.len());
            let connection_symbols: HashSet<String> = websocket_symbols[start..end].iter().cloned().collect();
            let conn_id = format!("Bitget-{}", i + 1);

            let pair = Arc::new(Mutex::new(ConnectionPair::new(
                conn_id.clone(),
                connection_symbols.clone(),
                self.symbol_to_pair_cache.clone(),
                self.tx.clone(),
            )));
            self.connections.push(pair);

            // Register in global connection metadata cache
            crate::hub::CONNECTION_METADATA_CACHE.insert(
                (Source::Bitget, conn_id.clone()),
                crate::hub::ConnectionMetadata {
                    symbols: connection_symbols.iter().cloned().collect(),
                    is_connected: false,
                    last_updated: crate::clock_sync::get_corrected_timestamp_ms(),
                }
            );

            info!("Created Bitget-{} connection with {} symbols", i + 1, connection_symbols.len());
        }
        
        // Start all connections
        for connection_pair in &self.connections {
            let mut conn = connection_pair.lock().await;
            if let Err(e) = conn.start().await {
                error!("Failed to start connection {}: {}", conn.id, e);
            }
        }
        
        // Start connection monitor
        self.start_connection_monitor();
        
        info!("✅ Bitget WebSocket connections established with {} connections", self.connections.len());
        Ok(())
    }
    
    async fn select_top_symbols() -> Result<(Vec<String>, HashSet<String>)> {
        // Priority tickers - same as Binance/Bybit for consistency, plus SWTCH
        let priority_tickers = vec![
            "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
            "DOGEUSDT", "ADAUSDT", "SUIUSDT", "AVAXUSDT", "LINKUSDT",
            "BTCUSDC", "ETHUSDC", "SOLUSDC", "USDCUSDT",
            "SWTCHUSDT" // SWTCH priority for Bitget
        ];
        
        // Get all symbols
        let all_symbols = Self::get_all_symbols().await?;
        
        // Fetch volumes - match other exchanges by propagating errors
        let volume_fetcher = crate::volume_cache::VolumeFetcher::global();
        let (volumes, _, _, _, _) = volume_fetcher.fetch_bitget_volumes().await?;
        
        // Collect priority symbols that exist on Bitget
        let mut websocket_symbols: Vec<String> = Vec::new();
        let mut used_symbols = HashSet::new();
        
        // Add priority tickers first
        for ticker in &priority_tickers {
            if all_symbols.contains(*ticker) {
                websocket_symbols.push(ticker.to_string());
                used_symbols.insert(ticker.to_string());
            }
        }
        
        let priority_count = websocket_symbols.len();
        info!("Added {} priority tickers to WebSocket", priority_count);
        
        // Sort remaining symbols by volume
        let mut symbol_volumes: Vec<(String, f64)> = all_symbols
            .iter()
            .filter(|s| !used_symbols.contains(*s))  // Exclude priority symbols
            .filter_map(|s| volumes.get(s).map(|v| (s.clone(), *v)))
            .collect();
        
        symbol_volumes.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        
        // Add top volume symbols up to limit
        let remaining_slots = TOP_SYMBOLS_COUNT.saturating_sub(priority_count);
        for (symbol, _volume) in symbol_volumes.iter().take(remaining_slots) {
            websocket_symbols.push(symbol.clone());
            used_symbols.insert(symbol.clone());
        }
        
        // Everything else stays in REST
        let rest_symbols: HashSet<String> = all_symbols
            .into_iter()
            .filter(|s| !used_symbols.contains(s))
            .collect();
        
        info!("Selected {} symbols for WebSocket ({} priority + {} by volume), {} for REST API", 
            websocket_symbols.len(), priority_count, websocket_symbols.len() - priority_count, rest_symbols.len());

        Ok((websocket_symbols, rest_symbols))
    }
    
    async fn fetch_and_cache_exchange_info() -> Result<usize> {
        info!("🔄 Fetching fresh Bitget exchange info");
        
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()?;
            
        let response = client
            .get("https://api.bitget.com/api/v2/spot/public/symbols")
            .send()
            .await?;
            
        #[derive(Debug, Deserialize)]
        struct SymbolsResponse {
            code: String,
            msg: String,
            data: Vec<SymbolInfo>,
        }
        
        #[derive(Debug, Deserialize)]
        struct SymbolInfo {
            symbol: String,
            #[serde(rename = "baseCoin")]
            base_coin: String,
            #[serde(rename = "quoteCoin")]
            quote_coin: String,
            status: String,
        }
        
        let info: SymbolsResponse = response.json().await?;
        
        if info.code != "00000" {
            return Err(anyhow!("Bitget API error: {} - {}", info.code, info.msg));
        }
        
        let allowed_quotes = get_allowed_quotes(Source::Bitget);
        let is_initial_fetch = BITGET_EXCHANGE_INFO.is_empty();
        
        if is_initial_fetch {
            info!("📝 Allowed quote currencies: {:?}", allowed_quotes);
        }
        
        let mut new_symbols = 0;
        let mut total_count = 0;
        
        for symbol_info in info.data {
            if symbol_info.status == "online" {
                let pair = Pair {
                    base: symbol_info.base_coin,
                    quote: symbol_info.quote_coin,
                };
                
                // Include if quote is in our allowed list
                if allowed_quotes.contains(&pair.quote.as_str()) {
                    total_count += 1;
                    
                    // Only insert if not already present (additive only)
                    if !BITGET_EXCHANGE_INFO.contains_key(&symbol_info.symbol) {
                        BITGET_EXCHANGE_INFO.insert(symbol_info.symbol.clone(), pair);
                        new_symbols += 1;
                        
                        // Log new discoveries after initial fetch
                        if !is_initial_fetch {
                            info!("🆕 Discovered new Bitget listing: {}", symbol_info.symbol);
                        }
                    }
                }
            }
        }
        
        if is_initial_fetch {
            info!("✅ Cached {} trading pairs with allowed quotes", new_symbols);
        } else if new_symbols > 0 {
            info!("✅ Added {} new Bitget symbols to cache (total: {})", new_symbols, total_count);
        } else {
            debug!("No new Bitget symbols discovered (total: {})", total_count);
        }
        
        Ok(new_symbols)
    }
    
    async fn get_all_symbols() -> Result<HashSet<String>> {
        let pairs = get_cached_pairs()?;
        let symbols: HashSet<String> = pairs
            .iter()
            .map(|pair| pair.as_bitget_str())
            .collect();
        Ok(symbols)
    }
    
    fn start_connection_monitor(&self) {
        let connections = self.connections.clone();
        let connection_state = self.connection_state.clone();
        
        crate::runtime_separation::spawn_on_ingestion_named("bitget-connection-monitor", async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(5));
            
            loop {
                interval.tick().await;
                
                let mut any_connected = false;
                
                for conn_pair in &connections {
                    if let Ok(conn) = conn_pair.try_lock() {
                        if !conn.symbols.is_empty() {
                            any_connected = true;
                            break;
                        }
                    }
                }
                
                connection_state.set_connected(any_connected);
            }
        });
    }
}

async fn process_ticker(_symbol_to_pair_cache: &Arc<DashMap<String, Pair>>, ticker: Ticker) -> Result<()> {
    if let Ok(pair) = Pair::from_task(&ticker.symbol) {
        let now = crate::clock_sync::get_corrected_timestamp_ms();
        let tick = crate::types::Tick {
            price: ticker.price,
            event_ts: ticker.timestamp,
            seen_at: now,
            verified_at: now,
        };
        
        let hub = crate::hub::SurgeHub::global();
        hub.update(Source::Bitget, pair.clone(), tick).await;
    } else {
        warn!("❌ Failed to parse ticker symbol: {}", ticker.symbol);
    }

    Ok(())
}

impl BitgetStream {
    /// Get WebSocket connection health for monitoring
    pub async fn get_websocket_connection_health(&self) -> Vec<crate::types::WebSocketConnectionHealth> {
        let mut health = Vec::new();
        let hub = crate::hub::SurgeHub::global();

        for conn_pair in &self.connections {
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

            // Get all cached Bitget pairs for lookup
            for symbol_str in &conn_pair.symbols {
                // Find the corresponding Pair in the cache
                if let Some(pair_entry) = BITGET_EXCHANGE_INFO.get(symbol_str) {
                    let pair = pair_entry.value();
                    if hub.is_websocket_active(Source::Bitget, pair) {
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
                exchange: "bitget".to_string(),
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


impl TickerStream for BitgetStream {
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
        Vec::new() // Would need to implement proper tracking
    }

    fn is_connected(&self) -> bool {
        self.connection_state.is_connected()
    }
}

impl Stream for BitgetStream {
    type Item = Result<Ticker>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}