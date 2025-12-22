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
use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::{mpsc, Mutex, RwLock};
use tracing::{debug, error, info, warn};

// Global cache for Coinbase exchange info
static COINBASE_EXCHANGE_INFO: Lazy<Arc<DashMap<String, Pair>>> = Lazy::new(|| Arc::new(DashMap::new()));

// Configuration constants
const TOP_SYMBOLS_COUNT: usize = 50; // Reduced to avoid rate limits
const COINBASE_WS_ENDPOINT: &str = "wss://ws-feed.exchange.coinbase.com";
const SUBSCRIPTION_BATCH_SIZE: usize = 5; // Reduced from 10 to be more conservative
const SUBSCRIPTION_BATCH_DELAY_MS: u64 = 1000; // 1 second delay (1 RPS) - much more conservative

// Warmup configuration - gradually increase symbols to avoid rate limits
const WARMUP_ENABLED: bool = true;
const WARMUP_INITIAL_SYMBOLS: usize = 20; // Start with only 20 symbols
const WARMUP_INCREMENT_SYMBOLS: usize = 10; // Add only 10 symbols at a time
const WARMUP_INTERVAL_SECS: u64 = 300; // 5 minutes between increases (was 2 minutes)
const CONNECTION_DELAY_SECS: u64 = 10; // Increased delay between connections to avoid rate limits
const HEALTH_CHECK_INTERVAL_SECS: u64 = 5; // More frequent health checks
const MAX_HEALTHY_LATENCY_MS: u64 = 10000; // 10 seconds - time since last message threshold  
const MAX_RECONNECT_DELAY_SECS: u64 = 60; // Maximum reconnection delay
const RECONNECT_BACKOFF_MULTIPLIER: u64 = 5; // Linear backoff multiplier
const MAX_RECONNECT_ATTEMPTS_BEFORE_REST: u32 = 10; // Move to REST after this many failures
const CONNECTION_RATE_LIMIT_SECS: u64 = 5; // Minimum time between connection attempts
const CIRCUIT_BREAKER_COOLDOWN_SECS: u64 = 300; // 5 minute cooldown after max failures
const CIRCUIT_BREAKER_RETRY_INTERVAL_SECS: u64 = 60; // Try once per minute after cooldown
const STARTUP_GRACE_PERIOD_MS: u64 = 60_000; // 60 second grace period after subscription

type CoinbaseWebSocket = FragmentCollector<TokioIo<Upgraded>>;

#[derive(Debug, Deserialize)]
struct CoinbaseMessage {
    #[serde(rename = "type")]
    msg_type: String,
    product_id: Option<String>,
    price: Option<String>,
    time: Option<String>,
    // For ticker_batch messages
    #[serde(default)]
    events: Vec<TickerEvent>,
}

#[derive(Debug, Deserialize)]
struct TickerEvent {
    #[serde(rename = "type")]
    event_type: String,
    product_id: String,
    price: String,
    time: String,
}

#[derive(Debug, Serialize)]
struct CoinbaseSubscribe {
    #[serde(rename = "type")]
    msg_type: String,
    product_ids: Vec<String>,
    channels: Vec<String>,
}

/// Get all cached Coinbase pairs
pub fn get_cached_pairs() -> Result<Vec<Pair>> {
    if COINBASE_EXCHANGE_INFO.is_empty() {
        return Err(anyhow!("Exchange info not fetched yet"));
    }
    
    let pairs: Vec<Pair> = COINBASE_EXCHANGE_INFO
        .iter()
        .map(|entry| entry.value().clone())
        .collect();
    
    Ok(pairs)
}

/// A single WebSocket connection to Coinbase
pub struct CoinbaseConnection {
    id: String,
    ws: Option<Arc<Mutex<CoinbaseWebSocket>>>,
    symbols: HashSet<String>,
    symbol_to_pair: Arc<DashMap<String, Pair>>,
    tx: mpsc::UnboundedSender<Vec<Ticker>>,
    is_connected: Arc<AtomicBool>,
    last_message_time: Arc<AtomicU64>,
    subscribed_symbols: Arc<RwLock<HashSet<String>>>,
    subscription_completed_time: Arc<AtomicU64>,
    connection_established_time: Arc<AtomicU64>,
    // Warmup tracking
    pending_symbols: Arc<RwLock<Vec<String>>>, // Symbols waiting to be added
}

impl CoinbaseConnection {
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
            subscription_completed_time: Arc::new(AtomicU64::new(0)),
            connection_established_time: Arc::new(AtomicU64::new(0)),
            pending_symbols: Arc::new(RwLock::new(Vec::new())),
        }
    }

    pub async fn connect(&mut self) -> Result<()> {
        info!("[{}] Connecting to Coinbase WebSocket", self.id);
        
        // Create HTTPS client
        let https = HttpsConnector::new();
        let client = Client::builder(TokioExecutor::new())
            .build::<_, Empty<Bytes>>(https);
        
        // Parse URI
        let uri: Uri = COINBASE_WS_ENDPOINT.replace("wss://", "https://").parse()?;
        
        // Create WebSocket upgrade request
        let req = Request::builder()
            .method("GET")
            .uri(&uri)
            .header("Host", "ws-feed.exchange.coinbase.com")
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
        
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
            
        self.is_connected.store(true, Ordering::Release);
        self.last_message_time.store(now, Ordering::Release);
        self.connection_established_time.store(now, Ordering::Release);
        
        let ws = Arc::new(Mutex::new(ws));
        self.ws = Some(ws.clone());
        
        // Spawn reader task
        let tx = self.tx.clone();
        let is_connected = self.is_connected.clone();
        let last_message_time = self.last_message_time.clone();
        let symbol_to_pair = self.symbol_to_pair.clone();
        let connection_id = self.id.clone();
        
        crate::runtime_separation::spawn_on_ingestion_named(
            &format!("coinbase-reader-{connection_id}"),
            async move {
                if let Err(e) = Self::read_loop(
                connection_id,
                ws,
                tx,
                is_connected.clone(),
                last_message_time.clone(),
                symbol_to_pair,
            ).await {
                error!("Coinbase read loop error: {}", e);
                is_connected.store(false, Ordering::Release);
                // Reset last_message_time to prevent stale latency calculations
                last_message_time.store(
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as u64,
                    Ordering::Release
                );
            }
        });
        
        info!("[{}] Connected successfully", self.id);
        Ok(())
    }

    pub async fn subscribe_gradually(&mut self) -> Result<()> {
        let ws = self.ws.as_ref().ok_or_else(|| anyhow!("Not connected"))?;
        let mut subscribed = self.subscribed_symbols.write().await;
        
        let symbols: Vec<String> = self.symbols.iter().cloned().collect();
        
        // Subscribe in batches with delays to avoid rate limits
        for (i, chunk) in symbols.chunks(SUBSCRIPTION_BATCH_SIZE).enumerate() {
            let mut product_ids = Vec::new();
            
            for symbol in chunk {
                if !subscribed.contains(symbol) {
                    product_ids.push(symbol.clone());
                    subscribed.insert(symbol.clone());
                }
            }
            
            if !product_ids.is_empty() {
                let subscribe_msg = CoinbaseSubscribe {
                    msg_type: "subscribe".to_string(),
                    product_ids,
                    channels: vec![
                        "ticker_batch".to_string(), // Use batch channel for reduced traffic
                        "heartbeat".to_string(),    // Keep connection alive during low activity
                    ],
                };
                
                if let Ok(msg) = serde_json::to_string(&subscribe_msg) {
                    let mut ws_guard = ws.lock().await;
                    ws_guard.write_frame(Frame::text(msg.as_bytes().into())).await?;
                    drop(ws_guard);
                    
                    debug!("[{}] Subscribed to batch {} ({} symbols)", 
                        self.id, i + 1, chunk.len());
                    
                    // Don't register pairs yet - wait until subscription is confirmed
                }
                
                // Delay between batches
                if i < symbols.len() / SUBSCRIPTION_BATCH_SIZE {
                    tokio::time::sleep(Duration::from_millis(SUBSCRIPTION_BATCH_DELAY_MS)).await;
                }
            }
        }
        
        info!("[{}] Subscribed to {} symbols gradually", self.id, subscribed.len());
        
        // Now register all subscribed pairs as WebSocket-active
        let pairs_to_register: Vec<crate::Pair> = subscribed.iter()
            .filter_map(|symbol| {
                COINBASE_EXCHANGE_INFO.get(symbol)
                    .map(|entry| entry.value().clone())
            })
            .collect();
        
        if !pairs_to_register.is_empty() {
            let hub = crate::hub::SurgeHub::global();
            hub.register_websocket_pairs(crate::Source::Coinbase, &pairs_to_register);
            info!("[{}] 📡 Registered {} symbols as WebSocket-active", 
                self.id, pairs_to_register.len());
        }
        
        // Mark subscription as completed
        self.subscription_completed_time.store(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            Ordering::Release
        );
        
        Ok(())
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
    
    /// Get health status considering startup grace period
    pub fn is_healthy(&self) -> bool {
        if !self.is_connected() {
            return false;
        }
        
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
            
        // Check if we're still in startup phase (60 seconds after subscription)
        let subscription_time = self.subscription_completed_time.load(Ordering::Acquire);
        if subscription_time > 0 {
            let time_since_subscription = now.saturating_sub(subscription_time);
            if time_since_subscription < STARTUP_GRACE_PERIOD_MS {
                debug!("[{}] Still in startup phase ({} ms since subscription)", 
                    self.id, time_since_subscription);
                return true;  // Consider healthy during startup
            }
        }
        
        // After startup, check message latency
        let latency = self.get_latency_ms();
        latency < MAX_HEALTHY_LATENCY_MS
    }

    async fn read_loop(
        connection_id: String,
        ws: Arc<Mutex<CoinbaseWebSocket>>,
        tx: mpsc::UnboundedSender<Vec<Ticker>>,
        is_connected: Arc<AtomicBool>,
        last_message_time: Arc<AtomicU64>,
        symbol_to_pair: Arc<DashMap<String, Pair>>,
    ) -> Result<()> {
        loop {
            let frame = {
                let mut ws_guard = ws.lock().await;
                match ws_guard.read_frame().await {
                    Ok(frame) => frame,
                    Err(e) => {
                        error!("[{}] Failed to read frame: {}", connection_id, e);
                        is_connected.store(false, Ordering::Release);
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
                    if let Ok(msg) = serde_json::from_str::<CoinbaseMessage>(&text) {
                        match msg.msg_type.as_str() {
                            "ticker" => {
                                // Handle single ticker message
                                if let (Some(product_id), Some(price_str)) = (&msg.product_id, &msg.price) {
                                    if let Some(pair) = symbol_to_pair.get(product_id) {
                                        if let Ok(price) = price_str.parse::<Decimal>() {
                                            let timestamp = if let Some(time_str) = &msg.time {
                                                chrono::DateTime::parse_from_rfc3339(time_str)
                                                    .map(|dt| dt.timestamp_millis() as u64)
                                                    .unwrap_or(0)
                                            } else {
                                                chrono::Utc::now().timestamp_millis() as u64
                                            };
                                            
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
                            "ticker_batch" => {
                                // Handle batch ticker message (multiple events)
                                let mut tickers = Vec::new();
                                for event in &msg.events {
                                    if event.event_type == "ticker" {
                                        if let Some(pair) = symbol_to_pair.get(&event.product_id) {
                                            if let Ok(price) = event.price.parse::<Decimal>() {
                                                let timestamp = chrono::DateTime::parse_from_rfc3339(&event.time)
                                                    .map(|dt| dt.timestamp_millis() as u64)
                                                    .unwrap_or_else(|_| chrono::Utc::now().timestamp_millis() as u64);
                                                
                                                tickers.push(Ticker {
                                                    symbol: pair.as_str(),
                                                    price,
                                                    timestamp,
                                                });
                                            }
                                        }
                                    }
                                }
                                if !tickers.is_empty() {
                                    let _ = tx.send(tickers);
                                }
                            }
                            "heartbeat" => {
                                // Update last message time to show connection is alive
                                debug!("[{}] Received heartbeat", connection_id);
                            }
                            _ => {} // Ignore other message types
                        }
                    }
                }
                OpCode::Close => {
                    info!("[{}] WebSocket closed", connection_id);
                    is_connected.store(false, Ordering::Release);
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
pub struct CoinbaseConnectionPair {
    pub id: String,
    pub primary: Arc<RwLock<CoinbaseConnection>>,
    pub secondary: Arc<RwLock<CoinbaseConnection>>,
    pub symbols: HashSet<String>,
    pub active_is_primary: Arc<AtomicBool>,
    monitoring_task: Option<tokio::task::JoinHandle<()>>,
    last_connection_time: Arc<RwLock<std::time::Instant>>,
    primary_reconnect_attempts: Arc<AtomicU32>,
    secondary_reconnect_attempts: Arc<AtomicU32>,
    circuit_breaker_open_time: Arc<RwLock<Option<std::time::Instant>>>,
}

impl CoinbaseConnectionPair {
    pub fn new(
        id: String,
        symbols: HashSet<String>,
        symbol_to_pair: Arc<DashMap<String, Pair>>,
        tx: mpsc::UnboundedSender<Vec<Ticker>>,
    ) -> Self {
        let primary = Arc::new(RwLock::new(CoinbaseConnection::new(
            format!("{id}-Primary"),
            symbols.clone(),
            symbol_to_pair.clone(),
            tx.clone(),
        )));
        
        let secondary = Arc::new(RwLock::new(CoinbaseConnection::new(
            format!("{id}-Secondary"),
            symbols.clone(),
            symbol_to_pair,
            tx,
        )));
        
        Self {
            id,
            primary,
            secondary,
            symbols,
            active_is_primary: Arc::new(AtomicBool::new(true)),
            monitoring_task: None,
            last_connection_time: Arc::new(RwLock::new(
                std::time::Instant::now() - Duration::from_secs(CONNECTION_RATE_LIMIT_SECS)
            )),
            primary_reconnect_attempts: Arc::new(AtomicU32::new(0)),
            secondary_reconnect_attempts: Arc::new(AtomicU32::new(0)),
            circuit_breaker_open_time: Arc::new(RwLock::new(None)),
        }
    }

    pub async fn start(&mut self) -> Result<()> {
        info!("[{}] Starting Coinbase connection pair", self.id);
        
        // Connect primary first
        {
            let mut primary = self.primary.write().await;
            if let Err(e) = primary.connect().await {
                error!("[{}] Primary connection failed: {}", self.id, e);
            } else if let Err(e) = primary.subscribe_gradually().await {
                error!("[{}] Primary subscription failed: {}", self.id, e);
            }
        }
        
        // Wait before starting secondary
        tokio::time::sleep(Duration::from_secs(CONNECTION_DELAY_SECS)).await;
        
        // Connect secondary
        {
            let mut secondary = self.secondary.write().await;
            if let Err(e) = secondary.connect().await {
                error!("[{}] Secondary connection failed: {}", self.id, e);
            } else if let Err(e) = secondary.subscribe_gradually().await {
                error!("[{}] Secondary subscription failed: {}", self.id, e);
            }
        }
        
        // Start monitoring task for automatic failover
        let primary = self.primary.clone();
        let secondary = self.secondary.clone();
        let active_is_primary = self.active_is_primary.clone();
        let connection_id = self.id.clone();
        
        let last_connection_time = self.last_connection_time.clone();
        let primary_reconnect_attempts = self.primary_reconnect_attempts.clone();
        let secondary_reconnect_attempts = self.secondary_reconnect_attempts.clone();
        let conn_symbols = self.symbols.clone();
        let circuit_breaker_open_time = self.circuit_breaker_open_time.clone();
        
        let monitoring_task = crate::runtime_separation::spawn_on_ingestion_named(
            &format!("coinbase-monitor-{connection_id}"),
            async move {
            Self::monitor_connections(
                connection_id,
                primary,
                secondary,
                active_is_primary,
                last_connection_time,
                primary_reconnect_attempts,
                secondary_reconnect_attempts,
                conn_symbols,
                circuit_breaker_open_time,
            ).await;
        });
        
        self.monitoring_task = Some(monitoring_task);
        
        Ok(())
    }
    
    /// Subscribe to additional symbols on active connections
    pub async fn subscribe_additional_symbols(&mut self, new_symbols: Vec<String>) -> Result<()> {
        // Update our symbol set
        self.symbols.extend(new_symbols.iter().cloned());
        
        // Check which connection is active
        let is_primary = self.active_is_primary.load(Ordering::Acquire);
        
        // Subscribe on the active connection
        if is_primary {
            let primary = self.primary.write().await;
            if primary.is_connected() {
                if let Some(ws) = &primary.ws {
                    let subscribe_msg = CoinbaseSubscribe {
                        msg_type: "subscribe".to_string(),
                        product_ids: new_symbols.clone(),
                        channels: vec![
                            "ticker_batch".to_string(),
                            "heartbeat".to_string(),
                        ],
                    };
                    
                    if let Ok(msg) = serde_json::to_string(&subscribe_msg) {
                        let mut ws_guard = ws.lock().await;
                        ws_guard.write_frame(Frame::text(msg.as_bytes().into())).await?;
                        
                        // Update subscribed symbols
                        let mut subscribed = primary.subscribed_symbols.write().await;
                        subscribed.extend(new_symbols.iter().cloned());
                        
                        // Add to pending symbols for tracking
                        let mut pending = primary.pending_symbols.write().await;
                        pending.extend(new_symbols);
                        
                        info!("[{}] Added {} symbols via warmup", self.id, pending.len());
                    }
                }
            }
        } else {
            let secondary = self.secondary.write().await;
            if secondary.is_connected() {
                if let Some(ws) = &secondary.ws {
                    let subscribe_msg = CoinbaseSubscribe {
                        msg_type: "subscribe".to_string(),
                        product_ids: new_symbols.clone(),
                        channels: vec![
                            "ticker_batch".to_string(),
                            "heartbeat".to_string(),
                        ],
                    };
                    
                    if let Ok(msg) = serde_json::to_string(&subscribe_msg) {
                        let mut ws_guard = ws.lock().await;
                        ws_guard.write_frame(Frame::text(msg.as_bytes().into())).await?;
                        
                        // Update subscribed symbols
                        let mut subscribed = secondary.subscribed_symbols.write().await;
                        subscribed.extend(new_symbols.iter().cloned());
                        
                        // Add to pending symbols for tracking
                        let mut pending = secondary.pending_symbols.write().await;
                        pending.extend(new_symbols);
                        
                        info!("[{}] Added {} symbols via warmup", self.id, pending.len());
                    }
                }
            }
        }
        
        Ok(())
    }

    async fn monitor_connections(
        connection_id: String,
        primary: Arc<RwLock<CoinbaseConnection>>,
        secondary: Arc<RwLock<CoinbaseConnection>>,
        active_is_primary: Arc<AtomicBool>,
        last_connection_time: Arc<RwLock<std::time::Instant>>,
        primary_reconnect_attempts: Arc<AtomicU32>,
        secondary_reconnect_attempts: Arc<AtomicU32>,
        _conn_symbols: HashSet<String>,
        circuit_breaker_open_time: Arc<RwLock<Option<std::time::Instant>>>,
    ) {
        let mut check_interval = tokio::time::interval(Duration::from_secs(HEALTH_CHECK_INTERVAL_SECS));
        let mut last_failover = std::time::Instant::now() - Duration::from_secs(60); // Allow immediate first failover
        let mut symbols_in_rest_failover = false;
        
        info!("[{}] 🔍 Connection monitoring started", connection_id);
        
        loop {
            check_interval.tick().await;
            
            // Check circuit breaker state
            let should_attempt_reconnect = {
                let breaker_time = circuit_breaker_open_time.read().await;
                match *breaker_time {
                    Some(open_time) => {
                        let elapsed = open_time.elapsed();
                        if elapsed < Duration::from_secs(CIRCUIT_BREAKER_COOLDOWN_SECS) {
                            // Still in cooldown
                            false
                        } else {
                            // After cooldown, try once per minute
                            elapsed.as_secs() % CIRCUIT_BREAKER_RETRY_INTERVAL_SECS == 0
                        }
                    }
                    None => true, // Circuit breaker not open
                }
            };
            
            let primary_healthy = primary.read().await.is_healthy();
            let secondary_healthy = secondary.read().await.is_healthy();
            let primary_connected = primary.read().await.is_connected();
            let secondary_connected = secondary.read().await.is_connected();
            let primary_latency = primary.read().await.get_latency_ms();
            let secondary_latency = secondary.read().await.get_latency_ms();
            
            // Get symbols from primary connection for REST failover
            let symbols = primary.read().await.symbols.clone();
            
            let currently_primary = active_is_primary.load(Ordering::Acquire);
            let now = std::time::Instant::now();
            
            // More detailed logging for debugging
            debug!("[{}] Health check - Primary: healthy={}, connected={}, latency={}ms | Secondary: healthy={}, connected={}, latency={}ms | Active: {}",
                connection_id, primary_healthy, primary_connected, primary_latency, 
                secondary_healthy, secondary_connected, secondary_latency,
                if currently_primary { "PRIMARY" } else { "SECONDARY" }
            );
            
            // Check if primary is unhealthy and we should failover to secondary
            if currently_primary && !primary_healthy {
                if secondary_healthy && now.duration_since(last_failover) > Duration::from_secs(5) {
                    let primary_startup = {
                        let p = primary.read().await;
                        let sub_time = p.subscription_completed_time.load(Ordering::Acquire);
                        if sub_time > 0 {
                            let now_ms = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap()
                                .as_millis() as u64;
                            now_ms.saturating_sub(sub_time) < STARTUP_GRACE_PERIOD_MS
                        } else {
                            false
                        }
                    };
                    
                    if primary_startup {
                        debug!("[{}] Primary unhealthy but still in startup grace period, not switching", connection_id);
                    } else {
                        error!("[{}] 🚨 PRIMARY CONNECTION UNHEALTHY! Switching to secondary - Primary: connected={}, latency={}ms", 
                            connection_id, primary_connected, primary_latency);
                        active_is_primary.store(false, Ordering::Release);
                        last_failover = now;
                    }
                    
                    // Try to reconnect primary in background with backoff (if circuit breaker allows)
                    if should_attempt_reconnect {
                        let primary = primary.clone();
                        let conn_id = connection_id.clone();
                        let last_conn_time = last_connection_time.clone();
                        let reconnect_attempts = primary_reconnect_attempts.clone();
                        let breaker_time = circuit_breaker_open_time.clone();
                        crate::runtime_separation::spawn_on_ingestion_named(
                            "coinbase-reconnect-primary",
                            async move {
                                Self::reconnect_with_backoff(
                                    conn_id,
                                    primary,
                                    last_conn_time,
                                    reconnect_attempts,
                                    "primary".to_string(),
                                breaker_time,
                            ).await;
                        });
                    }
                } else if !secondary_connected {
                    error!("[{}] 🚨 BOTH CONNECTIONS DOWN! Primary: connected={}, latency={}ms | Secondary: connected={}",
                        connection_id, primary_connected, primary_latency, secondary_connected);
                    
                    // Check if we should failover to REST API
                    let primary_attempts = primary_reconnect_attempts.load(Ordering::Acquire);
                    let secondary_attempts = secondary_reconnect_attempts.load(Ordering::Acquire);
                    
                    if !symbols_in_rest_failover {
                        if primary_attempts >= MAX_RECONNECT_ATTEMPTS_BEFORE_REST || 
                           secondary_attempts >= MAX_RECONNECT_ATTEMPTS_BEFORE_REST {
                            error!("[{}] 🚨 Too many reconnection failures (primary: {}, secondary: {}) - Opening circuit breaker", 
                                connection_id, primary_attempts, secondary_attempts);
                            
                            // Open circuit breaker
                            {
                                let mut breaker_time = circuit_breaker_open_time.write().await;
                                *breaker_time = Some(std::time::Instant::now());
                            }
                            
                            // Move symbols to REST API
                            error!("[{}] 🔄 Moving {} symbols to REST API during circuit breaker cooldown", 
                                connection_id, symbols.len());
                            
                            // Get pairs to remove from websocket cache
                            let pairs_to_remove: Vec<crate::Pair> = symbols.iter()
                                .filter_map(|symbol| {
                                    COINBASE_EXCHANGE_INFO.get(symbol)
                                        .map(|entry| entry.value().clone())
                                })
                                .collect();
                            
                            if !pairs_to_remove.is_empty() {
                                let hub = crate::hub::SurgeHub::global();
                                hub.remove_websocket_pairs(Source::Coinbase, &pairs_to_remove);
                                info!("[{}] 🔄 Removed {} pairs from WebSocket cache for REST failover", 
                                    connection_id, pairs_to_remove.len());
                            }
                            
                            // REST already polls all symbols - just need to remove from websocket cache
                            symbols_in_rest_failover = true;
                        } else {
                            warn!("[{}] 🔄 Both connections down but still attempting reconnection (primary attempts: {}, secondary attempts: {})", 
                                connection_id, primary_attempts, secondary_attempts);
                        }
                    }
                }
            }
            // Check if secondary is unhealthy and we should failover to primary  
            else if !currently_primary && !secondary_healthy {
                if primary_healthy && now.duration_since(last_failover) > Duration::from_secs(5) {
                    error!("[{}] 🚨 SECONDARY CONNECTION UNHEALTHY! Switching to primary - Secondary: connected={}, latency={}ms", 
                        connection_id, secondary_connected, secondary_latency);
                    active_is_primary.store(true, Ordering::Release);
                    last_failover = now;
                    
                    // Try to reconnect secondary in background with backoff (if circuit breaker allows)
                    if should_attempt_reconnect {
                        let secondary = secondary.clone();
                        let conn_id = connection_id.clone();
                        let last_conn_time = last_connection_time.clone();
                        let reconnect_attempts = secondary_reconnect_attempts.clone();
                        let breaker_time = circuit_breaker_open_time.clone();
                        crate::runtime_separation::spawn_on_ingestion_named(
                            "coinbase-reconnect-secondary",
                            async move {
                                Self::reconnect_with_backoff(
                                    conn_id,
                                    secondary,
                                    last_conn_time,
                                    reconnect_attempts,
                                    "secondary".to_string(),
                                breaker_time,
                            ).await;
                        });
                    }
                }
            }
            // Optimal case: prefer primary if both are healthy
            else if !currently_primary && primary_healthy && primary_latency < MAX_HEALTHY_LATENCY_MS / 2 {
                info!("[{}] 🔄 Failing back to healthy primary (latency: {}ms)", connection_id, primary_latency);
                active_is_primary.store(true, Ordering::Release);
            }
            
            // If either connection is back online and we had failed over to REST, restore WebSocket
            if symbols_in_rest_failover && (primary_connected || secondary_connected) {
                info!("[{}] ✅ WebSocket connection restored! Removing {} symbols from REST API", 
                    connection_id, symbols.len());
                
                // Re-register pairs as WebSocket-active
                let pairs_to_register: Vec<crate::Pair> = symbols.iter()
                    .filter_map(|symbol| {
                        COINBASE_EXCHANGE_INFO.get(symbol)
                            .map(|entry| entry.value().clone())
                    })
                    .collect();
                
                if !pairs_to_register.is_empty() {
                    let hub = crate::hub::SurgeHub::global();
                    hub.register_websocket_pairs(Source::Coinbase, &pairs_to_register);
                    info!("[{}] 📡 Re-registered {} pairs as WebSocket-active", 
                        connection_id, pairs_to_register.len());
                }
                
                // REST already polls all symbols - websocket cache handles skipping
                symbols_in_rest_failover = false;
                
                // Reset reconnect attempts since we have at least one working connection
                if primary_connected {
                    primary_reconnect_attempts.store(0, Ordering::Release);
                }
                if secondary_connected {
                    secondary_reconnect_attempts.store(0, Ordering::Release);
                }
                
                // Close circuit breaker if it was open
                {
                    let mut breaker_time = circuit_breaker_open_time.write().await;
                    if breaker_time.is_some() {
                        info!("[{}] ⚡ Circuit breaker closed - connections restored", connection_id);
                        *breaker_time = None;
                    }
                }
            }
        }
    }
    
    /// Helper function to reconnect with exponential backoff
    async fn reconnect_with_backoff(
        connection_id: String,
        connection: Arc<RwLock<CoinbaseConnection>>,
        last_connection_time: Arc<RwLock<std::time::Instant>>,
        reconnect_attempts: Arc<AtomicU32>,
        conn_type: String,
        circuit_breaker_open_time: Arc<RwLock<Option<std::time::Instant>>>,
    ) {
        loop {
            // Check if circuit breaker is open and if we should wait
            {
                let breaker_time = circuit_breaker_open_time.read().await;
                if let Some(open_time) = *breaker_time {
                    let elapsed = open_time.elapsed();
                    if elapsed < Duration::from_secs(CIRCUIT_BREAKER_COOLDOWN_SECS) {
                        let remaining = Duration::from_secs(CIRCUIT_BREAKER_COOLDOWN_SECS) - elapsed;
                        debug!("[{}] ⏳ Circuit breaker still in cooldown for {:?}", connection_id, remaining);
                        tokio::time::sleep(Duration::from_secs(CIRCUIT_BREAKER_RETRY_INTERVAL_SECS)).await;
                        continue;
                    }
                }
            }
            
            // Increment attempt counter
            let attempts = reconnect_attempts.fetch_add(1, Ordering::AcqRel) + 1;
            
            // If we've reached max attempts, open circuit breaker
            if attempts >= MAX_RECONNECT_ATTEMPTS_BEFORE_REST {
                let mut breaker_time = circuit_breaker_open_time.write().await;
                *breaker_time = Some(std::time::Instant::now());
                error!("[{}] 🚨 Max reconnection attempts reached for {} - circuit breaker opened", 
                    connection_id, conn_type);
                // Wait for cooldown period before continuing
                tokio::time::sleep(Duration::from_secs(CIRCUIT_BREAKER_COOLDOWN_SECS)).await;
                // Reset attempts counter after cooldown
                reconnect_attempts.store(0, Ordering::Release);
                continue;
            }
            
            // Calculate backoff delay (linear: 5s, 10s, 15s... up to 60s)
            let retry_delay = Duration::from_secs(
                (attempts as u64 * RECONNECT_BACKOFF_MULTIPLIER).min(MAX_RECONNECT_DELAY_SECS)
            );
            
            info!("[{}] 🔄 Attempting to reconnect {} (attempt {}) after {:?}", 
                connection_id, conn_type, attempts, retry_delay);
            
            // Wait for backoff period
            tokio::time::sleep(retry_delay).await;
            
            // Rate limit connections to prevent hammering
            {
                let mut last_time = last_connection_time.write().await;
                let elapsed = last_time.elapsed();
                if elapsed < Duration::from_secs(CONNECTION_RATE_LIMIT_SECS) {
                    let wait_time = Duration::from_secs(CONNECTION_RATE_LIMIT_SECS) - elapsed;
                    debug!("[{}] Rate limiting connection - waiting {:?}", connection_id, wait_time);
                    tokio::time::sleep(wait_time).await;
                }
                *last_time = std::time::Instant::now();
            }
            
            // Attempt reconnection
            let mut conn = connection.write().await;
            match conn.connect().await {
                Ok(_) => {
                    info!("[{}] ✅ {} reconnected successfully", connection_id, conn_type);
                    // Reset attempt counter on success
                    reconnect_attempts.store(0, Ordering::Release);
                    
                    // Try to resubscribe
                    if let Err(e) = conn.subscribe_gradually().await {
                        error!("[{}] ❌ {} resubscription failed: {}", connection_id, conn_type, e);
                        // Don't break - connection is still good, just subscription failed
                    } else {
                        info!("[{}] ✅ {} resubscribed successfully", connection_id, conn_type);
                    }
                    break; // Exit reconnection loop on success
                }
                Err(e) => {
                    error!("[{}] ❌ Failed to reconnect {} (attempt {}): {}", 
                        connection_id, conn_type, attempts, e);
                    // Continue loop to retry
                }
            }
        }
    }
}

pub struct CoinbaseStream {
    connection_pairs: Vec<Arc<Mutex<CoinbaseConnectionPair>>>,
    rx: mpsc::UnboundedReceiver<Result<Ticker>>,
    tx: mpsc::UnboundedSender<Vec<Ticker>>,
    symbol_to_pair_cache: Arc<DashMap<String, Pair>>,
    // Warmup tracking
    all_symbols: Arc<RwLock<Vec<String>>>, // All available symbols for gradual addition
    warmup_task: Option<tokio::task::JoinHandle<()>>,
    connection_state: ConnectionState,
}

impl CoinbaseStream {
    pub async fn new() -> Result<Self> {
        let (tx, rx) = mpsc::unbounded_channel();
        
        // Create internal tx/rx for aggregating from multiple connections
        let (internal_tx, mut internal_rx) = mpsc::unbounded_channel::<Vec<Ticker>>();
        
        // Spawn aggregator task
        let external_tx = tx.clone();
        let symbol_to_pair_cache = COINBASE_EXCHANGE_INFO.clone();
        let symbol_to_pair_cache_for_spawn = symbol_to_pair_cache.clone();

        crate::runtime_separation::spawn_on_ingestion_named(
            "coinbase-aggregator",
            async move {
                while let Some(tickers) = internal_rx.recv().await {
                    for ticker in tickers {
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
            symbol_to_pair_cache: COINBASE_EXCHANGE_INFO.clone(),
            all_symbols: Arc::new(RwLock::new(Vec::new())),
            warmup_task: None,
            connection_state: ConnectionState::new(),
        };
        stream.connect().await?;
        Ok(stream)
    }
    
    /// Pre-fetch exchange info without connecting WebSocket
    pub async fn prefetch_exchange_info() -> Result<()> {
        if COINBASE_EXCHANGE_INFO.is_empty() {
            Self::fetch_and_cache_exchange_info().await?;
        }
        Ok(())
    }

    async fn connect(&mut self) -> Result<()> {
        info!("Creating Coinbase WebSocket connections");
        
        // Get ALL symbols sorted by volume
        let all_symbols = Self::get_all_symbols_by_volume().await?;
        
        // Store all symbols for gradual warmup
        *self.all_symbols.write().await = all_symbols.clone();
        
        // Select initial symbols to start with
        let (initial_symbols, _) = Self::select_top_symbols().await?;
        
        info!("Coinbase: Starting with {} symbols, {} more available for warmup", 
            initial_symbols.len(), all_symbols.len() - initial_symbols.len());
        
        // Create 2 main connections: priority and volume (like original)
        let priority_symbols: HashSet<String> = vec![
            "BTC-USD", "ETH-USD", "SOL-USD", "BTC-USDC", "ETH-USDC"
        ].into_iter().map(|s| s.to_string()).collect();
        
        let volume_symbols: HashSet<String> = initial_symbols
            .into_iter()
            .filter(|s| !priority_symbols.contains(s))
            .collect();
        
        // Create priority connection pair
        if !priority_symbols.is_empty() {
            let pair = Arc::new(Mutex::new(CoinbaseConnectionPair::new(
                "Coinbase-Priority".to_string(),
                priority_symbols.clone(),
                self.symbol_to_pair_cache.clone(),
                self.tx.clone(),
            )));
            self.connection_pairs.push(pair);
        }
        
        // Create volume connection pair
        if !volume_symbols.is_empty() {
            let pair = Arc::new(Mutex::new(CoinbaseConnectionPair::new(
                "Coinbase-Volume".to_string(),
                volume_symbols.clone(),
                self.symbol_to_pair_cache.clone(),
                self.tx.clone(),
            )));
            self.connection_pairs.push(pair);
        }
        
        // Start connections
        for (i, pair) in self.connection_pairs.iter().enumerate() {
            let mut conn = pair.lock().await;
            if let Err(e) = conn.start().await {
                error!("Failed to start Coinbase connection pair {}: {}", i, e);
            }
            
            // Delay between connections
            if i < self.connection_pairs.len() - 1 {
                tokio::time::sleep(Duration::from_secs(CONNECTION_DELAY_SECS)).await;
            }
        }
        
        // Start warmup manager if enabled
        if WARMUP_ENABLED {
            self.start_warmup_manager().await;
        }
        
        // Start connection state monitor
        self.start_connection_monitor();
        
        Ok(())
    }
    
    /// Select top symbols by volume
    async fn select_top_symbols() -> Result<(Vec<String>, HashSet<String>)> {
        // Priority tickers for Coinbase (using their format)
        let priority_tickers = vec![
            "BTC-USD", "ETH-USD", "SOL-USD", "ADA-USD", "XRP-USD", 
            "DOGE-USD", "LINK-USD", "AVAX-USD", "DOT-USD", "MATIC-USD"
        ];

        // Ensure exchange info is cached
        if COINBASE_EXCHANGE_INFO.is_empty() {
            Self::fetch_and_cache_exchange_info().await?;
        }
        
        // Use ALL cached symbols - they're already filtered by allowed quotes
        let all_symbols: Vec<String> = COINBASE_EXCHANGE_INFO.iter()
            .map(|entry| entry.key().clone())
            .collect();
        
        info!("📊 Using {} symbols from Coinbase exchange info", all_symbols.len());

        // Get 24hr volume data to sort by volume
        let volume_fetcher = crate::volume_cache::VolumeFetcher::global();
        let volumes = match volume_fetcher.fetch_coinbase_volumes().await {
            Ok((vols, _, _, _, _)) => vols,
            Err(e) => {
                warn!("Failed to fetch Coinbase volumes (using empty): {}", e);
                HashMap::new()
            }
        };

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

        // Determine initial count based on warmup settings
        let initial_count = if WARMUP_ENABLED {
            WARMUP_INITIAL_SYMBOLS
        } else {
            TOP_SYMBOLS_COUNT
        };

        // Add top symbols by volume until we reach initial limit
        for (symbol, volume) in symbol_volumes {
            if !used.contains(&symbol) && websocket_symbols.len() < initial_count {
                websocket_symbols.push(symbol.clone());
                used.insert(symbol.clone());
                if volume > 0.0 {
                    debug!("📈 High volume symbol added: {} (volume: {})", symbol, volume);
                }
            }
        }

        // For Coinbase, we NEVER use REST API (no batch endpoint)
        let rest_symbols: HashSet<String> = HashSet::new();

        info!("🚀 Coinbase: Starting with {} symbols on WebSocket (warmup: {})", 
            websocket_symbols.len(), WARMUP_ENABLED);
        if WARMUP_ENABLED {
            info!("📈 Will gradually add more symbols every {} seconds", WARMUP_INTERVAL_SECS);
        }

        Ok((websocket_symbols, rest_symbols))
    }
    
    /// Get all symbols sorted by volume (for warmup)
    async fn get_all_symbols_by_volume() -> Result<Vec<String>> {
        // Ensure exchange info is cached
        if COINBASE_EXCHANGE_INFO.is_empty() {
            Self::fetch_and_cache_exchange_info().await?;
        }
        
        let all_symbols: Vec<String> = COINBASE_EXCHANGE_INFO.iter()
            .map(|entry| entry.key().clone())
            .collect();
        
        // Get volume data
        let volume_fetcher = crate::volume_cache::VolumeFetcher::global();
        let volumes = match volume_fetcher.fetch_coinbase_volumes().await {
            Ok((vols, _, _, _, _)) => vols,
            Err(e) => {
                warn!("Failed to fetch Coinbase volumes: {}", e);
                HashMap::new()
            }
        };
        
        // Sort by volume
        let mut symbol_volumes: Vec<(String, f64)> = all_symbols
            .into_iter()
            .map(|symbol| {
                let volume = volumes.get(&symbol).copied().unwrap_or(0.0);
                (symbol, volume)
            })
            .collect();
        
        symbol_volumes.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        
        Ok(symbol_volumes.into_iter().map(|(symbol, _)| symbol).collect())
    }
    
    async fn fetch_and_cache_exchange_info() -> Result<()> {
        info!("🔄 Fetching fresh Coinbase exchange info");
        
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()?;
            
        let response = client
            .get("https://api.exchange.coinbase.com/products")
            .send()
            .await?;
            
        #[derive(Debug, Deserialize)]
        struct ProductInfo {
            id: String,
            status: String,
            base_currency: String,
            quote_currency: String,
        }
        
        let products: Vec<ProductInfo> = response.json().await?;
        let allowed_quotes = get_allowed_quotes(Source::Coinbase);
        info!("📝 Allowed quote currencies for Coinbase: {:?}", allowed_quotes);
        
        // Cache ALL trading symbols with allowed quotes
        let mut cached_count = 0;
        let mut total_symbols = 0;
        
        for product in products {
            total_symbols += 1;
            
            if product.status == "online" {
                let pair = Pair {
                    base: product.base_currency,
                    quote: product.quote_currency,
                };
                
                // Include if quote is in our allowed list
                if allowed_quotes.contains(&pair.quote.as_str()) {
                    COINBASE_EXCHANGE_INFO.insert(product.id.clone(), pair);
                    cached_count += 1;
                }
            }
        }
        
        info!("📊 Processed {} total symbols from Coinbase", total_symbols);
        info!("✅ Cached {} trading pairs with allowed quotes", cached_count);
        
        Ok(())
    }
    
    /// Start a background task to monitor connection states
    fn start_connection_monitor(&self) {
        let connection_pairs = self.connection_pairs.clone();
        let connection_state = self.connection_state.clone();
        
        crate::runtime_separation::spawn_on_ingestion_named(
            "coinbase-connection-monitor",
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
    
    /// Start warmup manager that gradually adds more symbols
    async fn start_warmup_manager(&mut self) {
        let all_symbols = self.all_symbols.clone();
        let connection_pairs = self.connection_pairs.clone();
        let _symbol_to_pair_cache = self.symbol_to_pair_cache.clone();
        
        let task = crate::runtime_separation::spawn_on_ingestion_named(
            "coinbase-warmup-manager",
            async move {
                let mut interval = tokio::time::interval(Duration::from_secs(WARMUP_INTERVAL_SECS));
            let mut added_count = WARMUP_INITIAL_SYMBOLS;
            
            loop {
                interval.tick().await;
                
                // Get symbols to add
                let all_syms = all_symbols.read().await;
                if added_count >= all_syms.len() {
                    info!("Coinbase warmup complete - all {} symbols loaded", all_syms.len());
                    break;
                }
                
                let symbols_to_add: Vec<String> = all_syms
                    .iter()
                    .skip(added_count)
                    .take(WARMUP_INCREMENT_SYMBOLS)
                    .cloned()
                    .collect();
                
                if symbols_to_add.is_empty() {
                    break;
                }
                
                added_count += symbols_to_add.len();
                info!("Coinbase warmup: Adding {} more symbols (total: {})", 
                    symbols_to_add.len(), added_count);
                
                // Add to volume connection (second connection)
                if connection_pairs.len() > 1 {
                    let pair = &connection_pairs[1]; // Volume connection
                    let mut conn = pair.lock().await;
                    
                    // Subscribe to new symbols with rate limiting
                    for chunk in symbols_to_add.chunks(SUBSCRIPTION_BATCH_SIZE) {
                        if let Err(e) = conn.subscribe_additional_symbols(chunk.to_vec()).await {
                            error!("Failed to add warmup symbols: {}", e);
                        }
                        
                        // Rate limit between subscription batches
                        tokio::time::sleep(Duration::from_millis(SUBSCRIPTION_BATCH_DELAY_MS)).await;
                    }
                }
            }
            
            info!("Coinbase warmup manager finished");
        });
        
        self.warmup_task = Some(task);
    }
}

impl TickerStream for CoinbaseStream {
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
    // The ticker.symbol is already in "BTC/USD" format from CoinbaseConnection
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
        hub.update(Source::Coinbase, pair.clone(), tick).await;
        
    } else {
        warn!("❌ Failed to parse ticker symbol: {}", ticker.symbol);
    }

    Ok(())
}

impl Stream for CoinbaseStream {
    type Item = Result<Ticker>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}