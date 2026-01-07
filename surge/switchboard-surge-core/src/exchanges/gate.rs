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

// Global cache for Gate.io exchange info
static GATE_EXCHANGE_INFO: Lazy<Arc<DashMap<String, Pair>>> = Lazy::new(|| Arc::new(DashMap::new()));

// Configuration constants
const TOP_SYMBOLS_COUNT: usize = 150;
const GATE_WS_ENDPOINT: &str = "wss://api.gateio.ws/ws/v4/";
const SUBSCRIPTION_BATCH_SIZE: usize = 50; // Gate.io allows multiple symbols per subscription
const SUBSCRIPTION_BATCH_DELAY_MS: u64 = 500;
const CONNECTION_DELAY_SECS: u64 = 1;
const HEALTH_CHECK_INTERVAL_SECS: u64 = 5;
const MAX_HEALTHY_LATENCY_MS: u64 = 10000; // 10 seconds
const PING_INTERVAL_SECS: u64 = 20;

type GateWebSocket = FragmentCollector<TokioIo<Upgraded>>;

// Gate.io WebSocket message structures
#[derive(Debug, Deserialize)]
struct GateMessage {
    #[serde(default)]
    time: Option<u64>,
    channel: String,
    #[serde(default)]
    event: Option<String>,
    #[serde(default)]
    result: Option<GateTickerData>,
}

#[derive(Debug, Deserialize)]
struct GateTickerData {
    currency_pair: String,
    last: String,
}

#[derive(Debug, Serialize)]
struct GateSubscribe {
    time: u64,
    channel: String,
    event: String,
    payload: Vec<String>,
}

#[derive(Debug, Serialize)]
struct GatePing {
    time: u64,
    channel: String,
}

// Gate.io exchange info REST API response
#[derive(Debug, Deserialize)]
struct GateCurrencyPair {
    id: String,
    base: String,
    quote: String,
    trade_status: String,
}

/// Get all cached Gate.io pairs
pub fn get_cached_pairs() -> Result<Vec<Pair>> {
    if GATE_EXCHANGE_INFO.is_empty() {
        return Err(anyhow!("Exchange info not fetched yet"));
    }

    let pairs: Vec<Pair> = GATE_EXCHANGE_INFO
        .iter()
        .map(|entry| entry.value().clone())
        .collect();

    Ok(pairs)
}

/// A single WebSocket connection to Gate.io
pub struct GateConnection {
    id: String,
    ws: Option<Arc<Mutex<GateWebSocket>>>,
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

impl GateConnection {
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
        info!("[{}] Connecting to Gate.io WebSocket", self.id);

        // Create HTTPS client
        let https = HttpsConnector::new();
        let client = Client::builder(TokioExecutor::new())
            .build::<_, Empty<Bytes>>(https);

        // Parse URI - Gate.io uses wss://api.gateio.ws/ws/v4/
        let uri: Uri = GATE_WS_ENDPOINT.replace("wss://", "https://").parse()?;

        // Create WebSocket upgrade request
        let host = uri.host().ok_or(anyhow!("No host in URI"))?;
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
        let ws = WebSocket::after_handshake(TokioIo::new(upgraded), Role::Client);
        let ws = FragmentCollector::new(ws);

        self.ws = Some(Arc::new(Mutex::new(ws)));
        self.is_connected.store(true, Ordering::Release);
        self.last_message_time.store(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            Ordering::Release
        );

        // Update cache
        if let Some(mut entry) = crate::hub::CONNECTION_METADATA_CACHE.get_mut(&(Source::Gate, self.id.clone())) {
            entry.is_connected = true;
            entry.last_updated = crate::clock_sync::get_corrected_timestamp_ms();
        }

        info!("[{}] Connected to Gate.io WebSocket", self.id);
        Ok(())
    }

    fn start_ping_task(&mut self) {
        if let Some(ws) = &self.ws {
            let ws = ws.clone();
            let connection_id = self.id.clone();
            let is_connected = self.is_connected.clone();

            let handle = crate::runtime_separation::spawn_on_ingestion_named(
                &format!("gate-ping-{}", connection_id),
                async move {
                    let mut interval = tokio::time::interval(Duration::from_secs(PING_INTERVAL_SECS));

                    loop {
                        interval.tick().await;

                        if !is_connected.load(Ordering::Acquire) {
                            debug!("[{}] Connection closed, stopping ping task", connection_id);
                            break;
                        }

                        // Send Gate.io ping: {"time": <ts>, "channel": "spot.ping"}
                        let now = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_secs();

                        let ping = GatePing {
                            time: now,
                            channel: "spot.ping".to_string(),
                        };

                        if let Ok(ping_json) = serde_json::to_string(&ping) {
                            let mut ws_guard = ws.lock().await;
                            if let Err(e) = ws_guard.write_frame(Frame::text(fastwebsockets::Payload::Borrowed(ping_json.as_bytes()))).await {
                                error!("[{}] Failed to send ping: {}", connection_id, e);
                                break;
                            }
                            debug!("[{}] Sent ping to Gate.io", connection_id);
                        }
                    }
                },
            );

            self.ping_task_handle = Some(handle);
        }
    }

    pub async fn subscribe_gradually(&mut self) -> Result<()> {
        let ws = self.ws.clone().ok_or(anyhow!("Not connected"))?;

        // Subscribe in batches
        let symbols: Vec<String> = self.symbols.iter().cloned().collect();

        info!("[{}] Subscribing to {} symbols in batches of {}",
            self.id, symbols.len(), SUBSCRIPTION_BATCH_SIZE);

        for (batch_num, chunk) in symbols.chunks(SUBSCRIPTION_BATCH_SIZE).enumerate() {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();

            // Gate.io subscribe format: {"time": <ts>, "channel": "spot.tickers", "event": "subscribe", "payload": ["BTC_USDT", "ETH_USDT"]}
            let subscribe = GateSubscribe {
                time: now,
                channel: "spot.tickers".to_string(),
                event: "subscribe".to_string(),
                payload: chunk.to_vec(),
            };

            let subscribe_json = serde_json::to_string(&subscribe)?;

            {
                let mut ws_guard = ws.lock().await;
                ws_guard.write_frame(Frame::text(fastwebsockets::Payload::Borrowed(subscribe_json.as_bytes()))).await?;
            }

            // Track subscribed symbols
            {
                let mut subscribed = self.subscribed_symbols.write().await;
                for symbol in chunk {
                    subscribed.insert(symbol.clone());
                    // Initialize confirmation counter
                    self.symbol_confirmations.insert(symbol.clone(), 0);
                }
            }

            debug!("[{}] Subscribed batch {} ({} symbols): {:?}",
                self.id, batch_num + 1, chunk.len(), chunk);

            // Delay between batches
            if batch_num < symbols.len() / SUBSCRIPTION_BATCH_SIZE {
                tokio::time::sleep(Duration::from_millis(SUBSCRIPTION_BATCH_DELAY_MS)).await;
            }
        }

        // Mark subscription as complete
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        self.subscription_completed_time.store(now, Ordering::Release);

        info!("[{}] Subscription completed for {} symbols", self.id, symbols.len());

        // Start read loop after subscriptions
        self.start_read_loop();

        // Wait for confirmations
        let confirmed = self.wait_for_confirmations().await;

        // Mark confirmed symbols as websocket-active
        let hub = crate::hub::SurgeHub::global();
        let pairs: Vec<crate::pair::Pair> = confirmed.iter()
            .filter_map(|symbol| self.symbol_to_pair.get(symbol).map(|p| p.clone()))
            .collect();
        hub.register_websocket_pairs(Source::Gate, &pairs);

        info!("[{}] {} symbols confirmed and marked as websocket-active",
            self.id, confirmed.len());

        Ok(())
    }

    async fn wait_for_confirmations(&self) -> HashSet<String> {
        let subscribed = self.subscribed_symbols.read().await.clone();
        let timeout = Duration::from_secs(30);
        let start = std::time::Instant::now();

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

    pub fn get_message_rate(&self) -> f64 {
        let rate_fixed = self.last_rate_value.load(Ordering::Acquire);
        rate_fixed as f64 / 100.0
    }

    pub fn is_in_grace_period(&self) -> bool {
        let subscription_time = self.subscription_completed_time.load(Ordering::Acquire);
        if subscription_time == 0 {
            return true;
        }

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        now - subscription_time < STARTUP_GRACE_PERIOD_MS
    }

    pub async fn cleanup(&mut self) {
        debug!("[{}] Cleaning up connection", self.id);

        self.is_connected.store(false, Ordering::Release);

        if let Some(mut entry) = crate::hub::CONNECTION_METADATA_CACHE.get_mut(&(Source::Gate, self.id.clone())) {
            entry.is_connected = false;
        }

        if let Some(handle) = self.ping_task_handle.take() {
            handle.abort();
            debug!("[{}] Aborted ping task", self.id);
        }

        self.ws = None;
        self.read_loop_running.store(false, Ordering::Release);
        self.subscribed_symbols.write().await.clear();
        self.symbol_confirmations.clear();

        debug!("[{}] Cleanup complete", self.id);
    }

    pub fn start_read_loop(&mut self) {
        if self.read_loop_running.load(Ordering::Acquire) {
            warn!("[{}] Read loop already running, skipping start", self.id);
            return;
        }

        if let Some(ws) = &self.ws {
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
                &format!("gate-reader-{}", connection_id),
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
                        error!("[{}] Gate.io read loop error: {}", connection_id, e);
                        is_connected.store(false, Ordering::Release);

                        if let Some(mut entry) = crate::hub::CONNECTION_METADATA_CACHE.get_mut(&(Source::Gate, connection_id.clone())) {
                            entry.is_connected = false;
                        }

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
                    read_loop_running.store(false, Ordering::Release);
                });
        }
    }

    async fn read_loop(
        connection_id: String,
        ws: Arc<Mutex<GateWebSocket>>,
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

                        if let Some(mut entry) = crate::hub::CONNECTION_METADATA_CACHE.get_mut(&(Source::Gate, connection_id.clone())) {
                            entry.is_connected = false;
                        }

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

                    // Handle pong response: {"time": ..., "channel": "spot.pong"}
                    if text.contains("spot.pong") {
                        debug!("[{}] Received pong from Gate.io", connection_id);
                        continue;
                    }

                    // Log first few messages for debugging
                    static MSG_COUNT: AtomicU64 = AtomicU64::new(0);
                    let count = MSG_COUNT.fetch_add(1, Ordering::Relaxed);
                    if count < 5 {
                        debug!("[{}] Gate.io message {}: {}", connection_id, count, text);
                    }

                    // Parse Gate.io ticker message
                    if let Ok(msg) = serde_json::from_str::<GateMessage>(&text) {
                        // Gate.io ticker: {"channel": "spot.tickers", "event": "update", "result": {"currency_pair": "BTC_USDT", "last": "..."}}
                        if msg.channel == "spot.tickers" {
                            if let Some(ticker_data) = msg.result {
                                // Increment message count for rate tracking
                                message_count.fetch_add(1, Ordering::Relaxed);

                                // Check if it's time to calculate/log the rate
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

                                debug!("[{}] Gate.io ticker: symbol={}, price={}",
                                    connection_id, ticker_data.currency_pair, ticker_data.last);

                                // Track confirmations
                                let mut confirm_count = symbol_confirmations
                                    .entry(ticker_data.currency_pair.clone())
                                    .or_insert(0);
                                *confirm_count += 1;

                                if *confirm_count == 3 {
                                    debug!("[{}] Symbol {} confirmed with 3+ messages",
                                        connection_id, ticker_data.currency_pair);
                                }

                                // Look up the pair from our cache
                                if let Some(pair) = symbol_to_pair.get(&ticker_data.currency_pair) {
                                    if let Ok(price) = ticker_data.last.parse::<Decimal>() {
                                        let timestamp = msg.time.unwrap_or_else(|| {
                                            std::time::SystemTime::now()
                                                .duration_since(std::time::UNIX_EPOCH)
                                                .unwrap()
                                                .as_millis() as u64
                                        });

                                        let tick = Ticker {
                                            symbol: pair.as_str(),
                                            price,
                                            timestamp,
                                        };
                                        if tx.send(vec![tick]).is_err() {
                                            error!("[{}] Failed to send Gate.io ticker", connection_id);
                                        } else {
                                            debug!("[{}] Sent Gate.io ticker: {} @ {}",
                                                connection_id, pair.as_str(), price);
                                        }
                                    } else {
                                        warn!("[{}] Failed to parse Gate.io price: {}",
                                            connection_id, ticker_data.last);
                                    }
                                } else {
                                    warn!("[{}] Gate.io symbol not found in cache: {}",
                                        connection_id, ticker_data.currency_pair);
                                }
                            }
                        }
                    }
                }
                OpCode::Close => {
                    info!("[{}] WebSocket closed", connection_id);
                    is_connected.store(false, Ordering::Release);

                    if let Some(mut entry) = crate::hub::CONNECTION_METADATA_CACHE.get_mut(&(Source::Gate, connection_id.clone())) {
                        entry.is_connected = false;
                    }

                    monitor_trigger.notify_one();

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

/// Single connection with health monitoring
pub struct GateConnectionPair {
    pub id: String,
    pub connection: Arc<RwLock<GateConnection>>,
    pub symbols: HashSet<String>,
    monitoring_task: Option<tokio::task::JoinHandle<()>>,
}

impl GateConnectionPair {
    pub fn new(
        id: String,
        symbols: HashSet<String>,
        symbol_to_pair: Arc<DashMap<String, Pair>>,
        tx: mpsc::UnboundedSender<Vec<Ticker>>,
    ) -> Self {
        let connection = GateConnection::new(
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
        }

        // Start monitoring task
        let connection = self.connection.clone();
        let connection_id = self.id.clone();
        let symbols = self.symbols.clone();

        let monitoring_task = crate::runtime_separation::spawn_on_ingestion_named(
            &format!("gate-monitor-{connection_id}"),
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
        connection: Arc<RwLock<GateConnection>>,
        symbols: HashSet<String>,
    ) {
        let mut check_interval = tokio::time::interval(Duration::from_secs(HEALTH_CHECK_INTERVAL_SECS));
        let mut symbols_in_rest_failover = false;
        let mut last_reconnect_attempt: Option<std::time::Instant> = Some(std::time::Instant::now());
        const RECONNECT_INTERVAL_SECS: u64 = 300;

        let monitor_trigger = {
            let conn = connection.read().await;
            conn.monitor_trigger.clone()
        };

        // Spawn retry task for unconfirmed symbols
        let connection_retry = connection.clone();
        let conn_id_retry = connection_id.clone();
        let symbols_retry = symbols.clone();

        crate::runtime_separation::spawn_on_ingestion_named(
            &format!("gate-retry-{}", conn_id_retry),
            async move {
                tokio::time::sleep(Duration::from_secs(300)).await;

                loop {
                    tokio::time::sleep(Duration::from_secs(300)).await;

                    let is_connected = connection_retry.read().await.is_connected();
                    if !is_connected {
                        debug!("[{}] Skipping retry - connection not active", conn_id_retry);
                        continue;
                    }

                    let subscribed = connection_retry.read().await.subscribed_symbols.read().await.clone();

                    let hub = crate::hub::SurgeHub::global();

                    let active_symbols: HashSet<String> = symbols_retry.iter()
                        .filter(|symbol| {
                            if let Some(pair_entry) = GATE_EXCHANGE_INFO.get(*symbol) {
                                hub.is_websocket_active(Source::Gate, &pair_entry.value().clone())
                            } else {
                                false
                            }
                        })
                        .cloned()
                        .collect();

                    let unconfirmed: Vec<String> = subscribed.difference(&active_symbols)
                        .cloned()
                        .collect();

                    if !unconfirmed.is_empty() {
                        info!("[{}] 🔄 Retrying {} unconfirmed symbols: {:?}",
                            conn_id_retry, unconfirmed.len(), unconfirmed);

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
                _ = check_interval.tick() => {}
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

            if is_connected && !in_grace_period {
                let unhealthy =
                    latency_ms > MAX_HEALTHY_LATENCY_MS ||
                    message_rate < MIN_HEALTHY_MESSAGE_RATE;

                if unhealthy && !symbols_in_rest_failover {
                    warn!("[{}] 🚨 Connection unhealthy! Latency: {}ms, Rate: {:.1} msg/sec. Moving {} symbols to REST API",
                        connection_id, latency_ms, message_rate, symbols.len());

                    {
                        let conn = connection.write().await;
                        conn.is_connected.store(false, Ordering::Release);
                        conn.last_message_time.store(
                            std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap()
                                .as_millis() as u64,
                            Ordering::Release
                        );
                    }

                    // Clear symbols from WebSocket tracking
                    let pairs_to_clear: Vec<crate::Pair> = symbols.iter()
                        .filter_map(|symbol| {
                            GATE_EXCHANGE_INFO.get(symbol).map(|entry| entry.value().clone())
                        })
                        .collect();

                    if !pairs_to_clear.is_empty() {
                        let hub = crate::hub::SurgeHub::global();
                        hub.remove_websocket_pairs(Source::Gate, &pairs_to_clear);
                        info!("[{}] 📡 Removed {} symbols from WebSocket tracking for REST fallback",
                            connection_id, pairs_to_clear.len());
                    }

                    symbols_in_rest_failover = true;
                }
            } else if !is_connected && !symbols_in_rest_failover {
                error!("[{}] 🚨 Connection DOWN! Moving {} symbols to REST API",
                    connection_id, symbols.len());

                let pairs_to_clear: Vec<crate::Pair> = symbols.iter()
                    .filter_map(|symbol| {
                        GATE_EXCHANGE_INFO.get(symbol).map(|entry| entry.value().clone())
                    })
                    .collect();

                if !pairs_to_clear.is_empty() {
                    let hub = crate::hub::SurgeHub::global();
                    hub.remove_websocket_pairs(Source::Gate, &pairs_to_clear);
                    info!("[{}] 📡 Removed {} symbols from WebSocket tracking for REST fallback",
                            connection_id, pairs_to_clear.len());
                }

                symbols_in_rest_failover = true;
            }

            // Attempt reconnection if in failover mode
            if symbols_in_rest_failover {
                let should_reconnect = match last_reconnect_attempt {
                    None => true,
                    Some(last_attempt) => last_attempt.elapsed().as_secs() >= RECONNECT_INTERVAL_SECS,
                };

                if should_reconnect {
                    info!("[{}] 🔄 Attempting to reconnect WebSocket connection...", connection_id);
                    last_reconnect_attempt = Some(std::time::Instant::now());

                    let mut conn = connection.write().await;

                    conn.cleanup().await;
                    conn.symbols = symbols.clone();

                    match conn.connect().await {
                        Ok(_) => {
                            info!("[{}] ✅ WebSocket reconnected successfully", connection_id);

                            match conn.subscribe_gradually().await {
                                Ok(_) => {
                                    info!("[{}] ✅ Successfully re-subscribed to symbols after reconnection", connection_id);
                                    symbols_in_rest_failover = false;
                                }
                                Err(e) => {
                                    error!("[{}] ❌ Failed to re-subscribe after reconnection: {}", connection_id, e);
                                }
                            }
                        }
                        Err(e) => {
                            warn!("[{}] ❌ Failed to reconnect WebSocket: {}", connection_id, e);
                        }
                    }
                }
            }
        }
    }
}

/// Main Gate.io stream
pub struct GateStream {
    connection_pairs: Vec<Arc<Mutex<GateConnectionPair>>>,
    rx: mpsc::UnboundedReceiver<Result<Ticker>>,
    tx: mpsc::UnboundedSender<Vec<Ticker>>,
    symbol_to_pair_cache: Arc<DashMap<String, Pair>>,
    connection_state: ConnectionState,
}

impl GateStream {
    pub async fn new() -> Result<Self> {
        info!("🟢 Creating new Gate.io stream");

        // Fetch and cache exchange info
        Self::fetch_and_cache_exchange_info().await?;

        let (ticker_tx, mut ticker_rx) = mpsc::unbounded_channel::<Vec<Ticker>>();
        let (result_tx, result_rx) = mpsc::unbounded_channel::<Result<Ticker>>();

        // Forward tickers to result channel and process them
        crate::runtime_separation::spawn_on_ingestion_named(
            "gate-ticker-processor",
            async move {
                while let Some(tickers) = ticker_rx.recv().await {
                    for ticker in tickers {
                        // Process ticker through the hub
                        if let Ok(pair) = Pair::from_task(&ticker.symbol) {
                            let tick = crate::types::Tick {
                                price: ticker.price,
                                event_ts: ticker.timestamp,
                                seen_at: crate::clock_sync::get_corrected_timestamp_ms(),
                                verified_at: crate::clock_sync::get_corrected_timestamp_ms(),
                            };

                            let hub = crate::hub::SurgeHub::global();
                            hub.update(Source::Gate, pair, tick).await;
                        }

                        if result_tx.send(Ok(ticker)).is_err() {
                            break;
                        }
                    }
                }
            }
        );

        let mut stream = Self {
            connection_pairs: Vec::new(),
            rx: result_rx,
            tx: ticker_tx,
            symbol_to_pair_cache: GATE_EXCHANGE_INFO.clone(),
            connection_state: ConnectionState::new(),
        };

        // Connect
        stream.connect().await?;

        Ok(stream)
    }

    async fn fetch_and_cache_exchange_info() -> Result<()> {
        info!("📥 Fetching Gate.io exchange info...");

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()?;

        let response = client
            .get("https://api.gateio.ws/api/v4/spot/currency_pairs")
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(anyhow!("Gate.io API error: {}", response.status()));
        }

        let pairs: Vec<GateCurrencyPair> = response.json().await?;

        let allowed_quotes = get_allowed_quotes(Source::Gate);
        let mut cached_count = 0;

        for pair_info in pairs {
            // Filter by tradeable status and allowed quotes
            if pair_info.trade_status == "tradable" {
                let quote_upper = pair_info.quote.to_uppercase();
                if allowed_quotes.iter().any(|q| q.to_uppercase() == quote_upper) {
                    let pair = Pair {
                        base: pair_info.base.to_uppercase(),
                        quote: quote_upper,
                    };

                    // Store with Gate.io symbol format (BTC_USDT)
                    GATE_EXCHANGE_INFO.insert(pair_info.id.clone(), pair);
                    cached_count += 1;
                }
            }
        }

        info!("✅ Cached {} Gate.io trading pairs", cached_count);
        Ok(())
    }

    async fn connect(&mut self) -> Result<()> {
        info!("Creating Gate.io WebSocket connections");

        // Get top symbols by volume AND all symbols
        let (websocket_symbols, _) = Self::select_top_symbols().await?;

        // Initially, REST should poll ALL symbols
        let all_symbols = Self::get_all_symbols().await?;
        let all_symbols_set: HashSet<String> = all_symbols.into_iter().collect();

        info!("📊 Gate.io startup: REST polling {} symbols initially, WebSocket will handle {} high-volume symbols",
            all_symbols_set.len(), websocket_symbols.len());

        // Set up REST polling for ALL symbols initially
        let rest_manager = crate::rest_manager::RestManager::global();
        rest_manager.set_rest_symbols(Source::Gate, all_symbols_set).await;

        // Create connection pairs
        let symbols_per_connection = 50;
        let symbol_chunks: Vec<_> = websocket_symbols.chunks(symbols_per_connection)
            .map(|chunk| chunk.iter().cloned().collect::<HashSet<String>>())
            .collect();

        for (i, symbols) in symbol_chunks.into_iter().enumerate() {
            let conn_id = format!("Gate-{}", i + 1);
            let pair = Arc::new(Mutex::new(GateConnectionPair::new(
                conn_id.clone(),
                symbols.clone(),
                self.symbol_to_pair_cache.clone(),
                self.tx.clone(),
            )));
            self.connection_pairs.push(pair);

            // Register in global connection metadata cache
            crate::hub::CONNECTION_METADATA_CACHE.insert(
                (Source::Gate, conn_id),
                crate::hub::ConnectionMetadata {
                    symbols: symbols.iter().cloned().collect(),
                    is_connected: false,
                    last_updated: crate::clock_sync::get_corrected_timestamp_ms(),
                }
            );
        }

        // Start all connections with delays
        info!("Starting {} Gate.io connection pairs", self.connection_pairs.len());
        for (i, pair) in self.connection_pairs.iter().enumerate() {
            let mut conn = pair.lock().await;
            if let Err(e) = conn.start().await {
                error!("Failed to start Gate.io connection pair {}: {}", i, e);
            }

            if i < self.connection_pairs.len() - 1 {
                tokio::time::sleep(Duration::from_secs(CONNECTION_DELAY_SECS)).await;
            }
        }

        // Start connection state monitor
        self.start_connection_monitor();

        Ok(())
    }

    async fn get_all_symbols() -> Result<Vec<String>> {
        if GATE_EXCHANGE_INFO.is_empty() {
            let _ = Self::fetch_and_cache_exchange_info().await?;
        }

        let all_symbols: Vec<String> = GATE_EXCHANGE_INFO.iter()
            .map(|entry| entry.key().clone())
            .collect();

        Ok(all_symbols)
    }

    async fn select_top_symbols() -> Result<(Vec<String>, HashSet<String>)> {
        // Priority tickers (Gate.io format with underscore)
        let priority_tickers = vec![
            "BTC_USDT", "ETH_USDT", "SOL_USDT", "BNB_USDT", "XRP_USDT",
            "DOGE_USDT", "ADA_USDT", "SUI_USDT", "AVAX_USDT", "LINK_USDT"
        ];

        if GATE_EXCHANGE_INFO.is_empty() {
            let _ = Self::fetch_and_cache_exchange_info().await?;
        }

        let all_symbols: Vec<String> = GATE_EXCHANGE_INFO.iter()
            .map(|entry| entry.key().clone())
            .collect();

        info!("📊 Using {} symbols from Gate.io exchange info", all_symbols.len());

        // Get 24hr volume data to sort by volume
        let volume_fetcher = crate::volume_cache::VolumeFetcher::global();
        let (volumes, _, _, _, _) = volume_fetcher.fetch_gate_volumes().await?;

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

    fn start_connection_monitor(&self) {
        let connection_pairs = self.connection_pairs.clone();
        let connection_state = self.connection_state.clone();

        crate::runtime_separation::spawn_on_ingestion_named(
            "gate-connection-monitor",
            async move {
                let mut interval = tokio::time::interval(Duration::from_secs(5));

            loop {
                interval.tick().await;

                let mut any_connected = false;

                for pair in &connection_pairs {
                    let conn_pair = pair.lock().await;
                    let conn = conn_pair.connection.read().await;
                    if conn.is_connected() {
                        any_connected = true;
                        break;
                    }
                }

                connection_state.set_connected(any_connected);
            }
        });
    }

    pub fn start_read_loop(&mut self) {
        // Read loops are started in subscribe_gradually for each connection
    }

    /// Get WebSocket connection health information
    pub async fn get_websocket_connection_health(&self) -> Vec<crate::types::WebSocketConnectionHealth> {
        let hub = crate::hub::SurgeHub::global();
        let mut health = Vec::new();

        for pair in &self.connection_pairs {
            let conn_pair = pair.lock().await;
            let conn = conn_pair.connection.read().await;

            let is_connected = conn.is_connected();
            let latency = conn.get_latency_ms();
            let msg_rate = conn.get_message_rate();
            let in_grace = conn.is_in_grace_period();

            let mut websocket_active_count = 0;
            let mut rest_fallback_symbols = Vec::new();

            for symbol_str in &conn_pair.symbols {
                if let Some(pair_entry) = GATE_EXCHANGE_INFO.get(symbol_str) {
                    if hub.is_websocket_active(Source::Gate, pair_entry.value()) {
                        websocket_active_count += 1;
                    } else {
                        rest_fallback_symbols.push(symbol_str.clone());
                    }
                }
            }

            let total_symbols = conn_pair.symbols.len();
            let rest_count = rest_fallback_symbols.len();

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
                exchange: "Gate.io".to_string(),
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

impl TickerStream for GateStream {
    fn listen(&mut self, _pair: Pair) -> Result<()> {
        // Dynamic subscriptions not supported yet - all symbols subscribed on connect
        Ok(())
    }

    fn unlisten(&mut self, _pair: &Pair) -> Result<()> {
        // Dynamic unsubscriptions not supported yet
        Ok(())
    }

    fn subscriptions(&self) -> Vec<Pair> {
        GATE_EXCHANGE_INFO.iter()
            .map(|entry| entry.value().clone())
            .collect()
    }

    fn is_connected(&self) -> bool {
        self.connection_state.is_connected()
    }
}

impl Stream for GateStream {
    type Item = Result<Ticker>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.rx).poll_recv(cx)
    }
}
