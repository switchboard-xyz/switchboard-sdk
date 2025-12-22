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
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU32, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::{mpsc, Mutex, RwLock};
use tracing::{debug, error, info, warn};
use crate::exchanges::binance_connection::MIN_HEALTHY_MESSAGE_RATE;
use crate::exchanges::binance_connection::STARTUP_GRACE_PERIOD_MS;

// Global cache for Pyth price feeds
// Maps symbol (e.g., "BTC/USD") to (Pair, price_feed_id)
pub(crate) static PYTH_PRICE_FEEDS: Lazy<Arc<DashMap<String, (Pair, String)>>> = Lazy::new(|| Arc::new(DashMap::new()));

// Configuration constants
const TOP_SYMBOLS_COUNT: usize = 100;
const PYTH_HERMES_ENDPOINT: &str = "wss://hermes.pyth.network/ws";
const PYTH_REST_ENDPOINT: &str = "https://hermes.pyth.network";
const SUBSCRIPTION_BATCH_SIZE: usize = 20;
const SUBSCRIPTION_BATCH_DELAY_MS: u64 = 500;
const CONNECTION_DELAY_SECS: u64 = 1;
const HEALTH_CHECK_INTERVAL_SECS: u64 = 5;
const MAX_HEALTHY_LATENCY_MS: u64 = 10000;

type PythWebSocket = FragmentCollector<TokioIo<Upgraded>>;

#[derive(Debug, Deserialize)]
struct PythPriceFeedMeta {
    id: String,
    attributes: PythAttributes,
}

#[derive(Debug, Deserialize)]
struct PythAttributes {
    symbol: String,
    asset_type: String,
    quote_currency: Option<String>,
    base: Option<String>,
}

#[derive(Debug, Deserialize)]
struct PythStreamMessage {
    #[serde(rename = "type")]
    msg_type: String,
    price_feed: Option<PythPriceFeed>,
}

#[derive(Debug, Deserialize)]
struct PythPriceFeed {
    id: String,
    price: Option<PythPrice>,
    ema_price: Option<PythPrice>,
}

#[derive(Debug, Deserialize)]
struct PythPrice {
    price: String,
    conf: String,
    expo: i32,
    publish_time: i64,
}

#[derive(Debug, Serialize)]
struct PythSubscribe {
    #[serde(rename = "type")]
    msg_type: String,
    ids: Vec<String>,
}

/// Get all cached Pyth pairs
pub fn get_cached_pairs() -> Result<Vec<Pair>> {
    if PYTH_PRICE_FEEDS.is_empty() {
        return Err(anyhow!("Pyth price feeds not fetched yet"));
    }

    let pairs: Vec<Pair> = PYTH_PRICE_FEEDS
        .iter()
        .map(|entry| entry.value().0.clone())
        .collect();

    Ok(pairs)
}

/// A single WebSocket connection to Pyth Hermes
pub struct PythConnection {
    id: String,
    ws: Option<Arc<Mutex<PythWebSocket>>>,
    price_feed_ids: HashSet<String>,
    id_to_pair: Arc<DashMap<String, Pair>>,
    tx: mpsc::UnboundedSender<Vec<Ticker>>,
    is_connected: Arc<AtomicBool>,
    last_message_time: Arc<AtomicU64>,
    subscribed_ids: Arc<RwLock<HashSet<String>>>,
    message_count: Arc<AtomicU64>,
    last_rate_log_time: Arc<AtomicU64>,
    last_rate_value: Arc<AtomicU32>,
    subscription_completed_time: Arc<AtomicU64>,
    read_loop_running: Arc<AtomicBool>,
    ping_task_handle: Option<tokio::task::JoinHandle<()>>,
    monitor_trigger: Arc<tokio::sync::Notify>,
    id_confirmations: Arc<DashMap<String, u32>>,
}

impl PythConnection {
    pub fn new(
        id: String,
        price_feed_ids: HashSet<String>,
        id_to_pair: Arc<DashMap<String, Pair>>,
        tx: mpsc::UnboundedSender<Vec<Ticker>>,
    ) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        Self {
            id,
            ws: None,
            price_feed_ids,
            id_to_pair,
            tx,
            is_connected: Arc::new(AtomicBool::new(false)),
            last_message_time: Arc::new(AtomicU64::new(0)),
            subscribed_ids: Arc::new(RwLock::new(HashSet::new())),
            message_count: Arc::new(AtomicU64::new(0)),
            last_rate_log_time: Arc::new(AtomicU64::new(now)),
            last_rate_value: Arc::new(AtomicU32::new(0)),
            subscription_completed_time: Arc::new(AtomicU64::new(0)),
            read_loop_running: Arc::new(AtomicBool::new(false)),
            ping_task_handle: None,
            monitor_trigger: Arc::new(tokio::sync::Notify::new()),
            id_confirmations: Arc::new(DashMap::new()),
        }
    }

    pub async fn connect(&mut self) -> Result<()> {
        info!("[{}] Connecting to Pyth Hermes WebSocket", self.id);

        let https = HttpsConnector::new();
        let client = Client::builder(TokioExecutor::new())
            .build::<_, Empty<Bytes>>(https);

        let uri: Uri = PYTH_HERMES_ENDPOINT.replace("wss://", "https://").parse()?;

        let req = Request::builder()
            .method("GET")
            .uri(&uri)
            .header("Host", "hermes.pyth.network")
            .header(UPGRADE, "websocket")
            .header(CONNECTION, "upgrade")
            .header("Sec-WebSocket-Key", fastwebsockets::handshake::generate_key())
            .header("Sec-WebSocket-Version", "13")
            .body(Empty::<Bytes>::new())?;

        let res = client.request(req).await?;

        if res.status() != hyper::StatusCode::SWITCHING_PROTOCOLS {
            return Err(anyhow!("WebSocket handshake failed: {}", res.status()));
        }

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

        if let Some(mut entry) = crate::hub::CONNECTION_METADATA_CACHE.get_mut(&(Source::Pyth, self.id.clone())) {
            entry.is_connected = true;
            entry.last_updated = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
        }

        let ws = Arc::new(Mutex::new(ws));
        self.ws = Some(ws.clone());

        info!("[{}] Connected successfully", self.id);
        Ok(())
    }

    fn start_ping_task(&mut self) {
        if let Some(ws) = &self.ws {
            let ws = ws.clone();
            let connection_id = self.id.clone();
            let is_connected = self.is_connected.clone();

            let handle = crate::runtime_separation::spawn_on_ingestion_named(
                &format!("pyth-ping-{}", connection_id),
                async move {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    let mut interval = tokio::time::interval(Duration::from_secs(20));

                    loop {
                        interval.tick().await;

                        if !is_connected.load(Ordering::Acquire) {
                            warn!("[{}] Ping task stopping - connection closed", connection_id);
                            break;
                        }

                        let ping_msg = r#"{"type":"ping"}"#;

                        let mut ws_guard = ws.lock().await;
                        if let Err(e) = ws_guard.write_frame(Frame::text(ping_msg.as_bytes().into())).await {
                            error!("[{}] Failed to send ping: {}", connection_id, e);
                            is_connected.store(false, Ordering::Release);
                            break;
                        }
                        drop(ws_guard);
                        debug!("[{}] Sent ping to Pyth", connection_id);
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

        let hub = crate::hub::SurgeHub::global();
        let already_active = hub.get_websocket_pairs(Source::Pyth);
        let active_ids: HashSet<String> = already_active.iter()
            .filter_map(|pair| {
                PYTH_PRICE_FEEDS.iter()
                    .find(|entry| &entry.value().0 == pair)
                    .map(|entry| entry.value().1.clone())
            })
            .collect();

        let outstanding_ids: Vec<String> = self.price_feed_ids.iter()
            .filter(|id| !active_ids.contains(*id))
            .cloned()
            .collect();

        if outstanding_ids.is_empty() {
            info!("[{}] All {} price feeds already active in WebSocket",
                self.id, self.price_feed_ids.len());
            return Ok(());
        }

        info!("[{}] Will subscribe to {} outstanding price feeds (out of {} total)",
            self.id, outstanding_ids.len(), self.price_feed_ids.len());

        {
            let mut subscribed = self.subscribed_ids.write().await;

            for (i, chunk) in outstanding_ids.chunks(SUBSCRIPTION_BATCH_SIZE).enumerate() {
                let mut ids_to_subscribe: Vec<String> = Vec::new();

                for id in chunk.iter() {
                    if !subscribed.contains(id) {
                        subscribed.insert(id.clone());
                        ids_to_subscribe.push(id.clone());
                    }
                }

                if !ids_to_subscribe.is_empty() {
                    let subscribe_msg = PythSubscribe {
                        msg_type: "subscribe".to_string(),
                        ids: ids_to_subscribe.clone(),
                    };

                    if let Ok(msg) = serde_json::to_string(&subscribe_msg) {
                        debug!("[{}] Sending subscription message for {} feeds", self.id, ids_to_subscribe.len());
                        let mut ws_guard = ws.lock().await;
                        ws_guard.write_frame(Frame::text(msg.as_bytes().into())).await?;
                        drop(ws_guard);

                        info!("[{}] Subscribed to batch {}/{} ({} feeds, total subscribed: {})",
                            self.id,
                            i + 1,
                            (outstanding_ids.len() + SUBSCRIPTION_BATCH_SIZE - 1) / SUBSCRIPTION_BATCH_SIZE,
                            ids_to_subscribe.len(),
                            subscribed.len()
                        );
                    }

                    if i < outstanding_ids.len() / SUBSCRIPTION_BATCH_SIZE {
                        tokio::time::sleep(Duration::from_millis(SUBSCRIPTION_BATCH_DELAY_MS)).await;
                    }
                }
            }
        }

        info!("[{}] Subscribed to {} price feeds gradually", self.id, outstanding_ids.len());

        let already_active_count = self.price_feed_ids.iter()
            .filter(|id| active_ids.contains(*id))
            .count();
        let outstanding_count = outstanding_ids.len();

        let confirmed = self.confirm_prices(outstanding_ids.iter().cloned().collect()).await;

        let all_feeds: Vec<(Pair, String)> = PYTH_PRICE_FEEDS.iter()
            .map(|entry| entry.value().clone())
            .collect();

        if !confirmed.is_empty() {
            let pairs_to_register: Vec<Pair> = confirmed.iter()
                .filter_map(|id| {
                    all_feeds.iter()
                        .find(|(_, feed_id)| feed_id == id)
                        .map(|(pair, _)| pair.clone())
                })
                .collect();

            if !pairs_to_register.is_empty() {
                hub.register_websocket_pairs(Source::Pyth, &pairs_to_register);
                info!("[{}] Registered {} newly confirmed price feeds as WebSocket-active",
                    self.id, pairs_to_register.len());
            }
        }

        let total_active = already_active_count + confirmed.len();

        if total_active > 0 {
            if confirmed.is_empty() && outstanding_count > 0 {
                info!("[{}] Could not confirm {} new feeds, but {}/{} feeds remain active",
                    self.id, outstanding_count, total_active, self.price_feed_ids.len());
            } else if !confirmed.is_empty() {
                info!("[{}] Added {} new feeds. Total active: {}/{}",
                    self.id, confirmed.len(), total_active, self.price_feed_ids.len());
            } else {
                info!("[{}] All {} feeds already active", self.id, total_active);
            }

            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            self.subscription_completed_time.store(now, Ordering::Release);

            Ok(())
        } else {
            error!("[{}] No price feeds confirmed! Connection has 0 active feeds.", self.id);
            Err(anyhow!("No price feeds confirmed - connection has no active feeds"))
        }
    }

    async fn confirm_prices(&mut self, subscribed: HashSet<String>) -> HashSet<String> {
        info!("[{}] Waiting for price confirmations from {} feeds...", self.id, subscribed.len());

        self.id_confirmations.clear();
        for id in &subscribed {
            self.id_confirmations.insert(id.clone(), 0);
            debug!("[{}] Tracking confirmations for feed: {}", self.id, id);
        }

        tokio::time::sleep(Duration::from_millis(100)).await;

        info!("[{}] Starting read loop to process price messages", self.id);
        self.start_read_loop();

        let start = std::time::Instant::now();
        let timeout = Duration::from_secs(45);

        while start.elapsed() < timeout {
            tokio::time::sleep(Duration::from_millis(100)).await;

            let mut all_confirmed = true;
            let mut confirmed_count = 0;

            for entry in self.id_confirmations.iter() {
                if *entry.value() >= 3 {
                    confirmed_count += 1;
                } else {
                    all_confirmed = false;
                }
            }

            if all_confirmed && confirmed_count > 0 {
                info!("[{}] All {} price feeds confirmed with price data!", self.id, confirmed_count);
                break;
            }
        }

        let mut confirmed = HashSet::new();
        for entry in self.id_confirmations.iter() {
            let (id, count) = entry.pair();
            if *count >= 3 {
                confirmed.insert(id.clone());
            }
        }

        let failed: Vec<String> = subscribed.iter()
            .filter(|id| !confirmed.contains(*id))
            .cloned()
            .collect();

        if !failed.is_empty() {
            warn!("[{}] {} feeds did NOT receive 3 price updates",
                self.id, failed.len());
        }

        info!("[{}] {} out of {} feeds confirmed with price data",
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

        if let Some(mut entry) = crate::hub::CONNECTION_METADATA_CACHE.get_mut(&(Source::Pyth, self.id.clone())) {
            entry.is_connected = false;
        }

        if let Some(handle) = self.ping_task_handle.take() {
            handle.abort();
            debug!("[{}] Aborted ping task", self.id);
        }

        self.ws = None;
        self.read_loop_running.store(false, Ordering::Release);
        self.subscribed_ids.write().await.clear();
        self.id_confirmations.clear();

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
            let id_to_pair = self.id_to_pair.clone();
            let connection_id = self.id.clone();
            let message_count = self.message_count.clone();
            let last_rate_log_time = self.last_rate_log_time.clone();
            let last_rate_value = self.last_rate_value.clone();
            let read_loop_running = self.read_loop_running.clone();
            let monitor_trigger = self.monitor_trigger.clone();
            let id_confirmations = self.id_confirmations.clone();

            self.start_ping_task();

            crate::runtime_separation::spawn_on_ingestion_named(
                &format!("pyth-reader-{}", connection_id),
                async move {
                    info!("[{}] Starting read loop task", connection_id);
                    if let Err(e) = Self::read_loop(
                        connection_id.clone(),
                        ws,
                        tx,
                        is_connected.clone(),
                        last_message_time.clone(),
                        id_to_pair,
                        message_count,
                        last_rate_log_time,
                        last_rate_value,
                        id_confirmations,
                        monitor_trigger.clone(),
                    ).await {
                        error!("[{}] Pyth read loop error: {}", connection_id, e);
                        is_connected.store(false, Ordering::Release);

                        if let Some(mut entry) = crate::hub::CONNECTION_METADATA_CACHE.get_mut(&(Source::Pyth, connection_id.clone())) {
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
        ws: Arc<Mutex<PythWebSocket>>,
        tx: mpsc::UnboundedSender<Vec<Ticker>>,
        is_connected: Arc<AtomicBool>,
        last_message_time: Arc<AtomicU64>,
        id_to_pair: Arc<DashMap<String, Pair>>,
        message_count: Arc<AtomicU64>,
        last_rate_log_time: Arc<AtomicU64>,
        last_rate_value: Arc<AtomicU32>,
        id_confirmations: Arc<DashMap<String, u32>>,
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

                        if let Some(mut entry) = crate::hub::CONNECTION_METADATA_CACHE.get_mut(&(Source::Pyth, connection_id.clone())) {
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
                    if text.contains(r#""type":"pong"#) {
                        debug!("[{}] Received pong from Pyth", connection_id);
                        continue;
                    }

                    static MSG_COUNT: AtomicU64 = AtomicU64::new(0);
                    let count = MSG_COUNT.fetch_add(1, Ordering::Relaxed);
                    if count < 5 {
                        debug!("[{}] Pyth message {}: {}", connection_id, count, text);
                    }

                    if let Ok(msg) = serde_json::from_str::<PythStreamMessage>(&text) {
                        if msg.msg_type == "price_update" {
                            if let Some(price_feed) = msg.price_feed {
                                message_count.fetch_add(1, Ordering::Relaxed);

                                let now = std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .unwrap()
                                    .as_millis() as u64;
                                let last_log = last_rate_log_time.load(Ordering::Acquire);

                                if now - last_log >= 10_000 {
                                    let msg_count = message_count.load(Ordering::Acquire);
                                    let elapsed_ms = now - last_log;
                                    let rate = (msg_count as f64 * 1000.0) / elapsed_ms as f64;

                                    let rate_fixed = (rate * 100.0) as u32;
                                    last_rate_value.store(rate_fixed, Ordering::Release);

                                    if now - last_log >= 600_000 {
                                        message_count.store(0, Ordering::Release);

                                        if rate > 1.0 || msg_count > 10 {
                                            info!("[{}] Message rate: {:.1} msg/sec ({} messages in {:.1}s)",
                                                connection_id, rate, msg_count, elapsed_ms as f64 / 1000.0);
                                        }

                                        last_rate_log_time.store(now, Ordering::Release);
                                    }
                                }

                                // Track confirmations
                                let mut confirm_count = id_confirmations
                                    .entry(price_feed.id.clone())
                                    .or_insert(0);
                                *confirm_count += 1;

                                if *confirm_count == 3 {
                                    debug!("[{}] Price feed {} confirmed with 3+ messages",
                                        connection_id, price_feed.id);
                                }

                                // Process the price update
                                if let Some(price_data) = price_feed.price {
                                    if let Some(pair_entry) = id_to_pair.get(&price_feed.id) {
                                        let pair = pair_entry.value().clone();

                                        // Parse price: price_data.price is the raw value, expo is the exponent
                                        if let Ok(raw_price) = price_data.price.parse::<i64>() {
                                            // Pyth price = raw_price * 10^expo
                                            let price_f64 = raw_price as f64 * 10_f64.powi(price_data.expo);

                                            if let Ok(price) = Decimal::try_from(price_f64) {
                                                let tick = Ticker {
                                                    symbol: pair.as_str(),
                                                    price,
                                                    timestamp: (price_data.publish_time * 1000) as u64,
                                                };

                                                if tx.send(vec![tick]).is_err() {
                                                    error!("[{}] Failed to send Pyth ticker", connection_id);
                                                } else {
                                                    debug!("[{}] Sent Pyth ticker: {} @ {}",
                                                        connection_id, pair.as_str(), price);
                                                }
                                            } else {
                                                warn!("[{}] Failed to convert Pyth price to Decimal: {}",
                                                    connection_id, price_f64);
                                            }
                                        } else {
                                            warn!("[{}] Failed to parse Pyth price: {}",
                                                connection_id, price_data.price);
                                        }
                                    } else {
                                        warn!("[{}] Pyth price feed ID not found in cache: {}",
                                            connection_id, price_feed.id);
                                    }
                                }
                            }
                        }
                    } else if count < 10 {
                        warn!("[{}] Failed to parse Pyth message: {}", connection_id, text);
                    }
                }
                OpCode::Close => {
                    info!("[{}] WebSocket closed", connection_id);
                    is_connected.store(false, Ordering::Release);

                    if let Some(mut entry) = crate::hub::CONNECTION_METADATA_CACHE.get_mut(&(Source::Pyth, connection_id.clone())) {
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

pub struct PythConnectionPair {
    pub id: String,
    pub connection: Arc<RwLock<PythConnection>>,
    pub price_feed_ids: HashSet<String>,
    monitoring_task: Option<tokio::task::JoinHandle<()>>,
}

impl PythConnectionPair {
    pub fn new(
        id: String,
        price_feed_ids: HashSet<String>,
        id_to_pair: Arc<DashMap<String, Pair>>,
        tx: mpsc::UnboundedSender<Vec<Ticker>>,
    ) -> Self {
        let connection = PythConnection::new(
            id.clone(),
            price_feed_ids.clone(),
            id_to_pair,
            tx,
        );

        Self {
            id,
            connection: Arc::new(RwLock::new(connection)),
            price_feed_ids,
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

        let connection = self.connection.clone();
        let connection_id = self.id.clone();
        let price_feed_ids = self.price_feed_ids.clone();

        let monitoring_task = crate::runtime_separation::spawn_on_ingestion_named(
            &format!("pyth-monitor-{connection_id}"),
            async move {
                Self::monitor_connection(
                    connection_id,
                    connection,
                    price_feed_ids,
                ).await;
            });

        self.monitoring_task = Some(monitoring_task);

        Ok(())
    }

    async fn monitor_connection(
        connection_id: String,
        connection: Arc<RwLock<PythConnection>>,
        price_feed_ids: HashSet<String>,
    ) {
        let mut check_interval = tokio::time::interval(Duration::from_secs(HEALTH_CHECK_INTERVAL_SECS));
        let mut feeds_in_rest_failover = false;
        let mut last_reconnect_attempt: Option<std::time::Instant> = Some(std::time::Instant::now());
        const RECONNECT_INTERVAL_SECS: u64 = 300;

        let monitor_trigger = {
            let conn = connection.read().await;
            conn.monitor_trigger.clone()
        };

        loop {
            tokio::select! {
                _ = check_interval.tick() => {},
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

                if unhealthy && !feeds_in_rest_failover {
                    warn!("[{}] Connection unhealthy! Latency: {}ms, Rate: {:.1} msg/sec. Moving {} feeds to REST API",
                        connection_id, latency_ms, message_rate, price_feed_ids.len());

                    {
                        let mut conn = connection.write().await;
                        conn.is_connected.store(false, Ordering::Release);
                        conn.last_message_time.store(
                            std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap()
                                .as_millis() as u64,
                            Ordering::Release
                        );
                        conn.ws = None;
                        conn.subscribed_ids.write().await.clear();
                    }

                    let pairs_to_clear: Vec<Pair> = price_feed_ids.iter()
                        .filter_map(|id| {
                            PYTH_PRICE_FEEDS.iter()
                                .find(|entry| &entry.value().1 == id)
                                .map(|entry| entry.value().0.clone())
                        })
                        .collect();

                    if !pairs_to_clear.is_empty() {
                        let hub = crate::hub::SurgeHub::global();
                        hub.remove_websocket_pairs(Source::Pyth, &pairs_to_clear);
                        info!("[{}] Removed {} feeds from WebSocket tracking for REST fallback",
                            connection_id, pairs_to_clear.len());
                    }

                    feeds_in_rest_failover = true;
                }
            } else if !is_connected && !feeds_in_rest_failover {
                error!("[{}] Connection DOWN! Moving {} feeds to REST API",
                    connection_id, price_feed_ids.len());

                let pairs_to_clear: Vec<Pair> = price_feed_ids.iter()
                    .filter_map(|id| {
                        PYTH_PRICE_FEEDS.iter()
                            .find(|entry| &entry.value().1 == id)
                            .map(|entry| entry.value().0.clone())
                    })
                    .collect();

                if !pairs_to_clear.is_empty() {
                    let hub = crate::hub::SurgeHub::global();
                    hub.remove_websocket_pairs(Source::Pyth, &pairs_to_clear);
                    info!("[{}] Removed {} feeds from WebSocket tracking for REST fallback",
                        connection_id, pairs_to_clear.len());
                }

                feeds_in_rest_failover = true;
            }

            if feeds_in_rest_failover {
                let should_reconnect = match last_reconnect_attempt {
                    None => true,
                    Some(last_attempt) => last_attempt.elapsed().as_secs() >= RECONNECT_INTERVAL_SECS,
                };

                if should_reconnect {
                    info!("[{}] Attempting to reconnect WebSocket connection...", connection_id);
                    last_reconnect_attempt = Some(std::time::Instant::now());

                    let mut conn = connection.write().await;
                    conn.cleanup().await;
                    conn.price_feed_ids = price_feed_ids.clone();

                    match conn.connect().await {
                        Ok(_) => {
                            info!("[{}] WebSocket reconnected successfully", connection_id);

                            match conn.subscribe_gradually().await {
                                Ok(_) => {
                                    info!("[{}] Successfully re-subscribed to feeds after reconnection", connection_id);
                                    feeds_in_rest_failover = false;
                                }
                                Err(e) => {
                                    error!("[{}] Failed to re-subscribe after reconnection: {}", connection_id, e);
                                }
                            }
                        }
                        Err(e) => {
                            warn!("[{}] Failed to reconnect WebSocket: {}", connection_id, e);
                        }
                    }
                }
            }
        }
    }
}

pub struct PythStream {
    connection_pairs: Vec<Arc<Mutex<PythConnectionPair>>>,
    rx: mpsc::UnboundedReceiver<Result<Ticker>>,
    tx: mpsc::UnboundedSender<Vec<Ticker>>,
    id_to_pair_cache: Arc<DashMap<String, Pair>>,
    connection_state: ConnectionState,
}

impl PythStream {
    pub async fn new() -> Result<Self> {
        let (tx, rx) = mpsc::unbounded_channel();
        let (internal_tx, mut internal_rx) = mpsc::unbounded_channel::<Vec<Ticker>>();

        let external_tx = tx.clone();
        let id_to_pair_cache = Arc::new(DashMap::new());
        let id_to_pair_cache_for_spawn = id_to_pair_cache.clone();

        crate::runtime_separation::spawn_on_ingestion_named(
            "pyth-aggregator",
            async move {
                info!("Starting Pyth aggregator task");
                let mut count = 0u64;
                let mut last_log_count = 0u64;
                while let Some(tickers) = internal_rx.recv().await {
                    for ticker in tickers {
                        count += 1;
                        if count <= 5 {
                            info!("🟣 Pyth aggregator: Ticker #{}: {} @ {}",
                                count, ticker.symbol, ticker.price);
                        }
                        // Log every 100 updates for monitoring
                        if count - last_log_count >= 100 {
                            info!("🟣 Pyth streaming: Processed {} total price updates", count);
                            last_log_count = count;
                        }
                        let _ = external_tx.send(Ok(ticker.clone()));
                        if let Err(e) = process_ticker(&id_to_pair_cache_for_spawn, ticker).await {
                            error!("Failed to process Pyth ticker: {:?}", e);
                        }
                    }
                }
            }
        );

        let mut stream = Self {
            connection_pairs: Vec::new(),
            rx,
            tx: internal_tx,
            id_to_pair_cache,
            connection_state: ConnectionState::new(),
        };
        stream.connect().await?;
        Ok(stream)
    }

    pub async fn prefetch_exchange_info() -> Result<()> {
        if PYTH_PRICE_FEEDS.is_empty() {
            let _ = Self::fetch_and_cache_price_feeds().await?;
        }
        Ok(())
    }

    pub async fn refresh_exchange_info() -> Result<usize> {
        Self::fetch_and_cache_price_feeds().await
    }

    async fn connect(&mut self) -> Result<()> {
        info!("Creating Pyth WebSocket connections");

        let (websocket_feeds, _) = Self::select_top_feeds().await?;

        let all_feeds = Self::get_all_feeds().await?;
        let all_feeds_set: HashSet<String> = all_feeds.into_iter().collect();

        info!("Pyth startup: REST polling {} feeds initially, WebSocket will handle {} high-priority feeds",
            all_feeds_set.len(), websocket_feeds.len());

        let rest_manager = crate::rest_manager::RestManager::global();
        rest_manager.set_rest_symbols(Source::Pyth, all_feeds_set).await;

        // Map feed IDs to pairs for the connection
        for feed_id in &websocket_feeds {
            if let Some(entry) = PYTH_PRICE_FEEDS.iter().find(|e| &e.value().1 == feed_id) {
                self.id_to_pair_cache.insert(feed_id.clone(), entry.value().0.clone());
            }
        }

        let feeds_per_connection = 50;
        let feed_chunks: Vec<_> = websocket_feeds.chunks(feeds_per_connection)
            .map(|chunk| chunk.iter().cloned().collect::<HashSet<String>>())
            .collect();

        for (i, feeds) in feed_chunks.into_iter().enumerate() {
            let conn_id = format!("Pyth-{}", i + 1);
            let pair = Arc::new(Mutex::new(PythConnectionPair::new(
                conn_id.clone(),
                feeds.clone(),
                self.id_to_pair_cache.clone(),
                self.tx.clone(),
            )));
            self.connection_pairs.push(pair);

            crate::hub::CONNECTION_METADATA_CACHE.insert(
                (Source::Pyth, conn_id),
                crate::hub::ConnectionMetadata {
                    symbols: feeds.iter().cloned().collect(),
                    is_connected: false,
                    last_updated: crate::clock_sync::get_corrected_timestamp_ms(),
                }
            );
        }

        info!("Starting {} Pyth connection pairs", self.connection_pairs.len());
        for (i, pair) in self.connection_pairs.iter().enumerate() {
            let mut conn = pair.lock().await;
            if let Err(e) = conn.start().await {
                error!("Failed to start Pyth connection pair {}: {}", i, e);
            }

            if i < self.connection_pairs.len() - 1 {
                tokio::time::sleep(Duration::from_secs(CONNECTION_DELAY_SECS)).await;
            }
        }

        self.start_connection_monitor();

        Ok(())
    }

    async fn get_all_feeds() -> Result<Vec<String>> {
        if PYTH_PRICE_FEEDS.is_empty() {
            let _ = Self::fetch_and_cache_price_feeds().await?;
        }

        let all_feeds: Vec<String> = PYTH_PRICE_FEEDS.iter()
            .map(|entry| entry.value().1.clone())
            .collect();

        Ok(all_feeds)
    }

    async fn select_top_feeds() -> Result<(Vec<String>, HashSet<String>)> {
        if PYTH_PRICE_FEEDS.is_empty() {
            let _ = Self::fetch_and_cache_price_feeds().await?;
        }

        // Priority pairs - PYUSD, USDS, USDG stablecoins
        // Note: Pyth only provides USD pairs for these stablecoins
        let priority_symbols = vec![
            "PYUSD/USD",
            "USDS/USD",
            "USDG/USD"
        ];

        let all_feeds: Vec<(String, String)> = PYTH_PRICE_FEEDS.iter()
            .map(|entry| {
                let pair_symbol = entry.value().0.as_str();
                let feed_id = entry.value().1.clone();
                (feed_id, pair_symbol)
            })
            .collect();

        info!("Using {} PYUSD/USDS/USDG price feeds from Pyth", all_feeds.len());

        let mut websocket_feeds = Vec::new();
        let mut used = HashSet::new();

        // Add all available feeds (should only be PYUSD, USDS, USDG pairs)
        for priority_symbol in priority_symbols {
            if let Some((feed_id, _)) = all_feeds.iter().find(|(_, symbol)| symbol == &priority_symbol) {
                websocket_feeds.push(feed_id.clone());
                used.insert(feed_id.clone());
                info!("Pyth feed added: {}", priority_symbol);
            }
        }

        // Add any remaining feeds that weren't in priority list
        for (feed_id, symbol) in all_feeds {
            if !used.contains(&feed_id) {
                websocket_feeds.push(feed_id.clone());
                used.insert(feed_id.clone());
                info!("Additional Pyth feed added: {}", symbol);
            }
        }

        let rest_feeds: HashSet<String> = PYTH_PRICE_FEEDS.iter()
            .map(|entry| entry.value().1.clone())
            .filter(|id| !used.contains(id))
            .collect();

        Ok((websocket_feeds, rest_feeds))
    }

    fn start_connection_monitor(&self) {
        let connection_pairs = self.connection_pairs.clone();
        let connection_state = self.connection_state.clone();

        crate::runtime_separation::spawn_on_ingestion_named(
            "pyth-connection-monitor",
            async move {
                let mut interval = tokio::time::interval(Duration::from_secs(5));

                loop {
                    interval.tick().await;

                    let mut any_connected = false;

                    for conn_pair in &connection_pairs {
                        if let Ok(conn) = conn_pair.try_lock() {
                            if !conn.price_feed_ids.is_empty() {
                                any_connected = true;
                                break;
                            }
                        }
                    }

                    connection_state.set_connected(any_connected);
                }
            });
    }

    async fn fetch_and_cache_price_feeds() -> Result<usize> {
        info!("Fetching Pyth price feeds");

        // Fetch available price feeds from Pyth Hermes API
        // Note: The actual Hermes API endpoint for listing feeds may vary
        // This is a placeholder - you'll need to adjust based on actual Pyth API
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()?;

        // Pyth uses a different API structure - this is a simplified example
        // You may need to adjust this based on actual Pyth/Hermes API documentation
        let response = client
            .get(format!("{}/api/price_feed_ids", PYTH_REST_ENDPOINT))
            .send()
            .await?;

        let feed_ids: Vec<String> = response.json().await.unwrap_or_default();

        let allowed_quotes = get_allowed_quotes(Source::Pyth);
        let is_initial_fetch = PYTH_PRICE_FEEDS.is_empty();

        if is_initial_fetch {
            info!("Allowed quote currencies for Pyth: {:?}", allowed_quotes);
        }

        // Only track PYUSD, USDS, and USDG price feeds from Pyth
        // Feed IDs from Pyth Hermes API (https://hermes.pyth.network/v2/price_feeds?asset_type=crypto)
        let well_known_feeds: HashMap<String, (String, String)> = {
            let feeds: &[(&str, (&str, &str))] = &[
                // PYUSD/USD - PayPal USD
                ("c1da1b73d7f01e7ddd54b3766cf7fcd644395ad14f70aa706ec5384c59e76692", ("PYUSD", "USD")),

                // USDS/USD - USD Sky Protocol (formerly DAI Savings Rate)
                ("77f0971af11cc8bac224917275c1bf55f2319ed5c654a1ca955c82fa2d297ea1", ("USDS", "USD")),

                // USDG/USD - Global Dollar
                ("daa58c6a3ce7d4b9c46c32a6e646012c17c4a2b24c08dd8c5e476118b855a7da", ("USDG", "USD")),

                // Note: Pyth only provides USD pairs for these stablecoins (no USDT or USDC pairs)
            ];
            feeds.iter().map(|(id, (base, quote))| (id.to_string(), (base.to_string(), quote.to_string()))).collect()
        };

        // Filter for only PYUSD, USDS, and USDG base assets
        const ALLOWED_BASE_ASSETS: &[&str] = &["PYUSD", "USDS", "USDG"];

        let mut new_feeds = 0;

        for (feed_id, (base, quote)) in well_known_feeds {
            // Only process if base asset is in our allowed list
            if !ALLOWED_BASE_ASSETS.contains(&base.as_str()) {
                continue;
            }

            let pair = Pair {
                base: base.clone(),
                quote: quote.clone(),
            };

            if allowed_quotes.contains(&pair.quote.as_str()) {
                let symbol = pair.as_str();
                if !PYTH_PRICE_FEEDS.contains_key(&symbol) {
                    PYTH_PRICE_FEEDS.insert(symbol.clone(), (pair, feed_id));
                    new_feeds += 1;

                    if !is_initial_fetch {
                        info!("Discovered new Pyth feed: {}", symbol);
                    }
                }
            }
        }

        if is_initial_fetch {
            info!("Cached {} Pyth price feeds", new_feeds);
        } else if new_feeds > 0 {
            info!("Added {} new Pyth feeds to cache", new_feeds);
        }

        Ok(new_feeds)
    }
}

impl TickerStream for PythStream {
    fn listen(&mut self, _pair: Pair) -> Result<()> {
        Ok(())
    }

    fn unlisten(&mut self, _pair: &Pair) -> Result<()> {
        Ok(())
    }

    fn subscriptions(&self) -> Vec<Pair> {
        let mut all_pairs = Vec::new();
        for conn_pair in &self.connection_pairs {
            if let Ok(conn) = conn_pair.try_lock() {
                for feed_id in &conn.price_feed_ids {
                    if let Some(pair) = self.id_to_pair_cache.get(feed_id) {
                        all_pairs.push(pair.value().clone());
                    }
                }
            }
        }
        all_pairs
    }

    fn is_connected(&self) -> bool {
        self.connection_state.is_connected()
    }
}

async fn process_ticker(_id_to_pair_cache: &Arc<DashMap<String, Pair>>, ticker: Ticker) -> Result<()> {
    if let Ok(pair) = Pair::from_task(&ticker.symbol) {
        let now = crate::clock_sync::get_corrected_timestamp_ms();
        let tick = crate::types::Tick {
            price: ticker.price,
            event_ts: ticker.timestamp,
            seen_at: now,
            verified_at: now,
        };

        let hub = crate::hub::SurgeHub::global();
        hub.update(Source::Pyth, pair.clone(), tick).await;

        // Log price updates for monitoring (first 10, then every 100th)
        static UPDATE_COUNT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let count = UPDATE_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if count < 10 || count % 100 == 0 {
            info!("🟣 Pyth WebSocket: {} = ${} (update #{})",
                pair.as_str(), ticker.price, count + 1);
        }
    } else {
        warn!("Failed to parse Pyth ticker symbol: {}", ticker.symbol);
    }

    Ok(())
}

impl Stream for PythStream {
    type Item = Result<Ticker>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    #[tokio::test]
    #[ignore]
    async fn test_pyth_stream_pyusd() {
        println!("\n🔄 Starting Pyth PYUSD streaming test...\n");

        // Create the stream (automatically connects)
        println!("📡 Connecting to Pyth WebSocket...");
        println!("   Endpoint: {}", PYTH_HERMES_ENDPOINT);

        let mut stream = PythStream::new().await.expect("Failed to create Pyth stream");

        println!("✅ Connected to Pyth!\n");

        // Give it a moment to subscribe
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        // List subscribed pairs
        let subscribed = stream.subscriptions();
        println!("Watching {} pairs:", subscribed.len());
        for pair in &subscribed {
            println!("  • {}", pair.as_str());
        }
        println!();

        // Stream prices for 30 seconds
        println!("📊 Streaming PYUSD prices (will run for 30 seconds)...\n");

        let mut pyusd_updates = 0;
        let mut total_updates = 0;
        let start = std::time::Instant::now();
        let duration = std::time::Duration::from_secs(30);

        while start.elapsed() < duration {
            tokio::select! {
                Some(result) = stream.next() => {
                    match result {
                        Ok(ticker) => {
                            total_updates += 1;
                            if ticker.symbol.contains("PYUSD") {
                                pyusd_updates += 1;
                                println!("💰 PYUSD Update #{}: {} = ${} @ {}",
                                    pyusd_updates,
                                    ticker.symbol,
                                    ticker.price,
                                    ticker.timestamp
                                );
                            }
                        }
                        Err(e) => {
                            println!("⚠️  Error receiving ticker: {}", e);
                        }
                    }
                }
                _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {
                    // Continue loop
                }
            }
        }

        println!("\n✅ Test completed!");
        println!("   Total updates: {}", total_updates);
        println!("   PYUSD updates: {}", pyusd_updates);

        if pyusd_updates > 0 {
            println!("   ✅ Successfully received PYUSD price updates from Pyth!");
        } else {
            println!("   ⚠️  No PYUSD updates received (may not be in top feeds)");
        }
    }

    #[tokio::test]
    #[ignore]
    async fn test_pyth_stream_all_stablecoins() {
        println!("\n🔄 Starting Pyth stablecoin streaming test (PYUSD, USDS, USDG)...\n");

        // Create the stream (automatically connects)
        println!("📡 Connecting to Pyth WebSocket...");
        println!("   Endpoint: {}", PYTH_HERMES_ENDPOINT);

        let mut stream = PythStream::new().await.expect("Failed to create Pyth stream");

        println!("✅ Connected to Pyth!\n");

        // Give it a moment to subscribe
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        // List subscribed pairs
        let subscribed = stream.subscriptions();
        println!("Watching {} pairs:", subscribed.len());
        for pair in &subscribed {
            println!("  • {}", pair.as_str());
        }
        println!();

        // Stream prices for 30 seconds
        println!("📊 Streaming all stablecoin prices (will run for 30 seconds)...\n");

        let mut pyusd_updates = 0;
        let mut usds_updates = 0;
        let mut usdg_updates = 0;
        let mut total_updates = 0;
        let start = std::time::Instant::now();
        let duration = std::time::Duration::from_secs(30);

        while start.elapsed() < duration {
            tokio::select! {
                Some(result) = stream.next() => {
                    match result {
                        Ok(ticker) => {
                            total_updates += 1;

                            if ticker.symbol.contains("PYUSD") {
                                pyusd_updates += 1;
                                println!("💵 PYUSD Update #{}: {} = ${} @ {}",
                                    pyusd_updates,
                                    ticker.symbol,
                                    ticker.price,
                                    ticker.timestamp
                                );
                            } else if ticker.symbol.contains("USDS") {
                                usds_updates += 1;
                                println!("💎 USDS Update #{}: {} = ${} @ {}",
                                    usds_updates,
                                    ticker.symbol,
                                    ticker.price,
                                    ticker.timestamp
                                );
                            } else if ticker.symbol.contains("USDG") {
                                usdg_updates += 1;
                                println!("🌍 USDG Update #{}: {} = ${} @ {}",
                                    usdg_updates,
                                    ticker.symbol,
                                    ticker.price,
                                    ticker.timestamp
                                );
                            }
                        }
                        Err(e) => {
                            println!("⚠️  Error receiving ticker: {}", e);
                        }
                    }
                }
                _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {
                    // Continue loop
                }
            }
        }

        println!("\n✅ Test completed!");
        println!("   Total updates: {}", total_updates);
        println!("   PYUSD updates: {} 💵", pyusd_updates);
        println!("   USDS updates:  {} 💎", usds_updates);
        println!("   USDG updates:  {} 🌍", usdg_updates);
        println!();

        if pyusd_updates > 0 && usds_updates > 0 && usdg_updates > 0 {
            println!("   ✅ Successfully received updates from all 3 stablecoins!");
        } else {
            println!("   ⚠️  Missing updates from some stablecoins:");
            if pyusd_updates == 0 { println!("      - PYUSD: 0 updates"); }
            if usds_updates == 0 { println!("      - USDS: 0 updates"); }
            if usdg_updates == 0 { println!("      - USDG: 0 updates"); }
        }
    }
}
