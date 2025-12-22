use crate::messages::ServerMessage;
use dashmap::DashMap;
use once_cell::sync::Lazy;
use rust_decimal::Decimal;
use solana_sdk::pubkey::Pubkey;
use std::collections::HashSet;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Instant;
use switchboard_surge_core::{Pair, Source, Tick};
use tokio::sync::{mpsc, watch};

/// Global connection state
pub static CONNECTIONS: Lazy<DashMap<String, Arc<ConnectionState>>> = Lazy::new(DashMap::new);

/// Per-feed price tracking for individual subscriptions
pub static FEED_PRICE_TRACKING: Lazy<DashMap<(String, Source, Pair), FeedPriceState>> =
    Lazy::new(DashMap::new);

/// State for a single WebSocket connection
pub struct ConnectionState {
    pub id: String,
    pub api_key_hash: String,
    pub user_pubkey: Option<Pubkey>,
    /// Signing pubkey for signature verification (may differ from subscription owner for authorized users)
    pub signing_pubkey: Option<Pubkey>,
    pub tx: mpsc::UnboundedSender<ServerMessage>,
    pub subscriptions: DashMap<(Source, Pair), watch::Receiver<Tick>>,
    /// Cancellation flag for clean shutdown
    pub cancelled: Arc<AtomicBool>,
    /// Batch interval for this connection (in milliseconds)
    pub default_batch_interval_ms: Arc<std::sync::atomic::AtomicU64>,
    /// Oracle configuration for signing responses
    pub oracle_config: Arc<crate::consensus::OracleConfig>,
    /// Last signature check timestamp in milliseconds (for periodic verification)
    pub last_signature_check_ms: Arc<std::sync::atomic::AtomicU64>,
}

/// Per-feed price state for change detection
#[derive(Debug, Clone)]
pub struct FeedPriceState {
    pub last_sent_price: Option<Decimal>,
    pub last_sent_time: Instant,
    pub last_check_time: Instant,
}

impl Default for FeedPriceState {
    fn default() -> Self {
        Self {
            last_sent_price: None,
            last_sent_time: Instant::now(),
            last_check_time: Instant::now(),
        }
    }
}

/// Connection timing information for efficient batch dispatch
#[derive(Debug, Clone)]
pub struct ConnectionTiming {
    pub interval_ms: u64,
    pub last_sent: Instant,
}

/// Track timing for all connections
pub static CONNECTION_TIMINGS: Lazy<DashMap<String, ConnectionTiming>> = Lazy::new(DashMap::new);

/// Price update data
#[derive(Debug, Clone)]
pub struct PriceUpdate {
    pub source: Source,
    pub pair: Pair,
    pub price: Decimal,
    pub event_ts: u64,
    pub seen_at: u64,
    pub verified_at: u64,  // Track when price was last verified as current
}

/// Cache of latest prices for all feeds
pub static LATEST_PRICES: Lazy<DashMap<(Source, Pair), PriceUpdate>> = Lazy::new(DashMap::new);

/// Track which shared monitors are running and their subscriber count
pub struct SharedMonitorState {
    pub cancelled: Arc<AtomicBool>,
    pub subscriber_count: Arc<std::sync::atomic::AtomicUsize>,
}

/// Track shared monitors with subscriber counts
pub static SHARED_MONITORS: Lazy<DashMap<(Source, Pair), SharedMonitorState>> =
    Lazy::new(DashMap::new);

/// Flag to track if global dispatcher is running
pub static GLOBAL_DISPATCHER_RUNNING: Lazy<Arc<AtomicBool>> =
    Lazy::new(|| Arc::new(AtomicBool::new(false)));

/// Reverse index: pubkey → connection IDs (for signature auth tracking)
pub static PUBKEY_CONNECTIONS: Lazy<DashMap<Pubkey, HashSet<String>>> =
    Lazy::new(DashMap::new);
