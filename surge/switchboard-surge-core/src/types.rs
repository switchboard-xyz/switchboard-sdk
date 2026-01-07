use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Identify an exchange source
#[derive(Clone, Copy, Hash, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub enum Source {
    Binance,
    Bybit,
    Okx,
    Coinbase,
    Bitget,
    Gate,
    Pyth,
    Titan,
    Weighted, // Kept for backwards compatibility, will use AUTO logic internally
    Auto,     // New intelligent source selection based on volume and trade count
}

impl Source {
    pub fn as_str(&self) -> &'static str {
        match self {
            Source::Binance => "Binance",
            Source::Bybit => "Bybit",
            Source::Okx => "OKX",
            Source::Coinbase => "Coinbase",
            Source::Bitget => "Bitget",
            Source::Gate => "Gate.io",
            Source::Pyth => "Pyth",
            Source::Titan => "Titan",
            Source::Weighted => "WEIGHTED",
            Source::Auto => "AUTO",
        }
    }
}

/// A price tick from an exchange
#[derive(Clone, Copy, Debug)]
pub struct Tick {
    pub price: Decimal,
    pub event_ts: u64,    // Exchange timestamp
    pub seen_at: u64,     // Local timestamp when received
    pub verified_at: u64, // When last verified as current (even if price unchanged)
}

impl Default for Tick {
    fn default() -> Self {
        let now = crate::clock_sync::get_corrected_timestamp_ms();
        Self {
            price: Decimal::ZERO,
            event_ts: now,
            seen_at: now,
            verified_at: now,
        }
    }
}

/// Common ticker structure from exchanges
#[derive(Debug, Clone)]
pub struct Ticker {
    pub symbol: String,
    pub price: Decimal,
    pub timestamp: u64,
}

/// Type alias for price entries
pub type MapKey = (Source, crate::pair::Pair);
pub type PxEntry = (Tick, tokio::sync::watch::Sender<Tick>);

/// WebSocket connection health information for monitoring
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct WebSocketConnectionHealth {
    pub exchange: String,
    pub connection_type: String,  // "Priority", "HighVolume", "Regular1", etc.
    pub total_symbols: usize,
    pub websocket_active_symbols: usize,  // Actually on WebSocket
    pub rest_fallback_symbols: usize,     // Fallen back to REST
    pub msg_per_sec: f64,
    pub latency_ms: u64,
    pub status: String,  // "healthy", "degraded", "partial_rest_fallback", "all_rest_fallback", "disconnected"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rest_symbols: Option<Vec<String>>,  // Which symbols are on REST (for debugging)
}

/// Exchange-level WebSocket health aggregation
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ExchangeWebSocketHealth {
    pub total_websocket_symbols: usize,   // Across all connections
    pub total_rest_symbols: usize,        // Fallen back to REST
    pub connections: Vec<WebSocketConnectionHealth>,
}

/// Priority pairs WebSocket status summary
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PriorityPairsStatus {
    pub total_priority_pairs: usize,
    pub websocket_active: usize,
    pub rest_fallback: usize,
    pub pairs: HashMap<String, PriorityPairDetail>,  // "BTC/USDT" -> detailed info
}

/// Detailed status for a priority pair
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PriorityPairDetail {
    pub status: String,  // "ws_active", "ws_rest_failover", "rest"
    pub exchange: Option<String>,  // Which exchange it's on
    pub msg_per_sec: f64,  // Message rate from MESSAGE_RATE_CACHE
}

/// Detail for a requested feed (shows resolution and health)
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RequestedFeedDetail {
    pub original_request: String,  // "BTC/USD" or "Binance:BTC/USDT" - what was requested
    pub resolved_to: String,        // "BTC/FDUSD" - actual pair used
    pub exchange: String,           // "Binance" - which exchange
    pub msg_per_sec: f64,          // Message rate for this feed
    pub status: String,             // "ws_active", "ws_rest_failover", "rest"
    /// Secondary exchange for validation (optional, only present for AUTO-resolved feeds with secondary)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub secondary_exchange: Option<String>,
    /// Secondary pair on the secondary exchange (optional)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub secondary_pair: Option<String>,
    /// Message rate for secondary feed (optional)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub secondary_msg_per_sec: Option<f64>,
}
