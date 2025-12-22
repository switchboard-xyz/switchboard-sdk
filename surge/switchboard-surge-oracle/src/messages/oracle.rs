use base64_serde::base64_serde_type;
use serde::{Deserialize, Serialize};
use switchboard_surge_core::Pair;

base64_serde_type!(Base64Standard, base64::engine::general_purpose::STANDARD);

// Signature schemes for oracle signing (matches rust-feeds-oracle)
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq)]
pub enum SignatureScheme {
    Secp256k1 = 0,
    Ed25519 = 1,
    Bls = 2,
}

// Default to Ed25519 for new connections
impl Default for SignatureScheme {
    fn default() -> Self {
        SignatureScheme::Ed25519
    }
}

// Hashing schemes (for completeness, though we'll use Keccak256)
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum HashingScheme {
    Sha256,
    Keccak256,
}

/// Connection status types
#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum ConnectionStatusType {
    Connected,
    Disconnected,
    Reconnecting,
    DataStale,
    DataRestored,
}

/// Client -> Oracle messages (keeping existing format for compatibility)
#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
pub enum StreamMessage {
    Authenticate {
        #[serde(default)]
        api_key: String,
        session_token: String,
        // Signature-based auth fields (for subscription mode)
        #[serde(skip_serializing_if = "Option::is_none")]
        pubkey: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        signature: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        blockhash: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        timestamp: Option<u64>,
    },
    Subscribe {
        feed_bundles: Vec<FeedBundleRequest>,
        batch_interval_ms: Option<u64>,
        // Client-specified signature scheme (defaults to Ed25519 if not provided)
        #[serde(default)]
        signature_scheme: SignatureScheme,
        // Signature-based auth fields (required for subscription mode)
        #[serde(skip_serializing_if = "Option::is_none")]
        pubkey: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        signature: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        blockhash: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        timestamp: Option<u64>,
    },
    Unsubscribe {
        feed_bundle_ids: Vec<String>,
        // Signature-based auth fields (required for subscription mode)
        #[serde(skip_serializing_if = "Option::is_none")]
        pubkey: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        signature: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        blockhash: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        timestamp: Option<u64>,
    },
    Ping {
        // Signature-based auth fields (optional, included every 5 minutes)
        #[serde(skip_serializing_if = "Option::is_none")]
        pubkey: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        signature: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        blockhash: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        timestamp: Option<u64>,
    },
    Pong {
        // Signature-based auth fields (required when server requested signature)
        #[serde(skip_serializing_if = "Option::is_none")]
        pubkey: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        signature: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        blockhash: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        timestamp: Option<u64>,
    },
    /// Response to SignedPing with signature verification
    SignedPong {
        pubkey: String,
        signature: String,
        blockhash: String,
        timestamp: u64,
    },
}

/// Feed bundle request from client
#[derive(Debug, Deserialize, Clone)]
pub struct FeedBundleRequest {
    pub feeds: Vec<FeedPair>,
}

/// Individual feed in a bundle
#[derive(Debug, Deserialize, Clone)]
pub struct FeedPair {
    pub symbol: Pair,
    pub source: String,
}

/// Oracle -> Client messages (keeping existing format for compatibility)
#[derive(Debug, Serialize)]
#[serde(tag = "type")]
pub enum StreamResponse {
    Authenticated {
        message: String,
    },
    Subscribed {
        feed_bundles: Vec<FeedBundleInfo>,
    },
    Unsubscribed {
        feed_bundle_ids: Vec<String>,
    },
    BundledFeedUpdate {
        feed_bundle_id: String,
        feed_values: Vec<ConsensusStreamFeedValue>,
        oracle_response: StreamConsensusResponse,
        // New field: When oracle decided to broadcast (after signing)
        broadcast_ts_ms: u64,
        // Deprecated fields (for backwards compatibility with older SDKs)
        source_ts_ms: u64,
        seen_at_ts_ms: u64,
        triggered_on_price_change: bool,
        /// Timestamp when heartbeat was triggered (only set for heartbeat updates)
        /// Used by clients to measure SW latency for heartbeats
        #[serde(skip_serializing_if = "Option::is_none")]
        heartbeat_at_ts_ms: Option<u64>,
    },
    Error {
        message: String,
    },
    ConnectionStatus {
        source: String,
        symbol: String,
        status: ConnectionStatusType,
        message: String,
        timestamp_ms: u64,
    },
    ValidationError {
        invalid_feeds: Vec<FeedValidationError>,
    },
    Pong,
    /// Periodic signature verification ping (sent by server every 5 minutes)
    SignedPing,
    AllPricesSnapshot {
        feeds: Vec<UnsignedFeedUpdate>,
        timestamp_ms: u64,
    },
    SubscribedToAll {
        sources: Option<Vec<String>>,
    },
}

/// Feed bundle info in responses
#[derive(Debug, Serialize, Clone)]
pub struct FeedBundleInfo {
    pub feed_bundle_id: String,
    pub feeds: Vec<FeedInfo>,
}

/// Feed info in responses
#[derive(Debug, Serialize, Clone)]
pub struct FeedInfo {
    pub symbol: String,
    pub source: String,
}

/// Feed value with consensus data
#[derive(Debug, Serialize, Clone)]
pub struct ConsensusStreamFeedValue {
    pub value: String,
    pub feed_hash: String,
    pub symbol: String,
    pub source: String,
    pub event_ts: u64,      // Exchange timestamp
    pub seen_at: u64,       // Oracle reception time
    pub verified_at: u64,   // Oracle verification time
}

/// Consensus response with signature
#[derive(Debug, Serialize)]
pub struct StreamConsensusResponse {
    pub oracle_pubkey: String,
    pub eth_address: String,
    pub signature: String,
    #[serde(with = "Base64Standard")]
    pub checksum: Vec<u8>,
    pub recovery_id: u8,
    pub oracle_idx: u8,
    pub timestamp: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp_ms: Option<u64>,
    #[serde(default)]
    pub recent_hash: String,
    #[serde(default)]
    pub slot: u64,
    // Ed25519 enclave signer pubkey (for Ed25519 signature verification)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ed25519_enclave_signer: Option<String>,
}

/// Unsigned feed update for snapshots
#[derive(Debug, Serialize)]
pub struct UnsignedFeedUpdate {
    pub symbol: String,
    pub source: String,
    pub price: String,
    pub source_ts_ms: u64,
}

/// Feed validation error
#[derive(Debug, Serialize)]
pub struct FeedValidationError {
    pub symbol: String,
    pub source: String,
    pub reason: String,
}

/// Parse source string to enum
pub fn parse_source(source: &str) -> anyhow::Result<switchboard_surge_core::Source> {
    use switchboard_surge_core::Source;
    match source.to_uppercase().as_str() {
        "BINANCE" => Ok(Source::Binance),
        "BYBIT" => Ok(Source::Bybit),
        "OKX" => Ok(Source::Okx),
        "COINBASE" => Ok(Source::Coinbase),
        "BITGET" => Ok(Source::Bitget),
        "PYTH" => Ok(Source::Pyth),
        "WEIGHTED" => Ok(Source::Weighted),
        "AUTO" => Ok(Source::Auto),
        _ => Err(anyhow::anyhow!("Unknown source: {}", source)),
    }
}
