use crate::client::signature_auth::{SignatureAuth, SignatureAuthConfig};
use switchboard_utils::SbError;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use crate::solana_sdk::signature::Keypair;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock, Mutex};
use tokio::time::{sleep, Duration};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message, WebSocketStream, MaybeTlsStream};
use url::Url;

type WsStream = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

/// Raw gateway response structure (matches actual BundledFeedUpdate from server)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawGatewayResponse {
    #[serde(rename = "type")]
    pub type_: String,
    #[serde(alias = "feed_bundle_id")]
    pub feed_quote_id: Option<String>,
    pub feed_values: Option<Vec<FeedValue>>,
    pub oracle_response: Option<OracleResponse>,
    pub source_ts_ms: u64,
    pub seen_at_ts_ms: u64,
    pub triggered_on_price_change: bool,
    pub message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeedValue {
    pub value: String,
    pub feed_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OracleResponse {
    pub oracle_pubkey: String,
    pub eth_address: String,
    pub signature: String,
    pub checksum: String,
    pub recovery_id: u8,
    pub oracle_idx: u8,
    pub timestamp: u64,
    pub timestamp_ms: Option<u64>,
    pub recent_hash: String,
    pub slot: u64,
    pub ed25519_enclave_signer: Option<String>,
}

/// Raw unsigned price update structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawUnsignedPriceUpdate {
    #[serde(rename = "type")]
    pub type_: String,
    #[serde(alias = "feed_bundle_id")]
    pub feed_quote_id: String,
    pub feed_values: Vec<UnsignedFeedValue>,
    pub broadcast_ts_ms: u64,
    pub message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnsignedFeedValue {
    pub value: String,
    pub feed_id: String,
    pub symbol: String,
    pub source: String,
    pub source_ts_ms: u64,
    pub seen_at_ts_ms: u64,
}

/// WebSocket message types for subscription confirmations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscribedMessage {
    #[serde(rename = "type")]
    pub type_: String,
    pub message: Option<String>,
    #[serde(alias = "feed_bundles")]
    pub feed_quotes: Option<Vec<FeedQuote>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeedQuote {
    #[serde(alias = "feed_bundle_id")]
    pub feed_quote_id: String,
    pub feeds: Vec<Feed>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Feed {
    pub symbol: SymbolType,
    pub source: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SymbolType {
    String(String),
    Pair { base: String, quote: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthenticatedMessage {
    #[serde(rename = "type")]
    pub type_: String,
    pub message: Option<String>,
}

/// Validation error message type
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationErrorMessage {
    #[serde(rename = "type")]
    pub type_: String,
    pub message: Option<String>,
    pub error: Option<String>,
    pub invalid_feeds: Option<Vec<InvalidFeed>>,
    pub details: Option<HashMap<String, Value>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvalidFeed {
    pub symbol: Pair,
    pub source: String,
    pub error: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pair {
    pub base: String,
    pub quote: String,
}

/// Session response from gateway
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionResponse {
    pub session_token: String,
    #[serde(alias = "oracle_ws_url", alias = "simulator_ws_url")]
    pub ws_url: String,
}

/// Oracle response class that wraps raw gateway responses with convenient methods
#[derive(Debug, Clone)]
pub struct SurgeUpdate {
    raw_response: RawGatewayResponse,
}

#[cfg(feature = "client")]
use crate::Instruction;
#[cfg(feature = "client")]
use crate::solana_sdk::ed25519_instruction;

impl SurgeUpdate {
    pub fn new(raw_response: RawGatewayResponse) -> Self {
        Self { raw_response }
    }

    pub fn data(&self) -> &RawGatewayResponse {
        &self.raw_response
    }

    /// Get array of signed feed hashes
    pub fn get_signed_feeds(&self) -> Vec<String> {
        self.raw_response
            .feed_values
            .as_ref()
            .map(|feeds| feeds.iter().map(|f| f.feed_hash.clone()).collect())
            .unwrap_or_default()
    }

    /// Get array of price values (raw 18-decimal format)
    pub fn get_values(&self) -> Vec<String> {
        self.raw_response
            .feed_values
            .as_ref()
            .map(|feeds| feeds.iter().map(|f| f.value.clone()).collect())
            .unwrap_or_default()
    }

    /// Get formatted prices as readable dollar amounts
    pub fn get_formatted_prices(&self) -> HashMap<String, String> {
        let mut prices = HashMap::new();

        if let Some(feed_values) = &self.raw_response.feed_values {
            for feed in feed_values {
                // Parse value as u128
                if let Ok(value) = feed.value.parse::<u128>() {
                    let divisor = 10u128.pow(18);
                    let whole_part = value / divisor;
                    let fractional_part = value % divisor;

                    // Convert to full decimal representation
                    let full_decimal = format!("{:018}", fractional_part);
                    let decimals = full_decimal.trim_end_matches('0');
                    let decimals = if decimals.is_empty() { "0" } else { decimals };

                    // Format with commas
                    let whole_str = format_with_commas(whole_part);

                    let price = if decimals == "0" {
                        format!("${}", whole_str)
                    } else {
                        format!("${}.{}", whole_str, decimals)
                    };

                    prices.insert(feed.feed_hash.clone(), price);
                }
            }
        }

        prices
    }

    /// Check if this update was triggered by a price change (vs heartbeat)
    pub fn is_triggered_by_price_change(&self) -> bool {
        self.raw_response.triggered_on_price_change
    }

    /// Get the complete raw response from gateway
    pub fn get_raw_response(&self) -> &RawGatewayResponse {
        &self.raw_response
    }

    /// Get detailed latency breakdown for this oracle response
    pub fn get_latency_metrics(&self) -> LatencyMetrics {
        let source_time_ms = self.raw_response.source_ts_ms;
        let arrival_time_ms = chrono::Utc::now().timestamp_millis() as u64;
        let checksum_time_ms = self
            .raw_response
            .oracle_response
            .as_ref()
            .and_then(|r| r.timestamp_ms)
            .or_else(|| {
                self.raw_response
                    .oracle_response
                    .as_ref()
                    .map(|r| r.timestamp * 1000)
            })
            .unwrap_or(0);

        let is_heartbeat = !self.raw_response.triggered_on_price_change;
        let oracle_to_client_raw = arrival_time_ms.saturating_sub(checksum_time_ms) as i64;
        let end_to_end_raw = arrival_time_ms.saturating_sub(source_time_ms) as i64;
        let exchange_to_checksum_raw = checksum_time_ms.saturating_sub(source_time_ms) as i64;

        LatencyMetrics {
            exchange_to_oracle_update: handle_clock_drift(exchange_to_checksum_raw),
            oracle_update_to_client: handle_clock_drift(oracle_to_client_raw),
            end_to_end: handle_clock_drift(end_to_end_raw),
            is_scheduled_price_heartbeat: is_heartbeat,
        }
    }

    /// Convert to Solana instruction for quote verification
    /// Returns the Ed25519 signature verification instruction
    #[cfg(feature = "client")]
    pub fn to_quote_ix(&self, _instruction_idx: u16) -> Result<Instruction, SbError> {
        let oracle_response = self
            .raw_response
            .oracle_response
            .as_ref()
            .ok_or_else(|| SbError::CustomError {
                message: "No oracle response available".to_string(),
                source: Arc::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Missing oracle response",
                )),
            })?;

        // Check if using Ed25519 signature scheme
        if let Some(ed25519_signer) = &oracle_response.ed25519_enclave_signer {
            // Extract Ed25519 public key (first 32 bytes if 64-byte key)
            let pubkey_hex = if ed25519_signer.len() == 128 {
                &ed25519_signer[..64] // First 32 bytes (64 hex chars)
            } else {
                ed25519_signer.as_str()
            };

            let pubkey_bytes = hex::decode(pubkey_hex).map_err(|e| SbError::CustomError {
                message: format!("Failed to decode Ed25519 pubkey: {}", e),
                source: Arc::new(std::io::Error::new(std::io::ErrorKind::InvalidData, e)),
            })?;

            let signature_bytes =
                base64::decode(&oracle_response.signature).map_err(|e| SbError::CustomError {
                    message: format!("Failed to decode signature: {}", e),
                    source: Arc::new(std::io::Error::new(std::io::ErrorKind::InvalidData, e)),
                })?;

            let message_bytes =
                base64::decode(&oracle_response.checksum).map_err(|e| SbError::CustomError {
                    message: format!("Failed to decode message: {}", e),
                    source: Arc::new(std::io::Error::new(std::io::ErrorKind::InvalidData, e)),
                })?;

            // Create Ed25519 instruction with signature
            // Convert bytes to fixed-size arrays
            let pubkey_array: [u8; 32] = pubkey_bytes.try_into().map_err(|_| SbError::CustomError {
                message: "Invalid Ed25519 public key length".to_string(),
                source: Arc::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Expected 32 bytes",
                )),
            })?;

            let signature_array: [u8; 64] = signature_bytes.try_into().map_err(|_| SbError::CustomError {
                message: "Invalid Ed25519 signature length".to_string(),
                source: Arc::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Expected 64 bytes",
                )),
            })?;

            let message_array: [u8; 32] = message_bytes.try_into().map_err(|_| SbError::CustomError {
                message: "Invalid Ed25519 message hash length".to_string(),
                source: Arc::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Expected 32 bytes",
                )),
            })?;

            let ix = ed25519_instruction::new_ed25519_instruction_with_signature(
                &pubkey_array,
                &signature_array,
                &message_array,
            );

            Ok(ix)
        } else {
            // Secp256k1 path - would need secp256k1_instruction
            Err(SbError::CustomError {
                message: "Secp256k1 instruction conversion not yet implemented".to_string(),
                source: Arc::new(std::io::Error::new(
                    std::io::ErrorKind::Unsupported,
                    "Use Ed25519 signature scheme",
                )),
            })
        }
    }

    /// Get the feed quote ID if available
    pub fn get_quote_id(&self) -> Option<&str> {
        self.raw_response.feed_quote_id.as_deref()
    }
}

#[derive(Debug, Clone)]
pub struct LatencyMetrics {
    pub exchange_to_oracle_update: LatencyValue,
    pub oracle_update_to_client: LatencyValue,
    pub end_to_end: LatencyValue,
    pub is_scheduled_price_heartbeat: bool,
}

#[derive(Debug, Clone)]
pub enum LatencyValue {
    Ms(u64),
    ClockDrift(i64),
}

fn handle_clock_drift(value: i64) -> LatencyValue {
    if value < 0 {
        LatencyValue::ClockDrift(value)
    } else {
        LatencyValue::Ms(value as u64)
    }
}

fn format_with_commas(n: u128) -> String {
    let s = n.to_string();
    let chars: Vec<char> = s.chars().collect();
    let mut result = String::new();

    for (i, c) in chars.iter().enumerate() {
        if i > 0 && (chars.len() - i) % 3 == 0 {
            result.push(',');
        }
        result.push(*c);
    }

    result
}

/// Unsigned price update class
#[derive(Debug, Clone)]
pub struct UnsignedPriceUpdate {
    raw_response: RawUnsignedPriceUpdate,
}

impl UnsignedPriceUpdate {
    pub fn new(raw_response: RawUnsignedPriceUpdate) -> Self {
        Self { raw_response }
    }

    pub fn data(&self) -> &RawUnsignedPriceUpdate {
        &self.raw_response
    }

    pub fn get_feed_ids(&self) -> Vec<String> {
        self.raw_response
            .feed_values
            .iter()
            .map(|f| f.feed_id.clone())
            .collect()
    }

    pub fn get_prices(&self) -> Vec<String> {
        self.raw_response
            .feed_values
            .iter()
            .map(|f| f.value.clone())
            .collect()
    }

    pub fn get_symbols(&self) -> Vec<String> {
        self.raw_response
            .feed_values
            .iter()
            .map(|f| f.symbol.clone())
            .collect()
    }

    pub fn get_sources(&self) -> Vec<String> {
        self.raw_response
            .feed_values
            .iter()
            .map(|f| f.source.clone())
            .collect()
    }
}

/// Feed subscription input
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum FeedSubscription {
    Symbol {
        symbol: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        source: Option<String>,
    },
    FeedHash {
        #[serde(rename = "feedHash")]
        feed_hash: String,
    },
}

/// Configuration options for Surge
#[derive(Clone)]
pub struct SurgeConfig {
    /// API key for authentication (optional if keypair is provided)
    pub api_key: Option<String>,
    /// Optional keypair for signature-based authentication
    pub keypair: Option<Arc<Keypair>>,
    /// Chain identifier (defaults to "solana")
    pub chain: Option<String>,
    /// Network identifier
    pub network: Option<String>,
    /// Optional gateway URL override
    pub gateway_url: Option<String>,
    /// Signature scheme to use (defaults to 'ed25519')
    pub signature_scheme: Option<String>,
    /// Auto-reconnect on connection loss (defaults to true)
    pub auto_reconnect: bool,
    /// Maximum reconnection attempts (defaults to 5)
    pub max_reconnect_attempts: usize,
    /// Reconnection delay in ms (defaults to 1000)
    pub reconnect_delay: Duration,
    /// Verbose flag for added logging
    pub verbose: bool,
}

impl Default for SurgeConfig {
    fn default() -> Self {
        Self {
            api_key: None,
            keypair: None,
            chain: Some("solana".to_string()),
            network: Some("mainnet-beta".to_string()),
            gateway_url: None,
            signature_scheme: Some("ed25519".to_string()),
            auto_reconnect: true,
            max_reconnect_attempts: 5,
            reconnect_delay: Duration::from_millis(1000),
            verbose: false,
        }
    }
}

/// Connection states
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    Disconnected,
    Connecting,
    Connected,
    Authenticating,
    Authenticated,
    Error,
}

/// Event types that can be emitted by Surge
#[derive(Debug, Clone)]
pub enum SurgeEvent {
    Update(SurgeUpdate),
    UnsignedUpdate(UnsignedPriceUpdate),
    Connected,
    Disconnected,
    Error(String),
    Subscribed(SubscribedMessage),
}

/// Surge - WebSocket streaming client for Switchboard On-Demand feeds
///
/// Provides real-time streaming of price updates with automatic processing into
/// Solana transaction instructions. Supports both direct symbol/source subscriptions
/// and feedHash-based subscriptions with automatic detection and conversion.
pub struct Surge {
    config: SurgeConfig,
    state: Arc<RwLock<SurgeState>>,
    event_tx: tokio::sync::broadcast::Sender<SurgeEvent>,
    ws_tx: Arc<Mutex<Option<mpsc::UnboundedSender<Message>>>>,
    shutdown_tx: Arc<Mutex<Option<mpsc::UnboundedSender<()>>>>,
}

struct SurgeState {
    connection_state: ConnectionState,
    session_token: Option<String>,
    ws_url: Option<String>,
    subscriptions: Vec<FeedSubscription>,
    reconnect_attempts: usize,
    signature_auth: Option<SignatureAuth>,
    feed_quotes: HashMap<String, Vec<FeedSubscription>>, // quote_id -> feeds
}

impl Surge {
    /// Create a new Surge instance
    pub fn new(config: SurgeConfig) -> Self {
        let (event_tx, _) = tokio::sync::broadcast::channel(1000);

        // Initialize signature auth if keypair is provided
        let signature_auth = config.keypair.as_ref().map(|kp| {
            SignatureAuth::new(SignatureAuthConfig {
                keypair: kp.clone(),
                refresh_interval: Some(Duration::from_secs(5 * 60)),
            })
        });

        let state = SurgeState {
            connection_state: ConnectionState::Disconnected,
            session_token: None,
            ws_url: None,
            subscriptions: Vec::new(),
            reconnect_attempts: 0,
            signature_auth,
            feed_quotes: HashMap::new(),
        };

        Self {
            config,
            state: Arc::new(RwLock::new(state)),
            event_tx,
            ws_tx: Arc::new(Mutex::new(None)),
            shutdown_tx: Arc::new(Mutex::new(None)),
        }
    }

    /// Initialize a Surge instance with a gateway
    pub fn init(api_key: String, gateway_url: String, verbose: bool) -> Self {
        Self::new(SurgeConfig {
            api_key: Some(api_key),
            gateway_url: Some(gateway_url),
            verbose,
            ..Default::default()
        })
    }

    /// Get an event receiver for listening to updates
    /// Note: Multiple subscribers are supported via broadcast channel.
    pub fn subscribe_events(&self) -> tokio::sync::broadcast::Receiver<SurgeEvent> {
        self.event_tx.subscribe()
    }

    /// Subscribe to feeds
    pub async fn subscribe(&self, feeds: Vec<FeedSubscription>) -> Result<(), SbError> {
        let mut state = self.state.write().await;

        // Add feeds to subscriptions
        state.subscriptions.extend(feeds.clone());
        drop(state);

        // Convert feeds to the format the server expects
        let processed_feeds: Vec<Value> = feeds.iter().map(|feed| {
            match feed {
                FeedSubscription::Symbol { symbol, source } => {
                    // Split symbol into base/quote pair
                    let parts: Vec<&str> = symbol.split('/').collect();
                    let (base, quote) = if parts.len() == 2 {
                        (parts[0], parts[1])
                    } else {
                        (symbol.as_str(), "USD") // fallback
                    };

                    json!({
                        "symbol": {
                            "base": base,
                            "quote": quote
                        },
                        "source": source.as_deref().unwrap_or("WEIGHTED")
                    })
                },
                FeedSubscription::FeedHash { feed_hash } => {
                    json!({
                        "feed_hash": feed_hash
                    })
                }
            }
        }).collect();

        // Send subscription message matching TypeScript SDK format
        let subscribe_msg = json!({
            "type": "Subscribe",  // Capital S
            "feed_bundles": [{
                "feeds": processed_feeds
            }],
            "signature_scheme": "Ed25519"  // Must be Pascal case
        });

        if self.config.verbose {
            eprintln!("[Surge] Sending subscribe message: {}", subscribe_msg);
        }

        self.send_message(Message::Text(subscribe_msg.to_string())).await?;
        self.log("Subscribed to feeds");

        Ok(())
    }

    /// Request a session from the gateway
    async fn request_session(&self) -> Result<SessionResponse, SbError> {
        let gateway_url = self
            .config
            .gateway_url
            .as_ref()
            .ok_or_else(|| SbError::CustomError {
                message: "Gateway URL not configured".to_string(),
                source: Arc::new(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "No gateway URL",
                )),
            })?;

        let client = reqwest::Client::new();
        let url = format!("{}/gateway/api/v1/request_stream", gateway_url);

        let mut headers = reqwest::header::HeaderMap::new();
        if let Some(api_key) = &self.config.api_key {
            headers.insert(
                "X-API-Key",
                reqwest::header::HeaderValue::from_str(api_key)
                    .map_err(|e| SbError::CustomError {
                        message: format!("Invalid API key: {}", e),
                        source: Arc::new(std::io::Error::new(std::io::ErrorKind::InvalidInput, e)),
                    })?,
            );
        }

        let response = client
            .post(&url)
            .headers(headers)
            .json(&json!({
                "chain": self.config.chain.as_ref().unwrap_or(&"solana".to_string()),
                "network": self.config.network.as_ref().unwrap_or(&"mainnet-beta".to_string()),
            }))
            .send()
            .await
            .map_err(|e| SbError::CustomError {
                message: format!("Session request failed: {}", e),
                source: Arc::new(std::io::Error::new(std::io::ErrorKind::Other, e)),
            })?;

        if !response.status().is_success() {
            return Err(SbError::CustomError {
                message: format!("Session request failed with status: {}", response.status()),
                source: Arc::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "HTTP error",
                )),
            });
        }

        let session: SessionResponse = response.json().await.map_err(|e| SbError::CustomError {
            message: format!("Failed to parse session response: {}", e),
            source: Arc::new(std::io::Error::new(std::io::ErrorKind::InvalidData, e)),
        })?;

        self.log(&format!("Session created: {}", session.session_token));

        Ok(session)
    }

    /// Connect to the WebSocket server
    pub async fn connect(&self) -> Result<(), SbError> {
        self.log("Connecting to Surge WebSocket...");

        {
            let mut state = self.state.write().await;
            state.connection_state = ConnectionState::Connecting;
        }

        // Request session
        let session = self.request_session().await?;

        {
            let mut state = self.state.write().await;
            state.session_token = Some(session.session_token.clone());
            state.ws_url = Some(session.ws_url.clone());
        }

        // Connect to WebSocket with Authorization header
        let url = Url::parse(&session.ws_url).map_err(|e| SbError::CustomError {
            message: format!("Invalid WebSocket URL: {}", e),
            source: Arc::new(std::io::Error::new(std::io::ErrorKind::InvalidInput, e)),
        })?;

        self.log(&format!("Connecting to WebSocket: {}", url));

        // Build auth token: Bearer {api_key}:{session_token}
        let auth_token = if let Some(api_key) = &self.config.api_key {
            format!("Bearer {}:{}", api_key, session.session_token)
        } else {
            // For keypair-based auth (future support)
            format!("Bearer {}", session.session_token)
        };

        // Create WebSocket request with Authorization header
        use tokio_tungstenite::tungstenite::http::Request;

        let request = Request::builder()
            .method("GET")
            .uri(url.as_str())
            .header("Host", url.host_str().unwrap_or(""))
            .header("Connection", "Upgrade")
            .header("Upgrade", "websocket")
            .header("Sec-WebSocket-Version", "13")
            .header("Sec-WebSocket-Key", tokio_tungstenite::tungstenite::handshake::client::generate_key())
            .header("Authorization", &auth_token)
            .body(())
            .map_err(|e| SbError::CustomError {
                message: format!("Failed to create WebSocket request: {}", e),
                source: Arc::new(std::io::Error::new(std::io::ErrorKind::InvalidInput, e)),
            })?;

        let (ws_stream, _) = connect_async(request)
            .await
            .map_err(|e| SbError::CustomError {
                message: format!("WebSocket connection failed: {}", e),
                source: Arc::new(std::io::Error::new(std::io::ErrorKind::ConnectionRefused, e)),
            })?;

        self.log("WebSocket connected");

        {
            let mut state = self.state.write().await;
            state.connection_state = ConnectionState::Connected;
            state.reconnect_attempts = 0;
        }

        // Start event loop
        self.start_event_loop(ws_stream).await?;

        // Send connected event (ignore if no receivers)
        let _ = self.event_tx.send(SurgeEvent::Connected);

        // Authenticate if needed
        self.authenticate().await?;

        Ok(())
    }

    /// Authenticate with the WebSocket server
    async fn authenticate(&self) -> Result<(), SbError> {
        let state = self.state.read().await;

        if state.signature_auth.is_none() {
            return Ok(());
        }

        drop(state);

        {
            let mut state = self.state.write().await;
            state.connection_state = ConnectionState::Authenticating;
        }

        // Get authentication data
        let auth_data = {
            let state = self.state.read().await;
            if let Some(sig_auth) = &state.signature_auth {
                Some(sig_auth.get_auth_data().await?)
            } else {
                None
            }
        };

        if let Some(auth) = auth_data {
            let auth_msg = json!({
                "type": "authenticate",
                "signature": auth.signature,
                "publicKey": auth.public_key,
                "blockhash": auth.blockhash,
                "timestamp": auth.timestamp,
            });

            self.send_message(Message::Text(auth_msg.to_string())).await?;
            self.log("Authentication message sent");
        }

        {
            let mut state = self.state.write().await;
            state.connection_state = ConnectionState::Authenticated;
        }

        Ok(())
    }

    /// Start the WebSocket event loop
    async fn start_event_loop(&self, ws_stream: WsStream) -> Result<(), SbError> {
        let (ws_write, mut ws_read) = ws_stream.split();

        // Create channels for sending messages
        let (msg_tx, mut msg_rx) = mpsc::unbounded_channel::<Message>();
        let (shutdown_tx, mut shutdown_rx) = mpsc::unbounded_channel::<()>();

        *self.ws_tx.lock().await = Some(msg_tx.clone());
        *self.shutdown_tx.lock().await = Some(shutdown_tx);

        let event_tx = self.event_tx.clone();
        let state = self.state.clone();
        let config = self.config.clone();

        // Spawn write task
        let verbose_write = config.verbose;
        tokio::spawn(async move {
            let mut ws_write = ws_write;
            loop {
                tokio::select! {
                    Some(msg) = msg_rx.recv() => {
                        if verbose_write {
                            eprintln!("[Surge] Write task sending message to WebSocket");
                        }
                        if let Err(e) = ws_write.send(msg).await {
                            eprintln!("[Surge] WebSocket write error: {}", e);
                            break;
                        }
                        if verbose_write {
                            eprintln!("[Surge] Write task successfully sent message");
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        if verbose_write {
                            eprintln!("[Surge] Write task shutting down");
                        }
                        let _ = ws_write.close().await;
                        break;
                    }
                }
            }
        });

        // Spawn read task
        let event_tx_clone = event_tx.clone();
        let state_clone = state.clone();
        let verbose = config.verbose;
        tokio::spawn(async move {
            while let Some(msg_result) = ws_read.next().await {
                match msg_result {
                    Ok(Message::Text(text)) => {
                        if verbose {
                            eprintln!("[Surge] Received message: {}", &text[..text.len().min(200)]);
                        }
                        if let Err(e) = Self::handle_message(&text, &event_tx_clone, &state_clone, &config).await {
                            if verbose {
                                eprintln!("[Surge] Error handling message: {}", e);
                            }
                        }
                    }
                    Ok(Message::Ping(_data)) => {
                        if verbose {
                            eprintln!("[Surge] Received ping");
                        }
                        // Pong is automatically handled by tungstenite
                    }
                    Ok(Message::Close(_)) => {
                        if verbose {
                            eprintln!("[Surge] Received close");
                        }
                        let _ = event_tx_clone.send(SurgeEvent::Disconnected);
                        break;
                    }
                    Err(e) => {
                        if verbose {
                            eprintln!("[Surge] WebSocket error: {}", e);
                        }
                        let _ = event_tx_clone.send(SurgeEvent::Error(format!("WebSocket error: {}", e)));
                        break;
                    }
                    _ => {
                        if verbose {
                            eprintln!("[Surge] Received other message type");
                        }
                    }
                }
            }
            if verbose {
                eprintln!("[Surge] Read loop ended");
            }
        });

        Ok(())
    }

    /// Handle incoming WebSocket message
    async fn handle_message(
        text: &str,
        event_tx: &tokio::sync::broadcast::Sender<SurgeEvent>,
        _state: &Arc<RwLock<SurgeState>>,
        config: &SurgeConfig,
    ) -> Result<(), SbError> {
        // Try to parse as generic JSON first to check type
        let msg: Value = serde_json::from_str(text).map_err(|e| SbError::CustomError {
            message: format!("Failed to parse message: {}", e),
            source: Arc::new(std::io::Error::new(std::io::ErrorKind::InvalidData, e)),
        })?;

        let msg_type = msg
            .get("type")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");

        if config.verbose {
            eprintln!("[Surge] Message type: {}", msg_type);
        }

        match msg_type {
            "BundledFeedUpdate" => {
                let update: RawGatewayResponse = serde_json::from_str(text).map_err(|e| {
                    SbError::CustomError {
                        message: format!("Failed to parse gateway response: {}", e),
                        source: Arc::new(std::io::Error::new(std::io::ErrorKind::InvalidData, e)),
                    }
                })?;
                if config.verbose {
                    eprintln!("[Surge] Sending Update event to {} subscribers", event_tx.receiver_count());
                }
                let _ = event_tx.send(SurgeEvent::Update(SurgeUpdate::new(update)));
            }
            "UnsignedPriceUpdate" => {
                let update: RawUnsignedPriceUpdate =
                    serde_json::from_str(text).map_err(|e| SbError::CustomError {
                        message: format!("Failed to parse unsigned update: {}", e),
                        source: Arc::new(std::io::Error::new(std::io::ErrorKind::InvalidData, e)),
                    })?;
                if config.verbose {
                    eprintln!("[Surge] Sending UnsignedUpdate event to {} subscribers", event_tx.receiver_count());
                }
                let _ = event_tx.send(SurgeEvent::UnsignedUpdate(UnsignedPriceUpdate::new(
                    update,
                )));
            }
            "Subscribed" => {
                let subscribed: SubscribedMessage =
                    serde_json::from_str(text).map_err(|e| SbError::CustomError {
                        message: format!("Failed to parse subscribed message: {}", e),
                        source: Arc::new(std::io::Error::new(std::io::ErrorKind::InvalidData, e)),
                    })?;
                if config.verbose {
                    eprintln!("[Surge] Sending Subscribed event to {} subscribers", event_tx.receiver_count());
                }
                let _ = event_tx.send(SurgeEvent::Subscribed(subscribed));
            }
            "Authenticated" => {
                if config.verbose {
                    println!("[Surge] Authenticated");
                }
            }
            "ValidationError" => {
                let error: ValidationErrorMessage =
                    serde_json::from_str(text).map_err(|e| SbError::CustomError {
                        message: format!("Failed to parse validation error: {}", e),
                        source: Arc::new(std::io::Error::new(std::io::ErrorKind::InvalidData, e)),
                    })?;
                let error_msg = error.message.unwrap_or_else(|| "Validation error".to_string());
                let _ = event_tx.send(SurgeEvent::Error(error_msg));
            }
            _ => {
                eprintln!("[Surge] ⚠️  Unknown message type '{}': {}", msg_type, &text[..text.len().min(500)]);
            }
        }

        Ok(())
    }

    /// Send a message to the WebSocket
    async fn send_message(&self, message: Message) -> Result<(), SbError> {
        let ws_tx = self.ws_tx.lock().await;
        if let Some(tx) = ws_tx.as_ref() {
            if self.config.verbose {
                eprintln!("[Surge] Sending WebSocket message via channel");
            }
            tx.send(message).map_err(|_| SbError::CustomError {
                message: "Failed to send message".to_string(),
                source: Arc::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Channel send error",
                )),
            })?;
            if self.config.verbose {
                eprintln!("[Surge] Message sent successfully");
            }
            Ok(())
        } else {
            eprintln!("[Surge] ERROR: WebSocket not connected!");
            Err(SbError::CustomError {
                message: "WebSocket not connected".to_string(),
                source: Arc::new(std::io::Error::new(
                    std::io::ErrorKind::NotConnected,
                    "No connection",
                )),
            })
        }
    }

    /// Disconnect from the WebSocket server
    pub async fn disconnect(&self) -> Result<(), SbError> {
        self.log("Disconnecting from Surge WebSocket...");

        // Send shutdown signal
        if let Some(shutdown_tx) = self.shutdown_tx.lock().await.as_ref() {
            let _ = shutdown_tx.send(());
        }

        {
            let mut state = self.state.write().await;
            state.connection_state = ConnectionState::Disconnected;
        }

        let _ = self.event_tx.send(SurgeEvent::Disconnected);

        Ok(())
    }

    /// Reconnect with exponential backoff
    pub async fn reconnect(&self) -> Result<(), SbError> {
        let max_attempts = self.config.max_reconnect_attempts;
        let base_delay = self.config.reconnect_delay;

        let mut attempts = {
            let state = self.state.read().await;
            state.reconnect_attempts
        };

        while attempts < max_attempts {
            // Exponential backoff: base_delay * 2^attempts (capped at 30 seconds)
            let delay = base_delay * 2u32.pow(attempts as u32).min(30);
            self.log(&format!("Reconnecting in {:?} (attempt {}/{})", delay, attempts + 1, max_attempts));

            sleep(delay).await;

            // Try to reconnect
            match self.connect().await {
                Ok(_) => {
                    self.log("Reconnected successfully");

                    // Re-subscribe to feeds
                    let subscriptions = {
                        let state = self.state.read().await;
                        state.subscriptions.clone()
                    };

                    if !subscriptions.is_empty() {
                        self.log(&format!("Re-subscribing to {} feeds", subscriptions.len()));
                        if let Err(e) = self.subscribe(subscriptions).await {
                            self.log(&format!("Failed to re-subscribe: {}", e));
                        }
                    }

                    // Reset reconnect attempts
                    {
                        let mut state = self.state.write().await;
                        state.reconnect_attempts = 0;
                    }

                    return Ok(());
                }
                Err(e) => {
                    self.log(&format!("Reconnect attempt failed: {}", e));
                    attempts += 1;

                    {
                        let mut state = self.state.write().await;
                        state.reconnect_attempts = attempts;
                    }
                }
            }
        }

        Err(SbError::CustomError {
            message: format!("Failed to reconnect after {} attempts", max_attempts),
            source: Arc::new(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "Max reconnect attempts reached",
            )),
        })
    }

    /// Get feed quotes
    pub async fn get_feed_quotes(&self) -> HashMap<String, Vec<FeedSubscription>> {
        let state = self.state.read().await;
        state.feed_quotes.clone()
    }

    /// Get feeds for a specific quote
    pub async fn get_quote_feeds(&self, quote_id: &str) -> Option<Vec<FeedSubscription>> {
        let state = self.state.read().await;
        state.feed_quotes.get(quote_id).cloned()
    }

    /// Get the current connection state
    pub async fn get_state(&self) -> ConnectionState {
        self.state.read().await.connection_state
    }

    /// Check if connected
    pub async fn is_connected(&self) -> bool {
        matches!(
            self.state.read().await.connection_state,
            ConnectionState::Connected | ConnectionState::Authenticated
        )
    }

    fn log(&self, message: &str) {
        if self.config.verbose {
            println!("[Surge] {}", message);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_surge_config_default() {
        let config = SurgeConfig::default();
        assert_eq!(config.chain, Some("solana".to_string()));
        assert_eq!(config.network, Some("mainnet-beta".to_string()));
        assert_eq!(config.auto_reconnect, true);
        assert_eq!(config.max_reconnect_attempts, 5);
    }

    #[test]
    fn test_surge_update_formatting() {
        let response = RawGatewayResponse {
            type_: "BundledFeedUpdate".to_string(),
            feed_quote_id: Some("test".to_string()),
            feed_values: Some(vec![FeedValue {
                value: "50000000000000000000000".to_string(), // 50000 in 18 decimals
                feed_hash: "abc123".to_string(),
            }]),
            oracle_response: None,
            source_ts_ms: 0,
            seen_at_ts_ms: 0,
            triggered_on_price_change: true,
            message: None,
        };

        let update = SurgeUpdate::new(response);
        let prices = update.get_formatted_prices();

        assert!(prices.contains_key("abc123"));
        assert_eq!(prices.get("abc123").unwrap(), "$50,000");
    }

    #[tokio::test]
    async fn test_surge_creation() {
        let surge = Surge::new(SurgeConfig::default());
        assert_eq!(surge.get_state().await, ConnectionState::Disconnected);
    }

    #[test]
    fn test_feed_subscription_serialization() {
        let sub = FeedSubscription::Symbol {
            symbol: "BTC/USD".to_string(),
            source: Some("BINANCE".to_string()),
        };

        let json = serde_json::to_string(&sub).unwrap();
        assert!(json.contains("BTC/USD"));
        assert!(json.contains("BINANCE"));
    }
}
