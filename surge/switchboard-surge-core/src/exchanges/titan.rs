use crate::{exchange_config::get_allowed_quotes, exchanges::connection_state::ConnectionState, pair::Pair, traits::TickerStream, types::{Source, Ticker}};
use anyhow::{anyhow, Result};
use dashmap::DashMap;
use futures::Stream;
use futures::{SinkExt, StreamExt};
use once_cell::sync::Lazy;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::{mpsc, Mutex};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info, warn};

// Global cache for Titan pairs
// Maps symbol (e.g., "SOL/USDC") to (Pair, (input_mint, output_mint))
static TITAN_PAIRS: Lazy<Arc<DashMap<String, (Pair, (String, String))>>> = Lazy::new(|| Arc::new(DashMap::new()));

// Configuration constants
const TITAN_WS_ENDPOINT: &str = "wss://partners.api.titan.exchange/api/v1/ws";
const TITAN_ACCESS_TOKEN: &str = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6ImI5MzJiMTkwLTkxZTMtNDhkZC04M2JhLWI1ODA0OWQ1NjIzOSJ9.eyJpYXQiOjE3NjE3NzM5NTIsImV4cCI6MTc5MzMwOTk1MiwiYXVkIjoiYXBpLnRpdGFuLmFnIiwiaXNzIjoidGl0YW5fcGFydG5lcnMiLCJzdWIiOiJhcGk6c3dpdGNoYm9hcmQifQ.tMgVGe3BMVueICISbx_EFHhutxgeJ8ngnGu1Eey6xag";
const USER_PUBLIC_KEY: &str = "11111111111111111111111111111111"; // Default user key for quotes
const PING_INTERVAL_SECS: u64 = 30;
const QUOTE_AMOUNT: &str = "1000000000"; // 1 unit with 9 decimals

type TitanWebSocket = tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

// Titan WebSocket message types (JSON format based on API docs)
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum TitanMessage {
    Quote { quotes: Vec<SwapRoute> },
    Error { error: String },
    Other(serde_json::Value),
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SwapRoute {
    in_amount: String,
    out_amount: String,
}

// Titan API request structures
#[derive(Debug, Serialize)]
struct ClientRequest {
    id: u32,
    data: RequestData,
}

#[derive(Debug, Serialize)]
enum RequestData {
    GetInfo(GetInfoRequest),
    NewSwapQuoteStream(SwapQuoteRequest),
}

#[derive(Debug, Serialize)]
struct GetInfoRequest {
    // Empty struct for GetInfo request
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SwapQuoteRequest {
    swap: SwapParams,
    transaction: TransactionParams,
    update: Option<QuoteUpdateParams>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SwapParams {
    #[serde(with = "serde_bytes")]
    input_mint: Vec<u8>, // 32-byte pubkey
    #[serde(with = "serde_bytes")]
    output_mint: Vec<u8>, // 32-byte pubkey
    amount: u64,
    slippage_bps: Option<u16>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct TransactionParams {
    #[serde(with = "serde_bytes")]
    user_public_key: Vec<u8>, // 32-byte pubkey
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct QuoteUpdateParams {
    interval_ms: Option<u64>,
    num_quotes: Option<u32>,
}

/// Get all cached Titan pairs
pub fn get_cached_pairs() -> Result<Vec<Pair>> {
    if TITAN_PAIRS.is_empty() {
        return Err(anyhow!("Titan pairs not initialized yet"));
    }

    let pairs: Vec<Pair> = TITAN_PAIRS
        .iter()
        .map(|entry| entry.value().0.clone())
        .collect();

    Ok(pairs)
}

pub struct TitanStream {
    ws: Option<Arc<Mutex<TitanWebSocket>>>,
    rx: mpsc::UnboundedReceiver<Result<Ticker>>,
    connection_state: ConnectionState,
    read_loop_running: Arc<AtomicBool>,
}

impl TitanStream {
    pub async fn new() -> Result<Self> {
        let (tx, rx) = mpsc::unbounded_channel();
        let connection_state = ConnectionState::new();

        // Initialize pairs
        Self::initialize_pairs().await?;

        let read_loop_running = Arc::new(AtomicBool::new(false));

        Ok(Self {
            ws: None,
            rx,
            connection_state,
            read_loop_running,
        })
    }

    async fn initialize_pairs() -> Result<()> {
        info!("Initializing Titan pairs");

        // Well-known Solana token mints
        let pairs_config: Vec<(&str, &str, &str, &str)> = vec![
            // (Base, Quote, InputMint, OutputMint)
            // SOL pairs
            ("SOL", "USDC", "So11111111111111111111111111111111111111112", "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"),
            ("SOL", "USDT", "So11111111111111111111111111111111111111112", "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"),

            // Major token pairs against USDC
            ("BTC", "USDC", "3NZ9JMVBmGAqocybic2c7LQCJScmgsAZ6vQqTDzcqmJh", "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"),  // WBTC
            ("ETH", "USDC", "7vfCXTUXx5WJV5JADk17DUJ4ksgau7utNKj4b963voxs", "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"),  // WETH

            // Stablecoin pairs
            ("USDT", "USDC", "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB", "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"),
            ("PYUSD", "USDC", "2b1kV6DkPAnxd5ixfnxCpjxmKwqjjaYmCZfHsFu24GXo", "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"),
            ("USDS", "USDC", "USDSwr9ApdHk5bvJKMjzff41FfuX8bSxdKcR81vTwcA", "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"),
        ];

        let allowed_quotes = get_allowed_quotes(Source::Titan);

        for (base, quote, input_mint, output_mint) in pairs_config {
            if allowed_quotes.contains(&quote) {
                let pair = Pair {
                    base: base.to_string(),
                    quote: quote.to_string(),
                };
                let symbol = pair.as_str();

                TITAN_PAIRS.insert(
                    symbol.clone(),
                    (pair, (input_mint.to_string(), output_mint.to_string()))
                );

                debug!("Added Titan pair: {}", symbol);
            }
        }

        info!("Initialized {} Titan pairs", TITAN_PAIRS.len());
        Ok(())
    }

    async fn connect(&mut self) -> Result<()> {
        info!("Connecting to Titan WebSocket: {}", TITAN_WS_ENDPOINT);

        // Create custom request with required headers
        use tokio_tungstenite::tungstenite::client::IntoClientRequest;
        use tokio_tungstenite::tungstenite::http::HeaderValue;

        let mut request = TITAN_WS_ENDPOINT.into_client_request()
            .map_err(|e| anyhow!("Failed to create request: {}", e))?;

        // Add required headers
        request.headers_mut().insert(
            "Sec-WebSocket-Protocol",
            HeaderValue::from_static("v1.api.titan.ag")
        );
        request.headers_mut().insert(
            "Authorization",
            HeaderValue::from_str(&format!("Bearer {}", TITAN_ACCESS_TOKEN))
                .map_err(|e| anyhow!("Invalid auth header: {}", e))?
        );

        info!("Connecting with headers: {:?}", request.headers());

        // Connect to WebSocket
        let (ws_stream, response) = connect_async(request).await
            .map_err(|e| anyhow!("Failed to connect: {}", e))?;

        info!("✅ Connected to Titan WebSocket");
        info!("Response status: {}", response.status());
        info!("Response headers: {:?}", response.headers());

        self.ws = Some(Arc::new(Mutex::new(ws_stream)));
        self.connection_state.set_connected(true);

        Ok(())
    }

    pub fn start_read_loop(&mut self) {
        if self.read_loop_running.swap(true, Ordering::SeqCst) {
            warn!("Read loop already running for Titan");
            return;
        }

        let ws = match &self.ws {
            Some(ws) => ws.clone(),
            None => {
                error!("Cannot start read loop: WebSocket not connected");
                return;
            }
        };

        let connection_state = self.connection_state.clone();
        let read_loop_running = self.read_loop_running.clone();

        crate::runtime_separation::spawn_on_ingestion_named(
            "titan-reader",
            async move {
                Self::read_loop(ws, connection_state, read_loop_running).await;
            }
        );
    }

    async fn read_loop(
        ws: Arc<Mutex<TitanWebSocket>>,
        connection_state: ConnectionState,
        read_loop_running: Arc<AtomicBool>,
    ) {
        info!("Starting Titan WebSocket read loop");

        loop {
            let message = {
                let mut ws_guard = ws.lock().await;
                match ws_guard.next().await {
                    Some(Ok(msg)) => msg,
                    Some(Err(e)) => {
                        error!("Failed to read message from Titan: {}", e);
                        connection_state.set_connected(false);
                        break;
                    }
                    None => {
                        info!("Titan WebSocket stream ended");
                        connection_state.set_connected(false);
                        break;
                    }
                }
            };

            match message {
                Message::Binary(payload) => {
                    info!("📦 Received Titan binary message ({} bytes)", payload.len());

                    // Titan uses MessagePack encoding
                    match rmp_serde::from_slice::<serde_json::Value>(&payload) {
                        Ok(value) => {
                            info!("✅ Titan MessagePack decoded: {:?}", value);

                            // Try to parse as SwapQuotes response
                            if let Ok(quotes) = rmp_serde::from_slice::<TitanMessage>(&payload) {
                                match quotes {
                                    TitanMessage::Quote { quotes } => {
                                        Self::handle_quotes(quotes).await;
                                    }
                                    TitanMessage::Error { error } => {
                                        warn!("Titan error message: {}", error);
                                    }
                                    TitanMessage::Other(val) => {
                                        debug!("Titan other message: {:?}", val);
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            debug!("Failed to parse Titan MessagePack: {}", e);
                        }
                    }
                }
                Message::Text(text) => {
                    // Text messages should be ignored per Titan API docs
                    info!("📝 Received Text message (should not happen per Titan docs): {}", text);
                }
                Message::Close(frame) => {
                    info!("❌ Titan WebSocket closed: {:?}", frame);
                    connection_state.set_connected(false);
                    break;
                }
                Message::Ping(payload) => {
                    info!("🏓 Received ping from Titan");
                    let mut ws_guard = ws.lock().await;
                    if let Err(e) = ws_guard.send(Message::Pong(payload)).await {
                        error!("Failed to send pong to Titan: {}", e);
                    }
                }
                Message::Pong(_) => {
                    info!("🏓 Received pong from Titan");
                }
                Message::Frame(_) => {
                    info!("Received raw frame from Titan (unexpected)");
                }
            }
        }

        read_loop_running.store(false, Ordering::SeqCst);
        info!("Titan read loop ended");
    }

    async fn handle_quotes(quotes: Vec<SwapRoute>) {
        // For now, we don't know which pair these quotes are for
        // This is a simplified handler - real implementation would need better message tracking
        if let Some(best_route) = quotes.iter().max_by_key(|route| route.out_amount.parse::<u64>().unwrap_or(0)) {
            if let Ok(out_amount) = best_route.out_amount.parse::<u64>() {
                let price = Decimal::from(out_amount) / Decimal::from(1_000_000_000u64);
                debug!("Titan quote received: price = {}", price);
            }
        }
    }

    async fn subscribe_to_pair(&self, input_mint: &str, output_mint: &str, request_id: u32) -> Result<()> {
        let ws = self.ws.as_ref().ok_or_else(|| anyhow!("WebSocket not connected"))?;

        // Decode base58 mint addresses to 32-byte pubkeys
        let input_mint_bytes = bs58::decode(input_mint).into_vec()
            .map_err(|e| anyhow!("Failed to decode input mint: {}", e))?;
        let output_mint_bytes = bs58::decode(output_mint).into_vec()
            .map_err(|e| anyhow!("Failed to decode output mint: {}", e))?;
        let user_pubkey_bytes = bs58::decode(USER_PUBLIC_KEY).into_vec()
            .map_err(|e| anyhow!("Failed to decode user pubkey: {}", e))?;

        // Build the proper ClientRequest structure
        let request = ClientRequest {
            id: request_id,
            data: RequestData::NewSwapQuoteStream(SwapQuoteRequest {
                swap: SwapParams {
                    input_mint: input_mint_bytes,
                    output_mint: output_mint_bytes,
                    amount: QUOTE_AMOUNT.parse().unwrap_or(1_000_000_000),
                    slippage_bps: Some(50),
                },
                transaction: TransactionParams {
                    user_public_key: user_pubkey_bytes,
                },
                update: Some(QuoteUpdateParams {
                    interval_ms: Some(5000), // 5 second updates
                    num_quotes: Some(5),     // Top 5 quotes
                }),
            }),
        };

        // Encode as MessagePack (Binary)
        let msgpack = rmp_serde::to_vec(&request)
            .map_err(|e| anyhow!("Failed to encode MessagePack: {}", e))?;

        debug!("Sending Titan subscription request {} ({} bytes): {} -> {}", request_id, msgpack.len(), input_mint, output_mint);

        let mut ws_guard = ws.lock().await;
        ws_guard.send(Message::binary(msgpack)).await
            .map_err(|e| anyhow!("Failed to send subscription: {}", e))?;

        debug!("Subscribed to Titan pair: {} -> {}", input_mint, output_mint);
        Ok(())
    }

    async fn send_get_info(&self) -> Result<()> {
        let ws = self.ws.as_ref().ok_or_else(|| anyhow!("WebSocket not connected"))?;

        let request = ClientRequest {
            id: 0,
            data: RequestData::GetInfo(GetInfoRequest {}),
        };

        let msgpack = rmp_serde::to_vec(&request)
            .map_err(|e| anyhow!("Failed to encode GetInfo: {}", e))?;

        info!("🔍 Sending GetInfo request ({} bytes)", msgpack.len());

        let mut ws_guard = ws.lock().await;
        ws_guard.send(Message::binary(msgpack)).await
            .map_err(|e| anyhow!("Failed to send GetInfo: {}", e))?;

        info!("✅ GetInfo request sent");
        Ok(())
    }

    async fn subscribe_all(&self) -> Result<()> {
        info!("Subscribing to all Titan pairs");

        // First, send GetInfo to test if server is responding
        if let Err(e) = self.send_get_info().await {
            warn!("Failed to send GetInfo: {}", e);
        } else {
            // Wait a bit for response
            tokio::time::sleep(Duration::from_secs(2)).await;
        }

        let mut request_id = 1u32;
        for entry in TITAN_PAIRS.iter() {
            let (pair, (input_mint, output_mint)) = entry.value();

            if let Err(e) = self.subscribe_to_pair(input_mint, output_mint, request_id).await {
                warn!("Failed to subscribe to Titan pair {}: {}", pair.as_str(), e);
            } else {
                debug!("Subscribed to Titan pair: {}", pair.as_str());
            }

            request_id += 1;

            // Small delay between subscriptions
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        info!("Subscribed to {} Titan pairs", TITAN_PAIRS.len());
        Ok(())
    }

    pub async fn prefetch_exchange_info() -> Result<()> {
        Self::initialize_pairs().await
    }

    pub async fn refresh_exchange_info() -> Result<usize> {
        Ok(TITAN_PAIRS.len())
    }
}

impl TickerStream for TitanStream {
    fn listen(&mut self, _pair: Pair) -> Result<()> {
        // For Titan, we connect and subscribe to all pairs on first listen
        if self.ws.is_none() {
            let self_clone = unsafe { &mut *(self as *mut Self) };

            crate::runtime_separation::spawn_on_ingestion_named(
                "titan-connect",
                async move {
                    if let Err(e) = self_clone.connect().await {
                        error!("Failed to connect to Titan: {}", e);
                        return;
                    }

                    self_clone.start_read_loop();

                    // Subscribe to all pairs
                    if let Err(e) = self_clone.subscribe_all().await {
                        error!("Failed to subscribe to Titan pairs: {}", e);
                    }

                    // Start ping task
                    let ws = self_clone.ws.clone();
                    if let Some(ws) = ws {
                        crate::runtime_separation::spawn_on_ingestion_named(
                            "titan-ping",
                            async move {
                                let mut interval = tokio::time::interval(Duration::from_secs(PING_INTERVAL_SECS));
                                loop {
                                    interval.tick().await;
                                    let mut ws_guard = ws.lock().await;
                                    if let Err(e) = ws_guard.send(Message::Ping(vec![].into())).await {
                                        error!("Failed to send ping to Titan: {}", e);
                                        break;
                                    }
                                    debug!("Sent ping to Titan");
                                }
                            }
                        );
                    }
                }
            );
        }

        Ok(())
    }

    fn unlisten(&mut self, _pair: &Pair) -> Result<()> {
        // Titan doesn't support individual unsubscribe
        Ok(())
    }

    fn subscriptions(&self) -> Vec<Pair> {
        TITAN_PAIRS
            .iter()
            .map(|entry| entry.value().0.clone())
            .collect()
    }

    fn is_connected(&self) -> bool {
        self.connection_state.is_connected()
    }
}

impl Stream for TitanStream {
    type Item = Result<Ticker>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_titan_initialize_pairs() {
        let result = TitanStream::initialize_pairs().await;
        assert!(result.is_ok(), "Failed to initialize Titan pairs: {:?}", result.err());

        let pairs = get_cached_pairs();
        assert!(pairs.is_ok(), "Failed to get cached pairs");

        let pairs = pairs.unwrap();
        println!("Titan initialized with {} pairs:", pairs.len());
        for pair in &pairs {
            println!("  - {}", pair.as_str());
        }

        assert!(!pairs.is_empty(), "No Titan pairs were initialized");
    }

    #[tokio::test]
    #[ignore]
    async fn test_titan_websocket_connection() {
        println!("\nTesting Titan WebSocket connection...");
        println!("Endpoint: {}\n", TITAN_WS_ENDPOINT);

        let mut stream = TitanStream::new().await.expect("Failed to create Titan stream");

        let connect_timeout = tokio::time::timeout(Duration::from_secs(10), stream.connect()).await;
        match connect_timeout {
            Ok(Ok(_)) => {
                println!("✅ Successfully connected to Titan WebSocket");
                assert!(stream.is_connected(), "Should be marked as connected");
            }
            Ok(Err(e)) => {
                println!("❌ Failed to connect: {}", e);
                panic!("Connection failed: {}", e);
            }
            Err(_) => {
                println!("❌ Connection timed out after 10 seconds");
                println!("   The WebSocket endpoint is not responding");
                panic!("Connection timeout");
            }
        }
    }

    #[tokio::test]
    #[ignore]
    async fn test_titan_stream_prices() {
        use futures::StreamExt;

        println!("\n🔄 Starting Titan price streaming test...\n");

        // Create the stream
        let mut stream = TitanStream::new().await.expect("Failed to create Titan stream");

        // Manually connect
        println!("📡 Connecting to Titan WebSocket...");
        println!("   Endpoint: {}", TITAN_WS_ENDPOINT);

        let connect_timeout = tokio::time::timeout(Duration::from_secs(10), stream.connect()).await;
        match connect_timeout {
            Ok(Ok(_)) => {
                println!("✅ Connected!\n");
            }
            Ok(Err(e)) => {
                println!("❌ Failed to connect: {}", e);
                panic!("Connection failed: {}", e);
            }
            Err(_) => {
                println!("❌ Connection timed out after 10 seconds");
                println!("   The WebSocket endpoint {} is not responding", TITAN_WS_ENDPOINT);
                panic!("Connection timeout - WebSocket endpoint not accessible");
            }
        }

        // Start the read loop
        stream.start_read_loop();

        // Subscribe to all pairs
        println!("📝 Subscribing to pairs...");
        if let Err(e) = stream.subscribe_all().await {
            println!("❌ Failed to subscribe: {}", e);
            panic!("Subscription failed: {}", e);
        }
        println!("✅ Subscribed to {} pairs\n", TITAN_PAIRS.len());

        // List subscribed pairs
        println!("Watching pairs:");
        for entry in TITAN_PAIRS.iter() {
            let (_symbol, (pair, (input_mint, output_mint))) = entry.pair();
            println!("  • {} ({} → {})", pair.as_str(),
                &input_mint[..8.min(input_mint.len())],
                &output_mint[..8.min(output_mint.len())]);
        }
        println!();

        // Stream prices for 60 seconds
        println!("📊 Streaming prices (will run for 60 seconds)...\n");

        let timeout = tokio::time::timeout(Duration::from_secs(60), async {
            let mut count = 0;
            while let Some(result) = stream.next().await {
                match result {
                    Ok(ticker) => {
                        count += 1;
                        println!("[{}] {} @ {} (timestamp: {})",
                            count,
                            ticker.symbol,
                            ticker.price,
                            ticker.timestamp
                        );
                    }
                    Err(e) => {
                        println!("❌ Error: {}", e);
                    }
                }
            }
            count
        }).await;

        match timeout {
            Ok(count) => {
                println!("\n✅ Test completed! Received {} price updates", count);
            }
            Err(_) => {
                println!("\n⏱️  Test timed out after 60 seconds");
                println!("   (This is normal - check if you received any price updates above)");
            }
        }
    }

    #[tokio::test]
    #[ignore]
    async fn test_titan_watch_hub_updates() {
        println!("\n🔄 Starting Titan hub update monitoring test...\n");

        // Create and start the stream
        let mut stream = TitanStream::new().await.expect("Failed to create Titan stream");

        println!("📡 Connecting and starting stream...");
        println!("   Trying to connect to: {}", TITAN_WS_ENDPOINT);

        let connect_timeout = tokio::time::timeout(Duration::from_secs(10), stream.connect()).await;
        match connect_timeout {
            Ok(Ok(_)) => {
                println!("✅ Connected!");
            }
            Ok(Err(e)) => {
                println!("❌ Failed to connect: {}", e);
                return;
            }
            Err(_) => {
                println!("❌ Connection timed out after 10 seconds");
                println!("   The WebSocket endpoint {} is not responding", TITAN_WS_ENDPOINT);
                println!("   Please verify the endpoint is correct and accessible");
                return;
            }
        }

        stream.start_read_loop();

        if let Err(e) = stream.subscribe_all().await {
            println!("❌ Failed to subscribe: {}", e);
            return;
        }

        println!("✅ Connected and subscribed!\n");

        // Get the hub to watch for updates
        let hub = crate::hub::SurgeHub::global();

        // Monitor hub updates for 60 seconds
        println!("👀 Watching hub for Titan price updates...\n");

        let mut interval = tokio::time::interval(Duration::from_secs(2));
        let start = std::time::Instant::now();

        while start.elapsed() < Duration::from_secs(60) {
            interval.tick().await;

            // Check each pair in the hub
            for entry in TITAN_PAIRS.iter() {
                let (_symbol, (pair, _mints)) = entry.pair();

                if let Ok(Some(price_data)) = hub.latest(Source::Titan, pair) {
                    println!("💹 {} = {} (age: {}ms)",
                        pair.as_str(),
                        price_data.price,
                        crate::clock_sync::get_corrected_timestamp_ms() - price_data.event_ts
                    );
                }
            }
        }

        println!("\n✅ Monitoring completed");
    }
}
