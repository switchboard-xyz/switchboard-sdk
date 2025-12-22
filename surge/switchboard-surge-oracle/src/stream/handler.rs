use crate::auth::{hash_api_key, parse_auth_header, validate_session_token};
use crate::messages::{ClientMessage, ServerMessage, FeedBundleInfo, FeedInfo, FeedBundleRequest, parse_source};
use crate::stream::bundle_state::{
    BUNDLE_SUBSCRIBERS, CONNECTION_BUNDLES,
    BUNDLE_DEFINITIONS, FEED_TO_BUNDLES,
    generate_bundle_id, register_bundle_subscription, unregister_connection
};
use crate::stream::state::{ConnectionState, CONNECTIONS};
use crate::stream::bundle_monitor::{start_monitoring_for_connection as start_bundle_monitors, stop_bundle_monitor};
use crate::stream::monitor_optimized::release_shared_price_collector;
use anyhow::Result;
use fastwebsockets::{Frame, OpCode};
use http_body_util::Empty;
use hyper::body::Bytes;
use hyper::{Request, Response, StatusCode};
use std::future::Future;
use std::sync::Arc;
use switchboard_surge_core::{SurgeHub, Pair};
use tokio::sync::mpsc;
use tokio::time::Duration;
use std::sync::atomic::{AtomicBool, Ordering};
use tracing::{debug, error, info, warn};
use crate::messages::oracle::SignatureScheme;

/// Maximum number of feeds allowed per bundle
const MAX_FEEDS_PER_BUNDLE: usize = 5;

// Executor for hyper
pub struct SpawnExecutor;

impl<Fut> hyper::rt::Executor<Fut> for SpawnExecutor
where
    Fut: Future + Send + 'static,
    Fut::Output: Send + 'static,
{
    fn execute(&self, fut: Fut) {
        tokio::spawn(fut);
    }
}

/// Handle WebSocket upgrade request with oracle configuration
pub async fn handle_websocket_upgrade(
    mut req: Request<hyper::body::Incoming>,
    oracle_config: Arc<crate::consensus::OracleConfig>,
) -> Result<Response<Empty<Bytes>>> {
    // Extract auth from header or query
    let auth_header = if let Some(header) = req.headers().get("authorization") {
        header.to_str()?.to_string()
    } else if let Some(query) = req.uri().query() {
        // Parse query for authorization parameter
        let params: Vec<(String, String)> = serde_urlencoded::from_str(query)?;
        params.into_iter()
            .find(|(k, _)| k == "authorization")
            .map(|(_, v)| v)
            .ok_or_else(|| anyhow::anyhow!("Missing authorization"))?
    } else {
        return Ok(Response::builder()
            .status(StatusCode::UNAUTHORIZED)
            .body(Empty::new())?);
    };

    // Parse and validate auth
    let (api_key, session_token) = parse_auth_header(&auth_header)?;

    // Validate session token (no IP needed for WebSocket connections)
    if let Err(e) = validate_session_token(&session_token, &api_key) {
        warn!("Session validation failed: {}", e);
        return Ok(Response::builder()
            .status(StatusCode::UNAUTHORIZED)
            .body(Empty::new())?);
    }

    let api_key_hash = hash_api_key(&api_key);

    // Perform WebSocket handshake
    let (response, websocket) = match fastwebsockets::upgrade::upgrade(&mut req) {
        Ok(result) => result,
        Err(_) => {
            return Ok(Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Empty::new())?);
        }
    };

    // Spawn handler for the connection with oracle config
    tokio::spawn(handle_websocket_connection(websocket, api_key_hash, oracle_config));

    Ok(response)
}

/// Handle an established WebSocket connection with oracle signing
async fn handle_websocket_connection(
    websocket: fastwebsockets::upgrade::UpgradeFut,
    api_key_hash: String,
    oracle_config: Arc<crate::consensus::OracleConfig>,
) -> Result<()> {
    info!("Awaiting WebSocket upgrade completion...");
    let mut ws = websocket.await?;
    info!("WebSocket upgrade completed");

    // Configure WebSocket for optimal performance
    ws.set_auto_close(true);
    ws.set_auto_pong(true);

    let (tx, mut rx) = mpsc::unbounded_channel::<ServerMessage>();
    let connection_id = uuid::Uuid::new_v4().to_string();

    info!("WebSocket connection established: {}", connection_id);

    // Store connection state with oracle config
    let state = Arc::new(ConnectionState {
        id: connection_id.clone(),
        api_key_hash: api_key_hash.clone(),
        user_pubkey: None,  // Will be set by websocket_actor for signature auth
        signing_pubkey: None,  // Will be set by websocket_actor for signature auth
        tx: tx.clone(),
        subscriptions: dashmap::DashMap::new(),
        cancelled: Arc::new(AtomicBool::new(false)),
        default_batch_interval_ms: Arc::new(std::sync::atomic::AtomicU64::new(10)),
        oracle_config: oracle_config.clone(),
        last_signature_check_ms: Arc::new(std::sync::atomic::AtomicU64::new(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64
        )),
    });

    CONNECTIONS.insert(connection_id.clone(), state.clone());

    // Send authentication message immediately - SDK waits for this before sending subscribe messages
    let auth_msg = ServerMessage::Authenticated {
        message: format!(
            "Authentication successful. Connected with API key hash: {}...",
            &state.api_key_hash[..5.min(state.api_key_hash.len())]
        ),
    };

    match serde_json::to_string(&auth_msg) {
        Ok(json) => {
            info!("Sending auth message: {}", json);
            match ws.write_frame(Frame::text(json.as_bytes().into())).await {
                Ok(_) => {
                    info!("Successfully sent authentication message to client {}", connection_id);
                }
                Err(e) => {
                    error!("Failed to send authentication message to {}: {}", connection_id, e);
                    cleanup_connection(&connection_id).await;
                    return Ok(());
                }
            }
        }
        Err(e) => {
            error!("Failed to serialize auth message: {}", e);
            cleanup_connection(&connection_id).await;
            return Ok(());
        }
    }

    // Main event loop - handle reading and sending in one loop
    loop {
        tokio::select! {
            // Check for incoming messages from client
            frame_result = ws.read_frame() => {
                match frame_result {
                    Ok(frame) => {
                        match frame.opcode {
                            OpCode::Text => {
                                let text = String::from_utf8_lossy(&frame.payload);
                                info!("Received message from {}: {}", connection_id, text);
                                if let Err(e) = handle_client_message(&state, &text).await {
                                    error!("Error handling message: {}", e);
                                }
                            }
                            OpCode::Close => {
                                info!("WebSocket closed by client {}", connection_id);
                                break;
                            }
                            OpCode::Ping => {
                                // Auto-pong is enabled, so this is handled automatically
                                debug!("Received ping from {}", connection_id);
                            }
                            _ => {
                                debug!("Received unknown opcode {:?} from {}", frame.opcode, connection_id);
                            }
                        }
                    }
                    Err(e) => {
                        // Client disconnection is normal, not an error
                        match e.to_string().as_str() {
                            "Unexpected EOF" => {
                                info!("Client {} disconnected normally", connection_id);
                            }
                            _ => {
                                warn!("WebSocket read error for {}: {}", connection_id, e);
                            }
                        }
                        break;
                    }
                }
            }

            // Check for messages to send to client (price updates)
            Some(msg) = rx.recv() => {

                if let Ok(json) = serde_json::to_string(&msg) {
                    match ws.write_frame(Frame::text(json.as_bytes().into())).await {
                        Ok(_) => {}
                        Err(e) => {
                            error!("Failed to send message to {}: {}", connection_id, e);
                            break;
                        }
                    }
                } else {
                    error!("Failed to serialize message for {}", connection_id);
                }
            }

            // Check if connection was cancelled
            _ = tokio::time::sleep(Duration::from_millis(100)) => {
                if state.cancelled.load(Ordering::Relaxed) {
                    info!("Connection {} cancelled", connection_id);
                    break;
                }
            }
        }
    }

    // Cleanup
    cleanup_connection(&connection_id).await;
    info!("WebSocket connection closed: {}", connection_id);

    Ok(())
}

/// Handle incoming client messages
async fn handle_client_message(state: &Arc<ConnectionState>, text: &str) -> Result<()> {
    let msg: ClientMessage = serde_json::from_str(text)?;

    match msg {
        ClientMessage::Authenticate { .. } => {
            // Already authenticated at connection time
        }
        ClientMessage::Subscribe { feed_bundles, batch_interval_ms, signature_scheme, .. } => {
            // Note: Signature verification is handled by websocket_actor in rust-feeds-oracle
            handle_subscribe(state, feed_bundles, batch_interval_ms, signature_scheme).await?;
        }
        ClientMessage::Unsubscribe { feed_bundle_ids, .. } => {
            // Note: Signature verification is handled by websocket_actor in rust-feeds-oracle
            handle_unsubscribe(state, feed_bundle_ids).await?;
        }
        ClientMessage::Ping { .. } => {
            // Note: Signature verification is handled by websocket_actor in rust-feeds-oracle
            // Send pong
            let _ = state.tx.send(ServerMessage::Pong);
        }
        ClientMessage::Pong { .. } => {
            // Ignore pong responses
        }
        ClientMessage::SignedPong { .. } => {
            // Note: SignedPong verification is handled by websocket_actor in rust-feeds-oracle
        }
    // SubscribeAll not supported in oracle mode
    }

    Ok(())
}

/// Handle subscribe message with bundle-based tracking
async fn handle_subscribe(
    state: &Arc<ConnectionState>,
    feed_bundles: Vec<FeedBundleRequest>,
    batch_interval_ms: Option<u64>,
    signature_scheme: SignatureScheme,  // Add signature scheme parameter
) -> Result<()> {
    let _hub = SurgeHub::global();
    let mut subscribed_bundles = Vec::new();
    
    // Update batch interval if provided
    let interval_ms = batch_interval_ms.unwrap_or(state.default_batch_interval_ms.load(Ordering::Relaxed));
    
    // Process each bundle
    for bundle_request in feed_bundles {
        // Check feed limit first
        if bundle_request.feeds.len() > MAX_FEEDS_PER_BUNDLE {
            let error_msg = format!(
                "Bundle exceeds maximum feed limit: {} feeds provided, maximum {} allowed",
                bundle_request.feeds.len(),
                MAX_FEEDS_PER_BUNDLE
            );
            warn!("{}", error_msg);
            
            // Send error to client
            let _ = state.tx.send(ServerMessage::Error {
                message: error_msg,
            });
            continue;
        }
        
        // Validate all feeds in the bundle
        let mut valid_feeds = Vec::new();
        for feed in &bundle_request.feeds {
            // Validate source
            if let Ok(_source) = parse_source(&feed.source) {
                valid_feeds.push(feed.clone());
            } else {
                warn!("Invalid source {} for {}", feed.source, feed.symbol);
            }
        }
        
        if valid_feeds.is_empty() {
            continue;
        }
        
        // Generate bundle ID
        let bundle_id = generate_bundle_id(&valid_feeds);
        
        // Register bundle subscription
        register_bundle_subscription(
            state.id.clone(),
            bundle_id.clone(),
            valid_feeds.clone(),
            state.tx.clone(),
            interval_ms,
            signature_scheme,  // Pass signature scheme to bundle registration
        );

        // Fire an initial snapshot immediately (best-effort)
        // Convert feeds into (source, Pair) keys expected by consensus
        let feed_keys: Vec<(String, Pair)> = valid_feeds
            .iter()
            .map(|f| (f.source.clone(), f.symbol.clone()))
            .collect();
        // Fetch slot/hash (may be zeroed placeholder if unavailable)
        let (slot, recent_hash) = crate::consensus::current_slot_and_hash()
            .unwrap_or((0u64, "11111111111111111111111111111111".to_string()));
        match crate::consensus::generate_bundle_consensus_response(
            &feed_keys,
            &state.oracle_config,
            slot,
            recent_hash,
            Some(signature_scheme),  // Pass signature scheme for this bundle
            None, // No pre-captured prices for initial snapshot
            true, // Initial snapshot = triggered by subscription (price change)
            None, // Not a heartbeat
        ).await {
            Ok(response) => {
                let _ = state.tx.send(response);
                tracing::info!("Sent initial snapshot for bundle {}", bundle_id);
            }
            Err(e) => {
                tracing::warn!("Initial snapshot generation failed for bundle {}: {}", bundle_id, e);
            }
        }
        
        // Create bundle info for response
        let bundle_info = FeedBundleInfo {
            feed_bundle_id: bundle_id,
            feeds: valid_feeds.into_iter().map(|f| FeedInfo {
                symbol: f.symbol.to_string(),
                source: f.source,
            }).collect(),
        };
        
        subscribed_bundles.push(bundle_info);
    }
    
    // Start monitoring for feeds in bundles (per-bundle monitors)
    start_bundle_monitors(&state.id).await;
    
    // Send subscription response
    let response = ServerMessage::Subscribed {
        feed_bundles: subscribed_bundles,
    };
    
    let _ = state.tx.send(response);
    
    Ok(())
}

/// Handle unsubscribe message
async fn handle_unsubscribe(
    state: &Arc<ConnectionState>,
    feed_bundle_ids: Vec<String>,
) -> Result<()> {
    // Remove bundles for this connection
    if let Some(mut bundles) = CONNECTION_BUNDLES.get_mut(&state.id) {
        for bundle_id in &feed_bundle_ids {
            bundles.remove(bundle_id);

            // Remove this connection's subscriber entry for the bundle
            let should_cleanup_bundle = if let Some(subscribers) = BUNDLE_SUBSCRIBERS.get_mut(bundle_id) {
                subscribers.remove(&state.id);
                let is_empty = subscribers.is_empty();
                if is_empty {
                    drop(subscribers);
                    BUNDLE_SUBSCRIBERS.remove(bundle_id);
                }
                is_empty
            } else {
                false
            };

            // If no more subscribers for this bundle, clean up definitions and monitors
            if should_cleanup_bundle {
                if let Some((_, feed_keys)) = BUNDLE_DEFINITIONS.remove(bundle_id) {
                    // For each feed in the bundle, unlink the bundle and always release once to
                    // decrement the shared collector's refcount for this bundle.
                    for feed_key in feed_keys {
                        if let Some(mut bundle_set) = FEED_TO_BUNDLES.get_mut(&feed_key) {
                            bundle_set.remove(bundle_id);
                            if bundle_set.is_empty() {
                                // No bundles reference this feed -> drop mapping
                                drop(bundle_set);
                                FEED_TO_BUNDLES.remove(&feed_key);
                            }
                        }
                        // Decrement refcount regardless; collector cancels itself only at zero.
                        if let Ok(source) = parse_source(&feed_key.0) {
                            release_shared_price_collector(source, feed_key.1.clone()).await;
                        }
                    }
                }

                // Stop the bundle monitor if running
                stop_bundle_monitor(bundle_id).await;

                // Remove any residual bundle state
                crate::stream::bundle_state::BUNDLE_STATES.remove(bundle_id);
                
                // Remove bundle signature scheme
                crate::stream::bundle_state::BUNDLE_SIGNATURE_SCHEMES.remove(bundle_id);
            }
        }
    }
    
    // Send unsubscribe response
    let response = ServerMessage::Unsubscribed {
        feed_bundle_ids,
    };
    
    let _ = state.tx.send(response);
    
    Ok(())
}


/// Cleanup connection
async fn cleanup_connection(connection_id: &str) {
    // Unregister all bundles and clean up state
    unregister_connection(connection_id);
    
    // Stop monitoring if needed
    // Note: stop_monitoring_for_connection expects ConnectionState, not string
    // The unregister_connection already handles cleanup
    
    info!("Cleaned up connection {}", connection_id);
}