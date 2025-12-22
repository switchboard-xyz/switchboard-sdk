use anyhow::{anyhow, Result};
use base58::FromBase58;
use base64::Engine;
use faster_hex::hex_string;
use libsecp256k1::{sign, Message, SecretKey};
use sb_on_demand_schemas::FeedRequestV2;
use sha2::{Digest as Sha2Digest, Sha256};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{Keypair, Signer};
use std::sync::Arc;
use switchboard_surge_core::{Pair, Source, SurgeHub, get_or_create_feed_metadata};

use crate::messages::{ConsensusStreamFeedValue, StreamConsensusResponse};
use crate::messages::oracle::StreamResponse;
 use crate::messages::oracle::SignatureScheme;
use crate::stream::state::PriceUpdate;
/// Oracle configuration for signing
pub struct OracleConfig {
    pub secret: SecretKey,
    pub oracle_idx: u8,
    pub oracle_pubkey: Pubkey,
    pub eth_address: [u8; 20],
    pub mr_enclave: String,
    pub secp256k1_pubkey: String,
    
    // Ed25519 keys (always available from Environment)
    pub ed25519_secret: Arc<Keypair>,
    pub ed25519_pubkey: Pubkey,
}

impl OracleConfig {
    /// Check if this oracle has signing capability (enclave/TEE)
    pub fn has_signing_capability(&self) -> bool {
        // Check if enclave and secp256k1 keys are valid (not all zeros)
        !self.mr_enclave.starts_with("0000000000") && 
        !self.secp256k1_pubkey.starts_with("0000000000")
    }
}

/// Generate a consensus response for a bundle of feeds
pub async fn generate_bundle_consensus_response(
    feed_keys: &[(String, Pair)], // (source, symbol) pairs
    oracle_config: &OracleConfig,
    slot: u64,
    recent_hash: String,
    signature_scheme: Option<SignatureScheme>, // Optional signature scheme for this bundle
    captured_prices: Option<&[((String, Pair), PriceUpdate)]>, // Pre-captured prices with timestamps
    triggered_by_price_change: bool, // Whether this update was triggered by price change (vs heartbeat)
    heartbeat_at_ts_ms: Option<u64>, // Timestamp when heartbeat was triggered (for latency measurement)
) -> Result<StreamResponse> {
    let hub = SurgeHub::global();
    
    // Collect feed metadata and current values
    let mut feed_metadatas = Vec::new();
    let mut values = Vec::new();
    let mut feed_values = Vec::new();
    
    for (source_str, symbol) in feed_keys {
        // Parse source
        let source = crate::messages::parse_source(source_str).ok();

        // Check if we have pre-captured PriceUpdate (with timestamps)
        let captured_price_update = captured_prices
            .and_then(|prices| prices.iter().find(|((src, pair), _)| src == source_str && pair == symbol))
            .map(|(_, pu)| pu);

        // Handle different source types appropriately
        let (metadata, price, event_ts, seen_at, verified_at) = match source {
            // AUTO, WEIGHTED, or None: All use AUTO price selection
            Some(Source::Auto) | Some(Source::Weighted) | None => {
                // Determine metadata source:
                // - WEIGHTED uses WEIGHTED metadata (for backwards compatibility with existing feeds)
                // - AUTO and None (new feeds) use AUTO metadata
                let metadata_source = if source == Some(Source::Weighted) {
                    Some(Source::Weighted)  // WEIGHTED metadata for existing feeds
                } else {
                    Some(Source::Auto)      // AUTO metadata for AUTO and new feeds (None)
                };
                let metadata = get_or_create_feed_metadata(symbol, metadata_source)?;

                // Use AutoSourceCalculator to get the best price source
                let calculator = switchboard_surge_core::weighted_source::AutoSourceCalculator::new();
                if let Some((actual_source, actual_pair)) = calculator.get_from_cache_sync(symbol) {
                    let (price, event_ts, seen_at, verified_at) = if let Some(pu) = captured_price_update {
                        // Use pre-captured PriceUpdate (preserves timestamps)
                        (pu.price, pu.event_ts, pu.seen_at, pu.verified_at)
                    } else {
                        // Fallback: get fresh Tick from hub
                        let tick = hub
                            .get_price(actual_source, &actual_pair)
                            .ok_or_else(|| anyhow!(
                                "No price for AUTO-resolved {}/{} (actual {} on {})",
                                symbol,
                                source.map_or("AUTO", |s| s.as_str()),
                                actual_pair,
                                actual_source.as_str()
                            ))?;
                        (tick.price, tick.event_ts, tick.seen_at, tick.verified_at)
                    };
                    (metadata, price, event_ts, seen_at, verified_at)
                } else {
                    let source_name = source.map_or("AUTO", |s| s.as_str());
                    tracing::warn!("{} source not resolved in cache for {} yet", source_name, symbol.as_str());
                    return Err(anyhow!(
                        "{} source not resolved in cache for {}. Waiting for background update",
                        source_name,
                        symbol
                    ));
                }
            }
            // Specific exchange sources: use their own metadata and direct price
            Some(src) => {
                // This handles all other sources (exchanges) not covered above
                let metadata = get_or_create_feed_metadata(symbol, Some(src))?;
                let (price, event_ts, seen_at, verified_at) = if let Some(pu) = captured_price_update {
                    // Use pre-captured PriceUpdate (preserves timestamps)
                    (pu.price, pu.event_ts, pu.seen_at, pu.verified_at)
                } else {
                    // Fallback: get fresh Tick from hub
                    let tick = hub
                        .get_price(src, symbol)
                        .ok_or_else(|| anyhow!("No price for {}/{}", symbol, src.as_str()))?;
                    (tick.price, tick.event_ts, tick.seen_at, tick.verified_at)
                };
                (metadata, price, event_ts, seen_at, verified_at)
            }
        };
        
    // Scale to 18 decimals and convert to i128 (use rescale to avoid overflow)
    let mut scaled = price;
    scaled.rescale(18);
    let mantissa = scaled.mantissa();
        
    feed_metadatas.push(metadata.feed_request_v2.clone());
        values.push(mantissa);

        // Create feed value for response with timestamps
        feed_values.push(ConsensusStreamFeedValue {
            value: mantissa.to_string(),
            feed_hash: metadata.feed_id.clone(),
            symbol: symbol.to_string(),
            source: source_str.clone(),
            event_ts,      // Exchange timestamp
            seen_at,       // Oracle reception time
            verified_at,   // Oracle verification time
        });
    }
    
    // Generate consensus response
    let consensus = generate_stream_consensus_response(
        &feed_metadatas,
        &values,
        &oracle_config.secret,
        oracle_config.oracle_idx,
        slot,
        recent_hash.clone(),
        &oracle_config.oracle_pubkey,
        &oracle_config.eth_address,
        signature_scheme,  // Pass the signature scheme for this bundle
        Some(oracle_config),  // Pass full config for Ed25519 if available
    )?;

    // Capture broadcast timestamp AFTER signing is complete
    let broadcast_ts_ms = switchboard_surge_core::get_corrected_timestamp_ms();

    // For deprecated fields, use the most recent timestamps from the actual feed data
    // (not the broadcast time, which is when we send the message)
    let source_ts_ms = feed_values.iter().map(|f| f.event_ts).max().unwrap_or(broadcast_ts_ms);
    let seen_at_ts_ms = feed_values.iter().map(|f| f.seen_at).max().unwrap_or(broadcast_ts_ms);

    Ok(StreamResponse::BundledFeedUpdate {
        feed_bundle_id: generate_bundle_id(feed_keys),
        feed_values,
        oracle_response: consensus,
        broadcast_ts_ms,
        // Deprecated fields for backwards compatibility with older SDKs
        source_ts_ms,
        seen_at_ts_ms,
        triggered_on_price_change: triggered_by_price_change,
        heartbeat_at_ts_ms,
    })
}

/// Generate a unique bundle ID from feed keys
fn generate_bundle_id(feed_keys: &[(String, Pair)]) -> String {
    use sha2::{Sha256, Digest};
    let mut hasher = Sha256::new();

    // Sort by symbol then source to match subscription generation
    let mut sorted = feed_keys.to_vec();
    sorted.sort_by(|a, b| {
        a.1.to_string()
            .cmp(&b.1.to_string())
            .then(a.0.cmp(&b.0))
    });

    for (source, symbol) in sorted {
        // Match ordering: symbol first, then source
        hasher.update(symbol.to_string().as_bytes());
        hasher.update(source.as_bytes());
    }

    let result = hasher.finalize();
    hex_string(&result[..8]) // Use first 8 bytes for ID
}

/// Generate stream consensus response with signature
fn generate_stream_consensus_response(
    feed_requests: &[FeedRequestV2],
    values: &[i128],
    secret: &SecretKey,
    oracle_idx: u8,
    slot: u64,
    recent_hash: String,
    oracle_pubkey: &Pubkey,
    eth_address: &[u8; 20],
    signature_scheme: Option<SignatureScheme>,  // Optional signature scheme
    oracle_config: Option<&OracleConfig>,  // Optional full config for Ed25519
) -> Result<StreamConsensusResponse> {
    // Parse recent hash
    let recent_hash_bytes: Vec<u8> = recent_hash
        .from_base58()
        .map_err(|_| anyhow!("Invalid base58 recent hash"))?;
    
    if recent_hash_bytes.len() != 32 {
        return Err(anyhow!("Recent hash must be 32 bytes, got {}", recent_hash_bytes.len()));
    }
    
    let recent_hash_array: [u8; 32] = recent_hash_bytes
        .try_into()
        .map_err(|_| anyhow!("Failed to convert recent hash to array"))?;
    
    // Get timestamp - use actual timestamp instead of 0
    let timestamp: u64 = (switchboard_surge_core::get_corrected_timestamp_ms() / 1000) as u64;
    let timestamp_ms = switchboard_surge_core::get_corrected_timestamp_ms();
    
    // Check if we should use Ed25519 signature scheme
    let use_ed25519 = signature_scheme.unwrap_or(SignatureScheme::Ed25519) == SignatureScheme::Ed25519;
    
    // Generate signature based on scheme
    let (final_signature, final_recovery_id, checksum, ed25519_enclave_signer) = 
        if use_ed25519 && oracle_config.is_some() {
            // Ed25519 v0 scheme - matches feed_fetch_consensus.rs implementation
            let config = oracle_config.unwrap();
            let secret = &config.ed25519_secret;
            let ed25519_pubkey = config.ed25519_pubkey;
            
            // Create the signed message: signed_slothash + feed_infos
            // NOTE: oracle_idx is NOT part of the signed message (unlike Secp256k1)
            // It will be appended to the ED25519 instruction data by the client
            let mut msg = vec![];
            msg.extend_from_slice(&recent_hash_array); // 32 bytes (signed_slothash)
            
            // Add feed infos to signed message (matches feed_fetch_consensus.rs)
            for (i, feed_req) in feed_requests.iter().enumerate() {
                msg.extend_from_slice(&feed_req.feed_id()?); // 32 bytes
                msg.extend_from_slice(&values[i].to_le_bytes()); // 16 bytes (i128 LE)
                msg.extend_from_slice(&[feed_req.num_oracle_samples()]); // 1 byte
            }
            
            // Sign the message with Ed25519
            let ed25519_sig = secret.try_sign_message(&msg)
                .map_err(|e| anyhow!("Failed to sign with Ed25519: {}", e))?
                .as_ref()
                .to_vec();
            
            // For Ed25519, we use the message itself as the checksum
            (
                base64::engine::general_purpose::STANDARD.encode(ed25519_sig),
                0u8, // Ed25519 doesn't use recovery_id
                msg, // Use message as checksum
                Some(hex_string(&ed25519_pubkey.to_bytes()))
            )
        } else {
            // New V2 Secp256k1 SHA256 scheme - matches feed_fetch_consensus.rs
            let mut hasher = Sha256::new();
            println!("Generating Secp256k1 SHA256 signature");
            // Add slot number (8 bytes)
            hasher.update(&slot.to_le_bytes());
            
            // Add timestamp (8 bytes)
            hasher.update(&timestamp.to_le_bytes());
            
            // Add each feed's data
            for i in 0..feed_requests.len() {
                let feed_req = &feed_requests[i];
                let feed_id = feed_req.feed_id()?;
                let value = values[i];
                
                // Add feed_id (32 bytes)
                hasher.update(&feed_id);
                
                // Add value (16 bytes)
                hasher.update(&value.to_le_bytes());
                
                // Add min_oracle_samples (1 byte)
                hasher.update(&feed_req.num_oracle_samples().to_le_bytes());
            }
            
            let checksum: [u8; 32] = hasher.finalize().into();
            
            // Use SHA256 hash directly (no Keccak256)
            let (signature, recovery_id) = sign(&Message::parse(&checksum), secret);
            
            (
                base64::engine::general_purpose::STANDARD.encode(signature.serialize()),
                recovery_id.into(),
                checksum.to_vec(),
                None // No Ed25519 pubkey for Secp256k1
            )
        };
    
    Ok(StreamConsensusResponse {
        oracle_pubkey: hex_string(&oracle_pubkey.to_bytes()),
        eth_address: hex_string(eth_address),
        signature: final_signature,
        checksum,
        recovery_id: final_recovery_id,
        oracle_idx,
        timestamp,
        timestamp_ms: Some(timestamp_ms),
        recent_hash,
        slot,
        ed25519_enclave_signer,
    })
}