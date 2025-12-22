use crate::messages::oracle::SignatureScheme;
use crate::messages::{parse_source, FeedPair, ServerMessage};
use crate::stream::monitor_optimized::release_shared_price_collector;
pub use crate::stream::state::{ConnectionState, CONNECTIONS};
use dashmap::DashMap;
use once_cell::sync::Lazy;
use rust_decimal::Decimal;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;
use switchboard_surge_core::Pair;
use tokio::sync::mpsc;

/// Bundle ID type
pub type BundleId = String;

/// Connection ID type  
pub type ConnectionId = String;

/// Feed key type (source, symbol)
pub type FeedKey = (String, Pair);

/// Maps bundle IDs to their subscribed connections (support multiple subscribers per bundle)
pub static BUNDLE_SUBSCRIBERS: Lazy<DashMap<BundleId, DashMap<ConnectionId, Arc<ConnectionInfo>>>> =
    Lazy::new(DashMap::new);

/// Maps connections to their subscribed bundles
pub static CONNECTION_BUNDLES: Lazy<DashMap<ConnectionId, HashSet<BundleId>>> =
    Lazy::new(DashMap::new);

/// Maps bundle IDs to their feed definitions
pub static BUNDLE_DEFINITIONS: Lazy<DashMap<BundleId, Vec<FeedKey>>> = Lazy::new(DashMap::new);

/// Maps individual feeds to bundles that contain them (for triggering updates)
pub static FEED_TO_BUNDLES: Lazy<DashMap<FeedKey, HashSet<BundleId>>> = Lazy::new(DashMap::new);

/// Maps bundle IDs to their signature schemes (defaults to Ed25519 if not specified)
pub static BUNDLE_SIGNATURE_SCHEMES: Lazy<DashMap<BundleId, SignatureScheme>> =
    Lazy::new(DashMap::new);

/// Information about a connection subscribed to a bundle
#[derive(Clone)]
pub struct ConnectionInfo {
    pub connection_id: String,
    pub tx: mpsc::UnboundedSender<ServerMessage>,
    pub batch_interval_ms: u64,
    pub last_sent_time: Arc<parking_lot::RwLock<Instant>>,
}

/// Bundle price state for tracking updates
#[derive(Debug, Clone)]
pub struct BundlePriceState {
    pub bundle_id: String,
    pub last_updated: Instant,
    pub last_prices: DashMap<FeedKey, Decimal>,
}

/// Track bundle update states
pub static BUNDLE_STATES: Lazy<DashMap<BundleId, BundlePriceState>> = Lazy::new(DashMap::new);

/// Generate a unique bundle ID from feeds
pub fn generate_bundle_id(feeds: &[FeedPair]) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();

    // Sort feeds for consistent ID generation
    let mut sorted_feeds = feeds.to_vec();
    sorted_feeds.sort_by(|a, b| {
        a.symbol
            .to_string()
            .cmp(&b.symbol.to_string())
            .then(a.source.cmp(&b.source))
    });

    for feed in &sorted_feeds {
        hasher.update(feed.symbol.to_string().as_bytes());
        hasher.update(feed.source.as_bytes());
    }

    let result = hasher.finalize();
    faster_hex::hex_string(&result[..8]) // Use first 8 bytes for ID
}

/// Register a bundle subscription for a connection
pub fn register_bundle_subscription(
    connection_id: String,
    bundle_id: String,
    feeds: Vec<FeedPair>,
    tx: mpsc::UnboundedSender<ServerMessage>,
    batch_interval_ms: u64,
    signature_scheme: SignatureScheme, // Add signature scheme parameter
) {
    // Store bundle definition
    let feed_keys: Vec<FeedKey> = feeds
        .iter()
        .map(|f| (f.source.clone(), f.symbol.clone()))
        .collect();

    BUNDLE_DEFINITIONS.insert(bundle_id.clone(), feed_keys.clone());

    // Store the signature scheme for this bundle
    BUNDLE_SIGNATURE_SCHEMES.insert(bundle_id.clone(), signature_scheme);

    // Map each feed to this bundle
    for feed_key in &feed_keys {
        FEED_TO_BUNDLES
            .entry(feed_key.clone())
            .or_insert_with(HashSet::new)
            .insert(bundle_id.clone());
    }

    // Store connection info for this bundle
    let conn_info = Arc::new(ConnectionInfo {
        connection_id: connection_id.clone(),
        tx,
        batch_interval_ms,
        last_sent_time: Arc::new(parking_lot::RwLock::new(Instant::now())),
    });

    // Insert or update subscribers map for this bundle
    let subscribers = BUNDLE_SUBSCRIBERS
        .entry(bundle_id.clone())
        .or_insert_with(DashMap::new);
    subscribers.insert(connection_id.clone(), conn_info);

    // Track bundle for this connection
    CONNECTION_BUNDLES
        .entry(connection_id)
        .or_insert_with(HashSet::new)
        .insert(bundle_id.clone());

    // Initialize bundle state
    BUNDLE_STATES.insert(
        bundle_id.clone(),
        BundlePriceState {
            bundle_id: bundle_id.clone(),
            last_updated: Instant::now(),
            last_prices: DashMap::new(),
        },
    );
}

/// Unregister all bundles for a connection
pub fn unregister_connection(connection_id: &str) {
    // Get all bundles for this connection
    if let Some(bundles) = CONNECTION_BUNDLES.remove(connection_id) {
        for bundle_id in bundles.1 {
            // Remove this connection from bundle subscribers
            if let Some(subscribers) = BUNDLE_SUBSCRIBERS.get_mut(&bundle_id) {
                subscribers.remove(connection_id);
            }

            // Clean up bundle definition if no other subscribers
            if bundle_has_no_subscribers(&bundle_id) {
                if let Some((_, feed_keys)) = BUNDLE_DEFINITIONS.remove(&bundle_id) {
                    // For each feed in this bundle, unlink the bundle and always decrement
                    // the shared collector refcount once; drop the mapping when empty.
                    for feed_key in feed_keys {
                        if let Some(mut bundle_set) = FEED_TO_BUNDLES.get_mut(&feed_key) {
                            bundle_set.remove(&bundle_id);
                            if bundle_set.is_empty() {
                                drop(bundle_set);
                                FEED_TO_BUNDLES.remove(&feed_key);
                            }
                        }
                        if let Ok(source) = parse_source(&feed_key.0) {
                            // Fire-and-forget; safe to spawn on drop path
                            tokio::spawn(release_shared_price_collector(
                                source,
                                feed_key.1.clone(),
                            ));
                        }
                    }
                }

                // Remove bundle state
                BUNDLE_STATES.remove(&bundle_id);
                // Remove bundle signature scheme
                BUNDLE_SIGNATURE_SCHEMES.remove(&bundle_id);
                // Remove empty subscribers map
                BUNDLE_SUBSCRIBERS.remove(&bundle_id);
            }
        }
    }

    // Remove connection
    CONNECTIONS.remove(connection_id);

    // Remove from SubscribeAll if present (not used in oracle mode)
}

/// Find all bundles affected by a price update
pub fn find_affected_bundles(source: &str, symbol: &Pair) -> Vec<BundleId> {
    let feed_key = (source.to_string(), symbol.clone());

    if let Some(bundles) = FEED_TO_BUNDLES.get(&feed_key) {
        bundles.iter().cloned().collect()
    } else {
        Vec::new()
    }
}

/// Helper to check if a bundle currently has zero subscribers
pub fn bundle_has_no_subscribers(bundle_id: &BundleId) -> bool {
    if let Some(subscribers) = BUNDLE_SUBSCRIBERS.get(bundle_id) {
        subscribers.is_empty()
    } else {
        true
    }
}
