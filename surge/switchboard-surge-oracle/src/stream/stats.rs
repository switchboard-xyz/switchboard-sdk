use crate::stream::bundle_state::{BUNDLE_DEFINITIONS, CONNECTION_BUNDLES};
use crate::stream::state::{CONNECTIONS, PUBKEY_CONNECTIONS};
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;

/// User connection statistics for a specific pubkey
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct UserConnectionStats {
    pub pubkey: String,
    pub active_connections: usize,
    pub total_feeds: usize,
    pub connections: Vec<ConnectionDetail>,
}

/// Details about a single connection
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ConnectionDetail {
    pub connection_id: String,
    pub feed_count: usize,
    pub bundles: Vec<BundleDetail>,
}

/// Details about a bundle subscription
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BundleDetail {
    pub bundle_id: String,
    pub feed_count: usize,
}

/// Get comprehensive connection stats for a specific user pubkey
///
/// This function:
/// 1. Looks up all connections for the pubkey in PUBKEY_CONNECTIONS
/// 2. For each connection, queries CONNECTION_BUNDLES to get subscribed bundles
/// 3. For each bundle, queries BUNDLE_DEFINITIONS to count feeds
/// 4. Aggregates everything into UserConnectionStats
pub fn get_user_connection_stats(pubkey: &Pubkey) -> Option<UserConnectionStats> {
    // Get all connection IDs for this pubkey
    let connection_ids = PUBKEY_CONNECTIONS.get(pubkey)?;

    let mut total_feeds = 0;
    let mut connections_detail = Vec::new();

    for conn_id in connection_ids.iter() {
        // Verify connection still exists in CONNECTIONS
        if !CONNECTIONS.contains_key(conn_id) {
            continue;
        }

        // Get bundles for this connection
        let bundles_detail = if let Some(bundle_ids) = CONNECTION_BUNDLES.get(conn_id) {
            let mut bundles = Vec::new();
            let mut conn_feed_count = 0;

            for bundle_id in bundle_ids.iter() {
                // Get feed count from bundle definition
                if let Some(feeds) = BUNDLE_DEFINITIONS.get(bundle_id) {
                    let feed_count = feeds.len();
                    conn_feed_count += feed_count;

                    bundles.push(BundleDetail {
                        bundle_id: bundle_id.clone(),
                        feed_count,
                    });
                }
            }

            total_feeds += conn_feed_count;

            Some((conn_feed_count, bundles))
        } else {
            // Connection exists but has no bundles yet
            Some((0, Vec::new()))
        };

        if let Some((feed_count, bundles)) = bundles_detail {
            connections_detail.push(ConnectionDetail {
                connection_id: conn_id.clone(),
                feed_count,
                bundles,
            });
        }
    }

    Some(UserConnectionStats {
        pubkey: pubkey.to_string(),
        active_connections: connections_detail.len(),
        total_feeds,
        connections: connections_detail,
    })
}

/// Get active connection count for a pubkey (fast path)
pub fn get_user_connection_count(pubkey: &Pubkey) -> usize {
    PUBKEY_CONNECTIONS
        .get(pubkey)
        .map(|ids| {
            // Filter to only count connections that still exist
            ids.iter()
                .filter(|id| CONNECTIONS.contains_key(*id))
                .count()
        })
        .unwrap_or(0)
}

/// Get total feed count for a pubkey (fast path)
pub fn get_user_feed_count(pubkey: &Pubkey) -> usize {
    let connection_ids = match PUBKEY_CONNECTIONS.get(pubkey) {
        Some(ids) => ids,
        None => return 0,
    };

    let mut total = 0;
    for conn_id in connection_ids.iter() {
        if let Some(bundles) = CONNECTION_BUNDLES.get(conn_id) {
            for bundle_id in bundles.iter() {
                if let Some(feeds) = BUNDLE_DEFINITIONS.get(bundle_id) {
                    total += feeds.len();
                }
            }
        }
    }
    total
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_user_stats_no_connections() {
        let pubkey = Pubkey::new_unique();
        let stats = get_user_connection_stats(&pubkey);
        assert!(stats.is_none());
    }

    #[test]
    fn test_get_user_connection_count_empty() {
        let pubkey = Pubkey::new_unique();
        let count = get_user_connection_count(&pubkey);
        assert_eq!(count, 0);
    }

    #[test]
    fn test_get_user_feed_count_empty() {
        let pubkey = Pubkey::new_unique();
        let count = get_user_feed_count(&pubkey);
        assert_eq!(count, 0);
    }
}
