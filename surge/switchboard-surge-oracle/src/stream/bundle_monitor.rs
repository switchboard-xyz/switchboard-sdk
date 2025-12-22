use crate::consensus::generate_bundle_consensus_response;
use crate::stream::bundle_state::{
    BundleId, BUNDLE_DEFINITIONS, BUNDLE_SUBSCRIBERS, BUNDLE_SIGNATURE_SCHEMES,
    CONNECTIONS,
};
use crate::stream::monitor_optimized::ensure_shared_price_collector;
use crate::stream::state::{LATEST_PRICES, PriceUpdate};
use anyhow::{anyhow, Result};
use dashmap::DashMap;
use once_cell::sync::Lazy;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::time::interval;
use tracing::{debug, error, info, warn};
 use switchboard_surge_core::Pair;

/// Global bundle monitors - one per unique bundle
static BUNDLE_MONITORS: Lazy<DashMap<BundleId, tokio::task::JoinHandle<()>>> = 
    Lazy::new(DashMap::new);

/// Start monitoring for a specific bundle
pub async fn start_bundle_monitor(bundle_id: String) {
    // Check if monitor already exists
    if BUNDLE_MONITORS.contains_key(&bundle_id) {
        debug!("Bundle monitor already running for {}", bundle_id);
        return;
    }
    
    // Get bundle definition
    let feeds = match BUNDLE_DEFINITIONS.get(&bundle_id) {
        Some(def) => def.clone(),
        None => {
            warn!("No bundle definition found for {}", bundle_id);
            return;
        }
    };
    
    // Spawn monitor task with error handling
    let bundle_id_clone = bundle_id.clone();
    let handle = tokio::spawn(async move {
        if let Err(e) = monitor_bundle_task(bundle_id_clone.clone(), feeds).await {
            error!("Bundle monitor task failed for {}: {}", bundle_id_clone, e);
        }
    });
    BUNDLE_MONITORS.insert(bundle_id.clone(), handle);
    
    info!("Started bundle monitor for {}", bundle_id);
}

/// Stop monitoring for a specific bundle
pub async fn stop_bundle_monitor(bundle_id: &str) {
    if let Some((_, handle)) = BUNDLE_MONITORS.remove(bundle_id) {
        handle.abort();
        debug!("Stopped bundle monitor for {}", bundle_id);
    }
}

/// Monitor task for a single bundle
async fn monitor_bundle_task(bundle_id: String, feeds: Vec<(String, Pair)>) -> Result<()> {
    // Ensure shared collectors for each feed in this bundle
    for (source_str, pair) in &feeds {
        if let Ok(source) = crate::messages::parse_source(source_str) {
            ensure_shared_price_collector(source, pair.clone()).await;
        }
    }

    // Track last sent prices per subscriber to detect changes
    // Key: subscriber_id -> Map of (source, pair) -> last_sent_price
    let subscriber_last_prices: DashMap<String, DashMap<(String, Pair), Decimal>> = DashMap::new();

    // Track last heartbeat time per subscriber (separate from last_sent_time for batch interval)
    // This ensures heartbeats don't reset the batch interval timer
    let mut subscriber_last_heartbeat: HashMap<String, Instant> = HashMap::new();

    // Monitor for price changes via cache polling
    let mut last_update = Instant::now();
    let mut tick = interval(Duration::from_millis(5)); // match crossbar's 5ms cadence
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    // Heartbeat interval: send updates even if price unchanged after 2 seconds
    const HEARTBEAT_INTERVAL_MS: u64 = 2000;

    let mut check_count = 0u64;

    loop {
        tick.tick().await;
        check_count += 1;

        // Stop if no more subscribers
        if !BUNDLE_SUBSCRIBERS.contains_key(&bundle_id) {
            debug!("No more subscribers for bundle {}, stopping monitor", bundle_id);
            break;
        }

            // Snapshot all subscribers for this bundle
            let subscribers = match BUNDLE_SUBSCRIBERS.get(&bundle_id) {
                Some(map) => {
                    map.clone()
                },
                None => {
                    debug!("No subscribers found for bundle {} at check #{}, exiting monitor", 
                        bundle_id, check_count);
                    break;
                }
            };

        // Log monitoring status periodically
        if check_count % 1000 == 0 {
            info!(
                "[BUNDLE MONITOR] Bundle {}: check #{}, {} subscribers, {} feeds",
                bundle_id, check_count, subscribers.len(), feeds.len()
            );
        }

        // Check each subscriber if their interval has elapsed
        for sub_entry in subscribers.iter() {
            let (sub_id, sub) = sub_entry.pair();
            
            // Check if this subscriber's interval has elapsed
            let elapsed = {
                let last_sent = sub.last_sent_time.read();
                last_sent.elapsed().as_millis() as u64
            }; // Drop the lock before continuing

            if elapsed < sub.batch_interval_ms {
                continue;
            }

            // Check if heartbeat is due (for slow-moving tickers)
            // Use separate heartbeat tracking so heartbeats don't reset batch interval timer
            let should_send_heartbeat = subscriber_last_heartbeat
                .get(sub_id)
                .map(|t| t.elapsed().as_millis() as u64 >= HEARTBEAT_INTERVAL_MS)
                .unwrap_or(elapsed >= HEARTBEAT_INTERVAL_MS);

            // Log when we're about to send (for interval verification)
            debug!(
                "⏱️ Checking bundle update: bundle={}, subscriber={}, elapsed={}ms, tier_interval={}ms, heartbeat_due={}",
                bundle_id, sub_id, elapsed, sub.batch_interval_ms, should_send_heartbeat
            );
            
            // Get or create last prices map for this subscriber
            let sub_prices = subscriber_last_prices
                .entry(sub_id.clone())
                .or_insert_with(DashMap::new);

            // Check if any prices changed for this subscriber
            let mut any_changed = false;
            let mut current_prices: Vec<((String, Pair), PriceUpdate)> = Vec::new();

            for (source_str, pair) in &feeds {
                if let Ok(source) = crate::messages::parse_source(source_str) {
                    if let Some(price_update) = LATEST_PRICES.get(&(source, pair.clone())) {
                        let key = (source_str.clone(), pair.clone());
                        let current_price = price_update.price;

                        // Check freshness and warn about stale prices
                        // Use the MORE RECENT timestamp between seen_at and verified_at
                        // This ensures we always show the most accurate freshness
                        let most_recent_ts = std::cmp::max(
                            price_update.seen_at,
                            price_update.verified_at
                        );

                        let now_ms = switchboard_surge_core::get_corrected_timestamp_ms();
                        let age_ms = now_ms.saturating_sub(most_recent_ts);

                        if age_ms > 10000 {  // 10 seconds
                            warn!("Price for {}/{} is stale: {}ms old (seen_at: {}, verified_at: {}, using: {})",
                                source_str, pair.as_str(), age_ms,
                                price_update.seen_at, price_update.verified_at, most_recent_ts);
                        }

                        debug!("📍 PRICE from cache: {}/{} = {} (age: {}ms, event_ts: {}, seen_at: {}, verified_at: {})",
                            source_str, pair.as_str(), price_update.price, age_ms,
                            price_update.event_ts, price_update.seen_at, price_update.verified_at);

                        // Store full PriceUpdate for later (preserves timestamps)
                        current_prices.push((key.clone(), price_update.clone()));

                        // Check against subscriber's last sent price
                        if let Some(last_price) = sub_prices.get(&key) {
                            // Log detailed comparison
                            debug!("📊 COMPARING prices for {}/{}: last_sent={}, current={}, equal={}",
                                source_str, pair.as_str(), *last_price, current_price,
                                (*last_price == current_price));

                            if *last_price != price_update.price {
                                any_changed = true;
                                let diff = current_price - *last_price;
                                debug!("🔄 PRICE CHANGE DETECTED for {}/{}: {} -> {} (diff: {}) (subscriber: {}, check: {})",
                                    source_str, pair.as_str(), *last_price, current_price, diff, sub_id, check_count);
                            } else {
                                // Always log when price hasn't changed for debugging
                                debug!("✅ No price change for {}/{}: {} (subscriber: {}, check: {})",
                                    source_str, pair.as_str(), current_price, sub_id, check_count);
                            }
                        }
                        // Note: First prices will be sent via heartbeat mechanism (no immediate send)
                    } else {
                        // Log every 500 checks when no price found
                        if check_count % 500 == 0 {
                            debug!("No price found in LATEST_PRICES for {}/{} (check: {})",
                                source_str, pair.as_str(), check_count);
                        }
                    }
                } else {
                    error!("Failed to parse source: {}", source_str);
                }
            }

            // Log summary of update check
            debug!("📋 BUNDLE {} UPDATE CHECK: any_changed={}, heartbeat_due={}, subscriber={}, check={}",
                bundle_id, any_changed, should_send_heartbeat, sub_id, check_count);

            // Only skip if no price change AND heartbeat not due
            if !any_changed && !should_send_heartbeat {
                debug!("⏭️ SKIPPING update - no changes and heartbeat not due (elapsed: {}ms) for subscriber {}", elapsed, sub_id);
                continue;
            }

            // Determine reason for sending and capture heartbeat timestamp if applicable
            let heartbeat_at_ts_ms = if !any_changed && should_send_heartbeat {
                let ts = switchboard_surge_core::get_corrected_timestamp_ms();
                debug!("💓 SENDING HEARTBEAT for subscriber {} (no price change, elapsed: {}ms)", sub_id, elapsed);
                Some(ts)
            } else {
                if any_changed {
                    debug!("🔄 PRICE CHANGE - sending update for subscriber {} (check: {}, elapsed: {}ms)",
                        sub_id, check_count, elapsed);
                }
                None
            };

            // Update subscriber's last sent prices BEFORE sending to prevent race condition
            for (key, price_update) in &current_prices {
                debug!("💾 STORING PRICE for {}/{}: {} (subscriber: {})",
                    key.0, key.1.as_str(), price_update.price, sub_id);
                sub_prices.insert(key.clone(), price_update.price);
            }
            debug!("✅ UPDATED {} prices for subscriber {} (map size: {})",
                current_prices.len(), sub_id, sub_prices.len());

            // Only update last_sent_time on price changes to preserve batch interval timing
            // Heartbeats use separate tracking so they don't disrupt the update schedule
            if any_changed {
                *sub.last_sent_time.write() = Instant::now();
            }

            // Update heartbeat tracker on ANY send (price change or heartbeat)
            // This resets the 2-second heartbeat timer whenever we broadcast
            subscriber_last_heartbeat.insert(sub_id.clone(), Instant::now());

            // Get connection state (for oracle config)
            debug!("🔍 LOOKING UP connection with ID: {} (subscriber: {})", sub.connection_id, sub_id);
            let conn = match CONNECTIONS.get(&sub.connection_id) {
                Some(c) => {
                    debug!("✅ Found connection state for {} (subscriber: {})", sub.connection_id, sub_id);
                    c.clone()
                },
                None => {
                    error!("❌ No connection state found for {} (subscriber: {})", sub.connection_id, sub_id);
                    debug!("🗂️ Available connections in CONNECTIONS map: {:?}", 
                        CONNECTIONS.iter().map(|entry| entry.key().clone()).collect::<Vec<_>>());
                    continue;
                },
            };

            // Fetch slot and recent hash from provider - required for consensus
            let (slot, recent_hash) = crate::consensus::current_slot_and_hash()
                .ok_or_else(|| anyhow!("Failed to get current slot and hash for bundle {}", bundle_id))?;

            // Get the signature scheme for this bundle (defaults to Ed25519 if not found)
            let signature_scheme = BUNDLE_SIGNATURE_SCHEMES.get(&bundle_id)
                .map(|entry| *entry.value())
                .or(None);

            match generate_bundle_consensus_response(&feeds, &conn.oracle_config, slot, recent_hash, signature_scheme, Some(&current_prices), any_changed, heartbeat_at_ts_ms).await {
                Ok(response) => {
                    debug!("📡 SENDING message to channel for subscriber {}", sub_id);
                    if let Err(e) = sub.tx.send(response) {
                        error!("❌ CHANNEL SEND FAILED for {}: {}", sub_id, e);
                        // Connection might be closed, remove from tracking
                        subscriber_last_prices.remove(sub_id);
                    } else {
                        let now_ts = switchboard_surge_core::get_corrected_timestamp_ms();
                        debug!("✅ SENT to {} - bundle: {}, changed: {}, timestamp: {}",
                            sub_id, bundle_id, any_changed, now_ts);

                        // Log update timing for interval verification (every 10 updates)
                        if check_count % 10 == 0 {
                            debug!(
                                "⏱️ Bundle {} update sent: subscriber={}, elapsed={}ms, tier_interval={}ms, ts={}",
                                bundle_id, sub_id, elapsed, sub.batch_interval_ms, now_ts
                            );
                        }

                        last_update = Instant::now();

                        if any_changed {
                            debug!("📈 Sent bundle {} update to {} with price changes", bundle_id, sub_id);
                        } else {
                            debug!("📊 Sent bundle {} update to {} (no price change, first or freshness update)", bundle_id, sub_id);
                        }
                    }
                }
                Err(e) => {
                    error!("Failed to generate consensus response for bundle {}: {}", bundle_id, e);
                }
            }
        }

        // Clean up if bundle hasn't been active in a while
        if last_update.elapsed() > Duration::from_secs(300) {
            info!("Bundle {} inactive for 5 minutes, stopping monitor", bundle_id);
            break;
        }
        
    }

    // Clean up
    debug!("Bundle {} monitor exiting after {} checks", bundle_id, check_count);
    BUNDLE_MONITORS.remove(&bundle_id);
    Ok(())
}

/// Start monitoring for all bundles of a connection
pub async fn start_monitoring_for_connection(connection_id: &str) {
    use crate::stream::bundle_state::CONNECTION_BUNDLES;
    
    if let Some(bundles) = CONNECTION_BUNDLES.get(connection_id) {
        for bundle_id in bundles.iter() {
            start_bundle_monitor(bundle_id.clone()).await;
        }
    }
}

/// Stop monitoring for bundles that have no subscribers
pub async fn cleanup_inactive_bundles() {
    let mut to_remove = Vec::new();
    
    for entry in BUNDLE_MONITORS.iter() {
        let bundle_id = entry.key();
        if !BUNDLE_SUBSCRIBERS.contains_key(bundle_id) {
            to_remove.push(bundle_id.clone());
        }
    }
    
    for bundle_id in to_remove {
        stop_bundle_monitor(&bundle_id).await;
    }
}

/// Global cleanup task that runs periodically
pub fn start_cleanup_task() {
    tokio::spawn(async {
        let mut interval = interval(Duration::from_secs(60));
        loop {
            interval.tick().await;
            cleanup_inactive_bundles().await;
        }
    });
}