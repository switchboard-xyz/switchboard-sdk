use crate::messages::{ServerMessage, ConsensusStreamFeedValue, StreamConsensusResponse};
use crate::stream::state::{
    ConnectionState, ConnectionTiming, FeedPriceState, PriceUpdate, SharedMonitorState,
    CONNECTION_TIMINGS, CONNECTIONS, FEED_PRICE_TRACKING, GLOBAL_DISPATCHER_RUNNING,
    LATEST_PRICES, SHARED_MONITORS
};
use anyhow::Result;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use switchboard_surge_core::{Pair, Source, SurgeHub};
use tokio::time::interval;
use tracing::{debug, info, warn, error};
use rust_decimal::Decimal;

/// Optimized monitoring that uses shared collectors and dispatcher
pub async fn start_high_priority_monitoring(state: Arc<ConnectionState>) {
    let connection_id = state.id.clone();
    let batch_interval_ms = state.default_batch_interval_ms.load(Ordering::Relaxed);
    
    info!("Setting up shared monitoring for connection {} with {}ms interval", 
        connection_id, batch_interval_ms);
    
    // Register connection timing
    CONNECTION_TIMINGS.insert(
        connection_id.clone(),
        ConnectionTiming {
            interval_ms: batch_interval_ms,
            last_sent: Instant::now(),
        }
    );
    
    // Ensure shared price collectors exist for all subscribed feeds
    for entry in state.subscriptions.iter() {
        let ((source, pair), _) = entry.pair();
        ensure_shared_price_collector(*source, pair.clone()).await;
    }
    
    // Ensure global dispatcher is running
    ensure_global_dispatcher().await;
}

/// Stop monitoring feeds that a connection was subscribed to
pub async fn stop_monitoring_for_connection(state: &Arc<ConnectionState>) {
    let connection_id = &state.id;
    info!("Stopping monitoring for connection {}", connection_id);
    
    // Remove from connection timings
    CONNECTION_TIMINGS.remove(connection_id);
    
    // Check each subscribed feed and decrement subscriber count
    for entry in state.subscriptions.iter() {
        let ((source, pair), _) = entry.pair();
        let key = (*source, pair.clone());
        
        if let Some(monitor_state) = SHARED_MONITORS.get_mut(&key) {
            let prev_count = monitor_state.subscriber_count.fetch_sub(1, Ordering::SeqCst);
            let new_count = prev_count - 1;
            
            info!("Feed {:?} {} now has {} subscribers", source, pair.as_str(), new_count);
            
            // If no more subscribers, cancel the monitor
            if new_count == 0 {
                info!("No more subscribers for {:?} {}, stopping collector", source, pair.as_str());
                monitor_state.cancelled.store(true, Ordering::SeqCst);
                // The monitor will remove itself from SHARED_MONITORS when it exits
            }
        }
    }
}

/// Ensure a shared price collector exists for the given feed
pub(crate) async fn ensure_shared_price_collector(source: Source, pair: Pair) {
    let key = (source, pair.clone());
    
    // Check if monitor already exists and increment subscriber count
    if let Some(monitor_state) = SHARED_MONITORS.get_mut(&key) {
        // Check if the monitor is still active (not cancelled)
        if !monitor_state.cancelled.load(Ordering::Relaxed) {
            let count = monitor_state.subscriber_count.fetch_add(1, Ordering::SeqCst) + 1;
            info!("Feed {:?} {} now has {} subscribers", source, pair.as_str(), count);
            return;
        } else {
            // Monitor was cancelled, remove the stale entry
            info!("Found cancelled monitor for {:?} {}, removing and creating new one", source, pair.as_str());
            drop(monitor_state);
            SHARED_MONITORS.remove(&key);
            LATEST_PRICES.remove(&key);
        }
    }

    // This is a NEW monitor - clear any stale cache entry from previous sessions
    // This prevents serving old prices while waiting for the collector to update
    if LATEST_PRICES.remove(&key).is_some() {
        debug!("Cleared stale LATEST_PRICES entry for {:?} {} before starting new monitor",
              source, pair.as_str());
    }

    // Create new monitor state
    let cancelled = Arc::new(AtomicBool::new(false));
    let subscriber_count = Arc::new(AtomicUsize::new(1));
    
    SHARED_MONITORS.insert(key.clone(), SharedMonitorState {
        cancelled: cancelled.clone(),
        subscriber_count: subscriber_count.clone(),
    });
    
    info!("Starting shared price collector for {:?} {} (1 subscriber)", source, pair.as_str());
    
    // Spawn the shared collector with completion logging
    let pair_str = pair.as_str().to_string();
    tokio::spawn(async move {
        debug!("Starting shared price collector for {:?} {}", source, pair_str);
        shared_price_collector(source, pair, cancelled).await;
        debug!("Shared price collector task completed for {:?} {}", source, pair_str);
    });
}

/// Decrement subscriber count for a shared price collector and cancel it if no more subscribers
pub(crate) async fn release_shared_price_collector(source: Source, pair: Pair) {
    let key = (source, pair.clone());

    if let Some(monitor_state) = SHARED_MONITORS.get_mut(&key) {
        let prev = monitor_state.subscriber_count.fetch_sub(1, Ordering::SeqCst);
        let new_count = prev.saturating_sub(1);
        info!(
            "Feed {:?} {} subscriber removed; remaining {}",
            source,
            pair.as_str(),
            new_count
        );

        if new_count == 0 {
            info!(
                "No more subscribers for {:?} {}, cancelling shared collector",
                source,
                pair.as_str()
            );
            monitor_state.cancelled.store(true, Ordering::SeqCst);
            // shared task will remove itself from SHARED_MONITORS and LATEST_PRICES
        }
    }
}

/// Helper to decrement subscription count for a feed (same pattern as stop_monitoring_for_connection)
fn decrement_feed_subscription(source: Source, pair: &Pair) {
    let key = (source, pair.clone());
    
    if let Some(monitor_state) = SHARED_MONITORS.get_mut(&key) {
        let prev_count = monitor_state.subscriber_count.fetch_sub(1, Ordering::SeqCst);
        let new_count = prev_count - 1;
        
        info!("Feed {:?} {} now has {} subscribers", source, pair.as_str(), new_count);
        
        if new_count == 0 {
            info!("No more subscribers for {:?} {}, stopping collector", source, pair.as_str());
            monitor_state.cancelled.store(true, Ordering::SeqCst);
            // The monitor will remove itself from SHARED_MONITORS when it exits
        }
    } else {
        debug!("No shared monitor found for {:?} {} during decrement", source, pair.as_str());
    }
}

/// Ensure the global batch dispatcher is running
async fn ensure_global_dispatcher() {
    // Check if already running
    if GLOBAL_DISPATCHER_RUNNING.load(Ordering::Relaxed) {
        return;
    }
    
    // Try to set the flag (only one thread will succeed)
    if GLOBAL_DISPATCHER_RUNNING.compare_exchange(
        false,
        true,
        Ordering::SeqCst,
        Ordering::SeqCst
    ).is_ok() {
        info!("Starting global batch dispatcher");
        
        // Spawn the dispatcher
        tokio::spawn(async {
            global_batch_dispatcher().await;
        });
    }
}

/// Shared price collector - one per unique feed
async fn shared_price_collector(source: Source, pair: Pair, cancelled: Arc<AtomicBool>) {
    let hub = SurgeHub::global();
    let key = (source, pair.clone());
    
    // For weighted feeds, we need to track the original pair and current actual feed
    let original_pair = pair.clone();
    let mut current_source = source;
    let mut current_pair = pair.clone();
    
    // For weighted feeds, determine what the hub actually resolved this to
    if source == Source::Weighted || source == Source::Auto {
        // Use AutoSourceCalculator which handles both WEIGHTED and AUTO
        let calculator = switchboard_surge_core::weighted_source::AutoSourceCalculator::new();
        if let Some((resolved_source, resolved_pair)) = calculator.get_from_cache_sync(&original_pair) {
            current_source = resolved_source;
            current_pair = resolved_pair;
            info!("Weighted/Auto feed {} resolved to {} on {} for tracking", 
                original_pair.as_str(), current_pair.as_str(), current_source.as_str());
        }
    }
    
    // Get the watch receiver for this feed
    let rx = match hub.watch_price(source, &pair) {
        Some(rx) => rx,
        None => {
            warn!("Failed to get price feed for {:?} {}", source, pair.as_str());
            SHARED_MONITORS.remove(&key);
            return;
        }
    };
    
    let mut price_interval = interval(Duration::from_millis(5));
    price_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    
    // For weighted/auto feeds, increment subscription count for the ACTUAL underlying feed
    // Note: current_source and current_pair have already been resolved above
    if source == Source::Weighted || source == Source::Auto {
        // Only increment if we actually resolved to a different feed
        if current_source != source {
            let underlying_key = (current_source, current_pair.clone());
            if let Some(monitor_state) = SHARED_MONITORS.get_mut(&underlying_key) {
                let count = monitor_state.subscriber_count.fetch_add(1, Ordering::SeqCst) + 1;
                info!("Feed {:?} {} now has {} subscribers (weighted/auto feed started)", 
                    current_source, current_pair.as_str(), count);
            }
        }
    }

    // Oracle doesn't switch feeds mid-subscription - no re-evaluation needed

    let mut poll_count = 0u64;
    let mut last_logged_price = None;
    let mut last_price_change_at = 0u64;

    // Track last price for proper change detection (like crossbar)
    let mut last_price: Option<Decimal> = None;

    // Try to get initial price from hub to populate cache immediately
    // This avoids waiting for the first watch update which might be stale
    {
        let hub_result = if source == Source::Weighted || source == Source::Auto {
            // For weighted/auto, get the actual underlying feed price
            hub.get_price(current_source, &current_pair)
        } else {
            hub.get_price(source, &pair)
        };

        match hub_result {
            Some(current_tick) => {
                // Only update if we have a valid price and valid timestamps (not default/zero)
                // This prevents caching uninitialized/stale data
                if current_tick.price > Decimal::ZERO && current_tick.event_ts > 0 && current_tick.seen_at > 0 {
                    // Apply variance check to initial price, same as we do in the main loop
                    let should_cache_initial = match hub.check_variance(source, &original_pair, current_tick.price) {
                        Ok(()) => {
                            debug!("Initial price for {:?} {}: ${} - variance check passed",
                                source, original_pair.as_str(), current_tick.price);
                            true
                        },
                        Err(e) => {
                            warn!("Initial price variance check failed for {:?} {} - {}",
                                source, original_pair.as_str(), e);
                            false
                        }
                    };

                    if should_cache_initial {
                        // Update the LATEST_PRICES cache immediately with data from hub
                        let now = chrono::Utc::now().timestamp_millis() as u64;
                        info!("Initial cache for {:?} {}: price={}, event_ts={}, seen_at={}, verified_at={}, now={}",
                            source, original_pair.as_str(), current_tick.price,
                            current_tick.event_ts, current_tick.seen_at, current_tick.verified_at, now);

                        // Check staleness of the initial price
                        let staleness_ms = now.saturating_sub(current_tick.verified_at);
                        if staleness_ms > 5000 {
                            warn!("Initial price for {:?} {} is stale: {}ms old (verified_at={})",
                                source, original_pair.as_str(), staleness_ms, current_tick.verified_at);
                        }

                        LATEST_PRICES.insert(key.clone(), PriceUpdate {
                            source,
                            pair: original_pair.clone(),
                            price: current_tick.price,
                            event_ts: current_tick.event_ts,
                            seen_at: current_tick.seen_at,
                            verified_at: current_tick.verified_at,
                        });
                    }
                } else {
                    debug!("Skipping initial price for {:?} {} - uninitialized tick (price={}, event_ts={}, seen_at={})",
                        source, original_pair.as_str(), current_tick.price, current_tick.event_ts, current_tick.seen_at);
                }
            },
            None => {
                debug!("No initial price available for {:?} {}", source, original_pair.as_str());
            }
        }
    }

    loop {
        if cancelled.load(Ordering::Relaxed) {
            info!("Shared collector for {:?} {} cancelled", source, pair.as_str());
            break;
        }
        
        price_interval.tick().await;
        poll_count += 1;

        // Get current price
        let tick = rx.borrow().clone();

        // Skip if this is an uninitialized tick (default values with zero timestamps or zero price)
        // This prevents stale data from being cached or sent to clients
        // CRITICAL: Also check for zero/negative prices which should never be streamed
        if tick.event_ts == 0 || tick.seen_at == 0 || tick.price <= Decimal::ZERO {
            debug!("Skipping invalid tick for {:?} {} (price={}, event_ts={}, seen_at={})",
                source, pair.as_str(), tick.price, tick.event_ts, tick.seen_at);
            continue;
        }

        // CRITICAL: Skip obviously stale data from watch channel
        // Watch channels hold the last value which could be very old if no recent updates
        // Check if this tick is older than 30 seconds
        let current_time_ms = chrono::Utc::now().timestamp_millis() as u64;
        let tick_age_ms = current_time_ms.saturating_sub(tick.verified_at);
        if tick_age_ms > 30000 {
            // Check if we already have fresher data in cache
            if let Some(cached) = LATEST_PRICES.get(&key) {
                if cached.verified_at > tick.verified_at {
                    debug!("Skipping stale tick from watch channel for {:?} {} (age={}ms, cached is fresher)",
                        source, pair.as_str(), tick_age_ms);
                    continue;
                }
            }
            // Log warning but still process if no cached data or cached is equally old
            warn!("Processing old tick for {:?} {} - age={}ms, verified_at={}",
                source, pair.as_str(), tick_age_ms, tick.verified_at);
        }

        // Check if price changed (proper detection like crossbar)
        let price_changed = last_price.map_or(true, |last| last != tick.price);

        // Also check if verified_at has been updated (freshness confirmation)
        let verified_at_updated = if let Some(cached) = LATEST_PRICES.get(&key) {
            tick.verified_at > cached.verified_at
        } else {
            true  // No cache entry, should update
        };

        // Update cache if price changed OR verified_at is fresher
        let should_update_cache = if price_changed {
            // Price changed - validate ALL sources before updating cache
            match hub.check_variance(source, &original_pair, tick.price) {
                Ok(()) => {
                    debug!("✅ ORACLE VARIANCE CHECK: {:?} {} price changed, variance check passed",
                        source, original_pair.as_str());
                    true // Allow cache update
                },
                Err(e) => {
                    error!("❌ ORACLE VARIANCE ERROR: {:?} {} - {}", source, original_pair.as_str(), e);
                    error!("   Blocking price update to prevent unreliable data from affecting consensus");
                    false // Block cache update
                }
            }
        } else if verified_at_updated {
            // Price hasn't changed but verified_at is fresher
            // Still need to validate the price is within variance threshold
            match hub.check_variance(source, &original_pair, tick.price) {
                Ok(()) => {
                    debug!("Freshness update for {:?} {} - variance check passed",
                        source, original_pair.as_str());
                    true  // Variance still acceptable, update freshness
                },
                Err(e) => {
                    warn!("Variance check failed for unchanged price during freshness update for {:?} {} - {}",
                        source, original_pair.as_str(), e);
                    false  // Block even freshness updates if variance exceeded
                }
            }
        } else {
            // Neither price nor verified_at changed - skip
            false
        };
        
        // Heartbeat log every 1000 polls (5 seconds) for monitoring
        if poll_count > 0 && poll_count % 100000 == 0 {
            info!("[DEBUG PRICE LOG] {:?} {} poll #{}, price: {}, event_ts: {}", 
                source, pair.as_str(), poll_count, tick.price, tick.event_ts);
        }
        
        // Log price changes
        if last_logged_price != Some(tick.price) {
            debug!(
                "[PRICE CHANGE] {:?} {}: {} -> {} (after {} polls, event_ts: {}, seen_at: {})",
                source,
                pair.as_str(),
                last_logged_price.map_or("None".to_string(), |p| p.to_string()),
                tick.price,
                poll_count - last_price_change_at,
                tick.event_ts,
                tick.seen_at
            );
            last_price_change_at = poll_count;
            last_logged_price = Some(tick.price);
        }
        
        // Update last price tracking (like crossbar)
        let is_first_price = last_price.is_none();
        
        // Only update cache and last price if variance check passed
        if should_update_cache || is_first_price {
            last_price = Some(tick.price);

            let reason = if is_first_price {
                "first_price"
            } else if price_changed {
                "price_changed_and_validated"
            } else if verified_at_updated {
                "freshness_update"  // Track freshness updates
            } else {
                "unknown"
            };

            debug!("[CACHE UPDATE] {:?} {} updating cache: {} (reason: {}, event_ts: {}, seen_at: {}, verified_at: {})",
                source, pair.as_str(), tick.price, reason, tick.event_ts, tick.seen_at, tick.verified_at);

            // Only update if the new data is fresher than what's cached
            // This prevents overwriting fresh data with stale data from delayed watch updates
            let should_insert = if let Some(cached) = LATEST_PRICES.get(&key) {
                // Only update if:
                // 1. New data is strictly fresher (verified_at is newer)
                // 2. Same freshness but price changed (verified_at equal but price different)
                tick.verified_at > cached.verified_at ||
                    (tick.verified_at == cached.verified_at && tick.price != cached.price)
            } else {
                true  // No cache entry, always insert
            };

            if should_insert {
                LATEST_PRICES.insert(key.clone(), PriceUpdate {
                    source,
                    pair: original_pair.clone(),
                    price: tick.price,
                    event_ts: tick.event_ts,
                    seen_at: tick.seen_at,
                    verified_at: tick.verified_at,
                });
            } else {
                debug!("Skipping LATEST_PRICES update - cached data is fresher");
            }
        } else if price_changed {
            // Price changed but failed variance check - don't update
            debug!("🚫 ORACLE: Skipping cache update for {:?} {} due to variance check failure", 
                source, original_pair.as_str());
        }

    }
    
    // For AUTO and Weighted feeds, clean up the underlying feed subscription
    if source == Source::Auto || source == Source::Weighted {
        decrement_feed_subscription(current_source, &current_pair);
    }
    
    // Cleanup
    SHARED_MONITORS.remove(&key);
    LATEST_PRICES.remove(&key);
    
    debug!("Removed monitors and prices for {:?} {}", source, pair.as_str());
}

/// Global batch dispatcher - sends updates to all connections based on their intervals
async fn global_batch_dispatcher() {
    let mut interval = interval(Duration::from_millis(5));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    
    info!("Global batch dispatcher started");
    
    loop {
        interval.tick().await;
        
        let now = Instant::now();
        
        // Check each connection to see if it needs updates
        let mut connections_to_update = Vec::new();
        
        for timing_entry in CONNECTION_TIMINGS.iter() {
            let (conn_id, timing) = timing_entry.pair();
            
            // Check if this connection is due for an update
            if now.duration_since(timing.last_sent) >= Duration::from_millis(timing.interval_ms) {
                connections_to_update.push(conn_id.clone());
            }
        }
        
        // Process updates for each connection
        for conn_id in connections_to_update {
            // Get the connection state
            if let Some(conn_state) = CONNECTIONS.get(&conn_id) {
                // Send updates for this connection
                if let Err(e) = send_connection_updates(&conn_state, now).await {
                    debug!("Failed to send updates to {}: {}", conn_id, e);
                }
                
                // Update last sent time
                if let Some(mut timing) = CONNECTION_TIMINGS.get_mut(&conn_id) {
                    timing.last_sent = now;
                }
            }
        }
        
        // Also check for connections that have been removed
        // (cleanup happens in the connection cleanup function)
    }
}

/// Send price updates to a specific connection
async fn send_connection_updates(state: &Arc<ConnectionState>, now: Instant) -> Result<()> {
    let feed_values: Vec<ConsensusStreamFeedValue> = Vec::new();
    
    // Check each subscribed feed
    for entry in state.subscriptions.iter() {
        let ((source, pair), _) = entry.pair();
        let key = (*source, pair.clone());
        
        // Get latest price from cache
        if let Some(price_update) = LATEST_PRICES.get(&key) {
            let feed_key = (state.id.clone(), *source, pair.clone());
            
            // Check if price changed
            let should_send = if let Some(price_state) = FEED_PRICE_TRACKING.get(&feed_key) {
                price_state.last_sent_price.map_or(true, |last| last != price_update.price)
            } else {
                true
            };
            
            if should_send {
                // Update tracking state
                FEED_PRICE_TRACKING.insert(feed_key, FeedPriceState {
                    last_sent_price: Some(price_update.price),
                    last_sent_time: now,
                    last_check_time: now,
                });
                
                // Add to batch
                // Scale price to 18 decimals for BigInt compatibility
                let mut scaled_price = price_update.price;
                scaled_price.rescale(18);
                
                // In oracle mode, would create ConsensusStreamFeedValue here
                // feed_values.push(ConsensusStreamFeedValue {
                //     value: scaled_price.mantissa().to_string(),
                //     feed_hash: format!("{}_{}", source.as_str(), pair.as_str()),
                //     symbol: pair.as_str().to_string(),
                //     source: source.as_str().to_uppercase(),
                // });
            }
        }
    }
    
    // Send batch if we have updates
    if !feed_values.is_empty() {
        let bundle_id = format!("crossbar_{}", state.id);
        // For oracle mode, create a bundled update
        // Note: In production, this would include proper consensus/signing
        let now_ts = switchboard_surge_core::get_corrected_timestamp_ms();
        let msg = ServerMessage::BundledFeedUpdate {
            feed_bundle_id: bundle_id,
            feed_values,
            oracle_response: StreamConsensusResponse {
                oracle_pubkey: String::new(),
                eth_address: String::new(),
                signature: String::new(),
                checksum: Vec::new(),
                recovery_id: 0,
                oracle_idx: 0,
                timestamp: 0,
                timestamp_ms: Some(now_ts),
                recent_hash: String::new(),
                slot: 0,
                ed25519_enclave_signer: None,  // Not used in optimized monitor
            },
            broadcast_ts_ms: now_ts,
            source_ts_ms: now_ts,   // Deprecated - set to broadcast_ts_ms for compatibility
            seen_at_ts_ms: now_ts,  // Deprecated - set to broadcast_ts_ms for compatibility
            triggered_on_price_change: false,
            heartbeat_at_ts_ms: None,  // Not used in optimized monitor
        };
        
        state.tx.send(msg)?;
    }
    
    Ok(())
}
