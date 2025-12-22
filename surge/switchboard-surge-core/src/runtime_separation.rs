use once_cell::sync::Lazy;
use std::sync::Arc;
use std::collections::HashSet;
use tokio::runtime::Runtime;
use tracing::{info, warn, error, debug};
use anyhow::Result;
use crate::{Source, Pair};

/// Dedicated runtime for exchange data ingestion
/// This isolates all exchange connections, reconnections, and data processing
/// from the client-facing WebSocket broadcasting runtime
pub static INGESTION_RUNTIME: Lazy<Arc<Runtime>> = Lazy::new(|| {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8) // Dedicated threads for ingestion
        .thread_name("surge-ingestion")
        .enable_all()
        .build()
        .expect("Failed to create ingestion runtime");
    
    info!("🚀 Created dedicated ingestion runtime with 8 worker threads");
    Arc::new(runtime)
});

/// Initialize and start all exchanges in the ingestion runtime
pub async fn start_ingestion_runtime() -> anyhow::Result<()> {
    info!("🌊 Starting exchange ingestion in dedicated runtime...");
    
    // Spawn the entire SurgeHub initialization in the ingestion runtime
    let ingestion_handle = INGESTION_RUNTIME.spawn(async {
        use crate::{all_surge_sources, SurgeHub};
        
        info!("🚀 Initializing SurgeHub in ingestion runtime...");
        
        // Prefetch exchange info for all exchanges
        info!("🌊 Prefetching exchange info...");
        
        // Run these in parallel for faster startup
        let (binance_result, okx_result, coinbase_result, bybit_result, bitget_result) = tokio::join!(
            crate::exchanges::binance::BinanceStream::prefetch_exchange_info(),
            crate::exchanges::okx::OkxStream::prefetch_exchange_info(),
            crate::exchanges::coinbase::CoinbaseStream::prefetch_exchange_info(),
            crate::exchanges::bybit::BybitStream::prefetch_exchange_info(),
            crate::exchanges::bitget::BitgetStream::prefetch_exchange_info()
        );
        
        if let Err(e) = binance_result {
            warn!("Failed to prefetch Binance exchange info: {}", e);
        }
        if let Err(e) = okx_result {
            warn!("Failed to prefetch OKX exchange info: {}", e);
        }
        if let Err(e) = coinbase_result {
            warn!("Failed to prefetch Coinbase exchange info: {}", e);
        }
        if let Err(e) = bybit_result {
            warn!("Failed to prefetch Bybit exchange info: {}", e);
        }
        if let Err(e) = bitget_result {
            warn!("Failed to prefetch Bitget exchange info: {}", e);
        }
        
        // Initialize the global hub with all exchanges
        match SurgeHub::initialize_global().await {
            Ok(_) => {
                info!("✅ Global SurgeHub initialized successfully in ingestion runtime");
                
                let sources = all_surge_sources();
                info!("🌊 Initialized SurgeHub with {} possible sources: {:?}", sources.len(), sources);
                
                // Start weighted source background updates
                // DISABLED - Using AUTO only for now
                // crate::weighted_source::WeightedSourceCalculator::start_background_updates();
                
                // Start AUTO source background updates
                crate::weighted_source::AutoSourceCalculator::start_background_updates();
                
                // Start message rate tracking background updates
                crate::message_rate::start_message_rate_updater();
                
                // Start clock sync if not disabled
                crate::clock_sync::start_clock_sync_task();
                
                // Start dynamic exchange info refresh task
                start_exchange_info_refresher();
                
                Ok(())
            }
            Err(e) => {
                error!("❌ Failed to initialize SurgeHub: {}", e);
                Err(e)
            }
        }
    });
    
    // Wait for initialization to complete
    ingestion_handle.await?
}

/// Check if the ingestion runtime is healthy
pub fn is_ingestion_runtime_healthy() -> bool {
    // Check if we can submit a simple task
    let handle = INGESTION_RUNTIME.handle();
    
    // Try to spawn a test task
    let test_handle = handle.spawn(async {
        tokio::time::sleep(tokio::time::Duration::from_micros(1)).await;
    });
    
    // If we could spawn it, runtime is responsive
    drop(test_handle); // We don't need to wait for it
    true
}

/// Get metrics from the ingestion runtime
pub fn get_ingestion_metrics() -> (usize, usize, usize) {
    let metrics = INGESTION_RUNTIME.metrics();
    (
        metrics.num_workers(),
        0, // num_blocking_threads is not available in current tokio version
        0, // active_tasks_count is not available in current tokio version
    )
}

/// Spawn a task on the ingestion runtime
pub fn spawn_on_ingestion<F>(future: F) -> tokio::task::JoinHandle<F::Output>
where
    F: std::future::Future + Send + 'static,
    F::Output: Send + 'static,
{
    INGESTION_RUNTIME.spawn(future)
}

/// Spawn a named task on the ingestion runtime
pub fn spawn_on_ingestion_named<F>(name: &str, future: F) -> tokio::task::JoinHandle<F::Output>
where
    F: std::future::Future + Send + 'static,
    F::Output: Send + 'static,
{
    // tokio::task::Builder requires 'tracing' feature, fall back to regular spawn
    INGESTION_RUNTIME.handle().spawn(future)
}

/// Refresh exchange info for all exchanges to discover newly listed tokens
/// Returns total number of new symbols discovered across all exchanges
async fn refresh_all_exchange_info() -> anyhow::Result<usize> {
    info!("🔄 Refreshing exchange info for all exchanges");
    
    // Run all refreshes in parallel for efficiency
    let (binance_result, okx_result, bybit_result, bitget_result) = tokio::join!(
        crate::exchanges::binance::BinanceStream::refresh_exchange_info(),
        crate::exchanges::okx::OkxStream::refresh_exchange_info(),
        crate::exchanges::bybit::BybitStream::refresh_exchange_info(),
        crate::exchanges::bitget::BitgetStream::refresh_exchange_info()
    );
    
    let mut total_new = 0;
    
    // Process results and count new symbols
    if let Ok(new) = binance_result {
        total_new += new;
        if new > 0 {
            info!("🆕 Binance: {} new symbols", new);
        }
    } else if let Err(e) = binance_result {
        warn!("Failed to refresh Binance exchange info: {}", e);
    }
    
    if let Ok(new) = okx_result {
        total_new += new;
        if new > 0 {
            info!("🆕 OKX: {} new symbols", new);
        }
    } else if let Err(e) = okx_result {
        warn!("Failed to refresh OKX exchange info: {}", e);
    }
    
    if let Ok(new) = bybit_result {
        total_new += new;
        if new > 0 {
            info!("🆕 Bybit: {} new symbols", new);
        }
    } else if let Err(e) = bybit_result {
        warn!("Failed to refresh Bybit exchange info: {}", e);
    }
    
    if let Ok(new) = bitget_result {
        total_new += new;
        if new > 0 {
            info!("🆕 Bitget: {} new symbols", new);
        }
    } else if let Err(e) = bitget_result {
        warn!("Failed to refresh Bitget exchange info: {}", e);
    }
    
    if total_new > 0 {
        info!("✅ Discovered {} new symbols across all exchanges", total_new);
        
        // Trigger immediate AUTO cache update when new symbols are found
        info!("🔄 Triggering AUTO cache update for new symbols");
        crate::weighted_source::trigger_auto_cache_update();
        
        // CRITICAL: Subscribe to new symbols on WebSocket connections
        // Without this, new symbols won't stream prices until server restart
        info!("📡 Subscribing to new symbols on WebSocket connections");
        if let Err(e) = subscribe_to_new_symbols().await {
            warn!("Failed to subscribe to new symbols: {}", e);
        }
    } else {
        info!("No new symbols discovered in this refresh cycle");
    }
    
    Ok(total_new)
}

/// Subscribe to newly discovered symbols on WebSocket connections and update REST manager
async fn subscribe_to_new_symbols() -> Result<()> {
    let hub = crate::SurgeHub::global();
    let rest_manager = crate::rest_manager::RestManager::global();
    
    // Get all currently known pairs from the hub
    let existing_pairs: HashSet<(Source, Pair)> = hub.all_known_pairs()
        .into_iter()
        .filter(|(source, pair)| {
            // Only real exchange pairs, not weighted/auto or exchange-specific
            *source != Source::Weighted && *source != Source::Auto && !pair.base.contains(':')
        })
        .collect();
    
    // Get all pairs from exchange info (includes new symbols)
    let mut all_current_pairs: HashSet<(Source, Pair)> = HashSet::new();
    
    // Collect from each exchange's cached info
    for source in crate::exchange_config::get_active_exchanges() {
        if let Ok(pairs) = crate::exchange_config::get_cached_pairs_if_enabled(source) {
            for pair in pairs {
                all_current_pairs.insert((source, pair));
            }
        }
    }
    
    // Find new pairs (in exchange info but not in hub)
    let new_pairs: Vec<(Source, Pair)> = all_current_pairs
        .difference(&existing_pairs)
        .cloned()
        .collect();
    
    if new_pairs.is_empty() {
        return Ok(());
    }
    
    info!("📡 Found {} new pairs to update", new_pairs.len());
    
    // Update REST manager with ALL current symbols for each exchange
    // This ensures new symbols get polled via REST
    let mut symbols_by_exchange: std::collections::HashMap<Source, HashSet<String>> = std::collections::HashMap::new();
    
    for source in crate::exchange_config::get_active_exchanges() {
        if source == Source::Weighted || source == Source::Auto {
            continue;
        }
        
        let mut symbols = HashSet::new();
        if let Ok(pairs) = crate::exchange_config::get_cached_pairs_if_enabled(source) {
            for pair in pairs {
                // Convert to exchange-specific format
                let symbol = match source {
                    Source::Binance => pair.as_binance_str(),
                    Source::Bybit => pair.as_bybit_str(),
                    Source::Okx => pair.as_okx_str(),
                    Source::Coinbase => pair.as_coinbase_str().replace("/", "-"),
                    Source::Bitget => pair.as_bitget_str(),
                    _ => continue,
                };
                symbols.insert(symbol);
            }
        }
        
        if !symbols.is_empty() {
            symbols_by_exchange.insert(source, symbols);
        }
    }
    
    // Update REST manager for each exchange
    for (source, symbols) in symbols_by_exchange {
        info!("📊 Updating REST manager for {} with {} symbols", source.as_str(), symbols.len());
        rest_manager.set_rest_symbols(source, symbols).await;
    }
    
    // Note: We intentionally do NOT subscribe new symbols to WebSocket
    // They will be picked up by REST polling, which is sufficient for new/low-volume symbols
    // High-volume symbols can be added to WebSocket priority lists manually if needed
    
    info!("✅ Updated REST manager for {} new symbols (REST polling only)", new_pairs.len());
    Ok(())
}

/// Start background task to periodically refresh exchange info
fn start_exchange_info_refresher() {
    spawn_on_ingestion_named(
        "exchange-info-refresher",
        async {
            info!("🚀 Starting exchange info refresher (runs every 30 minutes)");

            // Wait 2 minutes after startup before first refresh
            // This gives exchanges time to stabilize and avoids hitting APIs too early
            tokio::time::sleep(tokio::time::Duration::from_secs(2 * 60)).await;

            // Then refresh every 5 minutes
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(5 * 60));
            interval.tick().await; // Consume first tick
            
            loop {
                interval.tick().await;
                
                info!("⏰ Starting scheduled exchange info refresh");
                match refresh_all_exchange_info().await {
                    Ok(new_count) => {
                        if new_count > 0 {
                            info!("✅ Exchange info refresh completed: {} new symbols discovered", new_count);
                        } else {
                            info!("Exchange info refresh completed: no new symbols");
                        }
                    }
                    Err(e) => {
                        error!("❌ Exchange info refresh failed: {}", e);
                    }
                }
            }
        }
    );
}