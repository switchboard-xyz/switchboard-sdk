use rust_decimal::Decimal;
use rust_decimal::prelude::*;
use tokio::sync::watch;
use crate::types::{Source, Tick};
use crate::pair::Pair;
use tracing::{debug, info, warn};
use std::sync::atomic::{AtomicI32, AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::time::{sleep, Duration};

/// Dynamic BIP correction cache - stores bips * 100 for precision
/// Example: -15 bips = -1500 in cache
pub static FDUSD_BIP_CORRECTION_CACHE: AtomicI32 = AtomicI32::new(-1500); // Default -15 bips * 100
static FDUSD_BIP_TIMESTAMP: AtomicU64 = AtomicU64::new(0);

/// Hard limit for BIP correction: ±30 bips maximum
const MAX_BIP_CORRECTION: i32 = 3000;  // 30 bips * 100

/// Fallback BIP corrections for non-FDUSD stablecoins
const USDT_BIP_CORRECTION: i32 = 0;     // USDT ~= USD
const USDC_BIP_CORRECTION: i32 = 0;     // USDC ~= USD

/// Apply BIP correction to convert stablecoin price to USD
/// Uses dynamic cache for FDUSD, fallback values for others
pub fn apply_bip_correction(price: Decimal, stablecoin: &str) -> Decimal {
    let bips_x100 = match stablecoin.to_uppercase().as_str() {
        "FDUSD" => {
            // Load dynamic BIP correction from atomic cache (never blocks)
            FDUSD_BIP_CORRECTION_CACHE.load(Ordering::Relaxed)
        },
        "USDT" => USDT_BIP_CORRECTION * 100,
        "USDC" => USDC_BIP_CORRECTION * 100,
        _ => 0,
    };

    if bips_x100 == 0 {
        return price;
    }

    // Convert bips*100 to multiplier: -1500 = -15 bips = 0.9985 multiplier (1 + (-15/10000))
    let bips_decimal = Decimal::from(bips_x100) / Decimal::from(100); // Convert back to bips
    let multiplier = Decimal::ONE + (bips_decimal / Decimal::from(10000));
    let corrected_price = price * multiplier;

    corrected_price
}

/// A wrapper around a watch::Receiver that applies BIP correction on-the-fly
/// Uses dynamic BIP correction cache for real-time updates
pub struct BipCorrectedReceiver {
    inner: watch::Receiver<Tick>,
    stablecoin: String,
}

impl BipCorrectedReceiver {
    pub fn new(inner: watch::Receiver<Tick>, stablecoin: &str) -> Self {
        Self {
            inner,
            stablecoin: stablecoin.to_string(),
        }
    }

    /// Get current dynamic BIP multiplier (never blocks)
    fn get_current_multiplier(&self) -> Decimal {
        if self.stablecoin.to_uppercase() != "FDUSD" {
            // Non-FDUSD stablecoins use static corrections
            let bips = match self.stablecoin.to_uppercase().as_str() {
                "USDT" => USDT_BIP_CORRECTION,
                "USDC" => USDC_BIP_CORRECTION,
                _ => 0,
            };
            return Decimal::ONE + (Decimal::from(bips) / Decimal::from(10000));
        }

        // FDUSD uses dynamic atomic cache (lock-free read)
        let bips_x100 = FDUSD_BIP_CORRECTION_CACHE.load(Ordering::Relaxed);
        let bips_decimal = Decimal::from(bips_x100) / Decimal::from(100);

        debug!("🔄 Using DYNAMIC BIP correction for {}: {:+} bips (raw: {})",
               self.stablecoin, bips_decimal, bips_x100);

        // Direct BIP correction: if FDUSD is -15 bips below USD, multiply by (1 + (-15/10000)) = 0.9985
        // This directly converts the BTC/FDUSD price to equivalent BTC/USD price
        Decimal::ONE + (bips_decimal / Decimal::from(10000))
    }

    /// Get the current corrected value
    pub fn borrow(&self) -> Tick {
        let tick = *self.inner.borrow();
        self.apply_correction(tick)
    }

    /// Wait for changes and get corrected value
    pub async fn changed(&mut self) -> Result<(), watch::error::RecvError> {
        self.inner.changed().await
    }

    fn apply_correction(&self, mut tick: Tick) -> Tick {
        let original_price = tick.price;
        let multiplier = self.get_current_multiplier();
        tick.price = original_price * multiplier;

        // Log the actual correction applied to streamed price
        let bips_x100 = if self.stablecoin.to_uppercase() == "FDUSD" {
            FDUSD_BIP_CORRECTION_CACHE.load(Ordering::Relaxed)
        } else {
            0
        };
        let bips_decimal = bips_x100 as f64 / 100.0;

        debug!(
            "🔄 BIP correction applied: {} {} ${} → USD ${} (FDUSD {:+} bips vs USD, conversion multiplier: {})",
            self.stablecoin,
            if self.stablecoin == "FDUSD" { "(DYNAMIC)" } else { "(STATIC)" },
            original_price,
            tick.price,
            bips_decimal,
            multiplier
        );

        tick
    }
}

// Convert to standard watch::Receiver for compatibility
impl From<BipCorrectedReceiver> for watch::Receiver<Tick> {
    fn from(mut corrected: BipCorrectedReceiver) -> Self {
        // Create a new channel for the corrected stream
        let (tx, rx) = watch::channel(Tick::default());

        // Drive the correction in a task that dies with the receiver
        tokio::spawn(async move {
            while corrected.changed().await.is_ok() {
                let corrected_tick = corrected.borrow();
                if tx.send(corrected_tick).is_err() {
                    // No receivers, stop the task
                    debug!("BIP correction task stopping - no more receivers");
                    break;
                }
            }
        });

        rx
    }
}

/// Check if BIP correction should be applied
pub fn should_apply_bip_correction(
    requested_source: Source,
    requested_quote: &str,
    actual_quote: &str,
) -> bool {
    // Only apply for AUTO or WEIGHTED sources
    let is_auto_source = matches!(requested_source, Source::Auto | Source::Weighted);

    // Only when user requested USD but got FDUSD specifically
    // USDT and USDC use 0 bips (no correction needed - they're stable)
    let requested_usd = requested_quote.to_uppercase() == "USD";
    let got_fdusd = actual_quote.to_uppercase() == "FDUSD";

    is_auto_source && requested_usd && got_fdusd
}

/// Calculate BIP difference from FDUSD/USDC or FDUSD/USDT price
/// Returns bips * 100 for atomic storage precision
fn calculate_bips_from_price(fdusd_price: Decimal) -> i32 {
    // BIP calculation: (fdusd_price - 1) * 10000 * 100
    // Example: FDUSD/USDC = 0.9985 -> (0.9985 - 1) * 1000000 = -1500 (-15 bips below USD)
    let bips_f64 = (fdusd_price - Decimal::ONE).to_f64().unwrap_or(0.0) * 1000000.0;
    let bips_i32 = bips_f64.round() as i32;

    // Apply hard limit: ±20 bips maximum
    bips_i32.max(-MAX_BIP_CORRECTION).min(MAX_BIP_CORRECTION)
}

/// Get current Unix timestamp
fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Start the BIP correction monitor service
/// Subscribes to real-time FDUSD/USDC (primary) and FDUSD/USDT (fallback) streams from active exchanges
pub async fn start_bip_correction_monitor() {
    tokio::spawn(async move {
        info!("📊 Starting FDUSD BIP correction monitor (Real-time subscription, ±30 bips limit)");

        // Wait for exchanges to initialize and connect
        info!("⏳ BIP monitor: Waiting 15 seconds for exchanges to initialize...");
        sleep(Duration::from_secs(15)).await;

        // Get active exchanges from configuration
        let active_exchanges = crate::exchange_config::get_active_exchanges();
        if active_exchanges.is_empty() {
            warn!("No active exchanges found - BIP correction monitor cannot start");
            return;
        }

        // Prefer Binance if available, otherwise use first active exchange
        let preferred_exchange = if active_exchanges.contains(&Source::Binance) {
            Source::Binance
        } else {
            active_exchanges[0]
        };

        info!("Using {:?} for FDUSD BIP correction monitoring", preferred_exchange);

        // Get global hub reference
        let hub = crate::hub::SurgeHub::global();

        // Define FDUSD pairs
        let fdusd_usdc_pair = Pair { base: "FDUSD".to_string(), quote: "USDC".to_string() };
        let fdusd_usdt_pair = Pair { base: "FDUSD".to_string(), quote: "USDT".to_string() };

        // Retry watching FDUSD prices until successful (with 5-second intervals)
        let (usdc_stream, usdt_stream) = loop {
            info!("Attempting to watch FDUSD pairs on {:?}...", preferred_exchange);

            let usdc_result = hub.watch_price(preferred_exchange, &fdusd_usdc_pair);
            let usdt_result = hub.watch_price(preferred_exchange, &fdusd_usdt_pair);

            match (usdc_result, usdt_result) {
                // At least one watch succeeded
                (Some(usdc_rx), Some(usdt_rx)) => {
                    info!("✅ Successfully watching both FDUSD/USDC and FDUSD/USDT on {:?}", preferred_exchange);
                    break (Some(usdc_rx), Some(usdt_rx));
                }
                (Some(usdc_rx), None) => {
                    info!("✅ Successfully watching FDUSD/USDC on {:?} (USDT not available in cache)", preferred_exchange);
                    break (Some(usdc_rx), None);
                }
                (None, Some(usdt_rx)) => {
                    info!("✅ Successfully watching FDUSD/USDT on {:?} (USDC not available in cache)", preferred_exchange);
                    break (None, Some(usdt_rx));
                }
                (None, None) => {
                    warn!("❌ No FDUSD pairs available in price cache for {:?}", preferred_exchange);
                    warn!("⏳ Retrying in 5 seconds...");
                    sleep(Duration::from_secs(5)).await;
                    continue;
                }
            }
        };

        // Determine which stream(s) we can use
        match (usdc_stream, usdt_stream) {
            (Some(mut usdc_rx), Some(mut usdt_rx)) => {
                info!("📡 Watching both FDUSD/USDC and FDUSD/USDT on {:?} - using USDC as primary", preferred_exchange);

                loop {
                    tokio::select! {
                        // Primary: FDUSD/USDC changes
                        result = usdc_rx.changed() => {
                            if result.is_ok() {
                                let tick = *usdc_rx.borrow();
                                debug!("FDUSD/USDC tick update on {:?}: price={}, event_ts={}", preferred_exchange, tick.price, tick.event_ts);

                                // Only update BIP correction if price actually changed
                                static mut LAST_USDC_PRICE: Option<Decimal> = None;
                                let price_changed = unsafe {
                                    let changed = LAST_USDC_PRICE.map_or(true, |last| last != tick.price);
                                    LAST_USDC_PRICE = Some(tick.price);
                                    changed
                                };

                                if price_changed {
                                    debug!("FDUSD/USDC price changed on {:?}: {}", preferred_exchange, tick.price);
                                    update_bip_correction(tick.price, &format!("FDUSD/USDC ({})", preferred_exchange.as_str()));
                                } else {
                                    debug!("FDUSD/USDC price unchanged on {:?}: {} (skipping BIP update)", preferred_exchange, tick.price);
                                }
                            } else {
                                warn!("FDUSD/USDC stream ended on {:?}, falling back to USDT only", preferred_exchange);
                                break;
                            }
                        }

                        // Keep USDT receiver alive but prioritize USDC
                        _ = usdt_rx.changed() => {
                            // Just consume the change to keep receiver alive
                            // USDC takes priority for BIP calculations
                        }
                    }
                }

                // If USDC stream fails, continue with USDT only
                info!("Continuing with FDUSD/USDT stream only on {:?}", preferred_exchange);
                monitor_single_stream(usdt_rx, &format!("FDUSD/USDT ({})", preferred_exchange.as_str())).await;
            }

            (Some(mut usdc_rx), None) => {
                info!("📡 Watching FDUSD/USDC only on {:?} (USDT unavailable)", preferred_exchange);
                monitor_single_stream(usdc_rx, &format!("FDUSD/USDC ({})", preferred_exchange.as_str())).await;
            }

            (None, Some(mut usdt_rx)) => {
                info!("📡 Watching FDUSD/USDT only on {:?} (USDC unavailable)", preferred_exchange);
                monitor_single_stream(usdt_rx, &format!("FDUSD/USDT ({})", preferred_exchange.as_str())).await;
            }

            (None, None) => {
                // This case should never happen due to retry loop above
                unreachable!("Both watch attempts returned None after retry loop");
            }
        }
    });

    info!("FDUSD BIP correction monitor started in background");
}

/// Get exchange name for logging
fn exchange_name(source: Source) -> &'static str {
    match source {
        Source::Binance => "Binance",
        Source::Bybit => "Bybit",
        Source::Okx => "OKX",
        Source::Coinbase => "Coinbase",
        Source::Bitget => "Bitget",
        Source::Gate => "Gate.io",
        Source::Pyth => "Pyth",
        Source::Titan => "Titan",
        Source::Weighted => "Weighted",
        Source::Auto => "Auto",
    }
}

/// Monitor a single FDUSD price stream
async fn monitor_single_stream(mut receiver: watch::Receiver<Tick>, pair_name: &str) {
    loop {
        if receiver.changed().await.is_ok() {
            let tick = *receiver.borrow();
            debug!("{} price update: {}", pair_name, tick.price);
            update_bip_correction(tick.price, pair_name);
        } else {
            warn!("{} stream ended - BIP correction monitor stopping", pair_name);
            break;
        }
    }
}

/// Update the BIP correction cache with new price
fn update_bip_correction(price: Decimal, pair_name: &str) {
    let bip_correction_x100 = calculate_bips_from_price(price);

    // Atomic update - no locks needed, never blocks readers
    let old_bips = FDUSD_BIP_CORRECTION_CACHE.swap(bip_correction_x100, Ordering::Relaxed);
    FDUSD_BIP_TIMESTAMP.store(now_unix(), Ordering::Relaxed);

    // Check if we hit the cap (needed for both logging and warning)
    let capped = bip_correction_x100.abs() == MAX_BIP_CORRECTION;

    // Log first change and every 100th BIP correction update to avoid spam
    static mut BIP_CHANGE_COUNTER: u64 = 0;
    let should_log = unsafe {
        BIP_CHANGE_COUNTER += 1;
        BIP_CHANGE_COUNTER == 1 || BIP_CHANGE_COUNTER % 100 == 0
    };

    if should_log {
        let old_bips_f = old_bips as f64 / 100.0;
        let new_bips_f = bip_correction_x100 as f64 / 100.0;
        let change_number = unsafe { BIP_CHANGE_COUNTER };

        info!(
            "📋 FDUSD BIP correction updated from {} @ ${}: {:+} bips → {:+} bips{} (change #{})",
            pair_name,
            price,
            old_bips_f,
            new_bips_f,
            if capped { " (CAPPED at ±30)" } else { "" },
            change_number
        );
    }

    // Warn if we hit the cap (always check, regardless of logging interval)
    if capped {
        warn!("FDUSD BIP correction hit ±30 bips limit - market spread may be wider than expected");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn test_calculate_bips_from_price() {
        // FDUSD trading at 0.9985 should give -15 bips * 100 = -1500 (15 bips below USD)
        let bips = calculate_bips_from_price(dec!(0.9985));
        assert_eq!(bips, -1500);

        // FDUSD trading at 1.0015 should give +15 bips * 100 = 1500 (15 bips above USD)
        let bips = calculate_bips_from_price(dec!(1.0015));
        assert_eq!(bips, 1500);

        // Test capping at 30 bips - 0.9965 is 35 bips below, should cap at -30 bips
        let bips = calculate_bips_from_price(dec!(0.9965)); // -35 bips, should cap at -30
        assert_eq!(bips, -3000); // -30 bips * 100
    }

    #[test]
    fn test_apply_bip_correction_fdusd_dynamic() {
        // Set a known value in the cache
        FDUSD_BIP_CORRECTION_CACHE.store(-1500, Ordering::Relaxed); // -15 bips (FDUSD below USD)

        let price = dec!(1000.0);
        let corrected = apply_bip_correction(price, "FDUSD");
        // -15 bips = 0.9985 multiplier (1 + (-15/10000))
        assert_eq!(corrected, dec!(998.5));
    }

    #[test]
    fn test_apply_bip_correction_usdt() {
        let price = dec!(1000.0);
        let corrected = apply_bip_correction(price, "USDT");
        // 0 bips = no change
        assert_eq!(corrected, price);
    }

    #[test]
    fn test_apply_bip_correction_usdc() {
        let price = dec!(1000.0);
        let corrected = apply_bip_correction(price, "USDC");
        // USDC has 0 bips correction
        assert_eq!(corrected, price);
    }

    #[test]
    fn test_should_apply_bip_correction() {
        // Should apply: AUTO source, USD requested, FDUSD actual
        assert!(should_apply_bip_correction(Source::Auto, "USD", "FDUSD"));

        // Should apply: WEIGHTED source, USD requested, FDUSD actual
        assert!(should_apply_bip_correction(Source::Weighted, "USD", "FDUSD"));

        // Should NOT apply: Direct exchange request
        assert!(!should_apply_bip_correction(Source::Binance, "USD", "FDUSD"));

        // Should NOT apply: Non-USD quote requested
        assert!(!should_apply_bip_correction(Source::Auto, "FDUSD", "FDUSD"));

        // Should NOT apply: USD to USD (no correction needed)
        assert!(!should_apply_bip_correction(Source::Auto, "USD", "USD"));
    }

    #[test]
    fn test_bip_correction_capping() {
        // Test that extreme spreads get capped at ±30 bips
        let extreme_low = dec!(0.960);  // -40 bips below USD
        let capped_bips = calculate_bips_from_price(extreme_low);
        assert_eq!(capped_bips, -3000); // Capped at -30 bips * 100

        let extreme_high = dec!(1.040); // +40 bips above USD
        let capped_bips = calculate_bips_from_price(extreme_high);
        assert_eq!(capped_bips, 3000); // Capped at +30 bips * 100
    }
}