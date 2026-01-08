//! Streaming accumulator for O(1) memory candle collection.
//!
//! Instead of buffering all ticks, we incrementally update:
//! - Time-weighted sum and observed duration (TWAP core)
//! - OHLC prices
//! - Downsampled prices for median (1 sample per second)

use std::sync::Arc;

use dashmap::DashMap;
use once_cell::sync::Lazy;
use rust_decimal::Decimal;

use super::types::RawCandle;
use super::{align_to_window, MAX_DT_MS, WINDOW_MS};

/// Global cache of streaming accumulators, keyed by "Exchange:Pair" composite string.
/// Example keys: "Binance:BTC/FDUSD", "OKX:ETH/USDT"
pub static CANDLE_ACCUMULATORS: Lazy<Arc<DashMap<String, StreamingAccumulator>>> =
    Lazy::new(|| Arc::new(DashMap::new()));

/// Generate accumulator key from exchange and pair.
///
/// # Example
/// ```ignore
/// let key = accumulator_key("Binance", "BTC/FDUSD");
/// assert_eq!(key, "Binance:BTC/FDUSD");
/// ```
pub fn accumulator_key(exchange: &str, pair: &str) -> String {
    format!("{}:{}", exchange, pair)
}

/// Snapshot of an in-progress accumulator for TWAP calculation.
///
/// Contains the minimum data needed to compute the accumulator's contribution
/// to a TWAP query, including clipping and hold-last-price extension.
#[derive(Clone, Debug)]
pub struct AccumulatorSnapshot {
    /// Window start timestamp (aligned to 5-minute boundary)
    pub window_start_ms: u64,
    /// Timestamp of the last price tick received
    pub last_event_ts: u64,
    /// Last price seen (used for hold-last-price extension)
    pub last_price: Decimal,
    /// Sum of (price × dt_ms) for TWAP calculation
    pub time_weighted_sum: Decimal,
    /// Total observed duration in milliseconds
    pub observed_duration_ms: u64,
}

/// Get snapshot of the in-progress accumulator for TWAP calculation.
///
/// Returns `None` if:
/// - No accumulator exists for this (exchange, pair)
/// - The accumulator is in an invalid state (no observed data yet)
///
/// # Arguments
/// * `exchange` - Exchange name (e.g., "Binance", "OKX")
/// * `pair` - Trading pair (e.g., "BTC/FDUSD")
pub fn get_accumulator_snapshot(exchange: &str, pair: &str) -> Option<AccumulatorSnapshot> {
    let key = accumulator_key(exchange, pair);
    let acc = CANDLE_ACCUMULATORS.get(&key)?;

    // Snapshot safety: require valid observed data
    // - observed_duration_ms > 0: has accumulated some time
    // - last_ts > window_start_ms: avoids weird startup states
    if acc.observed_duration_ms == 0 || acc.last_ts <= acc.window_start_ms {
        return None;
    }

    Some(AccumulatorSnapshot {
        window_start_ms: acc.window_start_ms,
        last_event_ts: acc.last_ts,
        last_price: acc.last_price,
        time_weighted_sum: acc.time_weighted_sum,
        observed_duration_ms: acc.observed_duration_ms,
    })
}

/// Streaming accumulator for a single (exchange, pair) combination.
///
/// Uses O(1) memory per pair regardless of tick rate by:
/// - Incrementally updating time-weighted sum (not storing all ticks)
/// - Downsampling to 1 price per second for median calculation
/// - Updating OHLC on each tick
#[derive(Clone, Debug)]
pub struct StreamingAccumulator {
    /// Exchange name (e.g., "Binance", "OKX") - must use Source::as_str() canonical names
    pub exchange: String,
    /// Trading pair on the exchange (e.g., "BTC/FDUSD", "BTC/USDT")
    pub pair: String,
    /// Current window start timestamp (aligned to 5-minute boundary)
    pub window_start_ms: u64,
    /// Session ID (UUID from oracle boot)
    pub session_id: String,

    // === Time-weighted integration ===
    /// Sum of (price × dt_ms) for TWAP calculation
    pub time_weighted_sum: Decimal,
    /// Total observed duration in milliseconds
    pub observed_duration_ms: u64,

    // === State for incremental updates ===
    /// Last price seen (used for time-weighting)
    pub last_price: Decimal,
    /// Last tick timestamp
    pub last_ts: u64,
    /// Whether we've received the first tick in this window
    pub first_tick_received: bool,

    // === OHLC ===
    /// First price in window (set on first tick)
    pub open_price: Option<Decimal>,
    /// Maximum price in window
    pub high_price: Decimal,
    /// Minimum price in window
    pub low_price: Decimal,
    /// Last price in window (updated on each tick)
    pub close_price: Decimal,

    // === Median calculation (downsampled) ===
    /// Prices sampled every 1 second (max ~300 per 5-min window)
    pub sampled_prices: Vec<Decimal>,
    /// Last sample timestamp
    pub last_sample_ts: u64,
    /// Total tick count
    pub tick_count: u32,
}

impl StreamingAccumulator {
    /// Create a new accumulator for an (exchange, pair) combination.
    ///
    /// # Arguments
    /// * `exchange` - Exchange name from Source::as_str() (e.g., "Binance", "OKX")
    /// * `pair` - Trading pair on the exchange (e.g., "BTC/FDUSD")
    /// * `window_start_ms` - Window start timestamp (aligned to 5-minute boundary)
    /// * `session_id` - UUID from oracle boot
    pub fn new(exchange: String, pair: String, window_start_ms: u64, session_id: String) -> Self {
        Self {
            exchange,
            pair,
            window_start_ms,
            session_id,
            time_weighted_sum: Decimal::ZERO,
            observed_duration_ms: 0,
            last_price: Decimal::ZERO,
            last_ts: 0,
            first_tick_received: false,
            open_price: None,
            high_price: Decimal::ZERO,
            low_price: Decimal::MAX,
            close_price: Decimal::ZERO,
            sampled_prices: Vec::with_capacity(300),
            last_sample_ts: 0,
            tick_count: 0,
        }
    }

    /// Update accumulator with a new price tick.
    ///
    /// Returns a vector of completed candles (usually 0 or 1, but may be more
    /// if the tick gap spans multiple windows).
    ///
    /// ## Algorithm
    ///
    /// 1. If first tick: record price and timestamp, no dt yet
    /// 2. Calculate raw dt, cap at MAX_DT_MS (gap protection)
    /// 3. Distribute observed time across windows if crossing boundaries
    /// 4. For each completed window: finalize and return candle
    /// 5. Update OHLC with new price
    /// 6. Downsample for median (every 1 second)
    pub fn update(&mut self, price: Decimal, ts: u64) -> Vec<RawCandle> {
        let mut completed_candles = Vec::new();

        // First tick in window - just record, no dt yet
        if !self.first_tick_received {
            self.first_tick_received = true;
            self.last_price = price;
            self.last_ts = ts;
            self.open_price = Some(price);
            self.high_price = price;
            self.low_price = price;
            self.close_price = price;
            self.tick_count = 1;

            // First sample
            self.sampled_prices.push(price);
            self.last_sample_ts = ts;

            return completed_candles;
        }

        // === GAP PROTECTION ===
        // Cap dt at MAX_DT_MS (5 seconds)
        // Excess beyond this is gap time (not observed, not integrated)
        let raw_dt_ms = ts.saturating_sub(self.last_ts);
        let observed_dt_ms = raw_dt_ms.min(MAX_DT_MS);

        // === DISTRIBUTE OBSERVED TIME ACROSS WINDOWS ===
        // Use loop to handle crossing multiple 5-minute boundaries
        let mut remaining_observed = observed_dt_ms;
        let mut current_ts = self.last_ts;

        while remaining_observed > 0 {
            let window_end_ms = self.window_start_ms + WINDOW_MS;
            let time_to_boundary = window_end_ms.saturating_sub(current_ts);

            if time_to_boundary == 0 {
                // Exactly at boundary - finalize and roll forward
                if let Some(candle) = self.finalize() {
                    completed_candles.push(candle);
                }
                self.reset_for_new_window(window_end_ms);
                continue;
            }

            if remaining_observed <= time_to_boundary {
                // All remaining observed time fits in current window
                self.time_weighted_sum += self.last_price * Decimal::from(remaining_observed);
                self.observed_duration_ms += remaining_observed;
                remaining_observed = 0;
            } else {
                // Integrate up to window boundary
                self.time_weighted_sum += self.last_price * Decimal::from(time_to_boundary);
                self.observed_duration_ms += time_to_boundary;

                // Finalize current window
                if let Some(candle) = self.finalize() {
                    completed_candles.push(candle);
                }

                // Reset for new window - OHLC starts with last_price (not new tick!)
                self.reset_for_new_window(window_end_ms);

                remaining_observed -= time_to_boundary;
                current_ts = window_end_ms;
            }
        }

        // === RECORD NEW TICK ===
        // Update OHLC with new price
        self.high_price = self.high_price.max(price);
        self.low_price = self.low_price.min(price);
        self.close_price = price;
        self.tick_count += 1;

        // Downsample for median (every 1 second)
        if ts.saturating_sub(self.last_sample_ts) >= 1000 {
            self.sampled_prices.push(price);
            self.last_sample_ts = ts;
        }

        // Update state for next tick
        self.last_price = price;
        self.last_ts = ts;

        completed_candles
    }

    /// Reset accumulator for a new window.
    ///
    /// IMPORTANT: OHLC starts with last_price, not the incoming tick.
    /// This ensures continuity at window boundaries.
    fn reset_for_new_window(&mut self, new_window_start: u64) {
        self.window_start_ms = new_window_start;
        self.time_weighted_sum = Decimal::ZERO;
        self.observed_duration_ms = 0;

        // OHLC starts with last_price (the price in effect at window start)
        self.open_price = Some(self.last_price);
        self.high_price = self.last_price;
        self.low_price = self.last_price;
        // close_price will be updated on next tick

        self.sampled_prices.clear();
        self.tick_count = 0;
        self.first_tick_received = true; // We have price context from previous window

        // Note: session_id preserved (not a restart)
    }

    /// Finalize the current window into a candle.
    ///
    /// Returns `None` if no data was received in this window.
    pub fn finalize(&self) -> Option<RawCandle> {
        if !self.first_tick_received || self.observed_duration_ms == 0 {
            return None;
        }

        let sampled_median = compute_median(&self.sampled_prices);
        let coverage_pct = (self.observed_duration_ms as f32 / WINDOW_MS as f32) * 100.0;

        Some(RawCandle {
            exchange: self.exchange.clone(),
            pair: self.pair.clone(),
            window_start_ms: self.window_start_ms,
            window_end_ms: self.window_start_ms + WINDOW_MS,
            time_weighted_sum: self.time_weighted_sum,
            observed_duration_ms: self.observed_duration_ms,
            sampled_median,
            tick_count: self.tick_count,
            open_price: self.open_price.unwrap_or(self.close_price),
            close_price: self.close_price,
            high_price: self.high_price,
            low_price: self.low_price,
            session_id: self.session_id.clone(),
            coverage_pct,
        })
    }

    /// Force finalize the current window (e.g., on shutdown or timeout).
    ///
    /// Unlike `finalize()`, this also resets the accumulator for the next window.
    pub fn force_close_window(&mut self) -> Option<RawCandle> {
        let candle = self.finalize();
        if candle.is_some() {
            let next_window = self.window_start_ms + WINDOW_MS;
            self.reset_for_new_window(next_window);
        }
        candle
    }
}

/// Compute median of a price slice.
pub fn compute_median(prices: &[Decimal]) -> Decimal {
    if prices.is_empty() {
        return Decimal::ZERO;
    }

    let mut sorted = prices.to_vec();
    sorted.sort();

    let len = sorted.len();
    if len % 2 == 0 {
        // Even: average of two middle values
        (sorted[len / 2 - 1] + sorted[len / 2]) / Decimal::from(2)
    } else {
        // Odd: middle value
        sorted[len / 2]
    }
}

/// Get or create an accumulator for an (exchange, pair) combination.
///
/// # Arguments
/// * `exchange` - Exchange name from Source::as_str() (e.g., "Binance", "OKX")
/// * `pair` - Trading pair on the exchange (e.g., "BTC/FDUSD")
/// * `current_ts` - Current timestamp for window alignment
/// * `session_id` - UUID from oracle boot
pub fn get_or_create_accumulator(
    exchange: &str,
    pair: &str,
    current_ts: u64,
    session_id: &str,
) -> dashmap::mapref::one::RefMut<'static, String, StreamingAccumulator> {
    let key = accumulator_key(exchange, pair);
    CANDLE_ACCUMULATORS
        .entry(key)
        .or_insert_with(|| {
            let window_start = align_to_window(current_ts);
            StreamingAccumulator::new(
                exchange.to_string(),
                pair.to_string(),
                window_start,
                session_id.to_string(),
            )
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn test_accumulator_key() {
        assert_eq!(accumulator_key("Binance", "BTC/FDUSD"), "Binance:BTC/FDUSD");
        assert_eq!(accumulator_key("OKX", "ETH/USDT"), "OKX:ETH/USDT");
    }

    #[test]
    fn test_compute_median_odd() {
        let prices = vec![dec!(1), dec!(3), dec!(2)];
        assert_eq!(compute_median(&prices), dec!(2));
    }

    #[test]
    fn test_compute_median_even() {
        let prices = vec![dec!(1), dec!(2), dec!(3), dec!(4)];
        assert_eq!(compute_median(&prices), dec!(2.5));
    }

    #[test]
    fn test_compute_median_empty() {
        let prices: Vec<Decimal> = vec![];
        assert_eq!(compute_median(&prices), Decimal::ZERO);
    }

    #[test]
    fn test_first_tick() {
        let mut acc = StreamingAccumulator::new(
            "Binance".to_string(),
            "BTC/FDUSD".to_string(),
            300_000, // 5 min mark
            "test-session".to_string(),
        );

        let candles = acc.update(dec!(50000), 300_100);

        assert!(candles.is_empty());
        assert!(acc.first_tick_received);
        assert_eq!(acc.open_price, Some(dec!(50000)));
        assert_eq!(acc.high_price, dec!(50000));
        assert_eq!(acc.low_price, dec!(50000));
        assert_eq!(acc.close_price, dec!(50000));
        assert_eq!(acc.tick_count, 1);
    }

    #[test]
    fn test_accumulation_within_window() {
        let mut acc = StreamingAccumulator::new(
            "Binance".to_string(),
            "BTC/FDUSD".to_string(),
            300_000,
            "test-session".to_string(),
        );

        // First tick at t=300100, price=100
        acc.update(dec!(100), 300_100);

        // Second tick at t=301100 (1000ms later), price=110
        let candles = acc.update(dec!(110), 301_100);

        assert!(candles.is_empty()); // Window not complete
        assert_eq!(acc.time_weighted_sum, dec!(100000)); // 100 * 1000ms
        assert_eq!(acc.observed_duration_ms, 1000);
        assert_eq!(acc.high_price, dec!(110));
        assert_eq!(acc.low_price, dec!(100));
        assert_eq!(acc.close_price, dec!(110));
    }

    #[test]
    fn test_gap_protection() {
        let mut acc = StreamingAccumulator::new(
            "Binance".to_string(),
            "BTC/FDUSD".to_string(),
            300_000,
            "test-session".to_string(),
        );

        // First tick
        acc.update(dec!(100), 300_100);

        // Second tick 10 seconds later (should cap at 5s)
        let candles = acc.update(dec!(110), 310_100);

        assert!(candles.is_empty());
        // Should only integrate 5000ms (MAX_DT_MS), not 10000ms
        assert_eq!(acc.observed_duration_ms, 5000);
        assert_eq!(acc.time_weighted_sum, dec!(500000)); // 100 * 5000ms
    }

    #[test]
    fn test_window_boundary_crossing() {
        let mut acc = StreamingAccumulator::new(
            "Binance".to_string(),
            "BTC/FDUSD".to_string(),
            300_000, // Window: 300000-600000
            "test-session".to_string(),
        );

        // First tick near end of window
        acc.update(dec!(100), 599_000);

        // Second tick after window end
        let candles = acc.update(dec!(110), 601_000);

        // Should have completed the first window
        assert_eq!(candles.len(), 1);
        let candle = &candles[0];
        assert_eq!(candle.exchange, "Binance");
        assert_eq!(candle.pair, "BTC/FDUSD");
        assert_eq!(candle.window_start_ms, 300_000);
        assert_eq!(candle.window_end_ms, 600_000);
        // Integrated 1000ms at price 100 (from 599000 to 600000)
        assert_eq!(candle.observed_duration_ms, 1000);
        assert_eq!(candle.time_weighted_sum, dec!(100000));

        // Accumulator should now be in new window
        assert_eq!(acc.window_start_ms, 600_000);
        // Integrated 1000ms at price 100 in new window (from 600000 to 601000)
        assert_eq!(acc.observed_duration_ms, 1000);
    }

    #[test]
    fn test_twap_calculation() {
        let mut acc = StreamingAccumulator::new(
            "OKX".to_string(),
            "BTC/USDT".to_string(),
            300_000,
            "test-session".to_string(),
        );

        // Price=100 for 2000ms
        acc.update(dec!(100), 300_000);
        acc.update(dec!(200), 302_000);

        // Price=200 for 3000ms
        acc.update(dec!(300), 305_000);

        // Finalize
        let candle = acc.finalize().unwrap();

        assert_eq!(candle.exchange, "OKX");
        assert_eq!(candle.pair, "BTC/USDT");

        // TWAP = (100*2000 + 200*3000) / 5000 = 800000 / 5000 = 160
        let twap = candle.twap().unwrap();
        assert_eq!(twap, dec!(160));
    }

    #[test]
    fn test_ohlc_on_rollover() {
        let mut acc = StreamingAccumulator::new(
            "Binance".to_string(),
            "ETH/FDUSD".to_string(),
            300_000,
            "test-session".to_string(),
        );

        // Price at end of first window
        acc.update(dec!(100), 599_900);

        // Cross into new window
        acc.update(dec!(150), 600_100);

        // New window's open should be last_price (100), not new tick (150)
        assert_eq!(acc.open_price, Some(dec!(100)));
        // But high/low should include the new tick
        assert_eq!(acc.high_price, dec!(150));
        assert_eq!(acc.low_price, dec!(100));
    }

    #[test]
    fn test_downsampling() {
        let mut acc = StreamingAccumulator::new(
            "Bybit".to_string(),
            "BTC/USDT".to_string(),
            300_000,
            "test-session".to_string(),
        );

        // Rapid ticks within 1 second - should only sample first
        acc.update(dec!(100), 300_000);
        acc.update(dec!(101), 300_100);
        acc.update(dec!(102), 300_200);

        assert_eq!(acc.sampled_prices.len(), 1);
        assert_eq!(acc.sampled_prices[0], dec!(100));

        // Tick after 1 second - should add new sample
        acc.update(dec!(110), 301_100);

        assert_eq!(acc.sampled_prices.len(), 2);
        assert_eq!(acc.sampled_prices[1], dec!(110));
    }

    #[test]
    fn test_coverage_percentage() {
        let mut acc = StreamingAccumulator::new(
            "Coinbase".to_string(),
            "BTC/USD".to_string(),
            300_000,
            "test-session".to_string(),
        );

        // Send 30 ticks, 5 seconds apart = 150 seconds of coverage
        // Gap protection caps each dt at MAX_DT_MS (5000ms), so we need
        // to send ticks at 5-second intervals to accumulate time
        for i in 0..31 {
            acc.update(dec!(100), 300_000 + i * 5000);
        }

        let candle = acc.finalize().unwrap();

        assert_eq!(candle.exchange, "Coinbase");
        assert_eq!(candle.pair, "BTC/USD");

        // 30 intervals * 5000ms = 150000ms observed
        // 150000ms / 300000ms = 50%
        assert!((candle.coverage_pct - 50.0).abs() < 0.1);
    }
}
