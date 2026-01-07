//! Core data types for TWAP candle collection.
//!
//! ## Key Types
//!
//! - `RawCandle`: Unsigned candle data with TWAP-essential and debug fields
//! - `SignedCandle`: Wrapper with oracle signature for verification

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Raw candle data before signing.
///
/// Contains both TWAP-essential fields (for calculation) and debug fields
/// (for auditing/monitoring). Only TWAP-essential fields are signed.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RawCandle {
    // === Identity ===
    /// Exchange name (e.g., "Binance", "OKX") - must use Source::as_str() canonical names
    pub exchange: String,
    /// Trading pair on the exchange (e.g., "BTC/FDUSD", "BTC/USDT")
    pub pair: String,

    // === Time Window ===
    /// Window start timestamp (aligned to :00, :05, :10, etc.)
    pub window_start_ms: u64,
    /// Window end timestamp (window_start_ms + 300_000)
    pub window_end_ms: u64,

    // === TWAP Core (signed, essential for calculation) ===
    /// Sum of (price × dt_ms) for time-weighted average
    /// TWAP = time_weighted_sum / observed_duration_ms
    pub time_weighted_sum: Decimal,
    /// Total observed time in milliseconds (not assumed 5 minutes)
    /// Gaps beyond MAX_DT_MS are excluded from this count
    pub observed_duration_ms: u64,

    // === Debug/Audit Fields (NOT signed) ===
    /// Median of 1-second sampled prices (for cross-oracle consensus in Phase 2)
    pub sampled_median: Decimal,
    /// Number of price ticks received in this window
    pub tick_count: u32,
    /// First price in window
    pub open_price: Decimal,
    /// Last price in window
    pub close_price: Decimal,
    /// Maximum price in window
    pub high_price: Decimal,
    /// Minimum price in window
    pub low_price: Decimal,

    // === Session Tracking ===
    /// UUID generated on oracle boot (detects restarts, prevents time bridging)
    pub session_id: String,
    /// Coverage percentage: observed_duration_ms / 300_000 * 100
    pub coverage_pct: f32,
}

/// Signed candle with oracle attestation.
///
/// The signature covers only TWAP-essential fields (117 bytes canonical format).
/// Debug fields (OHLC, sampled_median) are stored but not signed.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SignedCandle {
    /// The underlying candle data
    pub candle: RawCandle,
    /// Oracle's Ed25519 public key (Base58)
    pub oracle_pubkey: String,
    /// Ed25519 signature over canonical payload (Base64)
    pub signature: String,
    /// Timestamp when candle was signed
    pub signed_at_ms: u64,
}

impl RawCandle {
    /// Create a new empty candle for a window.
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
            window_end_ms: window_start_ms + super::WINDOW_MS,
            time_weighted_sum: Decimal::ZERO,
            observed_duration_ms: 0,
            sampled_median: Decimal::ZERO,
            tick_count: 0,
            open_price: Decimal::ZERO,
            close_price: Decimal::ZERO,
            high_price: Decimal::ZERO,
            low_price: Decimal::ZERO,
            session_id,
            coverage_pct: 0.0,
        }
    }

    /// Generate the canonical 117-byte signing payload.
    ///
    /// ## Format (117 bytes)
    /// ```text
    /// Version (1 byte):             0x01
    /// Pair (32 bytes):              null-padded UTF-8 (e.g., "BTC/FDUSD")
    /// Exchange (8 bytes):           null-padded UTF-8 (e.g., "Binance", "OKX")
    /// window_start_ms (8 bytes):    little-endian u64
    /// window_end_ms (8 bytes):      little-endian u64
    /// time_weighted_sum (16 bytes): i128 mantissa at scale 18, little-endian
    /// observed_duration_ms (8 bytes): little-endian u64
    /// session_id (36 bytes):        UUID string, null-padded
    /// ```
    ///
    /// ## NOT Signed
    /// - sampled_median, OHLC, tick_count, coverage_pct (debug only)
    pub fn to_signing_payload(&self) -> Vec<u8> {
        let mut payload = Vec::with_capacity(117);

        // Version (1 byte)
        payload.push(1u8);

        // Pair (32 bytes, null-padded)
        let mut pair_bytes = [0u8; 32];
        let pair_slice = self.pair.as_bytes();
        let pair_len = pair_slice.len().min(32);
        pair_bytes[..pair_len].copy_from_slice(&pair_slice[..pair_len]);
        payload.extend_from_slice(&pair_bytes);

        // Exchange (8 bytes, null-padded) - uses Source::as_str() canonical names
        let mut exchange_bytes = [0u8; 8];
        let exchange_slice = self.exchange.as_bytes();
        let exchange_len = exchange_slice.len().min(8);
        exchange_bytes[..exchange_len].copy_from_slice(&exchange_slice[..exchange_len]);
        payload.extend_from_slice(&exchange_bytes);

        // Timestamps (8 bytes each, little-endian)
        payload.extend_from_slice(&self.window_start_ms.to_le_bytes());
        payload.extend_from_slice(&self.window_end_ms.to_le_bytes());

        // Time-weighted sum (16 bytes as i128 mantissa at scale 18)
        let mut tws = self.time_weighted_sum;
        tws.rescale(18);
        payload.extend_from_slice(&tws.mantissa().to_le_bytes());

        // Observed duration (8 bytes)
        payload.extend_from_slice(&self.observed_duration_ms.to_le_bytes());

        // Session ID (36 bytes, null-padded)
        let mut session_bytes = [0u8; 36];
        let session_slice = self.session_id.as_bytes();
        let session_len = session_slice.len().min(36);
        session_bytes[..session_len].copy_from_slice(&session_slice[..session_len]);
        payload.extend_from_slice(&session_bytes);

        debug_assert_eq!(payload.len(), 117, "Signing payload must be 117 bytes");
        payload
    }

    /// Calculate the TWAP value for this candle.
    ///
    /// Returns `None` if there's no observed duration (empty candle).
    pub fn twap(&self) -> Option<Decimal> {
        if self.observed_duration_ms == 0 {
            return None;
        }
        Some(self.time_weighted_sum / Decimal::from(self.observed_duration_ms))
    }
}

impl SignedCandle {
    /// Create a new signed candle.
    pub fn new(
        candle: RawCandle,
        oracle_pubkey: String,
        signature: String,
        signed_at_ms: u64,
    ) -> Self {
        Self {
            candle,
            oracle_pubkey,
            signature,
            signed_at_ms,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn test_signing_payload_length() {
        let candle = RawCandle::new(
            "Binance".to_string(),
            "BTC/FDUSD".to_string(),
            1700000000000,
            "550e8400-e29b-41d4-a716-446655440000".to_string(),
        );

        let payload = candle.to_signing_payload();
        assert_eq!(payload.len(), 117);
    }

    #[test]
    fn test_signing_payload_deterministic() {
        let candle = RawCandle {
            exchange: "OKX".to_string(),
            pair: "ETH/USDT".to_string(),
            window_start_ms: 1700000000000,
            window_end_ms: 1700000300000,
            time_weighted_sum: dec!(1234567890.123456789012345678),
            observed_duration_ms: 290000,
            sampled_median: dec!(2500.50),
            tick_count: 500,
            open_price: dec!(2490.00),
            close_price: dec!(2510.00),
            high_price: dec!(2520.00),
            low_price: dec!(2480.00),
            session_id: "550e8400-e29b-41d4-a716-446655440000".to_string(),
            coverage_pct: 96.67,
        };

        let payload1 = candle.to_signing_payload();
        let payload2 = candle.to_signing_payload();

        assert_eq!(payload1, payload2, "Signing payload must be deterministic");
    }

    #[test]
    fn test_twap_calculation() {
        let mut candle = RawCandle::new(
            "Binance".to_string(),
            "BTC/FDUSD".to_string(),
            1700000000000,
            "test-session".to_string(),
        );

        // Empty candle has no TWAP
        assert!(candle.twap().is_none());

        // After accumulation: price=100 for 1000ms
        candle.time_weighted_sum = dec!(100000); // 100 * 1000
        candle.observed_duration_ms = 1000;

        let twap = candle.twap().unwrap();
        assert_eq!(twap, dec!(100));
    }

    #[test]
    fn test_pair_truncation() {
        // Very long pair name should be truncated to 32 bytes
        let long_pair = "A".repeat(50);
        let candle = RawCandle::new(
            "Binance".to_string(),
            long_pair,
            1700000000000,
            "session".to_string(),
        );

        let payload = candle.to_signing_payload();
        assert_eq!(payload.len(), 117);

        // First 32 bytes after version should be the truncated pair
        let pair_bytes = &payload[1..33];
        assert_eq!(&pair_bytes[..32], "A".repeat(32).as_bytes());
    }

    #[test]
    fn test_exchange_in_payload() {
        let candle = RawCandle::new(
            "Coinbase".to_string(),  // Exactly 8 bytes - max length
            "BTC/USD".to_string(),
            1700000000000,
            "session".to_string(),
        );

        let payload = candle.to_signing_payload();

        // Exchange is at bytes 33-40 (after version + pair)
        let exchange_bytes = &payload[33..41];
        assert_eq!(exchange_bytes, b"Coinbase");
    }
}
