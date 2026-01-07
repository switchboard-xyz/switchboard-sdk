//! TWAP Candle Collection Module
//!
//! This module provides the core functionality for collecting and persisting
//! Time-Weighted Average Price (TWAP) candles from price feeds.
//!
//! ## Architecture
//!
//! - `types`: Core data structures (`RawCandle`, `SignedCandle`)
//! - `buffer`: Streaming accumulator for O(1) memory per pair
//! - `db`: TimescaleDB persistence layer (requires `candle-db` feature)
//!
//! ## Usage
//!
//! The candle collector subscribes to AUTO-selected price feeds and accumulates
//! price data into 5-minute candles. Each candle is signed with the oracle's
//! Ed25519 keypair and persisted to local TimescaleDB.

pub mod buffer;
pub mod types;

#[cfg(feature = "candle-db")]
pub mod db;

// Re-export core types
pub use buffer::{StreamingAccumulator, CANDLE_ACCUMULATORS};
pub use types::{RawCandle, SignedCandle};

#[cfg(feature = "candle-db")]
pub use db::{
    create_candle_pool, fetch_candles, fetch_candles_all_exchanges, get_global_candle_pool,
    persist_candle,
};

/// Candle window duration in milliseconds (5 minutes)
pub const WINDOW_MS: u64 = 5 * 60 * 1000;

/// Maximum time delta between ticks for gap protection (5 seconds)
/// Excess time beyond this is considered a gap and not integrated into TWAP
pub const MAX_DT_MS: u64 = 5000;

/// Align a timestamp to the start of its 5-minute window
pub fn align_to_window(ts_ms: u64) -> u64 {
    (ts_ms / WINDOW_MS) * WINDOW_MS
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_align_to_window() {
        // Exact boundary
        assert_eq!(align_to_window(300_000), 300_000);

        // Just after boundary
        assert_eq!(align_to_window(300_001), 300_000);

        // Middle of window
        assert_eq!(align_to_window(450_000), 300_000);

        // Just before next boundary
        assert_eq!(align_to_window(599_999), 300_000);

        // Next boundary
        assert_eq!(align_to_window(600_000), 600_000);
    }
}
