pub mod auth;
pub mod bip_correction;
pub mod candle;
pub mod exchange_config;
pub mod exchanges;
pub mod feed_metadata;
pub mod hub;
pub mod message_rate;
pub mod pair;
pub mod rest_manager;
pub mod schema;
pub mod traits;
pub mod types;
pub mod volume_cache;
pub mod weighted_source;
pub mod rate_limiter;
pub mod keyed_channel;
pub mod batch_receiver;
pub mod clock_sync;
pub mod runtime_separation;

// Re-export main types
pub use exchange_config::{
    get_active_exchanges, get_allowed_quotes, get_cached_pairs_if_enabled, is_exchange_enabled,
    EXCHANGE_REGISTRY,
};
pub use hub::{all_surge_sources, get_all_surge_feeds, SurgeHub, WeightedSourceEntry, WEIGHTED_SOURCE_CACHE};
pub use pair::Pair;
pub use rest_manager::{start_rest_manager, RestManager};
pub use schema::{SurgeFeed, SurgeFeedInfo, SurgeFeedsRequest, SurgeFeedsResponse};
pub use traits::{TickerStream, TickerStreamExt};
pub use types::{Source, Tick, Ticker};
pub use volume_cache::VolumeFetcher;
pub use weighted_source::WeightedSourceCalculator;

// Re-export utilities
pub use auth::{validate_api_key, ApiKeyValidationResult};
pub use bip_correction::{apply_bip_correction, should_apply_bip_correction, BipCorrectedReceiver, start_bip_correction_monitor};
pub use rate_limiter::{ApiRateLimiter, TickerRateLimiter};
pub use keyed_channel::{keyed_channel, ticker_keyed_channel, KeyedReceiver, KeyedSender, TickerKeyedReceiver, TickerKeyedSender};
pub use batch_receiver::{BatchReceiver, BoundedBatchReceiver};
pub use clock_sync::{check_and_cache_clock_offset, get_corrected_timestamp_ms, start_clock_sync_task};
pub use message_rate::{start_message_rate_updater, MESSAGE_COUNTER, MESSAGE_RATE_CACHE};

// Re-export feed metadata
pub use feed_metadata::{FeedMetadata, get_or_create_feed_metadata, precompute_all_feed_metadata, clear_feed_metadata_cache};

// Re-export candle types
pub use candle::{RawCandle, SignedCandle, StreamingAccumulator, CANDLE_ACCUMULATORS, WINDOW_MS, MAX_DT_MS, align_to_window};
#[cfg(feature = "candle-db")]
pub use candle::{create_candle_pool, fetch_candles, persist_candle};
