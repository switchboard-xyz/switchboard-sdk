use crate::{Pair, Source};
use anyhow::Result;
use base64::Engine;
use dashmap::DashMap;
use faster_hex::hex_string;
use once_cell::sync::Lazy;
use prost::Message as ProstMessage;
use protos::{OracleJob, OracleFeed};
use protos::oracle_job::oracle_job::{SwitchboardSurgeTask, Task};
use sb_on_demand_schemas::FeedRequestV2;
use std::sync::Arc;

/// Cached metadata for a feed including pre-computed V2 request and feed_id
#[derive(Clone, Debug)]
pub struct FeedMetadata {
    /// The V2 feed request
    pub feed_request_v2: FeedRequestV2,
    /// Pre-computed hex-encoded feed_id
    pub feed_id: String,
    /// The oracle job for this feed
    pub oracle_job: OracleJob,
}

/// Global cache for feed metadata, indexed by (source_string, symbol)
pub static FEED_METADATA_CACHE: Lazy<Arc<DashMap<(String, Pair), FeedMetadata>>> = 
    Lazy::new(|| Arc::new(DashMap::new()));

// Constants for oracle feed configuration
const STREAMING_MIN_RESPONSES: u32 = 1;
const STREAMING_MIN_ORACLE_SAMPLES: u32 = 1;

/// Creates an oracle job for a given feed
fn create_oracle_job_for_feed(symbol: &Pair, source: Option<Source>) -> OracleJob {
    use protos::oracle_job::oracle_job::task::Task as TaskTask;
    
    let source_value = match source {
        Some(Source::Binance) => 1,
        Some(Source::Okx) => 2,
        Some(Source::Bybit) => 3,
        Some(Source::Coinbase) => 4,
        Some(Source::Bitget) => 5,
        Some(Source::Pyth) => 6,
        Some(Source::Titan) => 7,
        Some(Source::Weighted) | Some(Source::Auto) | None => 0, // Weighted/Auto median uses 0
    };

    OracleJob {
        weight: None,
        tasks: vec![Task {
            task: Some(TaskTask::SwitchboardSurgeTask(
                SwitchboardSurgeTask {
                    source: Some(source_value),
                    symbol: Some(symbol.to_string()),
                },
            )),
        }],
    }
}

/// Gets or creates feed metadata for the given symbol and source
pub fn get_or_create_feed_metadata(
    symbol: &Pair,
    source: Option<Source>,
) -> Result<FeedMetadata> {
    let source_str = match source {
        Some(src) => source_display_name(src),
        None => "WEIGHTED",
    };

    let cache_key = (source_str.to_string(), symbol.clone());

    // Check cache first
    if let Some(entry) = FEED_METADATA_CACHE.get(&cache_key) {
        return Ok(entry.clone());
    }

    // Create Oracle job for this symbol/source
    let oracle_job = create_oracle_job_for_feed(symbol, source);
    
    // Create OracleFeed proto
    let oracle_feed = OracleFeed {
        name: Some(format!("Surge Stream {}, {}", symbol, source_str)),
        jobs: vec![oracle_job.clone()],
        min_job_responses: Some(STREAMING_MIN_RESPONSES),
        min_oracle_samples: Some(STREAMING_MIN_ORACLE_SAMPLES),
        max_job_range_pct: Some(1), // 100% range for streaming
    };
    
    // Encode to base64
    let proto_bytes = oracle_feed.encode_length_delimited_to_vec();
    let feed_proto_b64 = base64::engine::general_purpose::STANDARD.encode(&proto_bytes);

    // Create V2 request
    let feed_request = FeedRequestV2::try_from(Some(feed_proto_b64), rand::random())?;
    
    // Compute the feed_id once
    let feed_id_bytes = feed_request.feed_id()?;
    let feed_id = hex_string(&feed_id_bytes);

    // Create the metadata
    let metadata = FeedMetadata {
        feed_request_v2: feed_request,
        feed_id,
        oracle_job,
    };
    
    // Cache it
    FEED_METADATA_CACHE.insert(cache_key, metadata.clone());

    Ok(metadata)
}

/// Pre-computes and caches metadata for all known feeds
/// This should be called during hub initialization
pub async fn precompute_all_feed_metadata() -> Result<()> {
    use crate::hub::SurgeHub;
    
    // Get all available sources
    let hub = SurgeHub::global();
    
    // Get all keys from the hub (source, pair combinations)
    let all_keys = hub.keys();
    
    for (source, pair) in all_keys {
        // Pre-compute metadata for this feed
        if let Err(e) = get_or_create_feed_metadata(&pair, Some(source)) {
            tracing::warn!(
                "Failed to pre-compute metadata for {}/{:?}: {}", 
                pair, source, e
            );
        }
    }
    
    tracing::info!(
        "Pre-computed metadata for {} feeds", 
        FEED_METADATA_CACHE.len()
    );
    
    Ok(())
}

/// Clears the feed metadata cache
pub fn clear_feed_metadata_cache() {
    FEED_METADATA_CACHE.clear();
}

/// Helper function to get source display name
fn source_display_name(source: Source) -> &'static str {
    match source {
        Source::Binance => "BINANCE",
        Source::Okx => "OKX",
        Source::Bybit => "BYBIT",
        Source::Coinbase => "COINBASE",
        Source::Bitget => "BITGET",
        Source::Pyth => "PYTH",
        Source::Titan => "TITAN",
        Source::Weighted => "WEIGHTED",
        Source::Auto => "AUTO",
    }
}

// /// Parse source string to enum
// fn parse_source(source: &str) -> Result<Source> {
//     match source.to_uppercase().as_str() {
//         "BINANCE" => Ok(Source::Binance),
//         "BYBIT" => Ok(Source::Bybit),
//         "OKX" => Ok(Source::Okx),
//         "COINBASE" => Ok(Source::Coinbase),
//         "BITGET" => Ok(Source::Bitget),
//         "WEIGHTED" => Ok(Source::Weighted),
//         _ => Err(anyhow!("Unknown source: {}", source)),
//     }
// }