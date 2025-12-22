use serde::{Deserialize, Serialize};
use crate::Pair;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SurgeFeed {
    pub symbol: Pair,
    pub feeds: Vec<SurgeFeedInfo>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SurgeFeedInfo {
    pub source: String,
    pub feed_id: String,
}

/// Request parameters for surge feeds endpoint
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SurgeFeedsRequest {
    /// Optional symbol filter (case-insensitive partial match)
    #[serde(default)]
    pub symbol: Option<String>,

    /// Optional exchange filter
    #[serde(default)]
    pub exchange: Option<String>,
}

/// Response containing available surge feeds
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SurgeFeedsResponse {
    /// Total number of feeds returned
    pub total: usize,

    /// Optional message (e.g., "No match for query")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,

    /// List of available feeds
    pub data: Vec<SurgeFeed>,
}