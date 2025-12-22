use anyhow::{anyhow, Error as AnyhowError};
use chrono::DateTime;
use chrono::Utc;
use serde::Deserialize;
use std::time::Duration;

#[derive(Debug, Deserialize)]
struct ValidateApiKeyResponse {
    valid: bool,
    #[allow(dead_code)]
    id: Option<String>,
    #[serde(default)]
    websocket_connection_limit: Option<i32>,
    #[serde(default)]
    no_of_feeds: Option<i32>,
    #[serde(default)]
    no_feeds_per_websocket: Option<i32>,
    #[serde(default)]
    privileged_access: Option<bool>,
    #[serde(default)]
    max_subscriptions_per_feed: Option<i32>,
    expires_at: Option<i64>, // milliseconds
    #[serde(default)]
    is_active: Option<bool>,
}

/// API key validation response with all limits
#[derive(Debug, Clone)]
pub struct ApiKeyValidationResult {
    pub valid: bool,
    pub websocket_connection_limit: Option<i32>,
    pub max_feeds: Option<i32>,  // Legacy field (no_of_feeds)
    pub no_feeds_per_websocket: Option<i32>,  // New field
    pub privileged_access: bool,
    pub max_subscriptions_per_feed: Option<i32>,
}

/// Validates an API key against the API key service
pub async fn validate_api_key(api_key: &str) -> Result<ApiKeyValidationResult, AnyhowError> {
    let api_key_service_url = std::env::var("API_KEY_SERVICE_URL")
        .unwrap_or_else(|_| "https://staging.api-key-service.switchboard.xyz".to_string());

    let client = reqwest::Client::new();
    let response = client
        .post(format!("{api_key_service_url}/api/v1/validate"))
        .timeout(Duration::from_secs(5))
        .json(&serde_json::json!({
            "api_key": api_key
        }))
        .send()
        .await
        .map_err(|e| {
            tracing::error!("API key service request failed: {}", e);
            anyhow!("Failed to contact API key service")
        })?;
    
    let status = response.status();
    if !status.is_success() {
        tracing::warn!(
            "API key service returned status {}: {}",
            status,
            response.text().await.unwrap_or_default()
        );
        return Err(anyhow!("API key service error"));
    }

    // Parse the response
    let validation_response: ValidateApiKeyResponse = response.json().await.map_err(|e| {
        tracing::error!("Failed to parse API key service response: {}", e);
        anyhow!("Invalid response from API key service")
    })?;
    
    if !validation_response.valid {
        tracing::debug!("API key validation failed - marked as invalid by service");
        return Err(anyhow!("Invalid API key"));
    }
    
    // Check if key is active
    if let Some(is_active) = validation_response.is_active {
        if !is_active {
            tracing::warn!("API key is deactivated");
            return Err(anyhow!("API key is deactivated"));
        }
    }

    // Check expiration date if provided (in milliseconds)
    if let Some(expires_at_ms) = validation_response.expires_at {
        // Use corrected timestamp to match Binance server time
        let now_ms = crate::clock_sync::get_corrected_timestamp_ms() as i64;

        if expires_at_ms < now_ms {
            let expired_at = DateTime::from_timestamp_millis(expires_at_ms)
                .unwrap_or_else(Utc::now);
            let current_time = DateTime::from_timestamp_millis(now_ms)
                .unwrap_or_else(Utc::now);
            tracing::warn!(
                "API key is expired. Expired at: {}, Current time (corrected): {}",
                expired_at,
                current_time
            );
            return Err(anyhow!("API key has expired"));
        }

        // Log how much time is left until expiration for monitoring
        let ms_until_expiry = expires_at_ms - now_ms;
        let days_until_expiry = ms_until_expiry / (1000 * 60 * 60 * 24);
        tracing::info!(
            "API key valid for {} more days",
            days_until_expiry
        );
    }

    // Key is valid and not expired
    tracing::debug!(
        "API key validated successfully, limits: websocket_connection_limit={:?}, no_of_feeds={:?}, no_feeds_per_websocket={:?}, privileged_access={:?}, max_subscriptions_per_feed={:?}",
        validation_response.websocket_connection_limit,
        validation_response.no_of_feeds,
        validation_response.no_feeds_per_websocket,
        validation_response.privileged_access,
        validation_response.max_subscriptions_per_feed
    );

    Ok(ApiKeyValidationResult {
        valid: true,
        websocket_connection_limit: validation_response.websocket_connection_limit,
        max_feeds: validation_response.no_of_feeds,
        no_feeds_per_websocket: validation_response.no_feeds_per_websocket,
        privileged_access: validation_response.privileged_access.unwrap_or(false),
        max_subscriptions_per_feed: validation_response.max_subscriptions_per_feed,
    })
}