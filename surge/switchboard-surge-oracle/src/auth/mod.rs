pub mod oracle_auth;

use anyhow::{anyhow, Result};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use sha2::{Digest, Sha256};
use std::time::{SystemTime, UNIX_EPOCH};

/// Create a session token for crossbar mode (no IP tracking)
pub fn create_session_token(api_key: &str) -> Result<(String, u64)> {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_secs();
    
    let expires_at = timestamp + 3600; // 1 hour
    
    // Create hash of session data (no IP)
    let mut hasher = Sha256::new();
    hasher.update(api_key.as_bytes());
    hasher.update(timestamp.to_le_bytes());
    
    // Combine hash + timestamp + expires_at
    let mut token_data = Vec::new();
    token_data.extend_from_slice(&hasher.finalize());
    token_data.extend_from_slice(&timestamp.to_le_bytes());
    token_data.extend_from_slice(&expires_at.to_le_bytes());
    
    let token = URL_SAFE_NO_PAD.encode(token_data);
    
    Ok((token, expires_at))
}

/// Validate a session token for crossbar mode (no IP tracking)
pub fn validate_session_token(token: &str, api_key: &str) -> Result<()> {
    // Decode token
    let decoded = URL_SAFE_NO_PAD
        .decode(token)
        .map_err(|_| anyhow!("Invalid token format"))?;
    
    if decoded.len() != 48 { // 32 bytes hash + 8 bytes timestamp + 8 bytes expires_at
        return Err(anyhow!("Invalid token length"));
    }
    
    // Extract components
    let stored_hash = &decoded[0..32];
    let timestamp = u64::from_le_bytes(
        decoded[32..40].try_into()
            .map_err(|_| anyhow!("Invalid timestamp bytes in token"))?
    );
    let expires_at = u64::from_le_bytes(
        decoded[40..48].try_into()
            .map_err(|_| anyhow!("Invalid expires_at bytes in token"))?
    );
    
    // Check if token is expired
    let current_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_secs();
    
    if current_time > expires_at {
        return Err(anyhow!("Session token expired"));
    }
    
    // Recreate hash and verify
    let mut hasher = Sha256::new();
    hasher.update(api_key.as_bytes());
    hasher.update(timestamp.to_le_bytes());
    
    let computed_hash = hasher.finalize();
    
    if stored_hash != computed_hash.as_slice() {
        return Err(anyhow!("Invalid session token"));
    }
    
    Ok(())
}

/// Hash an API key for storage/comparison
pub fn hash_api_key(api_key: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(api_key.as_bytes());
    faster_hex::hex_string(&hasher.finalize()).to_lowercase()
}

/// Parse auth header into API key and session token
pub fn parse_auth_header(auth_header: &str) -> Result<(String, String)> {
    if !auth_header.starts_with("Bearer ") {
        return Err(anyhow!("Invalid auth format - must start with 'Bearer '"));
    }
    
    let token = &auth_header[7..];
    let parts: Vec<&str> = token.split(':').collect();
    
    if parts.len() != 2 {
        return Err(anyhow!("Invalid auth format - expected 'Bearer api_key:session_token'"));
    }
    
    Ok((parts[0].to_string(), parts[1].to_string()))
}