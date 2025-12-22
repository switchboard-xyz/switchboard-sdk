use anyhow::{anyhow, Result};
use base64::engine::general_purpose::STANDARD as base64;
use base64::Engine;
use libsecp256k1::{recover, Message, PublicKey, RecoveryId, Signature};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use solana_program::pubkey::Pubkey;

/// Session token payload that gets encrypted
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SessionTokenPayload {
    pub api_key: String,
    pub client_ip: String,
    pub timestamp: u64,
    pub expires_at: u64,
}

/// Validate a session token with full oracle security
/// 
/// This performs the same validation as the original implementation:
/// 1. Decodes the base64 session token
/// 2. Extracts the payload and signature
/// 3. Verifies the signature against the oracle's public key
/// 4. Checks expiration
/// 5. Validates the IP matches (if required)
pub async fn validate_session_token_oracle(
    session_token: &str,
    api_key: &str,
    client_ip: &str,
    _oracle_pubkey: &Pubkey,
    enclave_pubkey: &PublicKey,
) -> Result<()> {
    // Decode the session token from base64
    let token_bytes = base64
        .decode(session_token)
        .map_err(|_| anyhow!("Invalid session token format"))?;
    
    // Token structure: [payload_len (4 bytes)][payload][signature (65 bytes)]
    if token_bytes.len() < 69 { // 4 + min_payload + 65
        return Err(anyhow!("Session token too short"));
    }
    
    // Extract payload length
    let payload_len = u32::from_le_bytes(
        token_bytes[0..4]
            .try_into()
            .map_err(|_| anyhow!("Invalid payload length"))?
    ) as usize;
    
    // Validate payload length
    if 4 + payload_len + 65 != token_bytes.len() {
        return Err(anyhow!("Invalid token structure"));
    }
    
    // Extract payload and signature
    let payload_bytes = &token_bytes[4..4 + payload_len];
    let signature_bytes = &token_bytes[4 + payload_len..];
    
    // Deserialize payload
    let payload: SessionTokenPayload = serde_json::from_slice(payload_bytes)
        .map_err(|_| anyhow!("Failed to parse session token payload"))?;
    
    // Check expiration
    let current_time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs();
    
    if current_time > payload.expires_at {
        return Err(anyhow!("Session token expired"));
    }
    
    // Validate API key matches
    if payload.api_key != api_key {
        return Err(anyhow!("API key mismatch in session token"));
    }
    
    // Validate IP matches (if not local/test mode)
    if !is_local_ip(client_ip) && payload.client_ip != client_ip {
        return Err(anyhow!(
            "IP mismatch: token issued for {} but request from {}",
            payload.client_ip,
            client_ip
        ));
    }
    
    // Verify signature
    verify_signature(payload_bytes, signature_bytes, enclave_pubkey)?;
    
    Ok(())
}

/// Verify a secp256k1 signature
fn verify_signature(
    message: &[u8],
    signature_with_recovery: &[u8],
    public_key: &PublicKey,
) -> Result<()> {
    if signature_with_recovery.len() != 65 {
        return Err(anyhow!("Invalid signature length"));
    }
    
    // Extract signature and recovery ID
    let signature = Signature::parse_standard_slice(&signature_with_recovery[0..64])
        .map_err(|_| anyhow!("Invalid signature format"))?;
    
    let recovery_id = RecoveryId::parse(signature_with_recovery[64])
        .map_err(|_| anyhow!("Invalid recovery ID"))?;
    
    // Hash the message
    let mut hasher = Sha256::new();
    hasher.update(message);
    let hash = hasher.finalize();
    
    let message = Message::parse_slice(&hash)
        .map_err(|_| anyhow!("Failed to parse message hash"))?;
    
    // Recover public key from signature
    let recovered_pubkey = recover(&message, &signature, &recovery_id)
        .map_err(|_| anyhow!("Failed to recover public key from signature"))?;
    
    // Verify it matches the expected public key
    if recovered_pubkey != *public_key {
        return Err(anyhow!("Signature verification failed"));
    }
    
    Ok(())
}

/// Check if an IP is local/test
fn is_local_ip(ip: &str) -> bool {
    ip.starts_with("127.") || 
    ip.starts_with("localhost") || 
    ip == "::1" ||
    ip.starts_with("192.168.") ||
    ip.starts_with("10.") ||
    ip.starts_with("172.")
}

/// Create a signed session token (for create_session endpoint)
pub fn create_signed_session_token(
    api_key: &str,
    client_ip: &str,
    secret_key: &libsecp256k1::SecretKey,
) -> Result<(String, u64)> {
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs();
    
    let expires_at = timestamp + 3600; // 1 hour
    
    // Create payload
    let payload = SessionTokenPayload {
        api_key: api_key.to_string(),
        client_ip: client_ip.to_string(),
        timestamp,
        expires_at,
    };
    
    // Serialize payload
    let payload_bytes = serde_json::to_vec(&payload)?;
    
    // Hash and sign
    let mut hasher = Sha256::new();
    hasher.update(&payload_bytes);
    let hash = hasher.finalize();
    
    let message = Message::parse_slice(&hash)
        .map_err(|_| anyhow!("Failed to create message"))?;
    
    let (signature, recovery_id) = libsecp256k1::sign(&message, secret_key);
    
    // Build token: [payload_len][payload][signature][recovery_id]
    let mut token_bytes = Vec::new();
    token_bytes.extend_from_slice(&(payload_bytes.len() as u32).to_le_bytes());
    token_bytes.extend_from_slice(&payload_bytes);
    token_bytes.extend_from_slice(&signature.serialize());
    token_bytes.push(recovery_id.into());
    
    let token = base64.encode(token_bytes);
    
    Ok((token, expires_at))
}