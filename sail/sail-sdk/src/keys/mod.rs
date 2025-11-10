use anyhow::Error as AnyhowError;
use cached::proc_macro::once;
use rand::SeedableRng;
use rand_chacha::ChaCha20Rng;
use reqwest;
use sev_snp_utilities::guest::derived_key::derived_key::DerivedKey;
use sev_snp_utilities::guest::derived_key::get_derived_key::DerivedKeyRequestBuilder;
use sev_snp_utilities::guest::derived_key::get_derived_key::DerivedKeyRequester;
use sha2::{Digest, Sha256};
use solana_sdk::signer::keypair::keypair_from_seed;

pub const DEFAULT_DK_URL: &str = "http://127.0.0.1:8006/aa/derived_key";

#[once(result = true)]
pub fn get_derived_key() -> Result<[u8; 32], AnyhowError> {
    let coco_attest_service = DEFAULT_DK_URL.to_string();
    println!("Debug: Using URL: {}", coco_attest_service);

    let client = reqwest::blocking::Client::new();

    let response = match client.get(coco_attest_service).send() {
        Ok(r) => {
            println!("Debug: Got response with status: {}", r.status());
            if !r.status().is_success() {
                return Err(anyhow::anyhow!(
                    "server returned error status: {}",
                    r.status()
                ));
            }
            r
        }
        Err(e) => {
            println!("Debug: Request failed: {:?}", e);
            return Err(e.into());
        }
    };

    let derived_key = match response.bytes() {
        Ok(b) => {
            if b.is_empty() {
                return Err(anyhow::anyhow!("server returned empty response"));
            }
            b
        }
        Err(e) => {
            println!("Debug: Failed to get text: {:?}", e);
            return Err(e.into());
        }
    };

    if derived_key.len() != 32 {
        return Err(anyhow::anyhow!(
            "expected 32 bytes, got {} bytes",
            derived_key.len()
        ));
    }

    let array: [u8; 32] = derived_key
        .to_vec()
        .try_into()
        .map_err(|_| anyhow::anyhow!("failed to convert bytes to array"))?;

    Ok(array)
}

pub struct EnclaveKeys;
impl EnclaveKeys {
    pub fn get_derived_key() -> Result<[u8; 32], AnyhowError> {
        get_derived_key()
    }

    pub fn get_derived_key_with_options(
        mut options: DerivedKeyRequestBuilder,
    ) -> Result<[u8; 32], AnyhowError> {
        let derived_key = DerivedKey::request(options.build())?;
        Ok(derived_key.into())
    }

    pub fn get_enclave_ed25519_keypair() -> Result<[u8; 64], AnyhowError> {
        let derived_key = Self::get_derived_key()?;
        let keypair =
            keypair_from_seed(&derived_key).map_err(|e| AnyhowError::msg(e.to_string()))?;
        Ok(keypair.to_bytes())
    }

    pub fn get_enclave_ed25519_keypair_with_seed(additional_seed: &[u8]) -> Result<[u8; 64], AnyhowError> {
        let derived_key = Self::get_derived_key()?;
        
        // Combine the derived key with the additional seed
        let mut hasher = Sha256::new();
        hasher.update(&derived_key);
        hasher.update(additional_seed);
        let combined_seed = hasher.finalize();
        
        let keypair =
            keypair_from_seed(&combined_seed).map_err(|e| AnyhowError::msg(e.to_string()))?;
        Ok(keypair.to_bytes())
    }

    pub fn get_enclave_secp256k1_keypair() -> Result<[u8; 32], AnyhowError> {
        let source = Self::get_derived_key()?;
        let mut rng = ChaCha20Rng::from_seed(Sha256::digest(&source).into());
        let secp_key = libsecp256k1::SecretKey::random(&mut rng);
        Ok(secp_key.serialize())
    }

    pub fn get_enclave_secp256k1_keypair_with_seed(additional_seed: &[u8]) -> Result<[u8; 32], AnyhowError> {
        let derived_key = Self::get_derived_key()?;
        
        // Combine the derived key with the additional seed
        let mut hasher = Sha256::new();
        hasher.update(&derived_key);
        hasher.update(additional_seed);
        let combined_seed = hasher.finalize();
        
        let mut rng = ChaCha20Rng::from_seed(combined_seed.into());
        let secp_key = libsecp256k1::SecretKey::random(&mut rng);
        Ok(secp_key.serialize())
    }
}
