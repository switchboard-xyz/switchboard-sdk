use anyhow::Context;
use anyhow::Error as AnyhowError;
use arrayref::array_ref;
use bytemuck;
use crate::solana_compat::solana_client::nonblocking::rpc_client::RpcClient;
#[cfg(not(feature = "client-v3"))]
use crate::solana_compat::solana_client::rpc_config::RpcAccountInfoConfig;
#[cfg(not(feature = "client-v3"))]
use crate::solana_compat::commitment_config::CommitmentConfig;
use std::result::Result;

#[repr(C)]
#[derive(bytemuck::Pod, bytemuck::Zeroable, Debug, Clone, Copy)]
pub struct SlotHash {
    pub slot: u64,
    pub hash: [u8; 32],
}

impl SlotHash {
    /// Returns the base58-encoded hash as a `String`.
    pub fn to_base58_hash(&self) -> String {
        bs58::encode(self.hash).into_string()
    }
}

pub struct SlotHashSysvar;
impl SlotHashSysvar {
    pub async fn get_latest_slothash(client: &RpcClient) -> Result<SlotHash, AnyhowError> {
        let slot_hashes_id = crate::solana_compat::solana_sdk::sysvar::slot_hashes::ID;
        // Need to use the right CommitmentConfig type for the v3 RPC client
        #[cfg(feature = "client-v3")]
        let config = {
            use crate::solana_compat::solana_client::rpc_config::RpcAccountInfoConfig as RpcConfig;
            RpcConfig {
                commitment: crate::solana_compat::solana_client::rpc_config::RpcContextConfig::default().commitment,
                ..Default::default()
            }
        };
        #[cfg(not(feature = "client-v3"))]
        let config = RpcAccountInfoConfig {
            commitment: Some(CommitmentConfig::confirmed()),
            ..Default::default()
        };
        let slots_data = client
            .get_account_with_config(
                &slot_hashes_id.to_bytes().into(),
                config,
            )
            .await
            .context("Failed to fetch slot hashes")?
            .value
            .context("Failed to fetch slot hashes")?
            .data;
        let slots: &[u8] = array_ref![slots_data, 8, 20_480];
        // 20_480 / 40 = 512
        let slots: &[SlotHash] = bytemuck::cast_slice::<u8, SlotHash>(slots);
        Ok(slots[0])
    }
}
