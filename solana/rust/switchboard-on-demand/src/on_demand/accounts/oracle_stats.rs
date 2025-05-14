#![allow(unused_attributes)]
use solana_program::pubkey::Pubkey;
use switchboard_common::cfg_client;

use crate::anchor_traits::*;
use crate::get_sb_program_id;

#[derive(Default)]
#[repr(C)]
#[derive(bytemuck::Zeroable, bytemuck::Pod, Debug, Copy, Clone)]
pub struct OracleEpochInfo {
    pub id: u64,
    pub reserved1: u64,
    pub slot_end: u64,
    pub slash_score: u64,
    pub reward_score: u64,
    pub stake_score: u64,
}

#[derive(Default)]
#[repr(C)]
#[derive(bytemuck::Zeroable, bytemuck::Pod, Debug, Copy, Clone)]
pub struct MegaSlotInfo {
    pub reserved1: u64,
    pub slot_end: u64,
    pub perf_goal: i64,
    pub current_signature_count: i64,
}

#[repr(C)]
#[derive(bytemuck::Zeroable, bytemuck::Pod, Debug, Copy, Clone)]
pub struct OracleStatsAccountData {
    pub owner: Pubkey,
    pub oracle: Pubkey,
    /// The last epoch that has completed. cleared after registered with the
    /// staking program.
    pub finalized_epoch: OracleEpochInfo,
    /// The current epoch info being used by the oracle. for stake. Will moved
    /// to finalized_epoch as soon as the epoch is over.
    pub current_epoch: OracleEpochInfo,
    pub mega_slot_info: MegaSlotInfo,
    pub last_transfer_slot: u64,
    pub bump: u8,
    pub padding1: [u8; 7],
    /// Reserved.
    pub _ebuf: [u8; 1024],
}
impl Owner for OracleStatsAccountData {
    fn owner() -> Pubkey {
        let pid = if cfg!(feature = "devnet") {
            get_sb_program_id("devnet")
        } else {
            get_sb_program_id("mainnet")
        };
        pid
    }
}
impl Discriminator for OracleStatsAccountData {
    const DISCRIMINATOR: [u8; 8] = [180, 157, 178, 234, 240, 27, 152, 179];
}
cfg_client! {
    use crate::impl_account_deserialize;

    impl_account_deserialize!(OracleStatsAccountData);
}
impl OracleStatsAccountData {
    cfg_client! {

        pub async fn fetch_async(
            client: &solana_client::nonblocking::rpc_client::RpcClient,
            pubkey: Pubkey,
        ) -> std::result::Result<Self, crate::OnDemandError> {
            crate::client::fetch_zerocopy_account_async(client, pubkey).await
        }

    }
}
