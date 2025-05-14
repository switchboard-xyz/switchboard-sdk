use bytemuck::{Pod, Zeroable};
use solana_program::pubkey::Pubkey;

use crate::anchor_traits::*;
#[allow(unused_imports)]
use crate::impl_account_deserialize;
use crate::{cfg_client, get_sb_program_id};

const STATE_SEED: &[u8] = b"STATE";

#[derive(Debug, Copy, Clone)]
pub struct StateEpochInfo {
    pub id: u64,
    pub reserved1: u64,
    pub slot_end: u64,
}

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct State {
    pub bump: u8,
    pub test_only_disable_mr_enclave_check: u8,
    padding1: [u8; 6],
    pub authority: Pubkey,
    pub guardian_queue: Pubkey,
    pub reserved1: u64,
    pub epoch_length: u64,
    pub current_epoch: StateEpochInfo,
    pub next_epoch: StateEpochInfo,
    pub finalized_epoch: StateEpochInfo,
    // xswitch vault
    pub stake_pool: Pubkey,
    pub stake_program: Pubkey,
    pub switch_mint: Pubkey,
    pub sgx_advisories: [u16; 32],
    pub advisories_len: u8,
    _ebuf4: [u8; 15],
    _ebuf3: [u8; 256],
    _ebuf2: [u8; 512],
    _ebuf1: [u8; 1024],
}
unsafe impl Pod for State {}
unsafe impl Zeroable for State {}

cfg_client! {
    impl_account_deserialize!(State);
}

impl Discriminator for State {
    const DISCRIMINATOR: [u8; 8] = [216, 146, 107, 94, 104, 75, 182, 177];
}

impl Owner for State {
    fn owner() -> Pubkey {
        let pid = if cfg!(feature = "devnet") {
            get_sb_program_id("devnet")
        } else {
            get_sb_program_id("mainnet")
        };
        pid
    }
}

impl State {
    pub fn size() -> usize {
        8 + std::mem::size_of::<State>()
    }

    pub fn get_pda() -> Pubkey {
        let pid = if cfg!(feature = "devnet") {
            get_sb_program_id("devnet")
        } else {
            get_sb_program_id("mainnet")
        };
        let (pda_key, _) = Pubkey::find_program_address(&[STATE_SEED], &pid);
        pda_key
    }

    pub fn get_program_pda(program_id: Option<Pubkey>) -> Pubkey {
        let pid = if cfg!(feature = "devnet") {
            get_sb_program_id("devnet")
        } else {
            get_sb_program_id("mainnet")
        };
        let (pda_key, _) = Pubkey::find_program_address(&[STATE_SEED], &program_id.unwrap_or(pid));
        pda_key
    }

    cfg_client! {
        pub async fn fetch_async(
            client: &solana_client::nonblocking::rpc_client::RpcClient,
        ) -> std::result::Result<Self, crate::OnDemandError> {
            let pubkey = State::get_pda();
            crate::client::fetch_zerocopy_account_async(client, pubkey).await
        }
    }
}
