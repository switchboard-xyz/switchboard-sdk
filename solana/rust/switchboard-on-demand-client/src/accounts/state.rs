use crate::get_switchboard_on_demand_program_id;
use solana_sdk::pubkey::Pubkey;

const STATE_SEED: &[u8] = b"STATE";

#[derive(Copy, Clone)]
#[repr(C)]
pub struct StateEpochInfo {
    pub id: u64,
    pub _reserved1: u64,
    pub slot_end: u64,
}

#[derive(Copy, Clone)]
#[repr(C)]
pub struct State {
    pub bump: u8,
    pub test_only_disable_mr_enclave_check: u8,
    pub enable_staking: u8,
    padding1: [u8; 5],
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
    padding2: u8,
    // When oracles receive a reward, this is the percent of the total rewards
    // that are distributed equally regardless of the stake amount.
    pub flat_reward_cut_percentage: u8,
    pub enable_slashing: u8,
    pub subsidy_amount: u32,
    pub lut_slot: u64,
    _ebuf3: [u8; 256],
    _ebuf2: [u8; 512],
    _ebuf1: [u8; 1024],
}
impl State {
    pub fn key() -> Pubkey {
        Pubkey::find_program_address(&[STATE_SEED], &Self::pid()).0
    }

    pub fn pid() -> Pubkey {
        get_switchboard_on_demand_program_id()
    }
}
