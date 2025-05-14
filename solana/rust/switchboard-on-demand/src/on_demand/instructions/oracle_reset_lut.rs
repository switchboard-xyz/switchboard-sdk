use borsh::BorshSerialize;
use solana_program::address_lookup_table::program::ID as address_lookup_table_program;
use solana_program::pubkey::Pubkey;
use solana_program::system_program;

use crate::anchor_traits::*;
use crate::cfg_client;
use crate::prelude::*;

pub struct OracleResetLut {}

#[derive(Clone, BorshSerialize, Debug)]
pub struct OracleResetLutParams {
    pub recent_slot: u64,
}

impl InstructionData for OracleResetLutParams {}

impl Discriminator for OracleResetLut {
    const DISCRIMINATOR: [u8; 8] = [147, 244, 108, 198, 152, 219, 0, 22];
}
impl Discriminator for OracleResetLutParams {
    const DISCRIMINATOR: [u8; 8] = OracleResetLut::DISCRIMINATOR;
}

pub struct OracleResetLutArgs {
    pub oracle: Pubkey,
    pub payer: Pubkey,
    pub recent_slot: u64,
}
pub struct OracleResetLutAccounts {
    pub oracle: Pubkey,
    pub authority: Pubkey,
    pub payer: Pubkey,
    pub system_program: Pubkey,
    pub state: Pubkey,
    pub lut_signer: Pubkey,
    pub lut: Pubkey,
    pub address_lookup_table_program: Pubkey,
}
impl ToAccountMetas for OracleResetLutAccounts {
    fn to_account_metas(&self, _: Option<bool>) -> Vec<AccountMeta> {
        let state_pubkey = State::get_pda();
        vec![
            AccountMeta::new(self.oracle, false),
            AccountMeta::new_readonly(self.authority, true),
            AccountMeta::new(self.payer, false),
            AccountMeta::new_readonly(system_program::ID, false),
            AccountMeta::new_readonly(state_pubkey, false),
            AccountMeta::new_readonly(self.lut_signer, false),
            AccountMeta::new(self.lut, false),
            AccountMeta::new_readonly(address_lookup_table_program, false),
        ]
    }
}

cfg_client! {
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::address_lookup_table::instruction::derive_lookup_table_address;
use crate::get_sb_program_id;
use crate::find_lut_signer;

impl OracleResetLut {
    pub async fn build_ix(client: &RpcClient, args: OracleResetLutArgs) -> Result<Instruction, OnDemandError> {
        let oracle_data = OracleAccountData::fetch_async(client, args.oracle).await?;
        let authority = oracle_data.authority;
        let payer = oracle_data.authority;
        let lut_signer = find_lut_signer(&args.oracle);
        let lut = derive_lookup_table_address(&lut_signer, args.recent_slot).0;
        let pid = if cfg!(feature = "devnet") {
            get_sb_program_id("devnet")
        } else {
            get_sb_program_id("mainnet")
        };
        let ix = crate::utils::build_ix(
            &pid,
            &OracleResetLutAccounts {
                oracle: args.oracle,
                state: State::get_pda(),
                authority,
                lut_signer,
                lut,
                address_lookup_table_program,
                payer,
                system_program: system_program::ID,
            },
            &OracleResetLutParams {
                recent_slot: args.recent_slot,
            }
        );
        Ok(ix)
    }
}
}
