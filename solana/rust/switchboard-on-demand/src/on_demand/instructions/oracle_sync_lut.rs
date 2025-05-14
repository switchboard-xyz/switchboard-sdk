use borsh::BorshSerialize;
use solana_program::address_lookup_table::program::ID as address_lookup_table_program;
use solana_program::pubkey::Pubkey;
use solana_program::system_program;

use crate::anchor_traits::*;
use crate::cfg_client;
use crate::prelude::*;

pub struct OracleSyncLut {}

#[derive(Clone, BorshSerialize, Debug)]
pub struct OracleSyncLutParams {}

impl InstructionData for OracleSyncLutParams {}

impl Discriminator for OracleSyncLut {
    const DISCRIMINATOR: [u8; 8] = [138, 99, 12, 59, 18, 170, 171, 45];
}
impl Discriminator for OracleSyncLutParams {
    const DISCRIMINATOR: [u8; 8] = OracleSyncLut::DISCRIMINATOR;
}

pub struct OracleSyncLutArgs {
    pub oracle: Pubkey,
    pub vault: Pubkey,
    pub payer: Pubkey,
    pub lut_slot: u64,
}
pub struct OracleSyncLutAccounts {
    pub oracle: Pubkey,
    pub queue: Pubkey,
    pub ncn: Pubkey,
    pub vault: Pubkey,
    pub state: Pubkey,
    pub authority: Pubkey,
    pub operator: Pubkey,
    pub ncn_operator_state: Pubkey,
    pub operator_vault_ticket: Pubkey,
    pub vault_operator_delegation: Pubkey,
    pub lut_signer: Pubkey,
    pub lut: Pubkey,
    pub address_lookup_table_program: Pubkey,
    pub payer: Pubkey,
    pub system_program: Pubkey,
}
impl ToAccountMetas for OracleSyncLutAccounts {
    fn to_account_metas(&self, _: Option<bool>) -> Vec<AccountMeta> {
        let state_pubkey = State::get_pda();
        vec![
            AccountMeta::new(self.oracle, false),
            AccountMeta::new_readonly(self.queue, false),
            AccountMeta::new_readonly(self.ncn, false),
            AccountMeta::new_readonly(self.vault, false),
            AccountMeta::new_readonly(state_pubkey, false),
            AccountMeta::new_readonly(self.authority, true),
            AccountMeta::new_readonly(self.operator, false),
            AccountMeta::new_readonly(self.ncn_operator_state, false),
            AccountMeta::new_readonly(self.operator_vault_ticket, false),
            AccountMeta::new_readonly(self.vault_operator_delegation, false),
            AccountMeta::new_readonly(self.lut_signer, false),
            AccountMeta::new(self.lut, false),
            AccountMeta::new_readonly(address_lookup_table_program, false),
            AccountMeta::new(self.payer, true),
            AccountMeta::new_readonly(system_program::ID, false),
        ]
    }
}

cfg_client! {
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::address_lookup_table::instruction::derive_lookup_table_address;
use jito_restaking_client::programs::JITO_RESTAKING_ID;
use jito_vault_client::programs::JITO_VAULT_ID;
use crate::get_sb_program_id;
use crate::find_lut_signer;

impl OracleSyncLut {
    pub async fn build_ix(client: &RpcClient, args: OracleSyncLutArgs) -> Result<Instruction, OnDemandError> {
        let oracle_data = OracleAccountData::fetch_async(client, args.oracle).await?;
        let queue = oracle_data.queue;
        let queue_data = QueueAccountData::fetch_async(client, queue).await?;
        let authority = oracle_data.authority;
        let operator = oracle_data.operator;
        let payer = oracle_data.authority;
        let lut_signer = find_lut_signer(&args.oracle);
        let lut = derive_lookup_table_address(&lut_signer, args.lut_slot).0;
        let ncn_operator_state = Pubkey::find_program_address(
            &[
                b"ncn_operator_state",
                &queue_data.ncn.to_bytes(),
                &operator.to_bytes(),
            ],
            &JITO_RESTAKING_ID,
        ).0;
        let operator_vault_ticket = Pubkey::find_program_address(
            &[
                b"operator_vault_ticket",
                &operator.to_bytes(),
                &args.vault.to_bytes(),
            ],
            &JITO_RESTAKING_ID,
        ).0;
        let vault_operator_delegation = Pubkey::find_program_address(
            &[
                b"vault_operator_delegation",
                &args.vault.to_bytes(),
                &operator.to_bytes(),
            ],
            &JITO_VAULT_ID,
        ).0;
        let pid = if cfg!(feature = "devnet") {
            get_sb_program_id("devnet")
        } else {
            get_sb_program_id("mainnet")
        };
        let ix = crate::utils::build_ix(
            &pid,
            &OracleSyncLutAccounts {
                oracle: args.oracle,
                queue,
                ncn: queue_data.ncn,
                vault: args.vault,
                state: State::get_pda(),
                authority,
                operator,
                ncn_operator_state,
                operator_vault_ticket,
                vault_operator_delegation,
                lut_signer,
                lut,
                address_lookup_table_program,
                payer,
                system_program: system_program::ID,
            },
            &OracleSyncLutParams { },
        );
        Ok(ix)
    }
}
}
