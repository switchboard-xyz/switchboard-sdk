use borsh::BorshSerialize;
use solana_program::pubkey::Pubkey;
use solana_program::pubkey;
use solana_program::system_program;
use switchboard_common::cfg_client;

use crate::anchor_traits::*;
use crate::prelude::*;

pub const JITO_VAULT_ID: Pubkey = pubkey!("Vau1t6sLNxnzB7ZDsef8TLbPLfyZMYXH8WTNqUdm9g8");


pub struct QueuePaySubsidy {}

#[derive(Clone, BorshSerialize, Debug)]
pub struct QueuePaySubsidyParams {}

impl InstructionData for QueuePaySubsidyParams {}
impl Discriminator for QueuePaySubsidy {
    const DISCRIMINATOR: [u8; 8] = [85, 84, 51, 251, 144, 57, 105, 200];
}
impl Discriminator for QueuePaySubsidyParams {
    const DISCRIMINATOR: [u8; 8] = QueuePaySubsidy::DISCRIMINATOR;
}

pub struct QueuePaySubsidyArgs {
    pub queue: Pubkey,
    pub vault: Pubkey,
    pub payer: Pubkey,
}
pub struct QueuePaySubsidyAccounts {
    pub queue: Pubkey,
    pub vault: Pubkey,
    pub switch_mint: Pubkey,
    pub payer: Pubkey,
    pub remaining_accounts: Vec<AccountMeta>,
}
impl ToAccountMetas for QueuePaySubsidyAccounts {
    fn to_account_metas(&self, _: Option<bool>) -> Vec<AccountMeta> {
        let program_state = State::get_pda();
        let token_program = spl_token::id();
        let associated_token_program = spl_associated_token_account::id();
        let system_program = system_program::id();
        let wsol_mint = spl_token::native_mint::id();
        let subsidy_vault = get_associated_token_address(&program_state, &self.switch_mint);
        let reward_vault = get_associated_token_address(&self.vault, &self.switch_mint);
        let vault_config = Pubkey::find_program_address(&[b"config"], &JITO_VAULT_ID).0;

        let mut accounts = vec![
            AccountMeta::new(self.queue, false),
            AccountMeta::new_readonly(program_state, false),
            AccountMeta::new_readonly(system_program, false),
            AccountMeta::new_readonly(self.vault, false),
            AccountMeta::new(reward_vault, false),
            AccountMeta::new(subsidy_vault, false),
            AccountMeta::new_readonly(token_program, false),
            AccountMeta::new_readonly(associated_token_program, false),
            AccountMeta::new_readonly(wsol_mint, false),
            AccountMeta::new_readonly(self.switch_mint, false),
            AccountMeta::new_readonly(vault_config, false),
            AccountMeta::new(self.payer, true),
        ];
        accounts.extend(self.remaining_accounts.clone());
        accounts
    }
}

cfg_client! {
use solana_client::nonblocking::rpc_client::RpcClient;
use crate::get_sb_program_id;
use futures::future::join_all;
use solana_program::address_lookup_table::AddressLookupTableAccount;

impl QueuePaySubsidy {
    pub async fn build_ix(client: &RpcClient, args: QueuePaySubsidyArgs) -> Result<Instruction, OnDemandError> {
        let state = State::fetch_async(client).await?;
        let switch_mint = state.switch_mint;
        let pid = if cfg!(feature = "devnet") {
            get_sb_program_id("devnet")
        } else {
            get_sb_program_id("mainnet")
        };
        let queue_data = QueueAccountData::fetch_async(client, args.queue).await?;
        let oracles = queue_data.oracle_keys[..queue_data.oracle_keys_len as usize].to_vec();
        let mut remaining_accounts = vec![];
        for oracle in oracles {
            remaining_accounts.push(AccountMeta::new(oracle, false));
            let oracle_stats = OracleAccountData::stats_key(&oracle);
            remaining_accounts.push(AccountMeta::new(oracle_stats, false));
            let oracle_data = OracleAccountData::fetch_async(client, oracle).await?;
            let operator = oracle_data.operator;
            if operator == Pubkey::default() {
                continue;
            }
            remaining_accounts.push(AccountMeta::new_readonly(operator, false));
            let oracle_subisidy_wallet = get_associated_token_address(&operator, &switch_mint);
            remaining_accounts.push(AccountMeta::new(oracle_subisidy_wallet, false));
        }
        Ok(crate::utils::build_ix(
            &pid,
            &QueuePaySubsidyAccounts {
                queue: args.queue,
                vault: args.vault,
                switch_mint: state.switch_mint,
                remaining_accounts,
                payer: args.payer,
            },
            &QueuePaySubsidyParams { },
        ))
    }

    pub async fn fetch_luts(client: &RpcClient, args: QueuePaySubsidyArgs) -> Result<Vec<AddressLookupTableAccount>, OnDemandError> {
        let queue_data = QueueAccountData::fetch_async(client, args.queue).await?;
        let oracles = queue_data.oracle_keys[..queue_data.oracle_keys_len as usize].to_vec();

        // Spawn parallel async tasks for fetching LUTs
        let lut_futures: Vec<_> = oracles
            .into_iter()
            .map(|oracle| {
                async move {
                    let oracle_data = OracleAccountData::fetch_async(&client, oracle).await.ok()?;
                    oracle_data.fetch_lut(&oracle, &client).await.ok()
                }
            })
        .collect();

        // Run all futures in parallel
        let luts: Vec<AddressLookupTableAccount> = join_all(lut_futures)
            .await
            .into_iter()
            .flatten()
            .collect();

        Ok(luts)
    }
}
}
