use borsh::BorshSerialize;
use solana_program::instruction::AccountMeta;
use crate::cfg_client;

use crate::anchor_traits::*;
use crate::prelude::*;
use crate::SYSTEM_PROGRAM_ID;
use crate::SPL_ASSOCIATED_TOKEN_ACCOUNT_PROGRAM_ID;
use crate::{solana_program, Pubkey};

/// Queue reward payment instruction
pub struct QueuePayReward {}

/// Parameters for queue reward payment instruction
#[derive(Clone, BorshSerialize, Debug)]
pub struct QueuePayRewardParams {}

impl InstructionData for QueuePayRewardParams {}
// Discriminator for "global:queue_pay_rewards" (plural)
const DISCRIMINATOR: &'static [u8] = &[67, 87, 149, 166, 56, 112, 20, 12];
impl Discriminator for QueuePayReward {
    const DISCRIMINATOR: &'static [u8] = DISCRIMINATOR;
}
impl Discriminator for QueuePayRewardParams {
    const DISCRIMINATOR: &'static [u8] = DISCRIMINATOR;
}

/// Arguments for building a queue reward payment instruction
#[derive(Clone, Debug)]
pub struct QueuePayRewardArgs {
    /// Queue account public key
    pub queue: Pubkey,
    /// Payer account public key
    pub payer: Pubkey,
}

/// Account metas for queue reward payment instruction
pub struct QueuePayRewardAccounts {
    /// Queue account public key
    pub queue: Pubkey,
    /// Jito Vault public key
    pub vault: Pubkey,
    /// Reward vault (wSOL ATA of queue)
    pub reward_vault: Pubkey,
    /// Payer account public key
    pub payer: Pubkey,
    /// Escrow (payer's wSOL ATA)
    pub escrow: Pubkey,
    /// Additional account metas required for the instruction (oracle, authority, stats per oracle)
    pub remaining_accounts: Vec<AccountMeta>,
}

impl ToAccountMetas for QueuePayRewardAccounts {
    fn to_account_metas(&self, _: Option<bool>) -> Vec<AccountMeta> {
        let program_state = State::get_pda();
        let token_program: Pubkey = crate::SPL_TOKEN_PROGRAM_ID.to_bytes().into();
        let associated_token_program = SPL_ASSOCIATED_TOKEN_ACCOUNT_PROGRAM_ID;
        let system_program = SYSTEM_PROGRAM_ID;
        let wsol_mint: Pubkey = crate::NATIVE_MINT.to_bytes().into();

        let mut accounts = vec![
            AccountMeta::new(self.queue, false),
            AccountMeta::new_readonly(program_state, false),
            AccountMeta::new_readonly(system_program.to_bytes().into(), false),
            AccountMeta::new(self.vault, false),
            AccountMeta::new(self.reward_vault, false),
            AccountMeta::new_readonly(token_program, false),
            AccountMeta::new_readonly(associated_token_program.to_bytes().into(), false),
            AccountMeta::new_readonly(wsol_mint, false),
            AccountMeta::new(self.payer, true),
            AccountMeta::new(self.escrow, false),
        ];
        accounts.extend(self.remaining_accounts.clone());
        accounts
    }
}

cfg_client! {
use crate::solana_compat::solana_client::nonblocking::rpc_client::RpcClient;
use crate::get_sb_program_id;
use crate::solana_compat::AddressLookupTableAccount;
use crate::get_associated_token_address;

impl QueuePayReward {
    pub async fn build_ix(client: &RpcClient, args: QueuePayRewardArgs) -> Result<Instruction, OnDemandError> {
        let pid = if crate::utils::is_devnet() {
            get_sb_program_id("devnet")
        } else {
            get_sb_program_id("mainnet")
        };

        let queue_data = QueueAccountData::fetch_async(client, args.queue).await?;
        let wsol_mint: Pubkey = crate::NATIVE_MINT.to_bytes().into();

        // Get the vault from queue data
        let vault = queue_data.vaults.iter()
            .find(|v| v.vault_key != Pubkey::default())
            .map(|v| v.vault_key)
            .ok_or(OnDemandError::AccountNotFound)?;

        // Reward vault is queue's wSOL ATA
        let reward_vault = get_associated_token_address(&args.queue, &wsol_mint);

        // Escrow is payer's wSOL ATA
        let escrow = get_associated_token_address(&args.payer, &wsol_mint);

        // Build remaining accounts for all oracles on the queue
        // Order: oracle, oracle_authority, oracle_stats (per oracle)
        let mut remaining_accounts = vec![];
        let oracle_keys = &queue_data.oracle_keys[..queue_data.oracle_keys_len as usize];

        for oracle_key in oracle_keys {
            if *oracle_key == Pubkey::default() {
                continue;
            }
            let oracle_data = OracleAccountData::fetch_async(client, *oracle_key).await?;
            let oracle_stats = OracleAccountData::stats_key(oracle_key);

            remaining_accounts.push(AccountMeta::new_readonly(*oracle_key, false));
            remaining_accounts.push(AccountMeta::new(oracle_data.authority, false));
            remaining_accounts.push(AccountMeta::new(oracle_stats, false));
        }

        let ix = crate::utils::build_ix(
            &pid,
            &QueuePayRewardAccounts {
                queue: args.queue,
                vault,
                reward_vault,
                payer: args.payer,
                escrow,
                remaining_accounts,
            },
            &QueuePayRewardParams { },
        );
        Ok(ix)
    }

    pub async fn fetch_luts(client: &RpcClient, args: QueuePayRewardArgs) -> Result<Vec<AddressLookupTableAccount>, OnDemandError> {
        let queue_data = QueueAccountData::fetch_async(client, args.queue).await?;
        let queue_lut = queue_data.fetch_lut(&args.queue, client).await?;
        let mut luts = vec![queue_lut];

        // Fetch LUTs for all oracles on the queue
        let oracle_keys = &queue_data.oracle_keys[..queue_data.oracle_keys_len as usize];
        for oracle_key in oracle_keys {
            if *oracle_key == Pubkey::default() {
                continue;
            }
            if let Ok(oracle_data) = OracleAccountData::fetch_async(client, *oracle_key).await {
                if let Ok(oracle_lut) = oracle_data.fetch_lut(oracle_key, client).await {
                    luts.push(oracle_lut);
                }
            }
        }

        Ok(luts)
    }
}
}
