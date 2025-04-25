#![allow(clippy::module_inception)]
pub mod instructions;
pub use instructions::*;
pub mod crossbar;
pub use crossbar::*;
pub mod gateway;
pub use gateway::*;
pub mod pull_feed;
pub use pull_feed::*;
pub mod associated_token_account;
pub mod oracle_job;
pub use associated_token_account::*;
pub mod recent_slothashes;
pub use recent_slothashes::*;
pub mod accounts;
pub use accounts::*;
pub mod lut_owner;
use crate::oracle_job::OracleJob;
use anyhow_ext::Error as AnyhowError;
use lazy_static::lazy_static;
pub use lut_owner::*;
use solana_sdk::hash;
use solana_sdk::instruction::Instruction;
use solana_sdk::message::Message;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signer;
use solana_sdk::signer::keypair::Keypair;
use solana_sdk::transaction::Transaction;
use std::str::FromStr;
pub use prost;
pub mod secp256k1;

lazy_static! {
    pub static ref ON_DEMAND_MAINNET_PID: Pubkey =
        Pubkey::from_str("SBondMDrcV3K4kxZR1HNVT7osZxAHVHgYXL5Ze1oMUv").unwrap();
    pub static ref ON_DEMAND_DEVNET_PID: Pubkey =
        Pubkey::from_str("Aio4gaXjXzJNVLtzwtNVmSqGKpANtXhybbkhtAC94ji2").unwrap();
    pub static ref SWITCHBOARD_ON_DEMAND_PROGRAM_ID: Pubkey = if cfg!(feature = "devnet") {
        *ON_DEMAND_DEVNET_PID
    } else {
        *ON_DEMAND_MAINNET_PID
    };
}

pub const STATE_SEED: &[u8] = b"STATE";
pub const ORACLE_FEED_STATS_SEED: &[u8] = b"OracleFeedStats";
pub const ORACLE_RANDOMNESS_STATS_SEED: &[u8] = b"OracleRandomnessStats";
pub const ORACLE_STATS_SEED: &[u8] = b"OracleStats";
pub const LUT_SIGNER_SEED: &[u8] = b"LutSigner";
pub const DELEGATION_SEED: &[u8] = b"Delegation";
pub const DELEGATION_GROUP_SEED: &[u8] = b"Group";
pub const REWARD_POOL_VAULT_SEED: &[u8] = b"RewardPool";

pub fn ix_to_tx(
    ixs: &[Instruction],
    signers: &[&Keypair],
    blockhash: hash::Hash,
) -> Result<Transaction, AnyhowError> {
    let msg = Message::new(ixs, Some(&signers[0].pubkey()));
    let mut tx = Transaction::new_unsigned(msg);
    tx.try_sign(&signers.to_vec(), blockhash)?;
    Ok(tx)
}
