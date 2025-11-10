//! # Switchboard On-Demand Client
//!
//! A comprehensive Rust client library for interacting with the Switchboard On-Demand Oracle Network
//! and Crossbar API. This crate provides high-level abstractions for fetching oracle data, submitting
//! responses, and managing Solana on-chain interactions with the Switchboard infrastructure.
//!
//! ## Features
//!
//! - **Oracle Data Feeds**: Fetch real-time price and data feeds from Switchboard oracles
//! - **Pull Feed Management**: Create, update, and manage pull-based oracle feeds
//! - **Gateway Integration**: Seamless communication with Switchboard's Crossbar gateway
//! - **Solana Integration**: Built-in support for Solana blockchain interactions
//! - **Consensus Mechanisms**: Support for oracle consensus and signature aggregation
//! - **Cryptographic Security**: SECP256k1 signature verification and validation
//!
//! ## Network Support
//!
//! This crate supports both Solana mainnet and devnet environments:
//! - Enable devnet via the `devnet` feature flag or `SB_ENV=devnet` environment variable
//! - Automatic program ID resolution based on network configuration
//!
//! ## Quick Start
//!
//! ```rust,no_run
//! use switchboard_on_demand_client::*;
//! use solana_sdk::pubkey::Pubkey;
//! //!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Initialize a gateway client
//!     let gateway = Gateway::new("https://api.switchboard.xyz");
//!
//!     // Get the Switchboard program ID for current network
//!     let program_id = get_switchboard_on_demand_program_id();
//!     println!("Using program ID: {}", program_id);
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Architecture
//!
//! The crate is organized into several key modules:
//!
//! - [`gateway`] - Crossbar API client for fetching oracle signatures and data
//! - [`pull_feed`] - Pull-based oracle feed management and utilities
//! - [`accounts`] - Solana account structures and deserialization
//! - [`instructions`] - Solana instruction builders for on-chain operations
//! - [`crossbar`] - Core Crossbar protocol implementations
//! - [`oracle_job`] - Oracle job definitions and serialization
//!
//! ## Error Handling
//!
//! This crate uses [`anyhow`] for error handling, providing rich error context and easy
//! error propagation. Most functions return `Result<T, anyhow::Error>` for consistent
//! error handling across the API.
//!
//! ## Security
//!
//! All cryptographic operations use industry-standard libraries and follow best practices:
//! - SECP256k1 signatures for oracle response verification
//! - Secure hash functions for data integrity
//! - Proper key management and validation
//!
//! ## Performance
//!
//! The client is designed for high-performance applications:
//! - Async/await support throughout the API
//! - Efficient serialization with zero-copy where possible
//! - Connection pooling and request batching capabilities
//!
//! [`anyhow`]: https://docs.rs/anyhow

#![allow(clippy::module_inception)]
#![allow(missing_docs)]
#![warn(clippy::all)]
/// Solana instruction builders for interacting with the Switchboard On-Demand program.
pub mod instructions;
pub use instructions::*;
/// Core Crossbar protocol implementations and client functionality.
pub mod crossbar;
pub use crossbar::*;
/// Gateway client for interfacing with Switchboard's Crossbar API.
pub mod gateway;
pub use gateway::*;
/// Pull-based oracle feed management and data fetching utilities.
pub mod pull_feed;
pub use pull_feed::*;
// pub mod pull_feed_fetch_update;
// pub use pull_feed_fetch_update::*;
/// Associated token account utilities and constants.
pub mod associated_token_account;
pub use associated_token_account::*;
/// Solana slot hash utilities and recent hash management.
pub mod recent_slothashes;
pub use recent_slothashes::*;
/// Switchboard account structures and deserialization utilities.
pub mod accounts;
pub use accounts::*;
/// Lookup table ownership and management functionality.
pub mod lut_owner;
use protos::OracleJob;
use anyhow_ext::Error as AnyhowError;
pub use lut_owner::*;
pub use prost;
use solana_sdk::hash;
use solana_sdk::instruction::Instruction;
use solana_sdk::message::Message;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signer;
use solana_sdk::signer::keypair::Keypair;
use solana_sdk::transaction::Transaction;
/// SECP256k1 cryptographic utilities and signature verification.
pub mod secp256k1;
use solana_sdk::pubkey;

/// Determines if the devnet environment is enabled.
///
/// Checks for devnet configuration via:
/// 1. The `devnet` feature flag at compile time
/// 2. The `SB_ENV` environment variable set to "devnet" at runtime
///
/// # Returns
///
/// `true` if devnet is enabled, `false` for mainnet
///
/// # Examples
///
/// ```rust
/// use switchboard_on_demand_client::is_devnet;
///
/// if is_devnet() {
///     println!("Running on devnet");
/// } else {
///     println!("Running on mainnet");
/// }
/// ```
pub fn is_devnet() -> bool {
    cfg!(feature = "devnet") || std::env::var("SB_ENV").unwrap_or_default() == "devnet"
}

/// The Switchboard On-Demand program ID for Solana mainnet.
pub const ON_DEMAND_MAINNET_PID: Pubkey = pubkey!("SBondMDrcV3K4kxZR1HNVT7osZxAHVHgYXL5Ze1oMUv");

/// The Switchboard On-Demand program ID for Solana devnet.
pub const ON_DEMAND_DEVNET_PID: Pubkey = pubkey!("Aio4gaXjXzJNVLtzwtNVmSqGKpANtXhybbkhtAC94ji2");

/// Returns the appropriate Switchboard On-Demand program ID for the current network.
///
/// Automatically selects between mainnet and devnet program IDs based on the
/// current environment configuration determined by [`is_devnet()`].
///
/// # Returns
///
/// - [`ON_DEMAND_DEVNET_PID`] if devnet is enabled
/// - [`ON_DEMAND_MAINNET_PID`] if mainnet is enabled (default)
///
/// # Examples
///
/// ```rust
/// use switchboard_on_demand_client::get_switchboard_on_demand_program_id;
///
/// let program_id = get_switchboard_on_demand_program_id();
/// println!("Using Switchboard program: {}", program_id);
/// ```
pub fn get_switchboard_on_demand_program_id() -> Pubkey {
    if is_devnet() {
        ON_DEMAND_DEVNET_PID
    } else {
        ON_DEMAND_MAINNET_PID
    }
}

/// Seed bytes for deriving the Switchboard state account PDA.
pub const STATE_SEED: &[u8] = b"STATE";

/// Seed bytes for deriving oracle feed statistics account PDAs.
pub const ORACLE_FEED_STATS_SEED: &[u8] = b"OracleFeedStats";

/// Seed bytes for deriving oracle randomness statistics account PDAs.
pub const ORACLE_RANDOMNESS_STATS_SEED: &[u8] = b"OracleRandomnessStats";

/// Seed bytes for deriving oracle statistics account PDAs.
pub const ORACLE_STATS_SEED: &[u8] = b"OracleStats";

/// Seed bytes for deriving lookup table signer account PDAs.
pub const LUT_SIGNER_SEED: &[u8] = b"LutSigner";

/// Seed bytes for deriving delegation account PDAs.
pub const DELEGATION_SEED: &[u8] = b"Delegation";

/// Seed bytes for deriving delegation group account PDAs.
pub const DELEGATION_GROUP_SEED: &[u8] = b"Group";

/// Seed bytes for deriving reward pool vault account PDAs.
pub const REWARD_POOL_VAULT_SEED: &[u8] = b"RewardPool";

/// Converts a set of instructions into a signed Solana transaction.
///
/// This utility function creates a new Solana transaction from the provided instructions,
/// signs it with the given keypairs, and includes the specified recent blockhash.
/// The first signer in the array is used as the fee payer.
///
/// # Arguments
///
/// * `ixs` - A slice of Solana instructions to include in the transaction
/// * `signers` - A slice of keypair references to sign the transaction with
/// * `blockhash` - A recent blockhash from the Solana network
///
/// # Returns
///
/// A `Result` containing the signed `Transaction` on success, or an `AnyhowError` on failure.
///
/// # Errors
///
/// Returns an error if:
/// - The transaction signing process fails
/// - The provided signers are invalid
/// - The message creation fails
///
/// # Examples
///
/// ```rust,no_run
/// use switchboard_on_demand_client::ix_to_tx;
/// use solana_sdk::{
///     instruction::Instruction,
///     signature::{Keypair, Signer},
///     system_instruction,
///     hash::Hash,
/// };
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let payer = Keypair::new();
/// let recipient = Keypair::new();
/// let recent_blockhash = Hash::default(); // In practice, fetch from RPC
///
/// let instruction = system_instruction::transfer(
///     &payer.pubkey(),
///     &recipient.pubkey(),
///     1_000_000, // lamports
/// );
///
/// let transaction = ix_to_tx(&[instruction], &[&payer], recent_blockhash)?;
/// # Ok(())
/// # }
/// ```
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
