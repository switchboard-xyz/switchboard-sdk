//! Solana version compatibility layer
//!
//! This module provides compatibility between different Solana versions,
//! ensuring the correct types and modules are available regardless of which
//! version of the Solana SDK is being used.

// ===== Compile-time feature compatibility checks =====

// Ensure only one Solana version is enabled
#[cfg(all(feature = "solana-v2", feature = "solana-v3"))]
compile_error!(
    "Cannot enable both 'solana-v2' and 'solana-v3' features at the same time. Choose one."
);

// Ensure only one client version is enabled
#[cfg(all(feature = "client", feature = "client-v3"))]
compile_error!("Cannot enable both 'client' and 'client-v3' features at the same time. Use 'client' for Solana v2 or 'client-v3' for Solana v3.");

#[cfg(all(feature = "client-v2", feature = "client-v3"))]
compile_error!("Cannot enable both 'client-v2' and 'client-v3' features at the same time. Use 'client-v2' for Solana v2 or 'client-v3' for Solana v3.");

#[cfg(all(feature = "pinocchio", not(any(feature = "solana-v2", feature = "solana-v3"))))]
compile_error!("The 'pinocchio' feature requires either 'solana-v2' or 'solana-v3'. Enable one Solana version feature explicitly.");

// When anchor is enabled, use anchor's solana_program (v2.x)
#[cfg(feature = "anchor")]
pub use anchor_lang::solana_program;

// When anchor is NOT enabled, use version-specific solana_program
// v3 takes precedence when both v2 and v3 are enabled
#[cfg(all(not(feature = "anchor"), feature = "solana-v2"))]
pub use solana_program_v2 as solana_program;
#[cfg(all(not(feature = "anchor"), feature = "solana-v3"))]
pub use solana_program_v3 as solana_program;
// Keep the intentionally invalid `pinocchio`-without-Solana-version build
// type-resolvable so the compile_error above is the primary diagnostic.
#[cfg(all(
    not(feature = "anchor"),
    feature = "pinocchio",
    not(any(feature = "solana-v2", feature = "solana-v3"))
))]
pub use solana_program_v2 as solana_program;

// ===== solana_sdk (only when client is enabled) =====
// Version-specific solana_sdk selection based on features

// When client-v3 is enabled, expose solana_sdk v3
#[cfg(feature = "client-v3")]
pub use solana_sdk_v3 as solana_sdk;

// When client is enabled (default v2), expose solana_sdk v2
#[cfg(all(feature = "client", not(feature = "client-v3")))]
pub use solana_sdk_v2 as solana_sdk;

// ===== Unified module re-exports for both v2 and v3 =====
// These modules exist in different places in v2 vs v3
// Re-export them here to provide a consistent API

// V2: these come from solana_sdk
#[cfg(all(
    any(feature = "client", feature = "client-v2"),
    not(feature = "client-v3")
))]
pub mod address_lookup_table {
    #[allow(deprecated)]
    pub use solana_sdk_v2::address_lookup_table::*;
}

#[cfg(all(
    any(feature = "client", feature = "client-v2"),
    not(feature = "client-v3")
))]
pub mod compute_budget {
    #[allow(deprecated)]
    pub use solana_sdk_v2::compute_budget::*;
}

#[cfg(all(
    any(feature = "client", feature = "client-v2"),
    not(feature = "client-v3")
))]
pub mod commitment_config {
    #[allow(deprecated)]
    pub use solana_sdk_v2::commitment_config::*;
}

// V3: These modules were moved to separate crates in Solana v3
// However, those crates are not yet published to crates.io
// WORKAROUND: Use v2 versions with to_bytes().into() conversions
#[cfg(feature = "client-v3")]
pub mod address_lookup_table {
    // Use v2 SDK modules as compat layer
    // Convert types using to_bytes().into() when interfacing with v3 types
    #[allow(deprecated)]
    pub use solana_sdk_v2::address_lookup_table::*;
}

#[cfg(feature = "client-v3")]
pub mod compute_budget {
    // Use v2 SDK modules as compat layer
    #[allow(deprecated)]
    pub use solana_sdk_v2::compute_budget::*;
}

#[cfg(feature = "client-v3")]
pub mod commitment_config {
    // Use v2 SDK modules as compat layer
    #[allow(deprecated)]
    pub use solana_sdk_v2::commitment_config::*;
}

#[cfg(feature = "client-v3")]
pub mod client {
    // SyncClient from v2 for compatibility
    #[allow(deprecated)]
    pub use solana_sdk_v2::client::*;
}

// ===== solana_client (when client or client-v3 is enabled) =====
// Version-specific solana-client selection based on features

// When client-v3 is enabled, use solana-client v3
#[cfg(feature = "client-v3")]
pub use solana_client_v3 as solana_client;

// When client is enabled (default v2), use solana-client v2
#[cfg(all(feature = "client", not(feature = "client-v3")))]
pub use solana_client_v2 as solana_client;

// ===== solana_account_decoder (when client or client-v3 is enabled) =====
// Version-specific solana-account-decoder selection based on features

// When client-v3 is enabled, use solana-account-decoder v3
#[cfg(feature = "client-v3")]
pub use solana_account_decoder_v3 as solana_account_decoder;

// When client is enabled (default v2), use solana-account-decoder v2
#[cfg(all(feature = "client", not(feature = "client-v3")))]
pub use solana_account_decoder_v2 as solana_account_decoder;

// Re-export common types for easier access
pub use solana_program::{
    account_info::AccountInfo,
    instruction::{AccountMeta, Instruction},
    msg,
    pubkey::{pubkey, Pubkey},
    sysvar,
};

// These modules are in different locations depending on which solana_program is used
#[cfg(not(feature = "anchor"))]
pub use solana_program::{ed25519_program, hash, syscalls};

// When anchor is enabled, use solana_program_v2 directly for these modules
#[cfg(feature = "anchor")]
pub use solana_program_v2::{ed25519_program, hash, syscalls};

extern "C" {
    pub fn sol_memcpy_(dst: *mut u8, src: *const u8, n: u64);
}

// AddressLookupTableAccount location differs between versions
// For anchor, use solana_program_v2 directly
#[cfg(feature = "anchor")]
pub use solana_program_v2::address_lookup_table::AddressLookupTableAccount;

// For v2, it's in solana_program
#[cfg(all(
    feature = "solana-v2",
    not(feature = "solana-v3"),
    not(feature = "anchor")
))]
pub use solana_program::address_lookup_table::AddressLookupTableAccount;

// For v3 with client-v3, use the v3-native message LUT account so client code speaks v3
// end-to-end and hands back the same `AddressLookupTableAccount` a v3 consumer builds v0
// messages with. The v2 `address_lookup_table` module above stays available for the on-chain
// `state` parser and `program::id()` only (not yet split out for v3).
#[cfg(all(feature = "solana-v3", feature = "client-v3"))]
pub use solana_sdk::message::AddressLookupTableAccount;

#[cfg(all(any(feature = "client", feature = "client-v2"), not(feature = "client-v3")))]
pub type MessageAddressLookupTableAccount = AddressLookupTableAccount;

#[cfg(feature = "client-v3")]
pub type MessageAddressLookupTableAccount = solana_sdk::message::AddressLookupTableAccount;

#[cfg(any(feature = "client", feature = "client-v2", feature = "client-v3"))]
pub fn to_message_address_lookup_table_account(
    lut: &AddressLookupTableAccount,
) -> MessageAddressLookupTableAccount {
    MessageAddressLookupTableAccount {
        key: lut.key.to_bytes().into(),
        addresses: lut
            .addresses
            .iter()
            .map(|address| address.to_bytes().into())
            .collect(),
    }
}

/// Cluster type enum for specifying Solana network environments
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterType {
    /// Solana mainnet-beta
    MainnetBeta,
    /// Solana testnet
    Testnet,
    /// Solana devnet
    Devnet,
    /// Local development environment
    Development,
}
