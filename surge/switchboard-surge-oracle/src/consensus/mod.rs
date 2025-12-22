pub mod signing;

pub use signing::{generate_bundle_consensus_response, OracleConfig};

use once_cell::sync::OnceCell;

/// Provider trait for slot and recent blockhash
pub trait SlotHashProvider: Send + Sync {
    fn get_slot_and_hash(&self) -> Option<(u64, String)>;
}

static SLOT_HASH_PROVIDER: OnceCell<Box<dyn SlotHashProvider>> = OnceCell::new();

/// Register a global slot/hash provider (called from rust-feeds-oracle)
pub fn set_slot_hash_provider<P: SlotHashProvider + 'static>(provider: P) {
    let _ = SLOT_HASH_PROVIDER.set(Box::new(provider));
}

/// Retrieve current slot and hash if provider is set
pub fn current_slot_and_hash() -> Option<(u64, String)> {
    SLOT_HASH_PROVIDER.get().and_then(|p| p.get_slot_and_hash())
}
