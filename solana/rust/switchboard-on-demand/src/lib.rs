#![allow(unused_attributes)]
#![allow(clippy::result_large_err)]

mod macros;

#[allow(unused_imports)]
use std::sync::Arc;

use solana_program::pubkey;

pub mod decimal;
pub use decimal::*;

pub mod on_demand;
pub use on_demand::*;

pub mod utils;
pub use utils::*;

pub mod anchor_traits;
pub use anchor_traits::*;

pub mod program_id;
pub use program_id::*;

pub mod accounts;
pub mod instructions;
pub mod types;

pub mod prelude;

pub mod sysvar;
pub use sysvar::*;

cfg_client! {
    use solana_sdk::signer::keypair::Keypair;
    pub type AnchorClient = anchor_client::Client<Arc<Keypair>>;
    mod client;
    pub mod clock;
    pub use clock::*;
}

cfg_ipfs! {
    pub mod ipfs {
        pub use switchboard_common::ipfs::*;
    }
}
