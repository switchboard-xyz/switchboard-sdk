#[allow(unused_imports)]
use std::str::FromStr;

use solana_program::pubkey::Pubkey;

use crate::*;

/// Program id for the Switchboard oracle program
/// SW1TCH7qEPTdLsDHRgPuMQjbQxKdH2aBStViMFnt64f
pub const SWITCHBOARD_PROGRAM_ID: Pubkey = pubkey!("SW1TCH7qEPTdLsDHRgPuMQjbQxKdH2aBStViMFnt64f");

pub const ON_DEMAND_MAINNET_PID: Pubkey = pubkey!("SBondMDrcV3K4kxZR1HNVT7osZxAHVHgYXL5Ze1oMUv");
pub const ON_DEMAND_DEVNET_PID: Pubkey = pubkey!("Aio4gaXjXzJNVLtzwtNVmSqGKpANtXhybbkhtAC94ji2");

pub const SWITCHBOARD_ON_DEMAND_PROGRAM_ID: Pubkey = if cfg!(feature = "devnet") {
    ON_DEMAND_DEVNET_PID
} else {
    ON_DEMAND_MAINNET_PID
};

pub fn get_sb_program_id(cluster: &str) -> Pubkey {
    if !cluster.starts_with("mainnet") {
        ON_DEMAND_DEVNET_PID
    } else {
        ON_DEMAND_MAINNET_PID
    }
}
