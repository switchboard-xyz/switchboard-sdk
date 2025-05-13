#[allow(unused_imports)]
use std::str::FromStr;

use solana_program::pubkey::Pubkey;

use crate::*;

pub const ON_DEMAND_MAINNET_PID: Pubkey = pubkey!("SBondMDrcV3K4kxZR1HNVT7osZxAHVHgYXL5Ze1oMUv");
pub const ON_DEMAND_DEVNET_PID: Pubkey = pubkey!("Aio4gaXjXzJNVLtzwtNVmSqGKpANtXhybbkhtAC94ji2");
// Program id for the Switchboard oracle program
// sbattyXrzedoNATfc4L31wC9Mhxsi1BmFhTiN8gDshx
#[cfg(feature = "devnet")]
pub const SWITCHBOARD_ON_DEMAND_PROGRAM_ID: Pubkey = ON_DEMAND_DEVNET_PID;

#[cfg(not(feature = "devnet"))]
pub const SWITCHBOARD_ON_DEMAND_PROGRAM_ID: Pubkey = ON_DEMAND_MAINNET_PID;

// #[cfg(feature = "pid_override")]
// lazy_static! {
// pub static ref _DEFAULT_PID: Pubkey =
// Pubkey::from_str("SBondMDrcV3K4kxZR1HNVT7osZxAHVHgYXL5Ze1oMUv").unwrap();
// pub static ref SWITCHBOARD_ON_DEMAND_PROGRAM_ID: Pubkey =
// Pubkey::from_str(&std::env::var("SWITCHBOARD_ON_DEMAND_PROGRAM_ID").unwrap_or_default())
// .unwrap_or(*_DEFAULT_PID);
// }

pub fn get_sb_program_id(cluster: &str) -> Pubkey {
    if !cluster.starts_with("mainnet") {
        ON_DEMAND_DEVNET_PID
    } else {
        ON_DEMAND_MAINNET_PID
    }
}
