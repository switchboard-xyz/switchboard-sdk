use borsh::BorshSerialize;
use solana_program::pubkey::Pubkey;

use crate::anchor_traits::*;
use crate::get_sb_program_id;
use crate::prelude::*;

pub struct QueueGarbageCollect {}

#[derive(Clone, BorshSerialize, Debug)]
pub struct QueueGarbageCollectParams {
    pub idx: u32,
}

impl InstructionData for QueueGarbageCollectParams {}
impl Discriminator for QueueGarbageCollect {
    const DISCRIMINATOR: [u8; 8] = [187, 208, 104, 247, 16, 91, 96, 98];
}
impl Discriminator for QueueGarbageCollectParams {
    const DISCRIMINATOR: [u8; 8] = QueueGarbageCollect::DISCRIMINATOR;
}

pub struct QueueGarbageCollectArgs {
    pub queue: Pubkey,
    pub oracle: Pubkey,
    pub idx: u32,
}
pub struct QueueGarbageCollectAccounts {
    pub queue: Pubkey,
    pub oracle: Pubkey,
}
impl ToAccountMetas for QueueGarbageCollectAccounts {
    fn to_account_metas(&self, _: Option<bool>) -> Vec<AccountMeta> {
        vec![
            AccountMeta::new(self.queue, false),
            AccountMeta::new(self.oracle, false),
        ]
    }
}

impl QueueGarbageCollect {
    pub fn build_ix(args: QueueGarbageCollectArgs) -> Result<Instruction, OnDemandError> {
        let pid = if cfg!(feature = "devnet") {
            get_sb_program_id("devnet")
        } else {
            get_sb_program_id("mainnet")
        };
        Ok(crate::utils::build_ix(
            &pid,
            &QueueGarbageCollectAccounts {
                queue: args.queue,
                oracle: args.oracle,
            },
            &QueueGarbageCollectParams { idx: args.idx },
        ))
    }
}
