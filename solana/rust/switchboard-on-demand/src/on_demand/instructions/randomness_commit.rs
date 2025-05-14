use borsh::BorshSerialize;
use solana_program::account_info::AccountInfo;
use solana_program::program_error::ProgramError;
use solana_program::pubkey::Pubkey;
use solana_program::sysvar::slot_hashes;

use crate::anchor_traits::*;
use crate::get_sb_program_id;
use crate::prelude::*;

pub struct RandomnessCommit {}

#[derive(Clone, BorshSerialize, Debug)]
pub struct RandomnessCommitParams {}

impl InstructionData for RandomnessCommitParams {}

impl Discriminator for RandomnessCommitParams {
    const DISCRIMINATOR: [u8; 8] = RandomnessCommit::DISCRIMINATOR;
}

impl Discriminator for RandomnessCommit {
    const DISCRIMINATOR: [u8; 8] = [52, 170, 152, 201, 179, 133, 242, 141];
}

pub struct RandomnessCommitAccounts {
    pub randomness: Pubkey,
    pub queue: Pubkey,
    pub oracle: Pubkey,
    pub recent_slothashes: Pubkey,
    pub authority: Pubkey,
}
impl ToAccountMetas for RandomnessCommitAccounts {
    fn to_account_metas(&self, _: Option<bool>) -> Vec<AccountMeta> {
        vec![
            AccountMeta::new(self.randomness, false),
            AccountMeta::new_readonly(self.queue, false),
            AccountMeta::new(self.oracle, false),
            AccountMeta::new_readonly(slot_hashes::ID, false),
            AccountMeta::new_readonly(self.authority, true),
        ]
    }
}

impl RandomnessCommit {
    pub fn build_ix(
        randomness: Pubkey,
        queue: Pubkey,
        oracle: Pubkey,
        authority: Pubkey,
    ) -> Result<Instruction, OnDemandError> {
        let pid = if cfg!(feature = "devnet") {
            get_sb_program_id("devnet")
        } else {
            get_sb_program_id("mainnet")
        };
        Ok(crate::utils::build_ix(
            &pid,
            &RandomnessCommitAccounts {
                randomness,
                queue,
                oracle,
                authority,
                recent_slothashes: slot_hashes::ID,
            },
            &RandomnessCommitParams {},
        ))
    }

    /// Invokes the `randomness_commit` Switchboard CPI call.
    ///
    /// This call commits a new randomness value to the randomness account.
    ///
    /// # Requirements
    ///
    /// - The `authority` must be a signer.
    ///
    /// # Parameters
    ///
    /// - **switchboard**: Switchboard program account.
    /// - **randomness**: Randomness account.
    /// - **queue**: Queue account associated with the randomness account.
    /// - **oracle**: Oracle account assigned for the randomness request.
    /// - **authority**: Authority of the randomness account.
    /// - **recent_slothashes**: Sysvar account to fetch recent slot hashes.
    /// - **seeds**: Seeds for the CPI call.
    ///
    pub fn invoke<'a>(
        switchboard: AccountInfo<'a>,
        randomness: AccountInfo<'a>,
        queue: AccountInfo<'a>,
        oracle: AccountInfo<'a>,
        authority: AccountInfo<'a>,
        recent_slothashes: AccountInfo<'a>,
        seeds: &[&[&[u8]]],
    ) -> Result<(), ProgramError> {
        let accounts = [
            randomness.clone(),
            queue.clone(),
            oracle.clone(),
            recent_slothashes.clone(),
            authority.clone(),
        ];
        let account_metas = RandomnessCommitAccounts {
            randomness: randomness.key.clone(),
            queue: queue.key.clone(),
            oracle: oracle.key.clone(),
            recent_slothashes: recent_slothashes.key.clone(),
            authority: authority.key.clone(),
        }
        .to_account_metas(None);
        let ix = Instruction {
            program_id: switchboard.key.clone(),
            accounts: account_metas,
            data: ix_discriminator("randomness_commit").to_vec(),
        };
        Ok(invoke_signed(&ix, &accounts, seeds)?)
    }
}

fn ix_discriminator(name: &str) -> [u8; 8] {
    let preimage = format!("global:{}", name);
    let mut sighash = [0u8; 8];
    sighash.copy_from_slice(&solana_program::hash::hash(preimage.as_bytes()).to_bytes()[..8]);
    sighash
}
