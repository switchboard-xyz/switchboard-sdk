use borsh::BorshSerialize;
use solana_program::pubkey::Pubkey;

use crate::anchor_traits::*;
use crate::get_sb_program_id;
use crate::prelude::*;

pub struct GuardianQuoteVerify {}

#[derive(Clone, Debug)]
pub struct GuardianQuoteVerifyParams {
    pub timestamp: i64,
    pub mr_enclave: [u8; 32],
    pub idx: u32,
    pub ed25519_key: Pubkey,
    pub secp256k1_key: [u8; 64],
    pub slot: u64,
    pub signature: [u8; 64],
    pub recovery_id: u8,
    pub advisories: Vec<u32>,
}

impl BorshSerialize for GuardianQuoteVerifyParams {
    fn serialize<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
        self.timestamp.serialize(writer)?;
        self.mr_enclave.serialize(writer)?;
        self.idx.serialize(writer)?;
        writer.write_all(self.ed25519_key.as_ref())?;
        self.secp256k1_key.serialize(writer)?;
        self.slot.serialize(writer)?;
        self.signature.serialize(writer)?;
        self.recovery_id.serialize(writer)?;
        self.advisories.serialize(writer)?;
        Ok(())
    }
}

impl InstructionData for GuardianQuoteVerifyParams {}

impl Discriminator for GuardianQuoteVerifyParams {
    const DISCRIMINATOR: [u8; 8] = GuardianQuoteVerify::DISCRIMINATOR;
}

impl Discriminator for GuardianQuoteVerify {
    const DISCRIMINATOR: [u8; 8] = [168, 36, 93, 156, 157, 150, 148, 45];
}

pub struct GuardianQuoteVerifyArgs {
    pub guardian: Pubkey,
    pub oracle: Pubkey,
    pub authority: Pubkey,
    pub guardian_queue: Pubkey,
    pub timestamp: i64,
    pub mr_enclave: [u8; 32],
    pub idx: u32,
    pub ed25519_key: Pubkey,
    pub secp256k1_key: [u8; 64],
    pub slot: u64,
    pub signature: [u8; 64],
    pub recovery_id: u8,
    pub advisories: Vec<u32>,
}
pub struct GuardianQuoteVerifyAccounts {
    pub guardian: Pubkey,
    pub oracle: Pubkey,
    pub authority: Pubkey,
    pub guardian_queue: Pubkey,
    pub state: Pubkey,
    pub recent_slothashes: Pubkey,
}
impl ToAccountMetas for GuardianQuoteVerifyAccounts {
    fn to_account_metas(&self, _: Option<bool>) -> Vec<AccountMeta> {
        vec![
            AccountMeta::new(self.guardian, false),
            AccountMeta::new(self.oracle, false),
            AccountMeta::new_readonly(self.authority, true),
            AccountMeta::new(self.guardian_queue, false),
            AccountMeta::new_readonly(self.state, false),
            AccountMeta::new_readonly(self.recent_slothashes, false),
        ]
    }
}

impl GuardianQuoteVerify {
    pub fn build_ix(args: GuardianQuoteVerifyArgs) -> Result<Instruction, OnDemandError> {
        let pid = if cfg!(feature = "devnet") {
            get_sb_program_id("devnet")
        } else {
            get_sb_program_id("mainnet")
        };
        Ok(crate::utils::build_ix(
            &pid,
            &GuardianQuoteVerifyAccounts {
                guardian: args.guardian,
                oracle: args.oracle,
                authority: args.authority,
                guardian_queue: args.guardian_queue,
                state: State::get_pda(),
                recent_slothashes: solana_program::sysvar::slot_hashes::ID,
            },
            &GuardianQuoteVerifyParams {
                timestamp: args.timestamp,
                mr_enclave: args.mr_enclave,
                idx: args.idx,
                ed25519_key: args.ed25519_key,
                secp256k1_key: args.secp256k1_key,
                slot: args.slot,
                signature: args.signature,
                recovery_id: args.recovery_id,
                advisories: args.advisories,
            },
        ))
    }
}
