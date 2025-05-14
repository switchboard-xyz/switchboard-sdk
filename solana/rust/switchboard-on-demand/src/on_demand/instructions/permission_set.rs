use borsh::BorshSerialize;
use solana_program::pubkey::Pubkey;

use crate::anchor_traits::*;
use crate::get_sb_program_id;
use crate::prelude::*;

#[repr(u32)]
#[derive(Copy, Clone)]
pub enum SwitchboardPermission {
    None = 0 << 0,
    PermitOracleHeartbeat = 1 << 0,
    PermitOracleQueueUsage = 1 << 1,
}

pub struct AttestationPermissionSet {}

#[derive(Clone, BorshSerialize, Debug)]
pub struct AttestationPermissionSetParams {
    pub permission: u8,
    pub enable: bool,
}

impl InstructionData for AttestationPermissionSetParams {}

impl Discriminator for AttestationPermissionSetParams {
    const DISCRIMINATOR: [u8; 8] = AttestationPermissionSet::DISCRIMINATOR;
}

impl Discriminator for AttestationPermissionSet {
    const DISCRIMINATOR: [u8; 8] = [211, 122, 185, 120, 129, 182, 55, 103];
}

pub struct AttestationPermissionSetAccounts {
    pub authority: Pubkey,
    pub granter: Pubkey,
    pub grantee: Pubkey,
}
impl ToAccountMetas for AttestationPermissionSetAccounts {
    fn to_account_metas(&self, _: Option<bool>) -> Vec<AccountMeta> {
        vec![
            AccountMeta::new_readonly(self.authority, true),
            AccountMeta::new_readonly(self.granter, false),
            AccountMeta::new(self.grantee, false),
        ]
    }
}

impl AttestationPermissionSet {
    pub fn build_ix(
        granter: Pubkey,
        authority: Pubkey,
        grantee: Pubkey,
        permission: SwitchboardPermission,
        enable: bool,
    ) -> Result<Instruction, OnDemandError> {
        let pid = if cfg!(feature = "devnet") {
            get_sb_program_id("devnet")
        } else {
            get_sb_program_id("mainnet")
        };
        Ok(crate::utils::build_ix(
            &pid,
            &AttestationPermissionSetAccounts {
                authority,
                granter,
                grantee,
            },
            &AttestationPermissionSetParams {
                permission: permission as u8,
                enable,
            },
        ))
    }
}
