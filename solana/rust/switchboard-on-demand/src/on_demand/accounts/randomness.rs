use std::cell::Ref;

use solana_program::clock::Clock;

use crate::pubkey::Pubkey;
use crate::*;

#[derive(Clone, Copy, Debug, bytemuck::Pod, bytemuck::Zeroable)]
#[repr(C)]
pub struct RandomnessAccountData {
    pub authority: Pubkey,
    pub queue: Pubkey,

    pub seed_slothash: [u8; 32],
    pub seed_slot: u64,
    pub oracle: Pubkey,

    pub reveal_slot: u64,
    pub value: [u8; 32],

    _ebuf2: [u8; 96],
    _ebuf1: [u8; 128],
}
impl Discriminator for RandomnessAccountData {
    const DISCRIMINATOR: [u8; 8] = [10, 66, 229, 135, 220, 239, 217, 114];
}
impl Owner for RandomnessAccountData {
    fn owner() -> Pubkey {
        let pid = if cfg!(feature = "devnet") {
            get_sb_program_id("devnet")
        } else {
            get_sb_program_id("mainnet")
        };
        pid
    }
}

cfg_client! {
    impl_account_deserialize!(RandomnessAccountData);
}
impl RandomnessAccountData {
    pub const fn size() -> usize {
        std::mem::size_of::<Self>() + 8
    }

    pub fn get_value(&self, clock: &Clock) -> std::result::Result<[u8; 32], OnDemandError> {
        if clock.slot != self.reveal_slot {
            return Err(OnDemandError::SwitchboardRandomnessTooOld.into());
        }
        Ok(self.value)
    }

    pub fn is_revealable(&self, clock: &Clock) -> bool {
        self.seed_slot < clock.slot
    }

    pub fn parse<'info>(
        data: Ref<'info, &mut [u8]>,
    ) -> std::result::Result<Ref<'info, Self>, OnDemandError> {
        if data.len() < Self::discriminator().len() {
            return Err(OnDemandError::InvalidDiscriminator);
        }

        let mut disc_bytes = [0u8; 8];
        disc_bytes.copy_from_slice(&data[..8]);
        if disc_bytes != Self::discriminator() {
            return Err(OnDemandError::InvalidDiscriminator);
        }

        Ok(Ref::map(data, |data: &&mut [u8]| {
            bytemuck::from_bytes(&data[8..std::mem::size_of::<Self>() + 8])
        }))
    }

    cfg_client! {
        pub async fn fetch_async(
            client: &solana_client::nonblocking::rpc_client::RpcClient,
            pubkey: Pubkey,
        ) -> std::result::Result<Self, crate::OnDemandError> {
            crate::client::fetch_zerocopy_account_async(client, pubkey).await
        }
    }
}
