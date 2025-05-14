use std::cell::Ref;

use bytemuck::{Pod, Zeroable};
use solana_program::account_info::AccountInfo;
use solana_program::pubkey::Pubkey;

use crate::anchor_traits::*;
#[allow(unused_imports)]
use crate::impl_account_deserialize;
#[allow(unused_imports)]
use crate::OracleAccountData;
use crate::{cfg_client, get_sb_program_id, OnDemandError};

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct QueueAccountData {
    /// The address of the authority which is permitted to add/remove allowed enclave measurements.
    pub authority: Pubkey,
    /// Allowed enclave measurements.
    pub mr_enclaves: [[u8; 32]; 32],
    /// The addresses of the quote oracles who have a valid
    /// verification status and have heartbeated on-chain recently.
    pub oracle_keys: [Pubkey; 128],
    /// The maximum allowable time until a EnclaveAccount needs to be re-verified on-chain.
    pub max_quote_verification_age: i64,
    /// The unix timestamp when the last quote oracle heartbeated on-chain.
    pub last_heartbeat: i64,
    pub node_timeout: i64,
    /// The minimum number of lamports a quote oracle needs to lock-up in order to heartbeat and verify other quotes.
    pub oracle_min_stake: u64,
    pub allow_authority_override_after: i64,

    /// The number of allowed enclave measurements.
    pub mr_enclaves_len: u32,
    /// The length of valid quote oracles for the given attestation queue.
    pub oracle_keys_len: u32,
    /// The reward paid to quote oracles for attesting on-chain.
    pub reward: u32,
    /// Incrementer used to track the current quote oracle permitted to run any available functions.
    pub curr_idx: u32,
    /// Incrementer used to garbage collect and remove stale quote oracles.
    pub gc_idx: u32,

    pub require_authority_heartbeat_permission: u8,
    pub require_authority_verify_permission: u8,
    pub require_usage_permissions: u8,
    pub signer_bump: u8,

    pub mint: Pubkey,
    pub lut_slot: u64,
    pub allow_subsidies: u8,

    _ebuf6: [u8; 15],
    pub ncn: Pubkey,
    _resrved: u64, // only necessary for multiple vaults at once, otherwise we can use the ncn
    // tickets
    pub vaults: [VaultInfo; 4],
    _ebuf4: [u8; 32],
    _ebuf2: [u8; 256],
    _ebuf1: [u8; 512],
}
unsafe impl Pod for QueueAccountData {}
unsafe impl Zeroable for QueueAccountData {}

#[repr(C)]
#[derive(PartialEq, Debug, Copy, Clone)]
pub struct VaultInfo {
    pub vault_key: Pubkey,
    pub last_reward_epoch: u64,
}
unsafe impl Pod for VaultInfo {}
unsafe impl Zeroable for VaultInfo {}

cfg_client! {
    impl_account_deserialize!(QueueAccountData);
}

impl Discriminator for QueueAccountData {
    const DISCRIMINATOR: [u8; 8] = [217, 194, 55, 127, 184, 83, 138, 1];
}

impl Owner for QueueAccountData {
    fn owner() -> Pubkey {
        let pid = if cfg!(feature = "devnet") {
            get_sb_program_id("devnet")
        } else {
            get_sb_program_id("mainnet")
        };
        pid
    }
}

impl QueueAccountData {
    pub fn size() -> usize {
        8 + std::mem::size_of::<QueueAccountData>()
    }

    /// Returns the deserialized Switchboard AttestationQueue account
    ///
    /// # Arguments
    ///
    /// * `attestation_queue_account_info` - A Solana AccountInfo referencing an existing Switchboard AttestationQueue
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use switchboard_solana::QueueAccountData;
    ///
    /// let attestation_queue = QueueAccountData::new(attestation_queue_account_info)?;
    /// ```
    pub fn new<'info>(
        attestation_queue_account_info: &'info AccountInfo<'info>,
    ) -> Result<Ref<'info, QueueAccountData>, OnDemandError> {
        let data = attestation_queue_account_info
            .try_borrow_data()
            .map_err(|_| OnDemandError::AccountBorrowError)?;
        if data.len() < QueueAccountData::discriminator().len() {
            return Err(OnDemandError::InvalidDiscriminator);
        }

        let mut disc_bytes = [0u8; 8];
        disc_bytes.copy_from_slice(&data[..8]);
        if disc_bytes != QueueAccountData::discriminator() {
            return Err(OnDemandError::InvalidDiscriminator);
        }

        Ok(Ref::map(data, |data| {
            bytemuck::from_bytes(&data[8..std::mem::size_of::<QueueAccountData>() + 8])
        }))
    }

    /// Returns the deserialized Switchboard AttestationQueue account
    ///
    /// # Arguments
    ///
    /// * `data` - A Solana AccountInfo's data buffer
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use switchboard_solana::QueueAccountData;
    ///
    /// let attestation_queue = QueueAccountData::new(attestation_queue_account_info.try_borrow_data()?)?;
    /// ```
    pub fn new_from_bytes(data: &[u8]) -> Result<&QueueAccountData, OnDemandError> {
        if data.len() < QueueAccountData::discriminator().len() {
            return Err(OnDemandError::InvalidDiscriminator);
        }

        let mut disc_bytes = [0u8; 8];
        disc_bytes.copy_from_slice(&data[..8]);
        if disc_bytes != QueueAccountData::discriminator() {
            return Err(OnDemandError::InvalidDiscriminator);
        }

        Ok(bytemuck::from_bytes(
            &data[8..std::mem::size_of::<QueueAccountData>() + 8],
        ))
    }

    pub fn has_mr_enclave(&self, mr_enclave: &[u8]) -> bool {
        self.mr_enclaves[..self.mr_enclaves_len as usize]
            .iter()
            .any(|x| x.to_vec() == mr_enclave.to_vec())
    }

    pub fn permitted_enclaves(&self) -> Vec<[u8; 32]> {
        self.mr_enclaves[..self.mr_enclaves_len as usize].to_vec()
    }

    pub fn garbage_collection_node(&self) -> Option<Pubkey> {
        let gc_node = self.oracle_keys[self.gc_idx as usize];
        if gc_node != Pubkey::default() {
            Some(gc_node)
        } else {
            None
        }
    }

    pub fn idx_of_oracle(&self, oracle: &Pubkey) -> Option<usize> {
        self.oracle_keys[..self.oracle_keys_len as usize]
            .iter()
            .position(|x| x == oracle)
    }

    pub fn oracle_keys(&self) -> Vec<Pubkey> {
        self.oracle_keys[..self.oracle_keys_len as usize].to_vec()
    }

    cfg_client! {
        pub async fn fetch_async(
            client: &solana_client::nonblocking::rpc_client::RpcClient,
            pubkey: Pubkey,
        ) -> std::result::Result<Self, crate::OnDemandError> {
            crate::client::fetch_zerocopy_account_async(client, pubkey).await
        }

        pub async fn fetch_oracles(
            &self,
            client: &solana_client::nonblocking::rpc_client::RpcClient,
        ) -> std::result::Result<Vec<(Pubkey, OracleAccountData)>, crate::OnDemandError> {
            let oracles = &self.oracle_keys[..self.oracle_keys_len as usize];
            let datas: Vec<_> = client
                .get_multiple_accounts(&oracles)
                .await
                .map_err(|_e| crate::OnDemandError::NetworkError)?
                .into_iter()
                .filter_map(|x| x)
                .map(|x| x.data.clone())
                .collect::<Vec<_>>()
                .iter()
                .map(|x| OracleAccountData::new_from_bytes(x))
                .filter_map(|x| x.ok())
                .map(|x| x.clone())
                .collect();
            Ok(oracles.iter().cloned().zip(datas).collect())
        }
    }
}
