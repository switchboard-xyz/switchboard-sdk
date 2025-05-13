use std::cell::Ref;

use solana_program::account_info::AccountInfo;
use solana_program::pubkey::Pubkey;
use solana_program::sysvar::clock::Clock;

use crate::anchor_traits::*;
#[allow(unused_imports)]
use crate::impl_account_deserialize;
use crate::{get_sb_program_id, OnDemandError, Quote};

pub const ORACLE_FEED_STATS_SEED: &[u8; 15] = b"OracleFeedStats";

#[repr(u8)]
#[derive(Copy, Clone, Default, Debug, Eq, PartialEq)]
pub enum VerificationStatus {
    #[default]
    None = 0,
    VerificationPending = 1 << 0,
    VerificationFailure = 1 << 1,
    VerificationSuccess = 1 << 2,
    VerificationOverride = 1 << 3,
}
impl From<VerificationStatus> for u8 {
    fn from(value: VerificationStatus) -> Self {
        match value {
            VerificationStatus::VerificationPending => 1 << 0,
            VerificationStatus::VerificationFailure => 1 << 1,
            VerificationStatus::VerificationSuccess => 1 << 2,
            VerificationStatus::VerificationOverride => 1 << 3,
            _ => 0,
        }
    }
}
impl From<u8> for VerificationStatus {
    fn from(value: u8) -> Self {
        match value {
            1 => VerificationStatus::VerificationPending,
            2 => VerificationStatus::VerificationFailure,
            4 => VerificationStatus::VerificationSuccess,
            8 => VerificationStatus::VerificationOverride,
            _ => VerificationStatus::default(),
        }
    }
}

#[repr(C)]
#[derive(bytemuck::Zeroable, bytemuck::Pod, Debug, Copy, Clone)]
pub struct OracleAccountData {
    /// Represents the state of the quote verifiers enclave.
    pub enclave: Quote,

    // Accounts Config
    /// The authority of the EnclaveAccount which is permitted to make account changes.
    pub authority: Pubkey,
    /// Queue used for attestation to verify a MRENCLAVE measurement.
    pub queue: Pubkey,

    // Metadata Config
    /// The unix timestamp when the quote was created.
    pub created_at: i64,

    /// The last time the quote heartbeated on-chain.
    pub last_heartbeat: i64,

    pub secp_authority: [u8; 64],

    /// URI location of the verifier's gateway.
    pub gateway_uri: [u8; 64],
    pub permissions: u64,
    /// Whether the quote is located on the AttestationQueues buffer.
    pub is_on_queue: u8,
    _padding1: [u8; 7],
    pub lut_slot: u64,
    pub last_reward_epoch: u64,

    pub operator: Pubkey,
    _ebuf3: [u8; 16],
    _ebuf2: [u8; 64],
    _ebuf1: [u8; 1024],
}


impl Discriminator for OracleAccountData {
    const DISCRIMINATOR: [u8; 8] = [128, 30, 16, 241, 170, 73, 55, 54];
}

impl Owner for OracleAccountData {
    fn owner() -> Pubkey {
        let pid = if cfg!(feature = "devnet") {
            get_sb_program_id("devnet")
        } else {
            get_sb_program_id("mainnet")
        };
        pid
    }
}

impl OracleAccountData {
    pub fn size() -> usize {
        8 + std::mem::size_of::<OracleAccountData>()
    }

    /// Returns the deserialized Switchboard Quote account
    ///
    /// # Arguments
    ///
    /// * `quote_account_info` - A Solana AccountInfo referencing an existing Switchboard QuoteAccount
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use switchboard_on_demand::OracleAccountData;
    ///
    /// let quote_account = OracleAccountData::new(quote_account_info)?;
    /// ```
    pub fn new<'info>(
        quote_account_info: &'info AccountInfo<'info>,
    ) -> Result<Ref<'info, OracleAccountData>, OnDemandError> {
        let data = quote_account_info
            .try_borrow_data()
            .map_err(|_| OnDemandError::AccountBorrowError)?;
        if data.len() < OracleAccountData::discriminator().len() {
            return Err(OnDemandError::InvalidDiscriminator);
        }

        let mut disc_bytes = [0u8; 8];
        disc_bytes.copy_from_slice(&data[..8]);
        if disc_bytes != OracleAccountData::discriminator() {
            return Err(OnDemandError::InvalidDiscriminator);
        }

        Ok(Ref::map(data, |data| {
            bytemuck::from_bytes(&data[8..std::mem::size_of::<OracleAccountData>() + 8])
        }))
    }

    /// Returns the deserialized Switchboard Quote account
    ///
    /// # Arguments
    ///
    /// * `data` - A Solana AccountInfo's data buffer
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use switchboard_on_demand::OracleAccountData;
    ///
    /// let quote_account = OracleAccountData::new(quote_account_info.try_borrow_data()?)?;
    /// ```
    pub fn new_from_bytes(data: &[u8]) -> Result<&OracleAccountData, OnDemandError> {
        if data.len() < OracleAccountData::discriminator().len() {
            return Err(OnDemandError::InvalidDiscriminator);
        }

        let mut disc_bytes = [0u8; 8];
        disc_bytes.copy_from_slice(&data[..8]);
        if disc_bytes != OracleAccountData::discriminator() {
            return Err(OnDemandError::InvalidDiscriminator);
        }

        Ok(bytemuck::from_bytes(
            &data[8..std::mem::size_of::<OracleAccountData>() + 8],
        ))
    }

    pub fn signer(&self) -> Pubkey {
        self.enclave.enclave_signer
    }

    pub fn is_verified(&self, clock: &Clock) -> bool {
        match self.enclave.verification_status.into() {
            VerificationStatus::VerificationOverride => true,
            VerificationStatus::VerificationSuccess => {
                self.enclave.valid_until > clock.unix_timestamp
            }
            _ => false,
        }
    }

    pub fn verify(&self, clock: &Clock) -> std::result::Result<(), OnDemandError> {
        if !self.is_verified(clock) {
            return Err(OnDemandError::InvalidQuote);
        }

        Ok(())
    }

    pub fn gateway_uri(&self) -> Option<String> {
        let uri = self.gateway_uri;
        let uri = String::from_utf8_lossy(&uri);
        let uri = uri
            .split_at(uri.find('\0').unwrap_or(uri.len()))
            .0
            .to_string();
        if uri.is_empty() {
            return None;
        }
        Some(uri)
    }

    pub fn ed25519_signer(&self) -> Option<Pubkey> {
        let key = self.enclave.enclave_signer;
        if key == Pubkey::default() {
            return None;
        }
        Some(key)
    }

    pub fn secp_authority(&self) -> Option<[u8; 64]> {
        let key = self.secp_authority;
        if key == [0u8; 64] {
            return None;
        }
        Some(key)
    }

    pub fn secp256k1_signer(&self) -> Option<[u8; 64]> {
        let key = self.enclave.secp256k1_signer;
        if key == [0u8; 64] {
            return None;
        }
        Some(key)
    }

    pub fn libsecp256k1_signer(&self) -> Option<libsecp256k1::PublicKey> {
        let bytes = self.secp256k1_signer()?;
        let tag_full_pubkey: Vec<u8> = vec![4u8];
        let bytes = [tag_full_pubkey, bytes.into()].concat().try_into().ok()?;
        libsecp256k1::PublicKey::parse(&bytes).ok()
    }

    pub fn stats_key(key: &Pubkey) -> Pubkey {
        let pid = OracleAccountData::owner();
        let oracle_stats_seed = b"OracleStats";
        let (key, _) =
            Pubkey::find_program_address(&[&oracle_stats_seed.as_slice(), &key.to_bytes()], &pid);
        key
    }

    pub fn feed_stats_key(feed: &Pubkey, oracle: &Pubkey) -> (Pubkey, u8) {
        let pid = OracleAccountData::owner();
        Pubkey::find_program_address(
            &Self::feed_stats_seed(&feed.to_bytes(), &oracle.to_bytes(), &[]),
            &pid,
        )
    }

    pub fn feed_stats_seed<'a>(feed: &'a [u8], oracle: &'a [u8], bump: &'a [u8]) -> [&'a [u8]; 4] {
        [&ORACLE_FEED_STATS_SEED.as_slice(), feed, oracle, bump]
    }

}
