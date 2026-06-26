use crate::*;
use anyhow_ext::anyhow;
use anyhow_ext::Context;
use bytemuck;
use rust_decimal::Decimal;
use solana_sdk::pubkey::Pubkey;

pub const PRECISION: u32 = 18;
pub const MAX_SAMPLES: usize = 32;

#[repr(C)]
#[derive(Debug, Clone, Copy, bytemuck::Pod, bytemuck::Zeroable)]
pub struct CurrentResult {
    /// The median value of the submissions needed for quorom size
    pub value: i128,
    /// The standard deviation of the submissions needed for quorom size
    pub std_dev: i128,
    /// The mean of the submissions needed for quorom size
    pub mean: i128,
    /// The range of the submissions needed for quorom size
    pub range: i128,
    /// The minimum value of the submissions needed for quorom size
    pub min_value: i128,
    /// The maximum value of the submissions needed for quorom size
    pub max_value: i128,
    /// The number of samples used to calculate this result
    pub num_samples: u8,
    pub padding1: [u8; 7],
    /// The slot at which this value was signed.
    pub slot: u64,
    /// The slot at which the first considered submission was made
    pub min_slot: u64,
    /// The slot at which the last considered submission was made
    pub max_slot: u64,
}
impl CurrentResult {
    /// The median value of the submissions needed for quorom size
    pub fn value(&self) -> Decimal {
        Decimal::from_i128_with_scale(self.value, PRECISION)
    }

    /// The standard deviation of the submissions needed for quorom size
    pub fn std_dev(&self) -> Decimal {
        Decimal::from_i128_with_scale(self.std_dev, PRECISION)
    }

    /// The mean of the submissions needed for quorom size
    pub fn mean(&self) -> Decimal {
        Decimal::from_i128_with_scale(self.mean, PRECISION)
    }

    /// The range of the submissions needed for quorom size
    pub fn range(&self) -> Decimal {
        Decimal::from_i128_with_scale(self.range, PRECISION)
    }

    /// The minimum value of the submissions needed for quorom size
    pub fn min_value(&self) -> Decimal {
        Decimal::from_i128_with_scale(self.min_value, PRECISION)
    }

    /// The maximum value of the submissions needed for quorom size
    pub fn max_value(&self) -> Decimal {
        Decimal::from_i128_with_scale(self.max_value, PRECISION)
    }

    pub fn result_slot(&self) -> u64 {
        self.slot
    }

    pub fn min_slot(&self) -> u64 {
        self.min_slot
    }

    pub fn max_slot(&self) -> u64 {
        self.max_slot
    }
}

#[repr(C)]
#[derive(Debug, Clone, Copy, bytemuck::Pod, bytemuck::Zeroable)]
pub struct OracleSubmission {
    /// The public key of the oracle that submitted this value.
    pub oracle: Pubkey,
    /// The slot at which this value was signed.
    pub slot: u64,
    /// The slot at which this value was landed on chain.
    pub landed_at: u64,
    /// The value that was submitted.
    pub value: i128,
}

/// A representation of the data in a pull feed account.
#[repr(C)]
#[derive(Debug, Clone, Copy, bytemuck::Pod, bytemuck::Zeroable)]
pub struct PullFeedAccountData {
    /// The oracle submissions for this feed.
    pub submissions: [OracleSubmission; 32],
    /// The public key of the authority that can update the feed hash that
    /// this account will use for registering updates.
    pub authority: Pubkey,
    /// The public key of the queue which oracles must be bound to in order to
    /// submit data to this feed.
    pub queue: Pubkey,
    /// SHA-256 hash of the job schema oracles will execute to produce data
    /// for this feed.
    pub feed_hash: [u8; 32],
    /// The slot at which this account was initialized.
    pub initialized_at: i64,
    pub permissions: u64,
    pub max_variance: u64,
    pub min_responses: u32,
    pub name: [u8; 32],
    _padding1: [u8; 3],
    pub min_sample_size: u8,
    pub last_update_timestamp: i64,
    pub lut_slot: u64,
    pub ipfs_hash: [u8; 32], // deprecated
    pub result: CurrentResult,
    pub max_staleness: u32,
    _ebuf4: [u8; 20],
    _ebuf3: [u8; 24],
    _ebuf2: [u8; 256],
    _ebuf1: [u8; 512],
}

impl OracleSubmission {
    pub fn is_empty(&self) -> bool {
        self.slot == 0
    }

    pub fn value(&self) -> Decimal {
        Decimal::from_i128_with_scale(self.value, PRECISION)
    }
}

impl PullFeedAccountData {
    /// Anchor account discriminator (`sha256("account:PullFeedAccountData")[..8]`).
    const DISCRIMINATOR: [u8; 8] = [196, 27, 108, 196, 10, 215, 219, 40];

    /// Parses pull feed account data from raw bytes into an owned value, with no alignment requirement.
    pub fn parse_unaligned(data: &[u8]) -> Result<Self, AnyhowError> {
        if data.len() < Self::DISCRIMINATOR.len() {
            return Err(anyhow!(
                "PullFeedAccountData::parse_unaligned: missing discriminator"
            ));
        }

        let mut disc_bytes = [0u8; 8];
        disc_bytes.copy_from_slice(&data[..8]);
        if disc_bytes != Self::DISCRIMINATOR {
            return Err(anyhow!(
                "PullFeedAccountData::parse_unaligned: invalid discriminator"
            ));
        }

        let expected_size = std::mem::size_of::<Self>() + 8;
        if data.len() < expected_size {
            return Err(anyhow!(
                "PullFeedAccountData::parse_unaligned: data too small"
            ));
        }

        bytemuck::try_pod_read_unaligned::<Self>(&data[8..expected_size])
            .map_err(|_| anyhow!("PullFeedAccountData::parse_unaligned: failed to parse data"))
    }

    /// The median value of the submissions needed for quorom size
    pub fn value(&self) -> Decimal {
        self.result.value()
    }

    /// The range of the submissions needed for quorom size
    pub fn range(&self) -> Decimal {
        self.result.range()
    }

    /// The minimum value of the submissions needed for quorom size
    pub fn min_value(&self) -> Decimal {
        self.result.min_value()
    }

    /// The maximum value of the submissions needed for quorom size
    pub fn max_value(&self) -> Decimal {
        self.result.max_value()
    }

    pub fn result_slot(&self) -> u64 {
        self.result.slot
    }

    pub fn feed_hash(&self) -> String {
        hex::encode(self.feed_hash)
    }

    pub async fn fetch_jobs(
        &self,
        crossbar: &CrossbarClient,
    ) -> Result<Vec<OracleJob>, AnyhowError> {
        let jobs_data = crossbar
            .fetch(&self.feed_hash())
            .await
            .context("PullFeed.fetchUpdateIx: Failed to fetch jobs")?;

        let jobs: Vec<OracleJob> = serde_json::from_value(jobs_data.get("jobs").unwrap().clone())
            .context("PullFeed.fetchUpdateIx: Failed to deserialize jobs")?;

        Ok(jobs)
    }
}

impl LutOwner for PullFeedAccountData {
    fn lut_slot(&self) -> u64 {
        self.lut_slot
    }
}
