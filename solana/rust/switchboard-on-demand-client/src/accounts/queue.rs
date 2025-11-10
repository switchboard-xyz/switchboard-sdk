use crate::{CrossbarClient, Gateway};
use crate::LutOwner;
use crate::OracleAccountData;
use anyhow_ext::{anyhow, Context};
use anyhow_ext::Error as AnyhowError;
use bytemuck::{Pod, Zeroable};
use futures::future::join_all;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct QueueAccountData {
    /// The address of the authority which is permitted to add/remove allowed enclave measurements.
    pub authority: Pubkey,
    /// Allowed enclave measurements.
    pub mr_enclaves: [[u8; 32]; 32],
    /// The addresses of the quote oracles who have a valid
    /// verification status and have heartbeated on-chain recently.
    pub oracle_keys: [Pubkey; 78],
    reserved1: [u8; 40],
    pub secp_oracle_signing_keys: [[u8; 20]; 30],
    pub ed25519_oracle_signing_keys: [Pubkey; 30],
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
    pub last_reward_epoch: u64,
    _ebuf4: [u8; 32],
    _ebuf2: [u8; 256],
    _ebuf1: [u8; 504], // was 512 change to 504 to make room for new u64
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

impl QueueAccountData {
    pub fn size() -> usize {
        8 + std::mem::size_of::<QueueAccountData>()
    }

    /// Loads the oracles currently in the queue.
    pub fn oracle_keys(&self) -> Vec<Pubkey> {
        self.oracle_keys[..self.oracle_keys_len as usize].to_vec()
    }

    /// Loads the QueueAccountData from the given key.
    pub async fn load(client: &RpcClient, key: &Pubkey) -> Result<QueueAccountData, AnyhowError> {
        let account = client.get_account_data(key).await?;
        let buf = account[8..].to_vec();
        let parsed: &QueueAccountData = bytemuck::try_from_bytes(&buf)
            .map_err(|e| anyhow!("Failed to parse QueueAccountData: {:?}", e))?;
        Ok(*parsed)
    }

    /// Fetches all oracle accounts from the oracle keys and returns them as a list of (Pubkey, OracleAccountData).
    pub async fn fetch_oracle_accounts(
        &self,
        client: &RpcClient,
    ) -> Result<Vec<(Pubkey, OracleAccountData)>, AnyhowError> {
        let keys = self.oracle_keys();
        let accounts_data = client
            .get_multiple_accounts(&keys)
            .await?
            .into_iter()
            .map(|account| {
                let buf = account.unwrap_or_default().data[8..].to_vec();
                let oracle_account: &OracleAccountData = bytemuck::try_from_bytes(&buf).unwrap();
                *oracle_account
            })
            .collect::<Vec<_>>();
        let result = keys
            .into_iter()
            .zip(accounts_data.into_iter())
            .collect::<Vec<_>>();
        Ok(result)
    }

    /// Fetches all gateways from the oracle accounts and tests them to see if they are reachable.
    /// Returns a list of reachable gateways.
    /// # Arguments
    /// * `client` - The RPC client to use for fetching the oracle accounts.
    /// # Returns
    /// A list of reachable gateways.
    pub async fn fetch_gateways(&self, client: &RpcClient) -> Result<Vec<Gateway>, AnyhowError> {
        let gateways = self
            .fetch_oracle_accounts(client)
            .await?
            .into_iter()
            .map(|x| x.1)
            .filter_map(|x| x.gateway_uri())
            .map(Gateway::new)
            .collect::<Vec<_>>();
        let mut test_futures = Vec::new();
        for gateway in gateways.iter() {
            test_futures.push(gateway.test_gateway());
        }
        let results = join_all(test_futures).await;
        let mut good_gws = Vec::new();
        for (i, is_good) in results.into_iter().enumerate() {
            if is_good {
                good_gws.push(gateways[i].clone());
            }
        }
        Ok(good_gws)
    }

    /// Fetches a gateway from the crossbar service
    ///
    /// # Arguments
    /// * `crossbar` - The crossbar client to use for fetching gateways
    ///
    /// # Returns
    /// * `Result<Gateway, AnyhowError>` - A Gateway instance ready for use
    pub async fn fetch_gateway_from_crossbar(&self, crossbar: &CrossbarClient) -> Result<Gateway, AnyhowError> {
        // Default to mainnet, but this could be made configurable
        let network = "mainnet";

        // Fetch gateways for the network
        let gateways = crossbar.fetch_gateways(network).await
            .context("Failed to fetch gateways from crossbar")?;

        let gateway_url = gateways
            .first()
            .ok_or_else(|| anyhow!("No gateways available for network: {}", network))?;

        Ok(Gateway::new(gateway_url.clone()))
    }
}

impl LutOwner for QueueAccountData {
    fn lut_slot(&self) -> u64 {
        self.lut_slot
    }
}

/// Higher-level Queue struct that matches the JavaScript pattern
///
/// This struct represents a queue instance with a specific pubkey,
/// similar to the JavaScript Queue class which has a program and pubkey.
pub struct Queue {
    pub pubkey: Pubkey,
    pub client: RpcClient,
}

impl Queue {
    /// Default devnet queue key
    pub const DEFAULT_DEVNET_KEY: &'static str = "EYiAmGSdsQTuCw413V5BzaruWuCCSDgTPtBGvLkXHbe7";

    /// Default mainnet queue key
    pub const DEFAULT_MAINNET_KEY: &'static str = "A43DyUGA7s8eXPxqEjJY6EBu1KKbNgfxF8h17VAHn13w";

    /// Creates a new Queue instance
    ///
    /// # Arguments
    /// * `client` - RPC client for Solana connections
    /// * `pubkey` - The public key of the queue account
    pub fn new(client: RpcClient, pubkey: Pubkey) -> Self {
        Self { pubkey, client }
    }

    /// Creates a Queue instance with the default mainnet key
    ///
    /// # Arguments
    /// * `client` - RPC client for Solana connections
    pub fn default_mainnet(client: RpcClient) -> Result<Self, AnyhowError> {
        let pubkey = Pubkey::from_str(Self::DEFAULT_MAINNET_KEY)
            .map_err(|e| anyhow!("Failed to parse mainnet queue key: {}", e))?;
        Ok(Self::new(client, pubkey))
    }

    /// Creates a Queue instance with the default devnet key
    ///
    /// # Arguments
    /// * `client` - RPC client for Solana connections
    pub fn default_devnet(client: RpcClient) -> Result<Self, AnyhowError> {
        let pubkey = Pubkey::from_str(Self::DEFAULT_DEVNET_KEY)
            .map_err(|e| anyhow!("Failed to parse devnet queue key: {}", e))?;
        Ok(Self::new(client, pubkey))
    }

    /// Loads the queue data from on-chain
    ///
    /// # Returns
    /// * `Result<QueueAccountData, AnyhowError>` - The queue account data
    pub async fn load_data(&self) -> Result<QueueAccountData, AnyhowError> {
        QueueAccountData::load(&self.client, &self.pubkey).await
    }

    /// Fetches a gateway from the crossbar service, automatically detecting network
    ///
    /// This method matches the JavaScript implementation exactly:
    /// 1. Tries to load data from the default mainnet queue
    /// 2. If that fails, assumes devnet
    /// 3. Fetches available gateways for the detected network
    /// 4. Returns the first gateway
    ///
    /// # Arguments
    /// * `crossbar` - The crossbar client to use for fetching gateways
    ///
    /// # Returns
    /// * `Result<Gateway, AnyhowError>` - A Gateway instance ready for use
    ///
    /// # Example
    /// ```rust,no_run
    /// use switchboard_on_demand_client::{CrossbarClient, accounts::queue::Queue};
    /// use solana_client::nonblocking::rpc_client::RpcClient;
    /// use std::str::FromStr;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = RpcClient::new("https://api.mainnet-beta.solana.com".to_string());
    /// let queue = Queue::default_mainnet(client)?;
    /// let crossbar = CrossbarClient::default();
    /// let gateway = queue.fetch_gateway_from_crossbar(&crossbar).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn fetch_gateway_from_crossbar(&self, crossbar: &CrossbarClient) -> Result<Gateway, AnyhowError> {
        let mut network = "mainnet";

        // Try to load data from the default mainnet queue to detect network
        let mainnet_client = RpcClient::new(self.client.url());
        let mainnet_queue = Queue::default_mainnet(mainnet_client)?;

        if mainnet_queue.load_data().await.is_err() {
            network = "devnet";
        }

        // Fetch gateways for the detected network
        let gateways = crossbar.fetch_gateways(network).await
            .context("Failed to fetch gateways from crossbar")?;

        let gateway_url = gateways
            .first()
            .ok_or_else(|| anyhow!("No gateways available for network: {}", network))?;

        Ok(Gateway::new(gateway_url.clone()))
    }
}
