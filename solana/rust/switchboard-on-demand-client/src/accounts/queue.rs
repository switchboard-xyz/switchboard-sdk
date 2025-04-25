use crate::Gateway;
use crate::LutOwner;
use crate::OracleAccountData;
use anyhow_ext::anyhow;
use anyhow_ext::Error as AnyhowError;
use bytemuck::{Pod, Zeroable};
use futures::future::join_all;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;

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
        let account = client.get_account_data(&key).await?;
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
}

impl LutOwner for QueueAccountData {
    fn lut_slot(&self) -> u64 {
        self.lut_slot
    }
}
