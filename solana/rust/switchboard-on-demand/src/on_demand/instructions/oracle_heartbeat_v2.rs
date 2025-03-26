use borsh::BorshSerialize;
use solana_program::pubkey::Pubkey;

use crate::anchor_traits::*;
use crate::cfg_client;
use crate::prelude::*;

pub struct OracleHeartbeatV2;

#[derive(Clone, BorshSerialize, Debug)]
pub struct OracleHeartbeatV2Params {
    pub uri: Option<[u8; 64]>,
}

impl InstructionData for OracleHeartbeatV2Params {}

impl Discriminator for OracleHeartbeatV2 {
    const DISCRIMINATOR: [u8; 8] = [122, 231, 66, 32, 226, 62, 144, 103];
}
impl Discriminator for OracleHeartbeatV2Params {
    const DISCRIMINATOR: [u8; 8] = OracleHeartbeatV2::DISCRIMINATOR;
}

pub struct OracleHeartbeatV2Args {
    pub oracle: Pubkey,
    pub oracle_signer: Pubkey,
    pub gc_node: Pubkey,
    pub uri: Option<[u8; 64]>,
}
pub struct OracleHeartbeatV2Accounts {
    pub oracle: Pubkey,
    pub oracle_signer: Pubkey,
    pub queue: Pubkey,
    pub gc_node: Pubkey,
}
impl ToAccountMetas for OracleHeartbeatV2Accounts {
    fn to_account_metas(&self, _: Option<bool>) -> Vec<AccountMeta> {
        let state_pubkey = State::get_pda();
        let accts = vec![
            AccountMeta::new(self.oracle, false),
            AccountMeta::new(OracleAccountData::stats_key(&self.oracle), false),
            AccountMeta::new_readonly(self.oracle_signer, true),
            AccountMeta::new(self.queue, false),
            AccountMeta::new(self.gc_node, false),
            AccountMeta::new(state_pubkey, false),
        ];
        accts
    }
}

cfg_client! {
use solana_client::nonblocking::rpc_client::RpcClient;
use crate::get_sb_program_id;

impl OracleHeartbeatV2 {
    pub async fn build_ix(client: &RpcClient, args: OracleHeartbeatV2Args) -> Result<Instruction, OnDemandError> {
        let oracle_data = OracleAccountData::fetch_async(client, args.oracle).await?;
        let pid = if cfg!(feature = "devnet") {
            get_sb_program_id("devnet")
        } else {
            get_sb_program_id("mainnet")
        };
        let ix = crate::utils::build_ix(
            &pid,
            &OracleHeartbeatV2Accounts {
                oracle: args.oracle,
                oracle_signer: args.oracle_signer,
                queue: oracle_data.queue,
                gc_node: args.gc_node,
            },
            &OracleHeartbeatV2Params { uri: args.uri },
        );
        Ok(ix)
    }
}
}
