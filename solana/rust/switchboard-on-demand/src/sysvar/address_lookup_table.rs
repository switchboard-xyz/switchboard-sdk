use crate::Pubkey;

use crate::{cfg_client, utils, ON_DEMAND_DEVNET_PID, ON_DEMAND_MAINNET_PID};

const LUT_SIGNER_SEED: &[u8] = b"LutSigner";

/// Finds the address lookup table signer PDA for a given key
pub fn find_lut_signer<K: AsRef<[u8]>, P: From<[u8; 32]>>(k: &K) -> P {
    let pid = if utils::is_devnet() {
        ON_DEMAND_DEVNET_PID
    } else {
        ON_DEMAND_MAINNET_PID
    };
    find_lut_signer_for_program(k, &pid)
}

/// Finds the address lookup table signer PDA for a given key and program ID.
///
/// Use this when one process serves more than one Solana cluster and cannot
/// rely on the process-wide `SB_ENV` setting.
pub fn find_lut_signer_for_program<K: AsRef<[u8]>, P: From<[u8; 32]>>(
    k: &K,
    program_id: &Pubkey,
) -> P {
    let (pk, _) = Pubkey::find_program_address(&[LUT_SIGNER_SEED, k.as_ref()], program_id);
    P::from(pk.to_bytes())
}

cfg_client! {
    use crate::OnDemandError;
    use crate::solana_compat::solana_client::nonblocking::rpc_client::RpcClient;
    use crate::solana_compat::address_lookup_table::state::AddressLookupTable;
    use crate::solana_compat::AddressLookupTableAccount;

    pub async fn fetch(client: &RpcClient, address: &Pubkey) -> Result<AddressLookupTableAccount, OnDemandError> {
        let converted_address: crate::solana_compat::solana_sdk::pubkey::Pubkey = address.to_bytes().into();
        let account = client.get_account_data(&converted_address)
            .await
            .map_err(|_| OnDemandError::AddressLookupTableFetchError)?;
        let lut = AddressLookupTable::deserialize(&account)
            .map_err(|_| OnDemandError::AddressLookupTableDeserializeError)?;
        let out = AddressLookupTableAccount {
            key: address.to_bytes().into(),
            addresses: lut.addresses.iter().map(|a| a.to_bytes().into()).collect(),
        };
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_program_ids_derive_distinct_lut_signers() {
        let key = Pubkey::new_from_array([7u8; 32]);
        let mainnet_signer: Pubkey = find_lut_signer_for_program(&key, &ON_DEMAND_MAINNET_PID);
        let devnet_signer: Pubkey = find_lut_signer_for_program(&key, &ON_DEMAND_DEVNET_PID);

        assert_ne!(mainnet_signer, devnet_signer);
        assert_eq!(
            mainnet_signer,
            Pubkey::find_program_address(&[LUT_SIGNER_SEED, key.as_ref()], &ON_DEMAND_MAINNET_PID,)
                .0
        );
        assert_eq!(
            devnet_signer,
            Pubkey::find_program_address(&[LUT_SIGNER_SEED, key.as_ref()], &ON_DEMAND_DEVNET_PID,)
                .0
        );
    }
}
