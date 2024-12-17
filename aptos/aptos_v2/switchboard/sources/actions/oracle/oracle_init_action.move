module switchboard::oracle_init_action {
    use aptos_framework::account::{Self, SignerCapability};
    use switchboard::errors;
    use switchboard::oracle;
    use switchboard::oracle_queue;
    use std::signer;
    use std::bcs;

    struct OracleInitParams has copy, drop {
        name: vector<u8>,
        metadata: vector<u8>,
        oracle_authority: address,
        queue_addr: address
    }

    public fun validate<CoinType>(account: &signer, params: &OracleInitParams) {
        assert!(oracle_queue::exist<CoinType>(params.queue_addr), errors::QueueNotFound());
        assert!(!oracle::exist(signer::address_of(account)), errors::OracleAlreadyExists());
    }

    fun actuate(account: &signer, params: &OracleInitParams, signer_cap: SignerCapability) {
        // Return queue + add oracle
        oracle::oracle_create(
            account, 
            signer_cap,
            params.name, 
            params.metadata, 
            params.oracle_authority, 
            params.queue_addr
        );
    }

    public entry fun run<CoinType>(
        account: &signer, 
        name: vector<u8>,
        metadata: vector<u8>,
        oracle_authority: address,
        queue_addr: address,
        seed: address,
    ) {
        let params = OracleInitParams {
            name, metadata,
            oracle_authority,
            queue_addr,
        };

        // validate that the user has permissions for this queue
        let (oracle_account, signer_cap) = account::create_resource_account(account, bcs::to_bytes(&seed));

        validate<CoinType>(&oracle_account, &params);
        actuate(&oracle_account, &params, signer_cap);
    }

}
