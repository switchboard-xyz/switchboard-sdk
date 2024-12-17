module switchboard::oracle_set_configs_action {
    use switchboard::errors;
    use switchboard::oracle;
    use switchboard::oracle_queue;

    struct OracleSetConfigsParams has copy, drop {
        oracle_addr: address,
        name: vector<u8>,
        metadata: vector<u8>,
        oracle_authority: address,
        queue_addr: address
    }

    public fun validate<CoinType>(account: &signer, params: &OracleSetConfigsParams) {
        assert!(oracle_queue::exist<CoinType>(params.queue_addr), errors::QueueNotFound());
        assert!(oracle::exist(params.oracle_addr), errors::OracleAlreadyExists());
        assert!(oracle::has_authority(params.oracle_addr, account), errors::InvalidAuthority())
    }

    fun actuate(_account: &signer, params: &OracleSetConfigsParams) {
        oracle::set_configs(
            params.oracle_addr,
            params.name, 
            params.metadata, 
            params.oracle_authority, 
            params.queue_addr,
        );
    }

    public entry fun run<CoinType>(
        account: &signer, 
        oracle_addr: address,
        name: vector<u8>,
        metadata: vector<u8>,
        oracle_authority: address,
        queue_addr: address,
    ) {
        let params = OracleSetConfigsParams {
            oracle_addr,
            name, 
            metadata,
            oracle_authority,
            queue_addr,
        };

        // validate that the user has permissions for this queue

        validate<CoinType>(account, &params);
        actuate(account, &params);
    }

}
