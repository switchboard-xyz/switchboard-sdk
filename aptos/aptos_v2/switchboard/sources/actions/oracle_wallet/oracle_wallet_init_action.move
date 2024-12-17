module switchboard::oracle_wallet_init_action {
    use switchboard::escrow;
    use switchboard::errors;
    use switchboard::oracle;
    use switchboard::oracle_queue;
    use std::coin;

    struct OracleWalletInitParams has drop {
        oracle_addr: address,
        queue_addr: address,
        withdraw_authority: address,
    }
    
    public fun validate<CoinType>(account: &signer, params: &OracleWalletInitParams) {
        assert!(oracle::exist(params.oracle_addr), errors::OracleNotFound());
        assert!(oracle_queue::exist<CoinType>(oracle::queue_addr(params.oracle_addr)), errors::QueueNotFound());
        assert!(oracle::has_authority(params.oracle_addr, account), errors::InvalidAuthority());
        assert!(!escrow::exist<CoinType>(params.oracle_addr, params.queue_addr), errors::OracleWalletAlreadyExists());
    }

    fun actuate<CoinType>(_account: &signer, params: &OracleWalletInitParams) {
        let coin = coin::zero<CoinType>(); 
        let oracle_account = oracle::get_oracle_account(params.oracle_addr);
        let oracle_wallet = escrow::new<CoinType>(
            params.withdraw_authority,
            coin,
        );
        escrow::create<CoinType>(&oracle_account, params.queue_addr, oracle_wallet);
    }

    public entry fun run<CoinType>(
        account: &signer,
        oracle_addr: address,
        queue_addr: address,
        withdraw_authority: address,
    ) {
        let params = OracleWalletInitParams {
            withdraw_authority,
            oracle_addr,
            queue_addr,
        };
        validate<CoinType>(account, &params);
        actuate<CoinType>(account, &params);
    }
}
