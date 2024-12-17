module switchboard::oracle_wallet_contribute_action {
    use switchboard::escrow;
    use switchboard::errors;
    use std::coin;

    struct OracleWalletContributeParams has drop {
        oracle_addr: address,
        queue_addr: address,
        load_amount: u64,
    }
    
    public fun validate<CoinType>(_account: &signer, params: &OracleWalletContributeParams) {
        assert!(escrow::exist<CoinType>(params.oracle_addr, params.queue_addr), errors::OracleWalletNotFound());
    }

    fun actuate<CoinType>(account: &signer, params: &OracleWalletContributeParams) {
        let coin = coin::withdraw<CoinType>(account, params.load_amount);
        escrow::deposit<CoinType>(
            params.oracle_addr,
            params.queue_addr,
            coin,
        );
    }

    public entry fun run<CoinType>(
        account: &signer, 
        oracle_addr: address,
        queue_addr: address,
        load_amount: u64
    ) {
        let params = OracleWalletContributeParams { 
            oracle_addr,
            queue_addr,
            load_amount,
        };
        validate<CoinType>(account, &params);
        actuate<CoinType>(account, &params);
    }
}
