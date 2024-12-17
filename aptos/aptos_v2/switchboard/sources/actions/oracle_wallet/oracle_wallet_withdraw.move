module switchboard::oracle_wallet_withdraw_action {
    use switchboard::oracle;
    use switchboard::escrow;
    use switchboard::oracle_queue;
    use switchboard::permission;
    use switchboard::errors;
    use switchboard::switchboard;
    use aptos_framework::timestamp;
    use std::signer;
    use std::coin;

    struct OracleWalletWithdrawParams has drop {
        oracle_addr: address,
        queue_addr: address,
        amount: u64,
    }
    
    public fun validate<CoinType>(account: &signer, params: &OracleWalletWithdrawParams) {
        assert!(escrow::exist<CoinType>(params.oracle_addr, params.queue_addr), errors::OracleWalletNotFound());
        assert!(escrow::authority<CoinType>(params.oracle_addr, params.queue_addr) == signer::address_of(account), errors::InvalidAuthority());
        assert!(escrow::balance<CoinType>(params.oracle_addr, params.queue_addr) >= params.amount, errors::OracleWalletInsufficientCoin());

        // TODO: re-enable the following section for security around oracle withdraws
        let queue = oracle::queue_addr(params.oracle_addr);
        let authority = oracle_queue::authority<CoinType>(queue);
        let pkey = permission::key(
            &authority,  
            &queue,
            &params.oracle_addr,
        );
        let _p = switchboard::permission_get(pkey);

        //enforce that hearbeat permission is off 
        //assert!(!permission::has(&p, permission::PERMIT_ORACLE_HEARTBEAT()), errors::PermissionDenied());

        // and enforce that permissions haven't changed in at least a minute
        //assert!(permission::seconds_since_last_update(&p) > 60, errors::PermissionDenied());
    }
    
    fun actuate<CoinType>(_account: &signer, params: &OracleWalletWithdrawParams) {

        let initial_balance = escrow::balance<CoinType>(params.oracle_addr, params.queue_addr);
        let coin = escrow::withdraw<CoinType>(
            params.oracle_addr,
            params.queue_addr,
            params.amount,
        );
        let authority = escrow::authority<CoinType>(params.oracle_addr, params.queue_addr);
        coin::deposit<CoinType>(authority, coin);

        let balance = escrow::balance<CoinType>(params.oracle_addr, params.queue_addr);
        switchboard::emit_lease_withdraw_event(
            params.oracle_addr,
            authority,
            initial_balance,
            balance,
            timestamp::now_seconds(),
        );
    }

    public entry fun run<CoinType>(
        account: signer, 
        oracle_addr: address,
        queue_addr: address,
        amount: u64
    ) {
        let params = OracleWalletWithdrawParams { 
            oracle_addr,
            queue_addr,
            amount,
        };
        validate<CoinType>(&account, &params);
        actuate<CoinType>(&account, &params);
    }
}
