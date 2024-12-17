module switchboard::lease_extend_action {
    use switchboard::aggregator;
    use switchboard::escrow;
    use switchboard::errors;
    use switchboard::oracle_queue;
    use switchboard::switchboard;
    use aptos_framework::timestamp;
    use std::signer;
    use std::coin;

    struct LeaseExtendParams has drop {
        aggregator_addr: address,
        load_amount: u64,
    }
    
    public fun validate<CoinType>(account: &signer, params: &LeaseExtendParams) {
        let queue_addr = aggregator::queue_addr(params.aggregator_addr);
        assert!(escrow::exist<CoinType>(params.aggregator_addr, queue_addr), errors::LeaseNotFound());
        assert!(!oracle_queue::lock_lease_funding<CoinType>(queue_addr), errors::PermissionDenied());
        assert!(coin::balance<CoinType>(signer::address_of(account)) >= params.load_amount, errors::InsufficientCoin());
    }

    fun actuate<CoinType>(account: &signer, params: &LeaseExtendParams) {
        let queue_addr = aggregator::queue_addr(params.aggregator_addr);
        let coin = coin::withdraw<CoinType>(account, params.load_amount);
        escrow::deposit<CoinType>(
            params.aggregator_addr, 
            queue_addr,
            coin,
        );

        switchboard::emit_lease_fund_event(
            params.aggregator_addr,
            signer::address_of(account),
            params.load_amount,
            timestamp::now_seconds(),
        );
    }

    public entry fun run<CoinType>(
        account: signer, 
        aggregator_addr: address, 
        load_amount: u64
    ) {
        let params = LeaseExtendParams { 
            aggregator_addr,
            load_amount,
        };
        validate<CoinType>(&account, &params);
        actuate<CoinType>(&account, &params);
    }
}
