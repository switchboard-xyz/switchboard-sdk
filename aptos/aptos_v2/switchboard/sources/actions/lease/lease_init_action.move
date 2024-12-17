module switchboard::lease_init_action {
    use switchboard::aggregator;
    use switchboard::escrow;
    use switchboard::errors;
    use switchboard::oracle_queue;
    use switchboard::switchboard;
    use aptos_framework::timestamp;
    use std::coin;
    use std::signer;

    struct LeaseInitParams has drop {
        aggregator_addr: address,
        queue_addr: address,
        withdraw_authority: address,
        initial_amount: u64,
    }
    
    public fun validate<CoinType>(account: &signer, params: &LeaseInitParams) {
        assert!(oracle_queue::exist<CoinType>(params.queue_addr), errors::QueueNotFound());
        assert!(!oracle_queue::lock_lease_funding<CoinType>(params.queue_addr), errors::PermissionDenied());
        assert!(aggregator::exist(params.aggregator_addr), errors::AggregatorNotFound());
        assert!(aggregator::has_authority(params.aggregator_addr, account), errors::InvalidAuthority());
        assert!(aggregator::queue_addr(params.aggregator_addr) == params.queue_addr, errors::QueueNotFound());
        assert!(!escrow::exist<CoinType>(params.aggregator_addr, params.queue_addr), errors::LeaseAlreadyExists());
    }

    fun actuate<CoinType>(account: &signer, params: &LeaseInitParams) {

        let coin = coin::withdraw<CoinType>(account, params.initial_amount);
        let lease = escrow::new<CoinType>(
            params.withdraw_authority,            
            coin,
        );

        switchboard::emit_lease_fund_event(
            params.aggregator_addr,
            signer::address_of(account),
            params.initial_amount,
            timestamp::now_seconds(),
        );

        
        let aggregator_account = aggregator::get_aggregator_account(params.aggregator_addr);
        escrow::create<CoinType>(&aggregator_account, params.queue_addr, lease);
    }

    public entry fun run<CoinType>(
        account: &signer, 
        aggregator_addr: address,
        queue_addr: address, 
        withdraw_authority: address, 
        initial_amount: u64
    ) {
        let params = LeaseInitParams{ 
            aggregator_addr,
            queue_addr, 
            withdraw_authority,
            initial_amount,
        };
        validate<CoinType>(account, &params);
        actuate<CoinType>(account, &params);
    }
}
