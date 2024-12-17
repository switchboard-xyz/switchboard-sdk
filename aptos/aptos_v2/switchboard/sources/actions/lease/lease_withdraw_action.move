module switchboard::lease_withdraw_action {
    use switchboard::aggregator;
    use switchboard::escrow;
    use switchboard::errors;
    use switchboard::oracle_queue;
    use switchboard::switchboard;
    use aptos_framework::timestamp;
    use std::signer;
    use std::coin;

    struct LeaseWithdrawParams has drop {
        aggregator_addr: address,
        queue_addr: address,
        amount: u64,
    }
    
    public fun validate<CoinType>(account: &signer, params: &LeaseWithdrawParams) {
        assert!(aggregator::exist(params.aggregator_addr), errors::AggregatorNotFound());
        assert!(escrow::exist<CoinType>(params.aggregator_addr, params.queue_addr), errors::LeaseNotFound());
        assert!(escrow::authority<CoinType>(params.aggregator_addr, params.queue_addr) == signer::address_of(account), errors::InvalidAuthority());
    }

    fun actuate<CoinType>(_account: &signer, params: &LeaseWithdrawParams) {

        let max_round_cost = oracle_queue::max_reward<CoinType>(
            params.queue_addr, 
            aggregator::batch_size(params.aggregator_addr),
        );
        let max_withdrawable = if (escrow::balance<CoinType>(params.aggregator_addr, params.queue_addr) < max_round_cost) {
          0
        } else {
          escrow::balance<CoinType>(params.aggregator_addr, params.queue_addr) - max_round_cost
        };
        
        let withdrawable_amount = if (params.amount < max_withdrawable) {
          params.amount
        } else {
          max_withdrawable
        };
        
        // withdraw `withdrawable_amount`u64 to the withdraw authority
        let coin = escrow::withdraw<CoinType>(
            params.aggregator_addr,
            params.queue_addr,
            withdrawable_amount,
        );
        let authority = escrow::authority<CoinType>(params.aggregator_addr, params.queue_addr);
        coin::deposit<CoinType>(authority, coin);

        // log withdraw event
        switchboard::emit_lease_withdraw_event(
            params.aggregator_addr,
            authority,
            max_withdrawable,
            escrow::balance<CoinType>(params.aggregator_addr, params.queue_addr),
            timestamp::now_seconds(),
        );
    }

    public entry fun run<CoinType>(
        account: signer, 
        aggregator_addr: address, 
        queue_addr: address,
        amount: u64
    ) {
        let params = LeaseWithdrawParams { 
            aggregator_addr,
            queue_addr,
            amount,
        };
        validate<CoinType>(&account, &params);
        actuate<CoinType>(&account, &params);
    }
}
