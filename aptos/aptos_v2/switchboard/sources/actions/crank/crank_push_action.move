module switchboard::crank_push_action {
    use aptos_framework::timestamp;
    use switchboard::aggregator;
    use switchboard::crank;
    use switchboard::errors;

    struct CrankPushParams has drop {
        crank_addr: address,
        aggregator_addr: address,
    }

    public fun validate<CoinType>(_account: &signer, params: &CrankPushParams) {
        assert!(crank::exist(params.crank_addr), errors::CrankNotFound());
        assert!(aggregator::exist(params.aggregator_addr), errors::AggregatorNotFound());
        assert!(!aggregator::crank_disabled(params.aggregator_addr), errors::CrankDisabled());
        assert!(aggregator::crank_row_count(params.aggregator_addr) == 0, errors::PermissionDenied());
    }

    fun actuate(account: &signer, params: &CrankPushParams) {
        aggregator::add_crank_row_count(params.aggregator_addr);

        // only the aggregator authority can set the crank 
        if (aggregator::has_authority(params.aggregator_addr, account)) {
            aggregator::set_crank(params.aggregator_addr, params.crank_addr);
        };

        // anybody can re-push, but they have to explicitly list the crank_addr
        assert!(aggregator::crank_addr(params.aggregator_addr) == params.crank_addr, errors::InvalidArgument());
        crank::push(aggregator::crank_addr(params.aggregator_addr), params.aggregator_addr, timestamp::now_seconds());
    }


    public entry fun run<CoinType>(account: &signer, crank_addr: address, aggregator_addr: address) {
        let params = CrankPushParams { 
            crank_addr,
            aggregator_addr
        };
        
        // enforce that aggregator is on this crank
        validate<CoinType>(account, &params);
        actuate(account, &params);
    }

}
