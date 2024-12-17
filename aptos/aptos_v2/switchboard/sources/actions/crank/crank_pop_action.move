module switchboard::crank_pop_action {
    use aptos_framework::timestamp;
    use switchboard::aggregator;
    use switchboard::crank;
    use switchboard::errors;
    use switchboard::aggregator_open_round_action;
    use switchboard::switchboard;
    use std::option;

    struct CrankPopParams has drop {
        crank_addr: address,
        pop_idx: u64,
    }

    public fun validate_and_actuate<CoinType>(account: &signer, params: &CrankPopParams) {

        // VALIDATE
        assert!(crank::exist(params.crank_addr), errors::CrankNotFound());
        let (
            aggregator_addr, 
            marked_allowed_timestamp,
            jitter_modifier,
        ) = crank::pop(params.crank_addr, params.pop_idx); // will abort if size == 0 
        assert!(timestamp::now_seconds() > marked_allowed_timestamp, errors::CrankNotReady());

        let open_round_params = aggregator_open_round_action::params(aggregator_addr, jitter_modifier);
        let (simulation_result, actuate_params) = aggregator_open_round_action::simulate<CoinType>(open_round_params);
        if (simulation_result == errors::PermissionDenied()) {
            switchboard::emit_aggregator_crank_eviction_event(
                params.crank_addr,
                aggregator_addr,
                simulation_result,
                timestamp::now_seconds()
            );
            return // no need to reschedule if not permitted
        };
        
        // ACTUATE
        let (
            next_scheduled_timestamp, 
            reschedule, 
        ) = aggregator::apply_open_round_simulate(aggregator_addr, simulation_result, jitter_modifier);
        
        if (simulation_result == errors::LeaseInsufficientCoin()) {
            switchboard::emit_crank_lease_insufficient_funds_event(aggregator_addr);
        };

        if (simulation_result == 0 && marked_allowed_timestamp == next_scheduled_timestamp) {
            next_scheduled_timestamp = aggregator_open_round_action::actuate<CoinType>(
                account, 
                option::extract(&mut actuate_params),
            );
        };

        if (reschedule) {
            crank::push(params.crank_addr, aggregator_addr, next_scheduled_timestamp);
        }
    }

    public entry fun run<CoinType>(account: signer, crank_addr: address, pop_idx: u64) {
        let params = CrankPopParams { crank_addr, pop_idx };
        validate_and_actuate<CoinType>(&account, &params); 
    }

}
