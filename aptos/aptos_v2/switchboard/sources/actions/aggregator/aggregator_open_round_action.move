module switchboard::aggregator_open_round_action {
    use switchboard::aggregator;
    use switchboard::escrow;
    use switchboard::errors;
    use switchboard::oracle;
    use switchboard::oracle_queue;
    use switchboard::switchboard;
    use switchboard::permission;
    use switchboard::math;
    use std::coin;
    use std::signer;
    use std::vector;
    use std::option::{Self, Option};
    use aptos_framework::timestamp;

    friend switchboard::crank_pop_action;

    struct AggregatorOpenRoundParams has copy, drop {
        aggregator_addr: address,
        jitter: u64,
    }

    struct AggregatorOpenRoundActuateParams has copy, drop {
        open_round_params: AggregatorOpenRoundParams,
        queue_addr: address,
        batch_size: u64,
        job_keys: vector<address>,
        reward: u64,
        open_round_reward: u64,
    }

    public(friend) fun params(aggregator_addr: address, jitter: u64): AggregatorOpenRoundParams {
        AggregatorOpenRoundParams { aggregator_addr, jitter }
    }

    public fun simulate<CoinType>(params: AggregatorOpenRoundParams): (u64, Option<AggregatorOpenRoundActuateParams>) {

        if (!aggregator::exist(params.aggregator_addr)) return (errors::AggregatorNotFound(), option::none<AggregatorOpenRoundActuateParams>());

        // Get relevant fields from config
        let (
            queue_addr,
            batch_size,
            _min_oracle_results,
        ) = aggregator::configs(params.aggregator_addr);

        let can_open_round = aggregator::can_open_round(params.aggregator_addr);
        if (!can_open_round) return (errors::AggregatorIllegalRoundOpenCall(), option::none<AggregatorOpenRoundActuateParams>());

        // ensure that we're using the queue's CoinType
        if (!oracle_queue::exist<CoinType>(queue_addr)) return (errors::QueueNotFound(), option::none<AggregatorOpenRoundActuateParams>());

        // gas saving fn to grab all relevant oracle config from oracle queue in one go
        let (
            queue_authority,
            reward,
            open_round_reward, 
            _save_reward, 
            save_confirmation_reward, 
            _slashing_penalty, 
            _slashing_enabled, 
            _variance_tolerance_multiplier,
            unpermissioned_feeds_enabled,
        ) = oracle_queue::configs(queue_addr);
        let data_len = oracle_queue::data_len(queue_addr);

        if (data_len < batch_size) return (errors::AggregatorQueueNotReady(), option::none<AggregatorOpenRoundActuateParams>());

        let max_round_cost = (reward + save_confirmation_reward) * (batch_size + 1);
        if (escrow::balance<CoinType>(params.aggregator_addr, queue_addr) < max_round_cost) return (errors::LeaseInsufficientCoin(), option::none<AggregatorOpenRoundActuateParams>());
        
        if (!unpermissioned_feeds_enabled) {
            let pkey = permission::key(&queue_authority, &queue_addr, &params.aggregator_addr);
            let p = switchboard::permission_get(pkey);
            if (!permission::has(&p, permission::PERMIT_ORACLE_QUEUE_USAGE())) return (errors::PermissionDenied(), option::none<AggregatorOpenRoundActuateParams>());
        };
        let job_keys = aggregator::job_keys(params.aggregator_addr);

        (
            0, 
            option::some<AggregatorOpenRoundActuateParams>(AggregatorOpenRoundActuateParams {
                open_round_params: params,
                queue_addr,
                batch_size,
                job_keys,
                reward,
                open_round_reward,
            }),
        )
    }

    public fun validate<CoinType>(_account: &signer, params: AggregatorOpenRoundParams): AggregatorOpenRoundActuateParams {
        assert!(aggregator::exist(params.aggregator_addr), errors::AggregatorNotFound());

        // Get relevant fields from config
        let (
            queue_addr,
            batch_size,
            _min_oracle_results,
        ) = aggregator::configs(params.aggregator_addr);

        let can_open_round = aggregator::can_open_round(params.aggregator_addr);
        assert!(can_open_round, errors::PermissionDenied());

        // ensure that we're using the queue's CoinType
        assert!(oracle_queue::exist<CoinType>(queue_addr), errors::QueueNotFound());

        // gas saving fn to grab all relevant oracle config from oracle queue in one go
        let (
            queue_authority,
            reward,
            open_round_reward, 
            _save_reward, 
            save_confirmation_reward, 
            _slashing_penalty, 
            _slashing_enabled, 
            _variance_tolerance_multiplier,
            unpermissioned_feeds_enabled,
        ) = oracle_queue::configs(queue_addr);
        let data_len = oracle_queue::data_len(queue_addr);

        assert!(data_len >= batch_size, errors::AggregatorQueueNotReady());
        let max_round_cost = (reward + save_confirmation_reward) * (batch_size + 1);
        assert!(
            escrow::balance<CoinType>(params.aggregator_addr, queue_addr) >= max_round_cost, 
            errors::LeaseInsufficientCoin()
        );
        
        if (!unpermissioned_feeds_enabled) {
            let pkey = permission::key(&queue_authority, &queue_addr, &params.aggregator_addr);
            let p = switchboard::permission_get(pkey);
            assert!(permission::has(&p, permission::PERMIT_ORACLE_QUEUE_USAGE()), errors::PermissionDenied());
        };
        let job_keys = aggregator::job_keys(params.aggregator_addr);

        AggregatorOpenRoundActuateParams {
            open_round_params: params,
            queue_addr,
            batch_size,
            job_keys,
            reward,
            open_round_reward,
        }
    }

    public(friend) fun actuate<CoinType>(account: &signer, params: AggregatorOpenRoundActuateParams): u64 {

        // perform payouts 
        // gas saving fn to grab current round / config items from aggregator
        let (
            current_round_result, 
            current_round_std_dev, 
            current_round_medians,
            current_round_errors,
            current_round_oracles,
        ) = aggregator::current_round_info(params.open_round_params.aggregator_addr);

        // disabled oracle rep updates for gas purposes
        let apply_rep_updates = false;

        // gas saving fn to grab all relevant oracle config from oracle queue in one go
        let (
            _authority,
            payout, 
            _open_round_reward,
            save_reward, 
            _save_confirmation_reward, 
            slashing_penalty, 
            slashing_enabled, 
            variance_tolerance_multiplier,
            _unpermissioned_feeds_enabled,
        ) = oracle_queue::configs(params.queue_addr);


        // set threshold to queue.variance_tolerance_multiplier * std_deviation
        let threshold = math::zero();
        math::mul(
            &current_round_std_dev, 
            &variance_tolerance_multiplier, 
            &mut threshold
        );

        // set upper bound
        let upper = math::add(
            &current_round_result, 
            &threshold,
        );

        // set lower bound
        let lower = math::sub(
            &current_round_result, 
            &threshold,
        );



        // go through each oracle, determining payout / slashing / no result
        let i = 0;
        let oracle_count = vector::length(&current_round_oracles);
        while (i < oracle_count) {
            
            let oracle_at_idx = *vector::borrow(&current_round_oracles, i);
            let is_error_fulfilled = *vector::borrow(&current_round_errors, i);
            let is_median_fulfilled = option::is_some(vector::borrow(&current_round_medians, i)); 

            // check if error reported for this oracle
            if (is_error_fulfilled == true) {

                // update reputation and payout 0 here
                if (apply_rep_updates) {
                    oracle::update_reputation(oracle_at_idx, oracle::OracleResponseError());
                };

            // check if oracle had any response
            } else if (is_median_fulfilled == false) {
                
                // update rep here
                // slash
                if (slashing_enabled) {

                    let slashing_penalty = payout + slashing_penalty;
                    switchboard::emit_oracle_slash_event(
                        params.open_round_params.aggregator_addr,
                        oracle_at_idx,
                        slashing_penalty,
                        timestamp::now_seconds()
                    );

                    // deposit the slash in the aggregator's lease
                    let coin = escrow::withdraw<CoinType>(oracle_at_idx, params.queue_addr, slashing_penalty);
                    escrow::deposit<CoinType>(params.open_round_params.aggregator_addr, params.queue_addr, coin);
                };

                if (apply_rep_updates) {
                    oracle::update_reputation(oracle_at_idx, oracle::OracleResponseNoResponse());
                };

            // if the oracle responded
            } else {
                
                let oracle_result = option::borrow(vector::borrow(&current_round_medians, i));

                // if value is within threshold - do payout
                // if 1 oracle responded, there is no std deviation so we just pay that one out 
                if (oracle_count == 1 || math::gte(oracle_result, &lower) && math::lte(oracle_result, &upper)) {

                    // payout value 
                    let full_payout = save_reward + payout;
                    switchboard::emit_oracle_reward_event(
                        params.open_round_params.aggregator_addr,
                        oracle_at_idx,
                        full_payout,
                        timestamp::now_seconds()
                    );

                    // deposit the reward in the oracle's lease
                    let coin = escrow::withdraw<CoinType>(params.open_round_params.aggregator_addr, params.queue_addr, full_payout);
                    escrow::deposit<CoinType>(oracle_at_idx, params.queue_addr, coin);

                } else {
                    
                    // update rep
                    if (apply_rep_updates) {
                        oracle::update_reputation(oracle_at_idx, oracle::OracleResponseDisagreement());
                    };

                    // else slash
                    if (slashing_enabled) {

                        let slashing_penalty = payout + slashing_penalty;

                        // deposit the slash in the aggregator's lease
                        let coin = escrow::withdraw<CoinType>(oracle_at_idx, params.queue_addr, slashing_penalty);
                        escrow::deposit<CoinType>(params.open_round_params.aggregator_addr, params.queue_addr, coin);
                        switchboard::emit_oracle_slash_event(
                            params.open_round_params.aggregator_addr,
                            oracle_at_idx,
                            slashing_penalty,
                            timestamp::now_seconds()
                        );
                    };
                };
            };
            i = i + 1;
        };

        // open round
        let oracle_keys = oracle_queue::next_n(params.queue_addr, params.batch_size);
        let next_allowed_timestamp = aggregator::open_round(params.open_round_params.aggregator_addr, params.open_round_params.jitter, &oracle_keys);
        let reward = params.reward + params.open_round_reward;
        let coin = escrow::withdraw<CoinType>(params.open_round_params.aggregator_addr, params.queue_addr, reward);

        // reward crank turner (directly)
        coin::deposit<CoinType>(
            signer::address_of(account),
            coin,
        );
        switchboard::emit_aggregator_open_round_event(
            params.open_round_params.aggregator_addr,
            params.job_keys,
            oracle_keys,
        );

        // (used by crank pop)
        next_allowed_timestamp
    }

    public entry fun run<CoinType>(
        account: signer,
        aggregator_addr: address,
        jitter: u64
    ) {
        let params = AggregatorOpenRoundParams { aggregator_addr, jitter };
        let actuate_params = validate<CoinType>(&account, params);
        actuate<CoinType>(&account, actuate_params);
    }    

    public entry fun run_many<CoinType>(
        account: &signer,
        aggregator_addrs: vector<address>,
        jitter: u64
    ) {
        let i = 0;
        let len = vector::length(&aggregator_addrs);
        while (i < len) {
            let params = AggregatorOpenRoundParams {
                aggregator_addr: *vector::borrow(&aggregator_addrs, i), 
                jitter
            };
            let (result, actuate_params) = simulate<CoinType>(params);
            if (result == 0) {
                actuate<CoinType>(account, option::extract(&mut actuate_params));
            };
            i = i + 1;
        };
    }

    public entry fun run_n<CoinType>(
        _account: signer,
        _aggregator_addrs: vector<address>,
        _jitter: u64
    ) {}
}
