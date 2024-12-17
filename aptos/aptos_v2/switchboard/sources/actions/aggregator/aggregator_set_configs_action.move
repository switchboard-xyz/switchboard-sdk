module switchboard::aggregator_set_configs_action {
    use switchboard::switchboard;
    use switchboard::aggregator::{Self, AggregatorConfigParams};
    use switchboard::errors;
    use switchboard::math;
    use switchboard::oracle_queue;

    public fun validate<CoinType>(account: &signer, params: &AggregatorConfigParams) {
        let addr = aggregator::addr_from_conf(params);
        assert!(oracle_queue::exist<CoinType>(aggregator::queue_from_conf(params)), errors::QueueNotFound());
        assert!(aggregator::exist(addr), errors::AggregatorNotFound());
        assert!(aggregator::has_authority(addr, account), errors::InvalidAuthority());
        assert!(!aggregator::is_locked(addr), errors::PermissionDenied());
        let batch_size = aggregator::batch_size_from_conf(params);
        let min_oracle_results = aggregator::min_oracle_results_from_conf(params);
        let min_update_delay_seconds = aggregator::min_update_delay_seconds_from_conf(params);
        assert!(batch_size > 0 && batch_size <= 10, errors::AggregatorInvalidBatchSize());
        assert!(min_oracle_results > 0, errors::AggregatorInvalidMinOracleResults());
        assert!(min_oracle_results <= batch_size, errors::AggregatorInvalidBatchSize());
        assert!(min_update_delay_seconds >= 5, errors::AggregatorInvalidUpdateDelay());
    }

    fun actuate<CoinType>(_account: &signer, params: &AggregatorConfigParams) {
        aggregator::set_config(params);
        let addr = aggregator::addr_from_conf(params);
        let authority = aggregator::authority_from_conf(params);
        let current_authority = aggregator::authority(addr);
        switchboard::aggregator_authority_set(addr, authority, current_authority);
    }

    public entry fun run<CoinType>(
        account: signer,
        addr: address,
        name: vector<u8>,
        metadata: vector<u8>,
        queue_addr: address,
        crank_addr: address,
        batch_size: u64,
        min_oracle_results: u64,
        min_job_results: u64,
        min_update_delay_seconds: u64,
        start_after: u64,
        variance_threshold_num: u128,
        variance_threshold_scale: u8,
        force_report_period: u64,
        expiration: u64,
        disable_crank: bool,
        history_limit: u64,
        read_charge: u64,
        reward_escrow: address,
        read_whitelist: vector<address>,
        limit_reads_to_whitelist: bool,
        authority: address
    ) {
        let params = aggregator::new_config(
                addr,
                name,
                metadata,
                queue_addr,
                crank_addr,
                batch_size,
                min_oracle_results,
                min_job_results,
                min_update_delay_seconds,
                start_after,
                math::new(variance_threshold_num, variance_threshold_scale, false),
                force_report_period,
                expiration,
                disable_crank,
                history_limit,
                read_charge,
                reward_escrow,
                read_whitelist,
                limit_reads_to_whitelist,
                authority
        );

        validate<CoinType>(&account, &params);
        actuate<CoinType>(&account, &params);
    }
}
