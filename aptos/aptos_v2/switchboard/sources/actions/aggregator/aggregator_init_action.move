module switchboard::aggregator_init_action {
    use switchboard::aggregator::{Self, AggregatorConfigParams};
    use aptos_framework::account::{Self, SignerCapability};
    use switchboard::switchboard;
    use switchboard::errors;
    use switchboard::math;
    use switchboard::oracle_queue;
    use std::signer;
    use std::bcs;
    
    public fun validate<CoinType>(account: &signer, params: &AggregatorConfigParams) {
        let queue = aggregator::queue_from_conf(params);
        assert!(oracle_queue::exist<CoinType>(queue), errors::QueueNotFound());
        assert!(!aggregator::exist(signer::address_of(account)), errors::AggregatorAlreadyExists());
        let batch_size = aggregator::batch_size_from_conf(params);
        let min_oracle_results = aggregator::min_oracle_results_from_conf(params);
        let min_update_delay_seconds = aggregator::min_update_delay_seconds_from_conf(params);
        assert!(batch_size > 0 && batch_size <= 10, errors::AggregatorInvalidBatchSize());
        assert!(min_oracle_results > 0, errors::AggregatorInvalidMinOracleResults());
        assert!(min_oracle_results <= batch_size, errors::AggregatorInvalidBatchSize());
        assert!(min_update_delay_seconds >= 5, errors::AggregatorInvalidUpdateDelay());
    }

    fun actuate<CoinType>(account: &signer, params: &AggregatorConfigParams, signer_cap: SignerCapability) {
        aggregator::aggregator_create(account, signer_cap, params);    
        let account_address = signer::address_of(account);
        let authority = aggregator::authority(account_address);
        switchboard::aggregator_authority_set(account_address, authority, @0x0);
        switchboard::emit_aggregator_init_event(signer::address_of(account));
    }

    // initialize aggregator for user
    // NOTE Type param CoinType is the Coin Type of the Oracle Queue
    public entry fun run<CoinType>(
        account: &signer,
        name: vector<u8>,
        metadata: vector<u8>,
        queue_addr: address,
        crank_addr: address,
        batch_size: u64,
        min_oracle_results: u64,
        min_job_results: u64,
        min_update_delay_seconds: u64,
        start_after: u64,
        variance_threshold_value: u128, 
        variance_threshold_scale: u8,
        force_report_period: u64,
        expiration: u64,
        disable_crank: bool,
        history_limit: u64,
        read_charge: u64,
        reward_escrow: address,
        read_whitelist: vector<address>,
        limit_reads_to_whitelist: bool,
        authority: address,
        seed: address,
    ) {

        // sender will be the authority
        let account_address = signer::address_of(account);
        let variance_threshold = math::new(variance_threshold_value, variance_threshold_scale, false);

        // here we may want to handle Optionality for other fields like name + metadata
        // generate params
        let params = aggregator::new_config(
            account_address,
            name,
            metadata,
            queue_addr,
            crank_addr,
            batch_size,
            min_oracle_results,
            min_job_results,
            min_update_delay_seconds,
            start_after,
            variance_threshold,
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

        let (aggregator_account, signer_cap) = account::create_resource_account(account, bcs::to_bytes(&seed));
        validate<CoinType>(&aggregator_account, &params);
        actuate<CoinType>(&aggregator_account, &params, signer_cap);
    }    
}
