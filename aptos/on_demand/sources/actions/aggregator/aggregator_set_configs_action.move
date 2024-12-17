module switchboard::aggregator_set_configs_action {
    use std::signer;
    use std::vector;
    use std::string::String;
    use aptos_framework::object::{Self, Object};
    use switchboard::aggregator::{Self, Aggregator};
    use switchboard::errors;
    use aptos_std::event;

    #[event]
    struct AggregatorConfigsUpdated has drop, store {
        aggregator: address,
        feed_hash: vector<u8>,
        min_sample_size: u64,
        max_staleness_seconds: u64,
        max_variance: u64,
        min_responses: u32,
    }

    struct AggregatorSetConfigsParams {
        aggregator: Object<Aggregator>,
        name: String,
        feed_hash: vector<u8>,
        min_sample_size: u64,
        max_staleness_seconds: u64,
        max_variance: u64,
        min_responses: u32,
    }

    fun params(
        aggregator: Object<Aggregator>,
        name: String,
        feed_hash: vector<u8>,
        min_sample_size: u64,
        max_staleness_seconds: u64,
        max_variance: u64,
        min_responses: u32,
    ): AggregatorSetConfigsParams {
        AggregatorSetConfigsParams {
            aggregator,
            name,
            feed_hash,
            min_sample_size,
            max_staleness_seconds,
            max_variance,
            min_responses,
        }
    }

    public fun validate(
        account: &signer,
        params: &AggregatorSetConfigsParams,
    ) {
        assert!(aggregator::aggregator_exists(params.aggregator), errors::aggregator_does_not_exist());
        assert!(params.max_staleness_seconds > 60, errors::invalid_max_staleness_seconds());
        assert!(params.min_sample_size > 0, errors::invalid_min_sample_size());
        assert!(params.min_responses > 0, errors::invalid_min_responses());
        assert!(vector::length(&params.feed_hash) == 32, errors::invalid_length());
        assert!(object::owner(params.aggregator) == signer::address_of(account), errors::invalid_authority());
        assert!(aggregator::has_authority(params.aggregator, signer::address_of(account)), errors::invalid_authority());
    }

    fun actuate(
        params: AggregatorSetConfigsParams,
    ) {
        let AggregatorSetConfigsParams {
            aggregator,
            name,
            feed_hash,
            min_sample_size,
            max_staleness_seconds,
            max_variance,
            min_responses,
        } = params;

        aggregator::set_configs(
            copy aggregator,
            name,
            min_sample_size,
            max_staleness_seconds,
            max_variance,
            min_responses,
            feed_hash,
        );

        event::emit(AggregatorConfigsUpdated {
            aggregator: object::object_address(&aggregator),
            feed_hash,
            min_sample_size,
            max_staleness_seconds,
            max_variance,
            min_responses,
        });
    }

    public entry fun run(
        account: &signer,
        aggregator: Object<Aggregator>,
        name: String,
        feed_hash: vector<u8>,
        min_sample_size: u64,
        max_staleness_seconds: u64,
        max_variance: u64,
        min_responses: u32,
    ) {
      
        let params = params(
            aggregator,
            name,
            feed_hash,
            min_sample_size,
            max_staleness_seconds,
            max_variance,
            min_responses,
        );

        validate(account, &params);
        actuate(params);
    }
}