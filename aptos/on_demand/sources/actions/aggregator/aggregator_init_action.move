module switchboard::aggregator_init_action {
    use std::vector;
    use aptos_framework::object::{Self, Object};
    use switchboard::aggregator;
    use switchboard::queue::{Self, Queue};
    use switchboard::errors;
    use std::string::String;
    use aptos_std::event;

    #[event]
    struct AggregatorCreated has drop, store {
        aggregator: address,
        name: String,
    }

    struct AggregatorInitActionParams {
        queue: Object<Queue>,
        name: String,
        feed_hash: vector<u8>,
        min_sample_size: u64,
        max_staleness_seconds: u64,
        max_variance: u64,
        min_responses: u32,
    }

    fun params(
        queue: Object<Queue>,
        name: String,
        feed_hash: vector<u8>,
        min_sample_size: u64,
        max_staleness_seconds: u64,
        max_variance: u64,
        min_responses: u32,
    ): AggregatorInitActionParams {
        AggregatorInitActionParams {
            queue,
            name,
            feed_hash,
            min_sample_size,
            max_staleness_seconds,
            max_variance,
            min_responses,
        }
    }

    public fun validate(
        params: &AggregatorInitActionParams,
    ) {
        assert!(params.max_staleness_seconds > 0, errors::invalid_max_staleness_seconds());
        assert!(params.min_sample_size > 0, errors::invalid_min_sample_size());
        assert!(params.min_responses > 0, errors::invalid_min_responses());
        assert!(vector::length(&params.feed_hash) == 32, errors::invalid_length());
    }

    fun actuate(
        account: &signer,
        params: AggregatorInitActionParams,
    ) {
        let AggregatorInitActionParams {
            queue,
            name,
            feed_hash,
            min_sample_size,
            max_staleness_seconds,
            max_variance,
            min_responses,
        } = params;

        let aggregator_address =aggregator::new(
            account,
            object::object_address(&queue),
            name,
            feed_hash,
            min_sample_size,
            max_staleness_seconds,
            max_variance,
            min_responses,
        );

        queue::add_aggregator(queue, aggregator_address);
        event::emit(AggregatorCreated {
            aggregator: aggregator_address,
            name,
        });
    }

    public entry fun run(
        account: &signer,
        queue: Object<Queue>,
        name: String,
        feed_hash: vector<u8>,
        min_sample_size: u64,
        max_staleness_seconds: u64,
        max_variance: u64,
        min_responses: u32,
    ) {

      let params = params(
        queue,
        name,
        feed_hash,
        min_sample_size,
        max_staleness_seconds,
        max_variance,
        min_responses,
      );

      validate(&params);
      actuate(account, params);
    }
}