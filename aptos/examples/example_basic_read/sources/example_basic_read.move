module example::example_basic_read {
    use aptos_std::event;
    use aptos_framework::object::{Self, Object};
    use aptos_framework::aptos_coin::AptosCoin;
    use switchboard::aggregator::{Self, Aggregator, CurrentResult};
    use switchboard::decimal::Decimal;
    use switchboard::update_action;

    #[event]
    struct AggregatorUpdated has drop, store {
        aggregator: address,
        value: Decimal,
        timestamp: u64,
    }

    public entry fun update_and_read_feed(
        account: &signer,
        update_data: vector<vector<u8>>,
    ) {
        
        // Update the feed with the provided data
        update_action::run<AptosCoin>(account, update_data);

        // Get the feed object - here it's testnet BTC/USD
        let aggregator: address = @0x4bac6bbbecfe7be5298358deaf1bf2da99c697fea16a3cf9b0e340cb557b05a8;
        let aggregator: Object<Aggregator> = object::address_to_object<Aggregator>(aggregator);

        // Get the latest update info for the feed
        let current_result: CurrentResult = aggregator::current_result(aggregator);

        // Access various result properties
        let result: Decimal = aggregator::result(&current_result);              // Update result
        let timestamp_seconds = aggregator::timestamp(&current_result);         // Timestamp in seconds

        // Emit an event with the updated result
        event::emit(AggregatorUpdated {
            aggregator: object::object_address(&aggregator),
            value: result,
            timestamp: timestamp_seconds,
        });
    }

    public entry fun update_and_read_feed_v2(
        account: &signer,
        update_data: vector<u8>,
    ) {
        update_action::run_v2<AptosCoin>(account, update_data);

        let aggregator: address = @0x4bac6bbbecfe7be5298358deaf1bf2da99c697fea16a3cf9b0e340cb557b05a8;
        let aggregator: Object<Aggregator> = object::address_to_object<Aggregator>(aggregator);
        let current_result: CurrentResult = aggregator::current_result(aggregator);
        let result: Decimal = aggregator::result(&current_result);
        let timestamp_seconds = aggregator::timestamp(&current_result);

        event::emit(AggregatorUpdated {
            aggregator: object::object_address(&aggregator),
            value: result,
            timestamp: timestamp_seconds,
        });
    }
}
