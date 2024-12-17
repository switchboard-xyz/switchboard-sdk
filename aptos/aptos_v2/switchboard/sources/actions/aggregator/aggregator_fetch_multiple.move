module switchboard::aggregator_fetch_multiple {
    use std::vector;
    use switchboard::aggregator;

    #[view]
    public fun run(
        addresses: vector<address>,
    ): vector<aggregator::AggregatorFull> { 
        let response = vector::empty<aggregator::AggregatorFull>();

        // Reverse the list of addresses because we will be popping from the back to iterate in order efficiently.
        vector::reverse<address>(&mut addresses);
        loop {
            if (vector::length(&addresses) == 0) break;
            let addr = vector::pop_back(&mut addresses);
            // If we come upon an aggregator address that doesn't exist, skip it.
            if(!aggregator::exist(addr)) continue;
            // If the aggregator is found, try to fetch the full aggregator and push it to the response array.
            vector::push_back(&mut response, aggregator::fetch_full(addr));
        };
        response
    }    
}
