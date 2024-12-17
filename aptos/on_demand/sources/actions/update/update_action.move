module switchboard::update_action {
    use std::vector;
    use aptos_framework::object;
    use switchboard::aggregator::{Self, Aggregator};
    use switchboard::queue::{Self, Queue};
    use switchboard::oracle::Oracle;
    use switchboard::serialization;
    use switchboard::aggregator_submit_result_action;
    use switchboard::oracle_attest_action;
    use switchboard::decimal;


    // No validation needed - done in subsequent actions
    public fun validate() {}

    fun actuate<CoinType>(
        account: &signer, 
        update_data: vector<vector<u8>>,
    ) { 
        let idx = 0;
        while (idx < vector::length(&update_data)) {
            let bytes = vector::borrow(&update_data, idx);
            let discriminator = serialization::get_message_discriminator(bytes);
            if (discriminator == 1) {

                // Extract data
                // NOTE: we don't currently use block number for anything, but it will likely be integrated
                // if we are able to access historical blockhashes on-chain, or some other switchboard epoch mechanism
                let (_, aggregator, value, r, s, v, _, timestamp, oracle_key) = serialization::parse_update_bytes(*bytes);
                let aggregator = object::address_to_object<Aggregator>(aggregator);
                let queue = aggregator::get_queue(aggregator);
                let (value, neg) = decimal::unpack(value);
                let oracle_address = queue::get_oracle_from_key(queue, oracle_key);
                let oracle = object::address_to_object<Oracle>(oracle_address);

                // Build signature
                let signature = r;
                vector::append(&mut signature, s);
                vector::push_back(&mut signature, v);

                // Submit result
                aggregator_submit_result_action::run<CoinType>(
                    account,
                    aggregator,
                    queue,
                    oracle,
                    value,
                    neg,
                    timestamp,
                    signature,
                );
            } else if (discriminator == 2) {

                // Extract data
                // Note: block number unused here too for now
                let (
                    _, 
                    oracle_address, 
                    queue_address, 
                    mr_enclave, 
                    secp256k1_key, 
                    _, 
                    r,
                    s,
                    v,
                    timestamp,
                    guardian_address
                ) = serialization::parse_attestation_bytes(*bytes);

                // Build signature
                let signature = r;
                vector::append(&mut signature, s);
                vector::push_back(&mut signature, v);

                // Build params
                let oracle = object::address_to_object<Oracle>(oracle_address);
                let queue = object::address_to_object<Queue>(queue_address);
                let guardian = object::address_to_object<Oracle>(guardian_address);

                // Run the oracle attestation action
                oracle_attest_action::run(
                    oracle,
                    queue,
                    guardian,
                    timestamp,
                    mr_enclave,
                    secp256k1_key,
                    signature,
                );
            };
            idx = idx + 1;
        };
    }


    /**
     *  - Takes an arbitrary vector of byte vectors
     *  - Removes the switchboard updates and runs them
     *  - Remaining vector will be a vector of non-switchboard byte vectors
     */
    public fun extract_and_run<CoinType>(account: &signer, update_data: &mut vector<vector<u8>>) {
        let idxs_to_remove = vector::empty<u64>();
        let switchboard_updates = vector::empty<vector<u8>>();
        let idx = 0;

        while (idx < vector::length(update_data)) {
            let bytes = vector::borrow(update_data, idx);
            let discriminator = serialization::get_message_discriminator(bytes);
            if (discriminator == 1 || discriminator == 2) {
                vector::push_back(&mut switchboard_updates, *bytes);
                vector::push_back(&mut idxs_to_remove, idx);
            };
        };        

        // Remove the updates we've processed back to front
        let idx = vector::length(&idxs_to_remove);
        while (idx > 0) {
            idx = idx - 1;
            let idx = *vector::borrow(&idxs_to_remove, idx);
            vector::remove(update_data, idx);
        };

        // run the updates
        if (vector::length(&switchboard_updates) > 0) {
            actuate<CoinType>(account, switchboard_updates);
        };
    }

    public entry fun run<CoinType>(account: &signer, update_data: vector<vector<u8>>) {
        validate();
        actuate<CoinType>(account, update_data);
    }

}