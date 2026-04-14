module switchboard::update_action {
    use std::option;
    use std::type_info;
    use std::vector;
    use aptos_std::secp256k1;
    use aptos_framework::object::{Self, Object};
    use aptos_framework::timestamp;
    use switchboard::aggregator::{Self, Aggregator};
    use switchboard::queue::{Self, Queue};
    use switchboard::oracle::{Self, Oracle};
    use switchboard::serialization;
    use switchboard::aggregator_submit_result_action;
    use switchboard::oracle_attest_action;
    use switchboard::decimal;
    use switchboard::errors;
    use switchboard::hash;


    // No validation needed - done in subsequent actions
    public fun validate() {}

    fun contains_address(addresses: &vector<address>, needle: address): bool {
        let idx = 0;
        while (idx < vector::length(addresses)) {
            if (*vector::borrow(addresses, idx) == needle) {
                return true
            };
            idx = idx + 1;
        };
        false
    }

    fun validate_v2<CoinType>(
        slot: u64,
        timestamp_seconds: u64,
        aggregator_addresses: &vector<address>,
        values: &vector<decimal::Decimal>,
        min_oracle_samples: &vector<u8>,
        oracle_addresses: &vector<address>,
        signatures: &vector<vector<u8>>,
    ): Object<Queue> {
        assert!(timestamp_seconds > 0, errors::invalid_timestamp());
        assert!(vector::length(aggregator_addresses) > 0, errors::invalid_length());
        assert!(vector::length(oracle_addresses) > 0, errors::invalid_length());
        assert!(vector::length(aggregator_addresses) == vector::length(values), errors::invalid_length());
        assert!(vector::length(aggregator_addresses) == vector::length(min_oracle_samples), errors::invalid_length());
        assert!(vector::length(oracle_addresses) == vector::length(signatures), errors::invalid_length());

        let first_aggregator =
            object::address_to_object<Aggregator>(*vector::borrow(aggregator_addresses, 0));
        assert!(
            aggregator::aggregator_exists(first_aggregator),
            errors::aggregator_does_not_exist()
        );
        let queue = aggregator::get_queue(first_aggregator);
        assert!(queue::queue_exists(queue), errors::queue_does_not_exist());
        assert!(
            queue::fee_coin_exists(copy queue, &type_info::type_of<CoinType>()),
            errors::invalid_fee_type()
        );

        let queue_address = object::object_address(&queue);
        let seen_aggregators = vector::empty<address>();
        let feed_hashes = vector::empty<vector<u8>>();
        let max_min_sample_size = 0;
        let feed_idx = 0;
        while (feed_idx < vector::length(aggregator_addresses)) {
            let aggregator_address = *vector::borrow(aggregator_addresses, feed_idx);
            assert!(
                !contains_address(&seen_aggregators, aggregator_address),
                errors::duplicate_aggregator()
            );
            vector::push_back(&mut seen_aggregators, aggregator_address);

            let aggregator_object = object::address_to_object<Aggregator>(aggregator_address);
            assert!(
                aggregator::aggregator_exists(aggregator_object),
                errors::aggregator_does_not_exist()
            );

            let aggregator_data = aggregator::get_aggregator(aggregator_object);
            assert!(
                aggregator::queue(&aggregator_data) == queue_address,
                errors::invalid_queue()
            );
            let current_result = aggregator::current_result(aggregator_object);
            assert!(
                timestamp_seconds > aggregator::timestamp(&current_result),
                errors::stale_result()
            );

            vector::push_back(&mut feed_hashes, aggregator::feed_hash(&aggregator_data));
            let aggregator_min_sample_size = aggregator::min_sample_size(&aggregator_data);
            if (aggregator_min_sample_size > max_min_sample_size) {
                max_min_sample_size = aggregator_min_sample_size;
            };
            feed_idx = feed_idx + 1;
        };

        assert!(
            vector::length(oracle_addresses) >= max_min_sample_size,
            errors::insufficient_signatures()
        );

        let checksum = hash::generate_v2_update_hash(
            slot,
            timestamp_seconds,
            copy feed_hashes,
            *values,
            *min_oracle_samples,
        );

        let seen_oracles = vector::empty<address>();
        let signature_idx = 0;
        while (signature_idx < vector::length(oracle_addresses)) {
            let oracle_address = *vector::borrow(oracle_addresses, signature_idx);
            assert!(
                !contains_address(&seen_oracles, oracle_address),
                errors::duplicate_oracle()
            );
            vector::push_back(&mut seen_oracles, oracle_address);

            let oracle_object = object::address_to_object<Oracle>(oracle_address);
            assert!(
                oracle::oracle_exists(oracle_object),
                errors::oracle_does_not_exist()
            );

            let oracle_data = oracle::get_oracle(oracle_object);
            assert!(oracle::queue(&oracle_data) == queue_address, errors::invalid_queue());
            assert!(
                timestamp::now_seconds() <= oracle::expiration_time(&oracle_data),
                errors::oracle_expired()
            );

            let signature = *vector::borrow(signatures, signature_idx);
            assert!(vector::length(&signature) == 65, errors::invalid_length());

            let recovery_id = vector::pop_back(&mut signature);
            let raw_signature = secp256k1::ecdsa_signature_from_bytes(signature);
            let recovered_pubkey =
                secp256k1::ecdsa_recover(copy checksum, recovery_id, &raw_signature);
            assert!(option::is_some(&recovered_pubkey), errors::invalid_signature());
            let recovered_pubkey = secp256k1::ecdsa_raw_public_key_to_bytes(
                &option::extract(&mut recovered_pubkey),
            );
            assert!(
                oracle::secp256k1_key(&oracle_data) == recovered_pubkey,
                errors::invalid_signature()
            );
            signature_idx = signature_idx + 1;
        };

        queue
    }

    fun actuate_v2<CoinType>(
        account: &signer,
        update_data: vector<u8>,
    ) {
        let (
            slot,
            timestamp_seconds,
            aggregator_addresses,
            values,
            min_oracle_samples,
            oracle_addresses,
            signatures,
        ) = serialization::parse_v2_update_bytes(update_data);

        let queue = validate_v2<CoinType>(
            slot,
            timestamp_seconds,
            &aggregator_addresses,
            &values,
            &min_oracle_samples,
            &oracle_addresses,
            &signatures,
        );

        let signature_idx = 0;
        while (signature_idx < vector::length(&oracle_addresses)) {
            let oracle_address = *vector::borrow(&oracle_addresses, signature_idx);
            let feed_idx = 0;
            while (feed_idx < vector::length(&aggregator_addresses)) {
                let aggregator = object::address_to_object<Aggregator>(
                    *vector::borrow(&aggregator_addresses, feed_idx),
                );
                let (value, neg) = decimal::unpack(*vector::borrow(&values, feed_idx));
                aggregator_submit_result_action::actuate_verified_result<CoinType>(
                    account,
                    aggregator,
                    copy queue,
                    oracle_address,
                    value,
                    neg,
                    timestamp_seconds,
                );
                feed_idx = feed_idx + 1;
            };
            signature_idx = signature_idx + 1;
        };
    }

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
            idx = idx + 1;
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

    public entry fun run_v2<CoinType>(account: &signer, update_data: vector<u8>) {
        validate();
        actuate_v2<CoinType>(account, update_data);
    }

    #[test_only]
    use std::bcs;
    #[test_only]
    use std::signer;
    #[test_only]
    use std::string;
    #[test_only]
    use aptos_framework::aptos_coin::AptosCoin;
    #[test_only]
    use switchboard::queue_override_oracle_action;
    #[test_only]
    use switchboard::oracle_init_action;

    #[test_only]
    const TEST_MAX_U128: u128 = 340282366920938463463374607431768211455;

    #[test_only]
    fun repeat_byte(byte: u8, len: u64): vector<u8> {
        let out = vector::empty<u8>();
        let idx = 0;
        while (idx < len) {
            vector::push_back(&mut out, byte);
            idx = idx + 1;
        };
        out
    }

    #[test_only]
    fun subvec(bytes: &vector<u8>, start_idx: u64, len: u64): vector<u8> {
        let out = vector::empty<u8>();
        let idx = 0;
        while (idx < len) {
            vector::push_back(&mut out, *vector::borrow(bytes, start_idx + idx));
            idx = idx + 1;
        };
        out
    }

    #[test_only]
    fun u64_be_bytes(value: u64): vector<u8> {
        let bytes = bcs::to_bytes(&value);
        vector::reverse(&mut bytes);
        bytes
    }

    #[test_only]
    fun decimal_i128_le_bytes(value: decimal::Decimal): vector<u8> {
        let (abs_value, neg) = decimal::unpack(value);
        let signed_value = if (neg) {
            TEST_MAX_U128 - abs_value + 1
        } else {
            abs_value
        };
        bcs::to_bytes(&signed_value)
    }

    #[test_only]
    fun decimal_i128_be_bytes(value: decimal::Decimal): vector<u8> {
        let bytes = decimal_i128_le_bytes(value);
        vector::reverse(&mut bytes);
        bytes
    }

    #[test_only]
    fun singleton_address(value: address): vector<address> {
        let out = vector::empty<address>();
        vector::push_back(&mut out, value);
        out
    }

    #[test_only]
    fun singleton_decimal(value: decimal::Decimal): vector<decimal::Decimal> {
        let out = vector::empty<decimal::Decimal>();
        vector::push_back(&mut out, value);
        out
    }

    #[test_only]
    fun singleton_u8(value: u8): vector<u8> {
        let out = vector::empty<u8>();
        vector::push_back(&mut out, value);
        out
    }

    #[test_only]
    fun singleton_signature(signature: vector<u8>): vector<vector<u8>> {
        let out = vector::empty<vector<u8>>();
        vector::push_back(&mut out, signature);
        out
    }

    #[test_only]
    fun build_v2_update_bytes(
        slot: u64,
        timestamp_seconds: u64,
        aggregator_addresses: vector<address>,
        values: vector<decimal::Decimal>,
        min_oracle_samples: vector<u8>,
        oracle_addresses: vector<address>,
        signatures: vector<vector<u8>>,
    ): vector<u8> {
        let update_data = bcs::to_bytes(&slot);
        vector::append(&mut update_data, bcs::to_bytes(&timestamp_seconds));
        vector::push_back(&mut update_data, vector::length(&aggregator_addresses) as u8);
        vector::push_back(&mut update_data, vector::length(&oracle_addresses) as u8);

        let feed_idx = 0;
        while (feed_idx < vector::length(&aggregator_addresses)) {
            let aggregator_address = *vector::borrow(&aggregator_addresses, feed_idx);
            vector::append(&mut update_data, bcs::to_bytes(&aggregator_address));
            vector::append(
                &mut update_data,
                decimal_i128_le_bytes(*vector::borrow(&values, feed_idx)),
            );
            vector::push_back(
                &mut update_data,
                *vector::borrow(&min_oracle_samples, feed_idx),
            );
            feed_idx = feed_idx + 1;
        };

        let signature_idx = 0;
        while (signature_idx < vector::length(&oracle_addresses)) {
            let oracle_address = *vector::borrow(&oracle_addresses, signature_idx);
            vector::append(&mut update_data, bcs::to_bytes(&oracle_address));
            vector::append(&mut update_data, *vector::borrow(&signatures, signature_idx));
            signature_idx = signature_idx + 1;
        };

        update_data
    }

    #[test_only]
    fun build_legacy_update_bytes(
        aggregator_address: address,
        value: decimal::Decimal,
        signature: vector<u8>,
        timestamp_seconds: u64,
        oracle_key: vector<u8>,
    ): vector<u8> {
        assert!(vector::length(&signature) == 65, 9000);
        let update = vector::empty<u8>();
        vector::push_back(&mut update, 1);
        vector::append(&mut update, bcs::to_bytes(&aggregator_address));
        vector::append(&mut update, decimal_i128_be_bytes(value));
        vector::append(&mut update, subvec(&signature, 0, 32));
        vector::append(&mut update, subvec(&signature, 32, 32));
        vector::push_back(&mut update, *vector::borrow(&signature, 64));
        vector::append(&mut update, repeat_byte(0, 8));
        vector::append(&mut update, u64_be_bytes(timestamp_seconds));
        vector::append(&mut update, oracle_key);
        update
    }

    #[test_only]
    fun new_test_queue(account: &signer, queue_key: vector<u8>, guardian_key: vector<u8>): Object<Queue> {
        let authority = signer::address_of(account);
        let guardian_queue_address = queue::new(
            guardian_key,
            authority,
            string::utf8(b"guardian"),
            0,
            authority,
            1,
            86400,
            @0x1,
        );
        let guardian_queue = object::address_to_object<Queue>(guardian_queue_address);
        queue::add_fee_type(copy guardian_queue, type_info::type_of<AptosCoin>());

        let queue_address = queue::new(
            queue_key,
            authority,
            string::utf8(b"queue"),
            0,
            authority,
            1,
            86400,
            guardian_queue_address,
        );
        let queue_object = object::address_to_object<Queue>(queue_address);
        queue::add_fee_type(copy queue_object, type_info::type_of<AptosCoin>());
        queue_object
    }

    #[test_only]
    fun new_test_aggregator(
        account: &signer,
        queue: Object<Queue>,
        feed_hash: vector<u8>,
        min_sample_size: u64,
        max_variance: u64,
        min_responses: u32,
    ): Object<Aggregator> {
        let aggregator_address = aggregator::new(
            account,
            object::object_address(&queue),
            string::utf8(b"aggregator"),
            feed_hash,
            min_sample_size,
            86400,
            max_variance,
            min_responses,
        );
        queue::add_aggregator(copy queue, aggregator_address);
        object::address_to_object<Aggregator>(aggregator_address)
    }

    #[test_only]
    fun new_test_oracle(
        account: &signer,
        queue: Object<Queue>,
        oracle_key: vector<u8>,
        secp256k1_key: vector<u8>,
        expiration_time_seconds: u64,
    ): Object<Oracle> {
        oracle_init_action::run(copy queue, copy oracle_key);
        let oracle_address = queue::get_oracle_from_key(copy queue, copy oracle_key);
        let oracle_object = object::address_to_object<Oracle>(oracle_address);
        queue_override_oracle_action::run(
            account,
            copy queue,
            copy oracle_object,
            secp256k1_key,
            repeat_byte(0xaa, 32),
            expiration_time_seconds,
        );
        oracle_object
    }

    #[test_only]
    fun seed_current_result(aggregator_object: Object<Aggregator>, timestamp_seconds: u64) {
        aggregator::add_result(
            aggregator_object,
            decimal::new(1, false),
            timestamp_seconds,
            @0x999,
        );
    }

    #[test(account = @0x1)]
    public fun test_run_v2_single_feed_single_signature(account: signer) {
        timestamp::set_time_has_started_for_testing(&account);
        timestamp::update_global_time_for_test_secs(1775676502);

        let queue = new_test_queue(
            &account,
            x"86807068432f186a147cf0b13a30067d386204ea9d6c8b04743ac2ef010b0752",
            repeat_byte(0x44, 32),
        );
        let aggregator_object = new_test_aggregator(
            &account,
            copy queue,
            x"bd22e0fdcebca0f206ab12c53edf3c09d0204a5a9974521d3ae940d036da91ec",
            1,
            1000000000,
            1,
        );
        let oracle_object = new_test_oracle(
            &account,
            copy queue,
            x"4b145dd321b1561f624910735c2440c2e4348d82ae2ec18fb9b65fb4b219bad5",
            x"e85523b05e0ec718342fd3682aa3efafd2ff560bc686a3201b0e394496d3a767e1025077d4a87d012c0efc62aaf422c4c27a1c784540387299a4ddf03d1e1786",
            1775677502,
        );

        let update_data = build_v2_update_bytes(
            411914623,
            1775676503,
            singleton_address(object::object_address(&aggregator_object)),
            singleton_decimal(decimal::new(928876591724118511, false)),
            singleton_u8(1),
            singleton_address(object::object_address(&oracle_object)),
            singleton_signature(
                x"149fd9d6b1c26f3e980a32456bb175a3256c8406a340e5d5c4ba7334fd54403222ccf4305465c53a557289d98bebaa6897885cf59ff08959d7fabf5335b95dbc00",
            ),
        );

        run_v2<AptosCoin>(&account, update_data);

        let current_result = aggregator::current_result(aggregator_object);
        assert!(
            aggregator::result(&current_result) == decimal::new(928876591724118511, false),
            9100
        );
        assert!(aggregator::timestamp(&current_result) == 1775676503, 9101);
    }

    #[test(account = @0x1)]
    public fun test_run_v2_multi_feed_single_signature(account: signer) {
        timestamp::set_time_has_started_for_testing(&account);
        timestamp::update_global_time_for_test_secs(1775676521);

        let queue = new_test_queue(
            &account,
            x"86807068432f186a147cf0b13a30067d386204ea9d6c8b04743ac2ef010b0752",
            repeat_byte(0x45, 32),
        );
        let truapt = new_test_aggregator(
            &account,
            copy queue,
            x"bd22e0fdcebca0f206ab12c53edf3c09d0204a5a9974521d3ae940d036da91ec",
            1,
            1000000000,
            1,
        );
        let cell = new_test_aggregator(
            &account,
            copy queue,
            x"0eccc395e537820b483bf95df78c80f61b2d997babb9d9694191f028d86d7924",
            1,
            1000000000,
            1,
        );
        let oracle_object = new_test_oracle(
            &account,
            copy queue,
            x"acb413831a0babf917c781324d0f1b31d42dac362f47a80da94daba626bb13db",
            x"2c51cd6f1c38fa3f657101df0a49eaed21f1fe5b778355eb11f13dd51f196d6308c0a53db7e9790f3a609090702541e5b2cb6019daa2261f1c1ac130421ce7a2",
            1775677521,
        );

        let aggregator_addresses = vector::empty<address>();
        vector::push_back(&mut aggregator_addresses, object::object_address(&truapt));
        vector::push_back(&mut aggregator_addresses, object::object_address(&cell));

        let values = vector::empty<decimal::Decimal>();
        vector::push_back(&mut values, decimal::new(928857611516587394, false));
        vector::push_back(&mut values, decimal::new(94173057580485, false));

        let min_oracle_samples = vector::empty<u8>();
        vector::push_back(&mut min_oracle_samples, 1);
        vector::push_back(&mut min_oracle_samples, 1);

        let update_data = build_v2_update_bytes(
            411914673,
            1775676521,
            aggregator_addresses,
            values,
            min_oracle_samples,
            singleton_address(object::object_address(&oracle_object)),
            singleton_signature(
                x"978303caa75ca4f357131d942cdde17f9aa1a7acf59c9969df89c0a6fe6de82c4b4e8eeb388c8655a8a9b42b138e7bf781cef05bbaa8fea45287b60061d9b80300",
            ),
        );

        run_v2<AptosCoin>(&account, update_data);

        let truapt_result = aggregator::current_result(truapt);
        let cell_result = aggregator::current_result(cell);
        assert!(
            aggregator::result(&truapt_result) == decimal::new(928857611516587394, false),
            9102
        );
        assert!(
            aggregator::result(&cell_result) == decimal::new(94173057580485, false),
            9103
        );
        assert!(aggregator::timestamp(&truapt_result) == 1775676521, 9104);
        assert!(aggregator::timestamp(&cell_result) == 1775676521, 9105);
    }

    #[test(account = @0x1)]
    public fun test_run_v2_single_feed_multiple_signatures(account: signer) {
        timestamp::set_time_has_started_for_testing(&account);
        timestamp::update_global_time_for_test_secs(1775676503);

        let queue = new_test_queue(
            &account,
            x"86807068432f186a147cf0b13a30067d386204ea9d6c8b04743ac2ef010b0752",
            repeat_byte(0x46, 32),
        );
        let aggregator_object = new_test_aggregator(
            &account,
            copy queue,
            x"bd22e0fdcebca0f206ab12c53edf3c09d0204a5a9974521d3ae940d036da91ec",
            2,
            1000000000,
            1,
        );
        let oracle_one = new_test_oracle(
            &account,
            copy queue,
            x"49519d81046597f86c4cc529195900eab598b817a61759bc54660924a84522ea",
            x"2db8b984c08d05ff1083833067bcff526893c5fffee23ad9dd4b76dba76af4b3486bcfc94a102edbadbc2d7f522e30b9b7ea46aef7315061bc8edb5a3725ca21",
            1775677503,
        );
        let oracle_two = new_test_oracle(
            &account,
            copy queue,
            x"acb413831a0babf917c781324d0f1b31d42dac362f47a80da94daba626bb13db",
            x"2c51cd6f1c38fa3f657101df0a49eaed21f1fe5b778355eb11f13dd51f196d6308c0a53db7e9790f3a609090702541e5b2cb6019daa2261f1c1ac130421ce7a2",
            1775677503,
        );

        let oracle_addresses = vector::empty<address>();
        vector::push_back(&mut oracle_addresses, object::object_address(&oracle_one));
        vector::push_back(&mut oracle_addresses, object::object_address(&oracle_two));

        let signatures = vector::empty<vector<u8>>();
        vector::push_back(
            &mut signatures,
            x"189b585a823b22f7f06a2e308e6104787e9ddbb6cb0d6e77feb28118992b39ab431dd14633b00c81d4d908566ef7a0c2da770cee9713be56296d32ee93e0d66801",
        );
        vector::push_back(
            &mut signatures,
            x"d847ab5f2ba4254b4d282ba3ffe0117a809234432735322c20b251ee90a72d422ebb9469ca888e28d093634763d8ce39bd9fcc74d06794f3a95f6e62dd23633900",
        );

        let update_data = build_v2_update_bytes(
            411914622,
            1775676503,
            singleton_address(object::object_address(&aggregator_object)),
            singleton_decimal(decimal::new(928876591724118511, false)),
            singleton_u8(1),
            oracle_addresses,
            signatures,
        );

        run_v2<AptosCoin>(&account, update_data);

        let current_result = aggregator::current_result(aggregator_object);
        assert!(
            aggregator::result(&current_result) == decimal::new(928876591724118511, false),
            9106
        );
        assert!(aggregator::timestamp(&current_result) == 1775676503, 9107);
    }

    #[test(account = @0x1)]
    #[expected_failure(abort_code = 1339, location = switchboard::serialization)]
    public fun test_run_v2_rejects_truncated_header(account: signer) {
        timestamp::set_time_has_started_for_testing(&account);
        run_v2<AptosCoin>(&account, repeat_byte(0, 17));
    }

    #[test(account = @0x1)]
    #[expected_failure(abort_code = 8, location = Self)]
    public fun test_run_v2_rejects_zero_timestamp(account: signer) {
        timestamp::set_time_has_started_for_testing(&account);
        timestamp::update_global_time_for_test_secs(1775676502);

        let queue = new_test_queue(&account, repeat_byte(0x10, 32), repeat_byte(0x11, 32));
        let aggregator_object = new_test_aggregator(
            &account,
            copy queue,
            repeat_byte(0x12, 32),
            1,
            1000000000,
            1,
        );
        let oracle_object = new_test_oracle(
            &account,
            copy queue,
            repeat_byte(0x13, 32),
            x"e85523b05e0ec718342fd3682aa3efafd2ff560bc686a3201b0e394496d3a767e1025077d4a87d012c0efc62aaf422c4c27a1c784540387299a4ddf03d1e1786",
            1775677502,
        );

        let update_data = build_v2_update_bytes(
            1,
            0,
            singleton_address(object::object_address(&aggregator_object)),
            singleton_decimal(decimal::new(10, false)),
            singleton_u8(1),
            singleton_address(object::object_address(&oracle_object)),
            singleton_signature(repeat_byte(0xaa, 65)),
        );

        run_v2<AptosCoin>(&account, update_data);
    }

    #[test(account = @0x1)]
    #[expected_failure(abort_code = 24, location = Self)]
    public fun test_run_v2_rejects_duplicate_aggregators(account: signer) {
        timestamp::set_time_has_started_for_testing(&account);
        timestamp::update_global_time_for_test_secs(1775676502);

        let queue = new_test_queue(&account, repeat_byte(0x20, 32), repeat_byte(0x21, 32));
        let aggregator_object = new_test_aggregator(
            &account,
            copy queue,
            repeat_byte(0x22, 32),
            1,
            1000000000,
            1,
        );

        let aggregators = vector::empty<address>();
        vector::push_back(&mut aggregators, object::object_address(&aggregator_object));
        vector::push_back(&mut aggregators, object::object_address(&aggregator_object));

        let values = vector::empty<decimal::Decimal>();
        vector::push_back(&mut values, decimal::new(10, false));
        vector::push_back(&mut values, decimal::new(10, false));

        let min_oracle_samples = vector::empty<u8>();
        vector::push_back(&mut min_oracle_samples, 1);
        vector::push_back(&mut min_oracle_samples, 1);

        let update_data = build_v2_update_bytes(
            1,
            1,
            aggregators,
            values,
            min_oracle_samples,
            singleton_address(@0x123),
            singleton_signature(repeat_byte(0xaa, 65)),
        );

        run_v2<AptosCoin>(&account, update_data);
    }

    #[test(account = @0x1)]
    #[expected_failure(abort_code = 25, location = Self)]
    public fun test_run_v2_rejects_insufficient_signatures(account: signer) {
        timestamp::set_time_has_started_for_testing(&account);
        timestamp::update_global_time_for_test_secs(1775676502);

        let queue = new_test_queue(&account, repeat_byte(0x30, 32), repeat_byte(0x31, 32));
        let aggregator_object = new_test_aggregator(
            &account,
            copy queue,
            repeat_byte(0x32, 32),
            2,
            1000000000,
            1,
        );
        let oracle_object = new_test_oracle(
            &account,
            copy queue,
            repeat_byte(0x33, 32),
            x"e85523b05e0ec718342fd3682aa3efafd2ff560bc686a3201b0e394496d3a767e1025077d4a87d012c0efc62aaf422c4c27a1c784540387299a4ddf03d1e1786",
            1775677502,
        );

        let update_data = build_v2_update_bytes(
            1,
            2,
            singleton_address(object::object_address(&aggregator_object)),
            singleton_decimal(decimal::new(10, false)),
            singleton_u8(1),
            singleton_address(object::object_address(&oracle_object)),
            singleton_signature(repeat_byte(0xaa, 65)),
        );

        run_v2<AptosCoin>(&account, update_data);
    }

    #[test(account = @0x1)]
    #[expected_failure(abort_code = 5, location = Self)]
    public fun test_run_v2_rejects_mixed_queue_batches(account: signer) {
        timestamp::set_time_has_started_for_testing(&account);
        timestamp::update_global_time_for_test_secs(1775676502);

        let queue_one = new_test_queue(&account, repeat_byte(0x40, 32), repeat_byte(0x41, 32));
        let queue_two = new_test_queue(&account, repeat_byte(0x42, 32), repeat_byte(0x43, 32));
        let aggregator_one = new_test_aggregator(
            &account,
            copy queue_one,
            repeat_byte(0x44, 32),
            1,
            1000000000,
            1,
        );
        let aggregator_two = new_test_aggregator(
            &account,
            copy queue_two,
            repeat_byte(0x45, 32),
            1,
            1000000000,
            1,
        );

        let aggregators = vector::empty<address>();
        vector::push_back(&mut aggregators, object::object_address(&aggregator_one));
        vector::push_back(&mut aggregators, object::object_address(&aggregator_two));

        let values = vector::empty<decimal::Decimal>();
        vector::push_back(&mut values, decimal::new(10, false));
        vector::push_back(&mut values, decimal::new(11, false));

        let min_oracle_samples = vector::empty<u8>();
        vector::push_back(&mut min_oracle_samples, 1);
        vector::push_back(&mut min_oracle_samples, 1);

        let update_data = build_v2_update_bytes(
            1,
            2,
            aggregators,
            values,
            min_oracle_samples,
            singleton_address(@0x456),
            singleton_signature(repeat_byte(0xaa, 65)),
        );

        run_v2<AptosCoin>(&account, update_data);
    }

    #[test(account = @0x1)]
    #[expected_failure(abort_code = 23, location = Self)]
    public fun test_run_v2_rejects_duplicate_oracles(account: signer) {
        timestamp::set_time_has_started_for_testing(&account);
        timestamp::update_global_time_for_test_secs(1775676502);

        let queue = new_test_queue(
            &account,
            x"86807068432f186a147cf0b13a30067d386204ea9d6c8b04743ac2ef010b0752",
            repeat_byte(0x47, 32),
        );
        let aggregator_object = new_test_aggregator(
            &account,
            copy queue,
            x"bd22e0fdcebca0f206ab12c53edf3c09d0204a5a9974521d3ae940d036da91ec",
            1,
            1000000000,
            1,
        );
        let oracle_object = new_test_oracle(
            &account,
            copy queue,
            x"4b145dd321b1561f624910735c2440c2e4348d82ae2ec18fb9b65fb4b219bad5",
            x"e85523b05e0ec718342fd3682aa3efafd2ff560bc686a3201b0e394496d3a767e1025077d4a87d012c0efc62aaf422c4c27a1c784540387299a4ddf03d1e1786",
            1775677502,
        );

        let oracle_addresses = vector::empty<address>();
        vector::push_back(&mut oracle_addresses, object::object_address(&oracle_object));
        vector::push_back(&mut oracle_addresses, object::object_address(&oracle_object));

        let signatures = vector::empty<vector<u8>>();
        vector::push_back(
            &mut signatures,
            x"149fd9d6b1c26f3e980a32456bb175a3256c8406a340e5d5c4ba7334fd54403222ccf4305465c53a557289d98bebaa6897885cf59ff08959d7fabf5335b95dbc00",
        );
        vector::push_back(
            &mut signatures,
            x"149fd9d6b1c26f3e980a32456bb175a3256c8406a340e5d5c4ba7334fd54403222ccf4305465c53a557289d98bebaa6897885cf59ff08959d7fabf5335b95dbc00",
        );

        let update_data = build_v2_update_bytes(
            411914623,
            1775676503,
            singleton_address(object::object_address(&aggregator_object)),
            singleton_decimal(decimal::new(928876591724118511, false)),
            singleton_u8(1),
            oracle_addresses,
            signatures,
        );

        run_v2<AptosCoin>(&account, update_data);
    }

    #[test(account = @0x1)]
    #[expected_failure(abort_code = 393223, location = 0x1::object)]
    public fun test_run_v2_rejects_unknown_oracles(account: signer) {
        timestamp::set_time_has_started_for_testing(&account);
        timestamp::update_global_time_for_test_secs(1775676502);

        let queue = new_test_queue(&account, repeat_byte(0x47, 32), repeat_byte(0x48, 32));
        let aggregator_object = new_test_aggregator(
            &account,
            copy queue,
            repeat_byte(0x49, 32),
            1,
            1000000000,
            1,
        );

        let update_data = build_v2_update_bytes(
            1,
            1775676503,
            singleton_address(object::object_address(&aggregator_object)),
            singleton_decimal(decimal::new(10, false)),
            singleton_u8(1),
            singleton_address(object::object_address(&queue)),
            singleton_signature(repeat_byte(0xaa, 65)),
        );

        run_v2<AptosCoin>(&account, update_data);
    }

    #[test(account = @0x1)]
    #[expected_failure(abort_code = 5, location = Self)]
    public fun test_run_v2_rejects_oracles_from_wrong_queue(account: signer) {
        timestamp::set_time_has_started_for_testing(&account);
        timestamp::update_global_time_for_test_secs(1775676502);

        let queue_one = new_test_queue(&account, repeat_byte(0x4a, 32), repeat_byte(0x4b, 32));
        let queue_two = new_test_queue(&account, repeat_byte(0x4c, 32), repeat_byte(0x4d, 32));
        let aggregator_object = new_test_aggregator(
            &account,
            copy queue_one,
            repeat_byte(0x4e, 32),
            1,
            1000000000,
            1,
        );
        let wrong_queue_oracle = new_test_oracle(
            &account,
            copy queue_two,
            repeat_byte(0x4f, 32),
            x"e85523b05e0ec718342fd3682aa3efafd2ff560bc686a3201b0e394496d3a767e1025077d4a87d012c0efc62aaf422c4c27a1c784540387299a4ddf03d1e1786",
            1775677502,
        );

        let update_data = build_v2_update_bytes(
            1,
            1775676503,
            singleton_address(object::object_address(&aggregator_object)),
            singleton_decimal(decimal::new(10, false)),
            singleton_u8(1),
            singleton_address(object::object_address(&wrong_queue_oracle)),
            singleton_signature(repeat_byte(0xaa, 65)),
        );

        run_v2<AptosCoin>(&account, update_data);
    }

    #[test(account = @0x1)]
    #[expected_failure(abort_code = 15, location = Self)]
    public fun test_run_v2_rejects_expired_oracles(account: signer) {
        timestamp::set_time_has_started_for_testing(&account);
        timestamp::update_global_time_for_test_secs(1775676502);

        let queue = new_test_queue(&account, repeat_byte(0x50, 32), repeat_byte(0x51, 32));
        let aggregator_object = new_test_aggregator(
            &account,
            copy queue,
            repeat_byte(0x52, 32),
            1,
            1000000000,
            1,
        );
        let expired_oracle = new_test_oracle(
            &account,
            copy queue,
            repeat_byte(0x53, 32),
            x"e85523b05e0ec718342fd3682aa3efafd2ff560bc686a3201b0e394496d3a767e1025077d4a87d012c0efc62aaf422c4c27a1c784540387299a4ddf03d1e1786",
            1775676503,
        );
        timestamp::update_global_time_for_test_secs(1775676504);

        let update_data = build_v2_update_bytes(
            1,
            1775676505,
            singleton_address(object::object_address(&aggregator_object)),
            singleton_decimal(decimal::new(10, false)),
            singleton_u8(1),
            singleton_address(object::object_address(&expired_oracle)),
            singleton_signature(repeat_byte(0xaa, 65)),
        );

        run_v2<AptosCoin>(&account, update_data);
    }

    #[test(account = @0x1)]
    #[expected_failure(abort_code = 22, location = Self)]
    public fun test_run_v2_rejects_invalid_signatures(account: signer) {
        timestamp::set_time_has_started_for_testing(&account);
        timestamp::update_global_time_for_test_secs(1775676502);

        let queue = new_test_queue(
            &account,
            x"86807068432f186a147cf0b13a30067d386204ea9d6c8b04743ac2ef010b0752",
            repeat_byte(0x48, 32),
        );
        let aggregator_object = new_test_aggregator(
            &account,
            copy queue,
            x"bd22e0fdcebca0f206ab12c53edf3c09d0204a5a9974521d3ae940d036da91ec",
            1,
            1000000000,
            1,
        );
        let oracle_object = new_test_oracle(
            &account,
            copy queue,
            x"4b145dd321b1561f624910735c2440c2e4348d82ae2ec18fb9b65fb4b219bad5",
            x"e85523b05e0ec718342fd3682aa3efafd2ff560bc686a3201b0e394496d3a767e1025077d4a87d012c0efc62aaf422c4c27a1c784540387299a4ddf03d1e1786",
            1775677502,
        );

        let invalid_signature =
            x"149fd9d6b1c26f3e980a32456bb175a3256c8406a340e5d5c4ba7334fd54403222ccf4305465c53a557289d98bebaa6897885cf59ff08959d7fabf5335b95dbc01";

        let update_data = build_v2_update_bytes(
            411914623,
            1775676503,
            singleton_address(object::object_address(&aggregator_object)),
            singleton_decimal(decimal::new(928876591724118511, false)),
            singleton_u8(1),
            singleton_address(object::object_address(&oracle_object)),
            singleton_signature(invalid_signature),
        );

        run_v2<AptosCoin>(&account, update_data);
    }

    #[test(account = @0x1)]
    #[expected_failure(abort_code = 22, location = Self)]
    public fun test_run_v2_rejects_signatures_with_wrong_values(account: signer) {
        timestamp::set_time_has_started_for_testing(&account);
        timestamp::update_global_time_for_test_secs(1775676502);

        let queue = new_test_queue(
            &account,
            x"86807068432f186a147cf0b13a30067d386204ea9d6c8b04743ac2ef010b0752",
            repeat_byte(0x49, 32),
        );
        let aggregator_object = new_test_aggregator(
            &account,
            copy queue,
            x"bd22e0fdcebca0f206ab12c53edf3c09d0204a5a9974521d3ae940d036da91ec",
            1,
            1000000000,
            1,
        );
        let oracle_object = new_test_oracle(
            &account,
            copy queue,
            x"4b145dd321b1561f624910735c2440c2e4348d82ae2ec18fb9b65fb4b219bad5",
            x"e85523b05e0ec718342fd3682aa3efafd2ff560bc686a3201b0e394496d3a767e1025077d4a87d012c0efc62aaf422c4c27a1c784540387299a4ddf03d1e1786",
            1775677502,
        );

        let update_data = build_v2_update_bytes(
            411914623,
            1775676503,
            singleton_address(object::object_address(&aggregator_object)),
            singleton_decimal(decimal::new(928876591724118512, false)),
            singleton_u8(1),
            singleton_address(object::object_address(&oracle_object)),
            singleton_signature(
                x"149fd9d6b1c26f3e980a32456bb175a3256c8406a340e5d5c4ba7334fd54403222ccf4305465c53a557289d98bebaa6897885cf59ff08959d7fabf5335b95dbc00",
            ),
        );

        run_v2<AptosCoin>(&account, update_data);
    }

    #[test(account = @0x1)]
    #[expected_failure(abort_code = 22, location = Self)]
    public fun test_run_v2_rejects_signatures_with_wrong_slots(account: signer) {
        timestamp::set_time_has_started_for_testing(&account);
        timestamp::update_global_time_for_test_secs(1775676502);

        let queue = new_test_queue(
            &account,
            x"86807068432f186a147cf0b13a30067d386204ea9d6c8b04743ac2ef010b0752",
            repeat_byte(0x4a, 32),
        );
        let aggregator_object = new_test_aggregator(
            &account,
            copy queue,
            x"bd22e0fdcebca0f206ab12c53edf3c09d0204a5a9974521d3ae940d036da91ec",
            1,
            1000000000,
            1,
        );
        let oracle_object = new_test_oracle(
            &account,
            copy queue,
            x"4b145dd321b1561f624910735c2440c2e4348d82ae2ec18fb9b65fb4b219bad5",
            x"e85523b05e0ec718342fd3682aa3efafd2ff560bc686a3201b0e394496d3a767e1025077d4a87d012c0efc62aaf422c4c27a1c784540387299a4ddf03d1e1786",
            1775677502,
        );

        let update_data = build_v2_update_bytes(
            411914624,
            1775676503,
            singleton_address(object::object_address(&aggregator_object)),
            singleton_decimal(decimal::new(928876591724118511, false)),
            singleton_u8(1),
            singleton_address(object::object_address(&oracle_object)),
            singleton_signature(
                x"149fd9d6b1c26f3e980a32456bb175a3256c8406a340e5d5c4ba7334fd54403222ccf4305465c53a557289d98bebaa6897885cf59ff08959d7fabf5335b95dbc00",
            ),
        );

        run_v2<AptosCoin>(&account, update_data);
    }

    #[test(account = @0x1)]
    #[expected_failure(abort_code = 22, location = Self)]
    public fun test_run_v2_rejects_signatures_with_wrong_feed_order(account: signer) {
        timestamp::set_time_has_started_for_testing(&account);
        timestamp::update_global_time_for_test_secs(1775676521);

        let queue = new_test_queue(
            &account,
            x"86807068432f186a147cf0b13a30067d386204ea9d6c8b04743ac2ef010b0752",
            repeat_byte(0x4b, 32),
        );
        let truapt = new_test_aggregator(
            &account,
            copy queue,
            x"bd22e0fdcebca0f206ab12c53edf3c09d0204a5a9974521d3ae940d036da91ec",
            1,
            1000000000,
            1,
        );
        let cell = new_test_aggregator(
            &account,
            copy queue,
            x"0eccc395e537820b483bf95df78c80f61b2d997babb9d9694191f028d86d7924",
            1,
            1000000000,
            1,
        );
        let oracle_object = new_test_oracle(
            &account,
            copy queue,
            x"acb413831a0babf917c781324d0f1b31d42dac362f47a80da94daba626bb13db",
            x"2c51cd6f1c38fa3f657101df0a49eaed21f1fe5b778355eb11f13dd51f196d6308c0a53db7e9790f3a609090702541e5b2cb6019daa2261f1c1ac130421ce7a2",
            1775677521,
        );

        let aggregator_addresses = vector::empty<address>();
        vector::push_back(&mut aggregator_addresses, object::object_address(&cell));
        vector::push_back(&mut aggregator_addresses, object::object_address(&truapt));

        let values = vector::empty<decimal::Decimal>();
        vector::push_back(&mut values, decimal::new(94173057580485, false));
        vector::push_back(&mut values, decimal::new(928857611516587394, false));

        let min_oracle_samples = vector::empty<u8>();
        vector::push_back(&mut min_oracle_samples, 1);
        vector::push_back(&mut min_oracle_samples, 1);

        let update_data = build_v2_update_bytes(
            411914673,
            1775676521,
            aggregator_addresses,
            values,
            min_oracle_samples,
            singleton_address(object::object_address(&oracle_object)),
            singleton_signature(
                x"978303caa75ca4f357131d942cdde17f9aa1a7acf59c9969df89c0a6fe6de82c4b4e8eeb388c8655a8a9b42b138e7bf781cef05bbaa8fea45287b60061d9b80300",
            ),
        );

        run_v2<AptosCoin>(&account, update_data);
    }

    #[test(account = @0x1)]
    #[expected_failure(abort_code = 1340, location = switchboard::serialization)]
    public fun test_run_v2_rejects_invalid_signature_lengths(account: signer) {
        timestamp::set_time_has_started_for_testing(&account);
        timestamp::update_global_time_for_test_secs(1775676502);

        let queue = new_test_queue(&account, repeat_byte(0x50, 32), repeat_byte(0x51, 32));
        let aggregator_object = new_test_aggregator(
            &account,
            copy queue,
            repeat_byte(0x52, 32),
            1,
            1000000000,
            1,
        );
        let oracle_object = new_test_oracle(
            &account,
            copy queue,
            repeat_byte(0x53, 32),
            x"e85523b05e0ec718342fd3682aa3efafd2ff560bc686a3201b0e394496d3a767e1025077d4a87d012c0efc62aaf422c4c27a1c784540387299a4ddf03d1e1786",
            1775677502,
        );

        let update_data = build_v2_update_bytes(
            1,
            2,
            singleton_address(object::object_address(&aggregator_object)),
            singleton_decimal(decimal::new(10, false)),
            singleton_u8(1),
            singleton_address(object::object_address(&oracle_object)),
            singleton_signature(repeat_byte(0xaa, 64)),
        );

        run_v2<AptosCoin>(&account, update_data);
    }

    #[test(account = @0x1)]
    #[expected_failure(abort_code = 26, location = Self)]
    public fun test_run_v2_rejects_stale_timestamps(account: signer) {
        timestamp::set_time_has_started_for_testing(&account);
        timestamp::update_global_time_for_test_secs(1775676502);

        let queue = new_test_queue(&account, repeat_byte(0x60, 32), repeat_byte(0x61, 32));
        let aggregator_object = new_test_aggregator(
            &account,
            copy queue,
            repeat_byte(0x62, 32),
            1,
            1000000000,
            1,
        );
        seed_current_result(copy aggregator_object, 1775676503);

        let update_data = build_v2_update_bytes(
            1,
            1775676503,
            singleton_address(object::object_address(&aggregator_object)),
            singleton_decimal(decimal::new(10, false)),
            singleton_u8(1),
            singleton_address(@0x1234),
            singleton_signature(repeat_byte(0xaa, 65)),
        );

        run_v2<AptosCoin>(&account, update_data);
    }

    #[test(account = @0x1)]
    public fun test_legacy_run_accepts_signed_fixture(account: signer) {
        timestamp::set_time_has_started_for_testing(&account);
        timestamp::update_global_time_for_test_secs(1731467943);

        let queue = new_test_queue(
            &account,
            x"c9477bfb5ff1012859f336cf98725680e7705ba2abece17188cfb28ca66ca5b0",
            repeat_byte(0x62, 32),
        );
        let aggregator_object = new_test_aggregator(
            &account,
            copy queue,
            x"2f24a24ce00a336bbf75f5e25086d32a6eb1d8717a013cf4f47610168405cd13",
            1,
            1000000000,
            1,
        );
        let oracle_key = repeat_byte(0xab, 32);
        let _oracle_object = new_test_oracle(
            &account,
            copy queue,
            copy oracle_key,
            x"072814bfdd26bcbeb9ecd2872f77b51012b11909726ce2ba64b3634f43d0ea12fa01e2fffe1c3b54305a83fe3365d4ee0579e98382ff9b4fb1e22baaee95dc7c",
            1731468943,
        );

        let update = build_legacy_update_bytes(
            object::object_address(&aggregator_object),
            decimal::new(88225582514986682302807, false),
            x"6cfbd56d878eb2e4ad74e27584a5ab99558f9092f0140fbeab43ced3680eff9e3853db14e03ed38679161b43aae490bb5a467617c620e531c0de50940cd93aa501",
            1731467944,
            oracle_key,
        );

        let updates = vector::empty<vector<u8>>();
        vector::push_back(&mut updates, update);
        run<AptosCoin>(&account, updates);

        let current_result = aggregator::current_result(aggregator_object);
        assert!(
            aggregator::result(&current_result) == decimal::new(88225582514986682302807, false),
            9108
        );
        assert!(aggregator::timestamp(&current_result) == 1731467944, 9109);
    }

    #[test(account = @0x1)]
    public fun test_extract_and_run_preserves_non_switchboard_payloads(account: signer) {
        timestamp::set_time_has_started_for_testing(&account);
        timestamp::update_global_time_for_test_secs(1731467943);

        let queue = new_test_queue(
            &account,
            x"c9477bfb5ff1012859f336cf98725680e7705ba2abece17188cfb28ca66ca5b0",
            repeat_byte(0x63, 32),
        );
        let aggregator_object = new_test_aggregator(
            &account,
            copy queue,
            x"2f24a24ce00a336bbf75f5e25086d32a6eb1d8717a013cf4f47610168405cd13",
            1,
            1000000000,
            1,
        );
        let oracle_key = repeat_byte(0xac, 32);
        let _oracle_object = new_test_oracle(
            &account,
            copy queue,
            copy oracle_key,
            x"072814bfdd26bcbeb9ecd2872f77b51012b11909726ce2ba64b3634f43d0ea12fa01e2fffe1c3b54305a83fe3365d4ee0579e98382ff9b4fb1e22baaee95dc7c",
            1731468943,
        );

        let payloads = vector::empty<vector<u8>>();
        vector::push_back(
            &mut payloads,
            build_legacy_update_bytes(
                object::object_address(&aggregator_object),
                decimal::new(88225582514986682302807, false),
                x"6cfbd56d878eb2e4ad74e27584a5ab99558f9092f0140fbeab43ced3680eff9e3853db14e03ed38679161b43aae490bb5a467617c620e531c0de50940cd93aa501",
                1731467944,
                oracle_key,
            ),
        );
        vector::push_back(&mut payloads, x"998877");

        extract_and_run<AptosCoin>(&account, &mut payloads);

        let current_result = aggregator::current_result(aggregator_object);
        assert!(
            aggregator::result(&current_result) == decimal::new(88225582514986682302807, false),
            9110
        );
        assert!(vector::length(&payloads) == 1, 9111);
        assert!(*vector::borrow(&payloads, 0) == x"998877", 9112);
    }

}
