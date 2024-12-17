module switchboard::aggregator_submit_result_action {
    use std::type_info;
    use std::vector;
    use std::option;
    use aptos_std::secp256k1;
    use aptos_std::event;
    use aptos_framework::object::{Self, Object};
    use aptos_framework::timestamp;
    use aptos_framework::aptos_account;
    use switchboard::aggregator::{Self, Aggregator};
    use switchboard::queue::{Self, Queue};
    use switchboard::oracle::{Self, Oracle};
    use switchboard::errors;
    use switchboard::decimal::{Self, Decimal};
    use switchboard::hash;

    #[event]
    struct AggregatorUpdated has drop, store {
        aggregator: address,
        oracle: address,
        value: Decimal,
        timestamp: u64,
    }

    #[event]
    struct AggregatorUpdateFailed has drop, store {
        aggregator: address,
        oracle: address,
        value: Decimal,
        timestamp: u64,
    }

    struct AggregatorSubmitResultParams {
        aggregator: Object<Aggregator>,
        queue: Object<Queue>,
        oracle: Object<Oracle>,
        value: u128,
        neg: bool,
        timestamp_seconds: u64,
        signature: vector<u8>,
    }

    fun params(
        aggregator: Object<Aggregator>,
        queue: Object<Queue>,
        oracle: Object<Oracle>,
        value: u128,
        neg: bool,
        timestamp_seconds: u64,
        signature: vector<u8>,
    ): AggregatorSubmitResultParams {
        AggregatorSubmitResultParams {
            aggregator,
            queue,
            oracle,
            value,
            neg,
            timestamp_seconds,
            signature,   
        }
    }

    public fun validate<CoinType>(
        params: &mut AggregatorSubmitResultParams,
    ): (bool, address) {

        //===============================================================================
        // Basic validity checks
        //===============================================================================

        // check that value is not zero
        assert!(params.timestamp_seconds > 0, errors::invalid_timestamp());

        // check that signature is valid
        assert!(vector::length(&params.signature) == 65, errors::invalid_length());

        // check elements exist
        assert!(aggregator::aggregator_exists(params.aggregator), errors::aggregator_does_not_exist());
        assert!(queue::queue_exists(params.queue), errors::queue_does_not_exist());
        assert!(oracle::oracle_exists(params.oracle), errors::oracle_does_not_exist());

        // get the aggregator state
        let aggregator = aggregator::get_aggregator(params.aggregator);

        // check that oracle queue is aggregator queue
        assert!(object::object_address(&params.queue) == aggregator::queue(&aggregator), errors::invalid_queue());

        // check that fee coin exists
        assert!(queue::fee_coin_exists(params.queue, &type_info::type_of<CoinType>()), errors::invalid_fee_type());

        //===============================================================================
        // Signature validity checks
        //===============================================================================

        // check the signature
        let msg = hash::generate_update_hash(
            &decimal::new(params.value, params.neg),
            queue::queue_key(params.queue),
            aggregator::feed_hash(&aggregator),
            x"0000000000000000000000000000000000000000000000000000000000000000",
            aggregator::max_variance(&aggregator),
            aggregator::min_responses(&aggregator),
            params.timestamp_seconds,
        );

        // check the signature
        let recovery_id = vector::pop_back(&mut params.signature);
        let signature = params.signature;
        let signature = secp256k1::ecdsa_signature_from_bytes(signature);
        let recovered_pubkey = secp256k1::ecdsa_recover(
            msg, 
            recovery_id,
            &signature, 
        );

        // check that the recovered pubkey is valid
        if (option::is_none(&recovered_pubkey)) {
            return (false, @0x0)
        };

        // check the oracle key
        let recovered_pubkey = secp256k1::ecdsa_raw_public_key_to_bytes(&option::extract(&mut recovered_pubkey));

        //================================================================================
        // Oracle validity checks
        //================================================================================
        
        // get the oracle object
        let oracle = oracle::get_oracle(params.oracle);
        let oracle_address = object::object_address(&params.oracle);

        // check that recovered pubkey is oracle's signing key
        if (oracle::secp256k1_key(&oracle) != recovered_pubkey) {
            return (false, @0x0)
        };

        // check that queue is equivalent to oracle queue
        if (object::object_address(&params.queue) != oracle::queue(&oracle)) {
            return (false, oracle_address)
        };
        
        // make sure that oracle is still valid
        if (timestamp::now_seconds() > oracle::expiration_time(&oracle)) {
            return (false, oracle_address)
        };
        
        // success
        (true, oracle_address)
    }

    fun actuate<CoinType>(
        account: &signer,
        params: AggregatorSubmitResultParams,
        extracted_oracle_address: address,
    ) {
        let aggregator_address = object::object_address(&params.aggregator);
        let AggregatorSubmitResultParams {
            aggregator: aggregator_object,
            queue,
            oracle: _,
            value,
            neg,
            timestamp_seconds,
            signature: _,
        } = params;

        // add result to aggregator
        aggregator::add_result(
            aggregator_object,
            decimal::new(value, neg),
            timestamp_seconds,
            extracted_oracle_address,
        );

        // send fee to fee recipient if there is a fee
        let fee = queue::fee(copy queue);
        if (fee > 0) {
            let fee_recipient = queue::fee_recipient(copy queue);
            aptos_account::transfer_coins<CoinType>(
                account,
                fee_recipient,
                fee,
            );
        };

        // emit event
        event::emit(AggregatorUpdated {
            aggregator: aggregator_address,
            oracle: extracted_oracle_address,
            value: decimal::new(value, neg),
            timestamp: timestamp_seconds,
        });
    }

    public entry fun run<CoinType>(
        account: &signer,
        aggregator: Object<Aggregator>,
        queue: Object<Queue>,
        oracle: Object<Oracle>,
        value: u128,
        neg: bool,
        timestamp_seconds: u64,
        signature: vector<u8>,
    ) {
      let params = params(
        aggregator,
        queue,
        oracle,
        value,
        neg,
        timestamp_seconds,
        signature,
      );
      let (success, extracted_oracle_address) = validate<CoinType>(&mut params);
      if (success) {
        actuate<CoinType>(account, params, extracted_oracle_address);
      } else {
        let AggregatorSubmitResultParams {
            aggregator,
            value,
            timestamp_seconds,
            neg,
            signature: _,
            queue: _,
            oracle: _,
        } = params;
        event::emit(AggregatorUpdateFailed {
            aggregator: object::object_address(&aggregator),
            value: decimal::new(value, neg),
            timestamp: timestamp_seconds,
            oracle: extracted_oracle_address,
        });
      }

    }
}