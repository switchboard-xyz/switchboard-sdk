module switchboard::oracle_attest_action {
    use std::vector;
    use std::option;
    use aptos_std::secp256k1;
    use aptos_std::event;
    use aptos_framework::object::{Self, Object};
    use aptos_framework::timestamp;
    use switchboard::queue::{Self, Queue};
    use switchboard::oracle::{Self, Oracle};
    use switchboard::errors;
    use switchboard::hash;

    const ATTESTATION_VALIDITY_SECONDS: u64 = 60 * 60 * 10;

    #[event]
    struct AttestationCreated has drop, store {
        oracle: address,
        guardian: address,
        secp256k1_key: vector<u8>,
        timestamp: u64,
    }

    struct OracleAttestParams {
        oracle: Object<Oracle>,
        queue: Object<Queue>,
        guardian: Object<Oracle>,
        timestamp_seconds: u64,
        mr_enclave: vector<u8>,
        secp256k1_key: vector<u8>,
        signature: vector<u8>,
    }

    fun params(
        oracle: Object<Oracle>,
        queue: Object<Queue>,
        guardian: Object<Oracle>,
        timestamp_seconds: u64,
        mr_enclave: vector<u8>,
        secp256k1_key: vector<u8>,
        signature: vector<u8>,

    ): OracleAttestParams {
        OracleAttestParams {
            oracle,
            queue,
            guardian,
            timestamp_seconds,
            mr_enclave,
            secp256k1_key,
            signature,
        }
    }

    public fun validate(
        params: &mut OracleAttestParams,
    ): bool {

        assert!(queue::queue_exists(params.queue), errors::queue_does_not_exist());
        assert!(oracle::oracle_exists(params.oracle), errors::oracle_does_not_exist());
        assert!(oracle::oracle_exists(params.guardian), errors::guardian_does_not_exist());

        // pull the oracle from the oracle object
        let oracle = oracle::get_oracle(params.oracle);

        // pull the guardian from the guardian object
        let guardian = oracle::get_oracle(params.guardian);

        assert!(vector::length(&params.mr_enclave) == 32, errors::invalid_length());
        assert!(vector::length(&params.secp256k1_key) == 64, errors::invalid_length());
        assert!(vector::length(&params.signature) == 65, errors::invalid_length());
        assert!(object::object_address(&params.queue) == oracle::queue(&oracle), errors::invalid_queue());
        assert!(queue::guardian_queue(params.queue) == oracle::queue(&guardian), errors::invalid_queue());

        if (timestamp::now_seconds() > oracle::expiration_time(&guardian)) {
            return false
        };

        if (params.timestamp_seconds + ATTESTATION_VALIDITY_SECONDS < timestamp::now_seconds()) {
            return false
        };

        let msg = hash::generate_attestation_hash(
            oracle::oracle_key(&oracle),
            queue::queue_key(params.queue),
            params.mr_enclave,
            x"0000000000000000000000000000000000000000000000000000000000000000",
            params.secp256k1_key,
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
            return false
        };

        // check the oracle key
        let recovered_pubkey = secp256k1::ecdsa_raw_public_key_to_bytes(&option::extract(&mut recovered_pubkey));
        let oracle_key = oracle::oracle_key(&guardian);
        if (recovered_pubkey != oracle_key) {
            return false
        };
    
        // success
        true
    }

    fun actuate(
        params: OracleAttestParams,
    ) {
        let OracleAttestParams {
            oracle: oracle_object,
            queue: queue_object,
            guardian: guardian_object,
            timestamp_seconds,
            mr_enclave,
            secp256k1_key,
            signature: _,
        } = params;

        let attestation = oracle::new_attestation( 
            object::object_address(&guardian_object),
            secp256k1_key,
            timestamp_seconds,
        );

        oracle::add_attestation(oracle_object, attestation, timestamp::now_seconds());

        // grab oracle snapshot and check if it should be enabled
        let oracle = oracle::get_oracle(oracle_object);
        let valid_attestations = oracle::valid_attestation_count(&oracle, secp256k1_key);

        // if the oracle has enough attestations, enable it
        if (valid_attestations >= queue::min_attestations(queue_object)) {
            let expiration_time = timestamp::now_seconds() + queue::oracle_validity_length(queue_object);
            oracle::enable_oracle(oracle_object, secp256k1_key, mr_enclave, expiration_time);
        };

        event::emit(AttestationCreated {
            oracle: object::object_address(&oracle_object),
            guardian: object::object_address(&guardian_object),
            secp256k1_key,
            timestamp: timestamp_seconds,
        });
    }

    public entry fun run(
        oracle: Object<Oracle>,
        queue: Object<Queue>,
        guardian: Object<Oracle>,
        timestamp_seconds: u64,
        mr_enclave: vector<u8>,
        secp256k1_key: vector<u8>,
        signature: vector<u8>,
    ) {
      let params = params(
        oracle,
        queue,
        guardian,
        timestamp_seconds,
        mr_enclave,
        secp256k1_key,
        signature,
      );
      let success = validate(&mut params);
      if (success) {
        actuate(params);
      } else {
        let OracleAttestParams {
            oracle: _,
            queue: _,
            guardian: _,
            timestamp_seconds: _,
            mr_enclave: _,
            secp256k1_key: _,
            signature: _,
        } = params;
      }
    }
}