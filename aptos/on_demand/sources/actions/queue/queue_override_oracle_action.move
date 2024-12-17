module switchboard::queue_override_oracle_action {
    use std::signer;
    use std::vector;
    use aptos_std::event;
    use aptos_framework::object::{Self, Object};
    use aptos_framework::timestamp;
    use switchboard::queue::{Self, Queue};
    use switchboard::oracle::{Self, Oracle};
    use switchboard::errors;

    #[event]
    struct QueueOracleOverride has drop, store {
        queue: address,
        oracle: address,
        secp256k1_key: vector<u8>,
        mr_enclave: vector<u8>,
        expiration_time: u64,
    }

    struct QueueOverrideOracleParams {
        queue: Object<Queue>,
        oracle: Object<Oracle>,
        secp256k1_key: vector<u8>,
        mr_enclave: vector<u8>,
        expiration_time_seconds: u64,
    }

    fun params(
        queue: Object<Queue>,
        oracle: Object<Oracle>,
        secp256k1_key: vector<u8>,
        mr_enclave: vector<u8>,
        expiration_time_seconds: u64,
    ): QueueOverrideOracleParams {
        QueueOverrideOracleParams {
            queue,
            oracle,
            secp256k1_key,
            mr_enclave,
            expiration_time_seconds,
        }
    }

    public fun validate(
        account: &signer,
        params: &QueueOverrideOracleParams,
    ) {
        assert!(queue::queue_exists(params.queue), errors::queue_does_not_exist());
        assert!(oracle::oracle_exists(params.oracle), errors::oracle_does_not_exist());
        assert!(queue::has_authority(params.queue, signer::address_of(account)), errors::invalid_authority());
        assert!(vector::length(&params.secp256k1_key) == 64, errors::invalid_length());
        assert!(vector::length(&params.mr_enclave) == 32, errors::invalid_length());
        assert!(timestamp::now_seconds() < params.expiration_time_seconds, errors::invalid_expiration_time());
    }

    fun actuate(
        params: QueueOverrideOracleParams,
    ) { 

        let QueueOverrideOracleParams {
            queue,
            oracle,
            secp256k1_key,
            mr_enclave,
            expiration_time_seconds,
        } = params;

        oracle::enable_oracle(
            oracle,
            mr_enclave,
            secp256k1_key,
            expiration_time_seconds,
        );

        event::emit(QueueOracleOverride {
            queue: object::object_address(&queue),
            oracle: object::object_address(&oracle),
            secp256k1_key,
            mr_enclave,
            expiration_time: expiration_time_seconds,
        });
    }

    public entry fun run(
        account: &signer,
        queue: Object<Queue>,
        oracle: Object<Oracle>,
        secp256k1_key: vector<u8>,
        mr_enclave: vector<u8>,
        expiration_time_seconds: u64,
    ) {
      
      let params = params(
          queue,
          oracle,
          secp256k1_key,
          mr_enclave,
          expiration_time_seconds,
      );

      validate(account, &params);
      actuate(params);
    }
}