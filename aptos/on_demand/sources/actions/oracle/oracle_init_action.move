module switchboard::oracle_init_action {
    use std::vector;
    use aptos_framework::object::{Self, Object};
    use aptos_std::event;
    use switchboard::queue::{Self, Queue};
    use switchboard::oracle;
    use switchboard::errors;

    #[event]
    struct OracleCreated has drop, store {
        oracle: address,
        queue: address,
        oracle_key: vector<u8>,
    }

    struct OracleInitParams {
        queue: Object<Queue>,
        oracle_key: vector<u8>,
    }

    fun params(
        queue: Object<Queue>,
        oracle_key: vector<u8>,
    ): OracleInitParams {
        OracleInitParams {
            queue,
            oracle_key,
        }
    }

    public fun validate(
        params: &OracleInitParams,
    ) {
        assert!(queue::queue_exists(params.queue), errors::queue_does_not_exist());
        assert!(vector::length(&params.oracle_key) == 32, errors::invalid_length());
        assert!(!queue::existing_oracles_contains(params.queue, params.oracle_key), errors::invalid_oracle());
    }

    fun actuate(
        params: OracleInitParams,
    ) {
        let OracleInitParams {
            queue,
            oracle_key,
        } = params;
        let queue_address = object::object_address(&queue);
        let queue_key = queue::queue_key(queue);
        let oracle_address = oracle::new(
            queue_address,
            oracle_key,
            queue_key,
        );
        queue::add_oracle(queue, oracle_key, oracle_address);
        event::emit(OracleCreated {
            oracle: oracle_address,
            queue: queue_address,
            oracle_key,
        });
    }

    public entry fun run(
        queue: Object<Queue>,
        oracle_key: vector<u8>,
    ) {

      let params = params(
        queue,
        oracle_key,
      );

      validate(&params);
      actuate(params);
    }


}