module switchboard::oracle_queue_init_action {
    use std::signer;
    use std::string::String;
    use aptos_std::event;
    use aptos_framework::object::{Self, Object};
    use switchboard::queue::{Self, Queue};
    use switchboard::errors;
    
    #[event]
    struct OracleQueueCreated has drop, store {
        queue: address,
        guardian_queue: address,
        queue_key: vector<u8>,
    }

    struct OracleQueueInitParams {
        queue_key: vector<u8>,
        authority: address,
        name: String,
        fee: u64,
        fee_recipient: address,
        min_attestations: u64,
        oracle_validity_length_seconds: u64,
        guardian_queue: Object<Queue>
    }

    fun params(
        queue_key: vector<u8>,
        authority: address,
        name: String,
        fee: u64,
        fee_recipient: address,
        min_attestations: u64,
        oracle_validity_length_seconds: u64,
        guardian_queue: Object<Queue>
    ): OracleQueueInitParams {
        OracleQueueInitParams {
            queue_key,
            authority,
            name,
            fee,
            fee_recipient,
            min_attestations,
            oracle_validity_length_seconds,
            guardian_queue,
        }
    }

    public fun validate(
        params: &OracleQueueInitParams,
    ) {
        assert!(queue::queue_exists(params.guardian_queue), errors::queue_does_not_exist());
        assert!(params.oracle_validity_length_seconds > 60, errors::invalid_oracle_validity_length());
    }

    fun actuate(
        params: OracleQueueInitParams,
    ) {
        let OracleQueueInitParams {
            queue_key,
            authority,
            name,
            fee,
            fee_recipient,
            min_attestations,
            oracle_validity_length_seconds,
            guardian_queue,
        } = params;
        let queue_address = queue::new(
            queue_key,
            authority,
            name,
            fee,
            fee_recipient,
            min_attestations,
            oracle_validity_length_seconds,
            object::object_address(&guardian_queue),
        );
        event::emit(OracleQueueCreated {
            queue: queue_address,
            guardian_queue: object::object_address(&guardian_queue),
            queue_key,
        });
    }

    public entry fun run(
        account: &signer,
        queue_key: vector<u8>,
        name: String,
        fee: u64,
        fee_recipient: address,
        min_attestations: u64,
        oracle_validity_length_seconds: u64,
        guardian_queue: Object<Queue>
    ) {
        let params = params(
            queue_key,
            signer::address_of(account),
            name,
            fee,
            fee_recipient,
            min_attestations,
            oracle_validity_length_seconds,
            guardian_queue,
        );
        validate(&params);
        actuate(params);
    }
}