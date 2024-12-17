module switchboard::guardian_queue_init_action {
    use std::signer;
    use std::string::String;
    use switchboard::queue;
    use switchboard::errors;
    use aptos_std::event;

    #[event]
    struct GuardianQueueCreated has drop, store {
        queue: address,
        queue_key: vector<u8>,
    }

    struct GuardianQueueInitParams {
        queue_key: vector<u8>,
        authority: address,
        name: String,
        fee: u64,
        fee_recipient: address,
        min_attestations: u64,
        guardian_validity_length_seconds: u64,
    }

    fun params(
        queue_key: vector<u8>,
        authority: address,
        name: String,
        fee: u64,
        fee_recipient: address,
        min_attestations: u64,
        guardian_validity_length_seconds: u64,
    ): GuardianQueueInitParams {
        GuardianQueueInitParams {
            queue_key,
            authority,
            name,
            fee,
            fee_recipient,
            min_attestations,
            guardian_validity_length_seconds,
        }
    }

    public fun validate(
        params: &GuardianQueueInitParams,
    ) {
        assert!(params.guardian_validity_length_seconds > 60, errors::invalid_guardian_validity_length());
    }

    fun actuate(
        params: GuardianQueueInitParams,
    ) {
        let GuardianQueueInitParams {
            queue_key,
            authority,
            name,
            fee,
            fee_recipient,
            min_attestations,
            guardian_validity_length_seconds,
        } = params;
        let queue_address = queue::new(
            queue_key,
            authority,
            name,
            fee,
            fee_recipient,
            min_attestations,
            guardian_validity_length_seconds,
            @0x0,
        );
        event::emit(GuardianQueueCreated {
            queue: queue_address,
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
        guardian_validity_length_seconds: u64,
    ) {
        let params = params(
            queue_key,
            signer::address_of(account),
            name,
            fee,
            fee_recipient,
            min_attestations,
            guardian_validity_length_seconds,
        );
        validate(&params);
        actuate(params);
    }
}