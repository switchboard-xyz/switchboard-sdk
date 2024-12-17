module switchboard::oracle {
    friend switchboard::oracle_init_action;
    friend switchboard::oracle_attest_action;
    friend switchboard::queue_override_oracle_action;

    use std::vector;
    use aptos_framework::timestamp;
    use aptos_framework::object::{Self, Object};
    use switchboard::queue::{Self, Queue};

    struct Attestation has copy, drop, store {
        guardian: address, 
        secp256k1_key: vector<u8>,
        timestamp: u64,
    }

    struct Oracle has copy, drop, key {
        oracle_key: vector<u8>,
        queue: address,
        queue_key: vector<u8>,        
        expiration_time: u64,
        mr_enclave: vector<u8>,
        secp256k1_key: vector<u8>,
        valid_attestations: vector<Attestation>,
        created_at: u64,
    }

    struct ViewOracle has copy, drop {
        oracle_address: address,
        oracle_key: vector<u8>,
        queue: address,
        queue_key: vector<u8>,        
        expiration_time: u64,
        mr_enclave: vector<u8>,
        secp256k1_key: vector<u8>,
        valid_attestations: vector<Attestation>,
        created_at: u64,
    }


    // -- read oracle fields --

    public fun oracle_exists(oracle: Object<Oracle>): bool {
        exists<Oracle>(object::object_address(&oracle))
    }

    #[view]
    public fun get_oracle(oracle_object: Object<Oracle>): Oracle acquires Oracle {
        let oracle_address = object::object_address(&oracle_object);
        let oracle = borrow_global<Oracle>(oracle_address);
        *oracle
    }

    public fun oracle_key(self: &Oracle): vector<u8> {
        self.oracle_key
    }

    public fun queue(self: &Oracle): address {
        self.queue
    }

    public fun queue_key(self: &Oracle): vector<u8> {
        self.queue_key
    }

    public fun expiration_time(self: &Oracle): u64 {
        self.expiration_time
    }

    public fun mr_enclave(self: &Oracle): vector<u8> {
        self.mr_enclave
    }

    public fun secp256k1_key(self: &Oracle): vector<u8> {
        self.secp256k1_key
    }

    public fun valid_attestations(self: &Oracle): vector<Attestation> {
        self.valid_attestations
    }

    public fun valid_attestation_count(self: &Oracle, secp256k1_key: vector<u8>): u64 {
        let count = 0;
        let idx = 0;
        while (idx < vector::length(&self.valid_attestations)) {
            let a = vector::borrow(&self.valid_attestations, idx);
            if (a.secp256k1_key == secp256k1_key) {
                count = count + 1;
            };
            idx = idx + 1;
        };
        count
    }

    // -- write oracle fields --

    public(friend) fun new_attestation(
        guardian: address,
        secp256k1_key: vector<u8>,
        timestamp: u64,
    ): Attestation {
        Attestation {
            guardian,
            secp256k1_key,
            timestamp,
        }
    }

    public(friend) fun add_attestation(
      oracle: Object<Oracle>, 
      attestation: Attestation, 
      timestamp: u64,
    ) acquires Oracle {
        let oracle = borrow_global_mut<Oracle>(object::object_address(&oracle));
        let new_valid_attestations = vector::empty<Attestation>();
        let idx = 0;
        while (idx < vector::length(&oracle.valid_attestations)) {
            let a = vector::borrow(&oracle.valid_attestations, idx);
            if (a.timestamp + oracle.expiration_time > timestamp && a.guardian != attestation.guardian) {
                vector::push_back(&mut new_valid_attestations, *a);
            };
            idx = idx + 1;
        };
        oracle.valid_attestations = new_valid_attestations;
        vector::push_back(&mut oracle.valid_attestations, attestation);
    }

    public(friend) fun enable_oracle(
        oracle: Object<Oracle>,
        mr_enclave: vector<u8>,
        secp256k1_key: vector<u8>,
        expiration_time: u64,
    ) acquires Oracle {
        let oracle = borrow_global_mut<Oracle>(object::object_address(&oracle));
        oracle.mr_enclave = mr_enclave;
        oracle.secp256k1_key = secp256k1_key;
        oracle.expiration_time = expiration_time;
    }

    public(friend) fun new(
        queue: address,
        oracle_key: vector<u8>,
        queue_key: vector<u8>,
    ): address {
        let oracle = Oracle {
            queue,
            oracle_key,
            queue_key,
            expiration_time: 0,
            mr_enclave: vector::empty(),
            secp256k1_key: vector::empty(),
            valid_attestations: vector::empty(),
            created_at: timestamp::now_seconds(),
        };
   
        // give the authority of the aggregator to the authority
        let constructor_ref = object::create_object(@switchboard);
        let object_signer = object::generate_signer(&constructor_ref);

        // assign the aggregator to the object
        move_to(&object_signer, oracle);

        // return the address of the aggregator
        object::address_from_constructor_ref(&constructor_ref)
    }

    // -- external view functions --

    #[view]
    public fun view_oracle(oracle_object: Object<Oracle>): ViewOracle acquires Oracle {
        let oracle = get_oracle(oracle_object);
        ViewOracle {
            oracle_address: object::object_address(&oracle_object),
            oracle_key: oracle.oracle_key,
            queue: oracle.queue,
            queue_key: oracle.queue_key,
            expiration_time: oracle.expiration_time,
            mr_enclave: oracle.mr_enclave,
            secp256k1_key: oracle.secp256k1_key,
            valid_attestations: oracle.valid_attestations,
            created_at: oracle.created_at,
        }
    }

    #[view]
    public fun view_queue_oracles(queue: Object<Queue>): vector<ViewOracle> acquires Oracle {
        let oracles = vector::empty<ViewOracle>();
        let oracle_addresses = queue::view_queue_oracles(queue);
        let idx = 0;
        while (idx < vector::length(&oracle_addresses)) {
            let oracle_object = object::address_to_object<Oracle>(*vector::borrow(&oracle_addresses, idx));
            let oracle = view_oracle(oracle_object);
            vector::push_back(&mut oracles, oracle);
            idx = idx + 1;
        };
        oracles
    }



}