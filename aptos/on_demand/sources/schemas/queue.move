module switchboard::queue {

    friend switchboard::guardian_queue_init_action;
    friend switchboard::oracle_queue_init_action;
    friend switchboard::oracle_init_action;
    friend switchboard::oracle_attest_action;
    friend switchboard::queue_override_oracle_action;
    friend switchboard::queue_add_fee_coin_action;
    friend switchboard::queue_remove_fee_coin_action;
    friend switchboard::queue_set_configs_action;
    friend switchboard::queue_set_authority_action;
    friend switchboard::aggregator_init_action;
    friend switchboard::aggregator_submit_result_action;
    friend switchboard::update_action;

    use std::vector;
    use std::string::String;
    use std::type_info::{Self, TypeInfo};
    use aptos_framework::timestamp;
    use aptos_framework::aptos_coin::AptosCoin;
    use aptos_framework::object::{Self, Object};
    use aptos_framework::smart_table::{Self, SmartTable};
    use aptos_std::smart_vector::{Self, SmartVector};

    struct ExistingOracle has copy, drop, store {
        oracle: address,
        oracle_key: vector<u8>,
    }

    struct Queue has key {

        // queue on source chain
        queue_key: vector<u8>,

        // authority of the queue
        authority: address,

        // queue name string
        name: String,

        // fee to be paid upon updates
        fee: u64,

        // address to receive the fee
        fee_recipient: address,

        // minimum number of attestations required
        min_attestations: u64,

        // validity length of the oracle
        oracle_validity_length: u64,
        
        // last queue override
        last_queue_override: u64,

        // created at timestamp
        created_at: u64,

        // fee types accepted
        fee_types: vector<TypeInfo>,

        // guardian queue
        guardian_queue: address,

        // all oracles
        existing_oracles: SmartTable<vector<u8>, address>,

        // [view only] all existing oracles
        all_oracles: SmartVector<ExistingOracle>,

        // [view only] all existing feeds
        all_feeds: SmartVector<address>,
    }
    
    // Queue
    struct ViewQueue has copy, drop {
        queue_address: address,
        queue_key: vector<u8>,
        authority: address,
        name: String,
        fee: u64,
        fee_recipient: address,
        min_attestations: u64,
        oracle_validity_length: u64,
        last_queue_override: u64,
        created_at: u64,
        guardian_queue: address,
        fee_types: vector<TypeInfo>,
        all_oracles: vector<ExistingOracle>,
    }

    
    // -- read queue fields --

    #[view]
    public fun queue_exists(queue: Object<Queue>): bool {
        exists<Queue>(object::object_address(&queue))
    }

    #[view]
    public fun authority(queue: Object<Queue>): address acquires Queue {
        let queue = borrow_global<Queue>(object::object_address(&queue));
        queue.authority
    }

    #[view]
    public fun name(queue: Object<Queue>): String acquires Queue {
        let queue = borrow_global<Queue>(object::object_address(&queue));
        queue.name
    }

    #[view]
    public fun fee(queue: Object<Queue>): u64 acquires Queue {
        let queue = borrow_global<Queue>(object::object_address(&queue));
        queue.fee
    }

    #[view]
    public fun fee_recipient(queue: Object<Queue>): address acquires Queue {
        let queue = borrow_global<Queue>(object::object_address(&queue));
        queue.fee_recipient
    }

    #[view]
    public fun min_attestations(queue: Object<Queue>): u64 acquires Queue {
        let queue = borrow_global<Queue>(object::object_address(&queue));
        queue.min_attestations
    }

    #[view]
    public fun oracle_validity_length(queue: Object<Queue>): u64 acquires Queue {
        let queue = borrow_global<Queue>(object::object_address(&queue));
        queue.oracle_validity_length
    }

    #[view]
    public fun last_queue_override(queue: Object<Queue>): u64 acquires Queue {
        let queue = borrow_global<Queue>(object::object_address(&queue));
        queue.last_queue_override
    }

    #[view]
    public fun fee_types(queue: Object<Queue>): vector<TypeInfo> acquires Queue {
        let queue = borrow_global<Queue>(object::object_address(&queue));
        queue.fee_types
    }

    #[view]
    public fun guardian_queue(queue: Object<Queue>): address acquires Queue {
        let queue = borrow_global<Queue>(object::object_address(&queue));
        queue.guardian_queue
    }
    
    #[view]
    public fun has_authority(queue: Object<Queue>, authority: address): bool acquires Queue {
        let queue = borrow_global<Queue>(object::object_address(&queue));
        queue.authority == authority
    }

    #[view]
    public fun queue_key(queue: Object<Queue>): vector<u8> acquires Queue {
        let queue = borrow_global<Queue>(object::object_address(&queue));
        queue.queue_key
    }

    #[view]
    public fun get_oracle_from_key(queue: Object<Queue>, oracle_key: vector<u8>): address acquires Queue {
        let queue = borrow_global<Queue>(object::object_address(&queue));
        *smart_table::borrow(&queue.existing_oracles, oracle_key)
    }

    public fun fee_coin_exists(queue: Object<Queue>, fee_type: &TypeInfo): bool acquires Queue {
        let queue = borrow_global<Queue>(object::object_address(&queue));
        vector::contains(&queue.fee_types, fee_type)
    }

    // -- write queue --

    public(friend) fun add_fee_type(queue: Object<Queue>, fee_type: TypeInfo) acquires Queue {
        let queue_address = object::object_address(&queue);
        let queue = borrow_global_mut<Queue>(queue_address);
        vector::push_back(&mut queue.fee_types, fee_type);
    }

    public(friend) fun remove_fee_type(queue: Object<Queue>, fee_type: TypeInfo) acquires Queue {
        let queue_address = object::object_address(&queue);
        let queue = borrow_global_mut<Queue>(queue_address);
        vector::remove_value(&mut queue.fee_types, &fee_type);        
    }

    public(friend) fun set_last_queue_override(queue: Object<Queue>, last_queue_override: u64) acquires Queue {
        let queue_address = object::object_address(&queue);
        let queue = borrow_global_mut<Queue>(queue_address);
        queue.last_queue_override = last_queue_override;
    }

    public(friend) fun add_oracle(queue: Object<Queue>, oracle_key: vector<u8>, oracle: address) acquires Queue {
        let queue_address = object::object_address(&queue);
        let queue = borrow_global_mut<Queue>(queue_address);
        smart_table::add(&mut queue.existing_oracles, oracle_key, oracle);
        smart_vector::push_back(&mut queue.all_oracles, ExistingOracle { oracle, oracle_key });
    }

    public(friend) fun existing_oracles_contains(queue: Object<Queue>, oracle_key: vector<u8>): bool acquires Queue {
        let queue_address = object::object_address(&queue);
        let queue = borrow_global<Queue>(queue_address);
        smart_table::contains(&queue.existing_oracles, oracle_key)
    }

    public(friend) fun set_authority(queue: Object<Queue>, authority: address) acquires Queue {
        let queue_address = object::object_address(&queue);
        let queue = borrow_global_mut<Queue>(queue_address);
        queue.authority = authority;
    }

    public(friend) fun set_configs(
        queue: Object<Queue>,
        name: String,
        fee: u64,
        fee_recipient: address,
        min_attestations: u64,
        oracle_validity_length: u64,
    ) acquires Queue {
        let queue_address = object::object_address(&queue);
        let queue = borrow_global_mut<Queue>(queue_address);
        queue.name = name;
        queue.fee = fee;
        queue.fee_recipient = fee_recipient;
        queue.min_attestations = min_attestations;
        queue.oracle_validity_length = oracle_validity_length;
    }

    public(friend) fun new(
        queue_key: vector<u8>,
        authority: address,
        name: String,
        fee: u64,
        fee_recipient: address,
        min_attestations: u64,
        oracle_validity_length: u64,
        guardian_queue: address,
    ): address {

        let created_at = timestamp::now_seconds();

        // create the queue object
        let queue = Queue {
            queue_key,
            authority,
            name,
            fee,
            fee_recipient,
            min_attestations,
            oracle_validity_length,
            last_queue_override: 0,
            existing_oracles: smart_table::new(),
            fee_types: vector::empty(),
            guardian_queue,
            created_at,
            all_oracles: smart_vector::empty<ExistingOracle>(),
            all_feeds: smart_vector::empty<address>(),
        };

        vector::push_back(&mut queue.fee_types, type_info::type_of<AptosCoin>());

        // give the authority of the aggregator to the authority
        let constructor_ref = object::create_object(authority);

        let queue_address = object::address_from_constructor_ref(&constructor_ref);
        if (queue.guardian_queue == @0x0) {
            queue.guardian_queue = copy queue_address;
        };

        let object_signer = object::generate_signer(&constructor_ref);

        // assign the current result to the object
        move_to(&object_signer, queue);

        // return the address of the aggregator
        queue_address
    }

    public(friend) fun add_aggregator(queue: Object<Queue>, feed: address) acquires Queue {
        let queue_address = object::object_address(&queue);
        let queue = borrow_global_mut<Queue>(queue_address);
        smart_vector::push_back(&mut queue.all_feeds, feed);
    }

    // -- view functions --

    #[view]
    public fun view_queue(queue_object: Object<Queue>): ViewQueue acquires Queue {
        let queue = borrow_global<Queue>(object::object_address(&queue_object));
        let all_oracles = vector::empty<ExistingOracle>();
        let idx = 0;

        while (idx < smart_vector::length(&queue.all_oracles)) {
            vector::push_back(&mut all_oracles, *smart_vector::borrow(&queue.all_oracles, idx));
            idx = idx + 1;
        };

        // return the view
        ViewQueue {
            queue_address: object::object_address(&queue_object),
            queue_key: queue.queue_key,
            authority: queue.authority,
            name: queue.name,
            fee: queue.fee,
            fee_recipient: queue.fee_recipient,
            min_attestations: queue.min_attestations,
            oracle_validity_length: queue.oracle_validity_length,
            last_queue_override: queue.last_queue_override,
            created_at: queue.created_at,
            guardian_queue: queue.guardian_queue,
            fee_types: queue.fee_types,
            all_oracles,
        }
    }
    
    #[view]
    public fun view_queue_oracles(queue: Object<Queue>): vector<address> acquires Queue {
        let queue = borrow_global<Queue>(object::object_address(&queue));
        let all_oracles = vector::empty<address>();
        let idx = 0;
        while (idx < smart_vector::length(&queue.all_oracles)) {
          vector::push_back(&mut all_oracles, smart_vector::borrow(&queue.all_oracles, idx).oracle);
          idx = idx + 1;
        };
        all_oracles
    }

    #[view]
    public fun view_queue_aggregators(queue: Object<Queue>): vector<address> acquires Queue {
        let queue = borrow_global<Queue>(object::object_address(&queue));
        let all_feeds = vector::empty<address>();
        let idx = 0;
        while (idx < smart_vector::length(&queue.all_feeds)) {
            vector::push_back(&mut all_feeds, *smart_vector::borrow(&queue.all_feeds, idx));
            idx = idx + 1;
        };
        all_feeds
    }
}