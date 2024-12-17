module switchboard::aggregator {

    friend switchboard::aggregator_init_action;
    friend switchboard::aggregator_set_authority_action;
    friend switchboard::aggregator_set_configs_action;
    friend switchboard::aggregator_submit_result_action;
    friend switchboard::update_action;

    use std::string::{String};
    use std::option::{Self, Option};
    use std::vector;
    use std::signer;
    use aptos_std::math64;
    use aptos_std::math128;
    use aptos_framework::timestamp;
    use aptos_framework::object::{Self, Object};
    use switchboard::decimal::{Self, Decimal};
    use switchboard::queue::{Self, Queue};

    const MAX_RESULTS: u64 = 16;
    const MAX_U64: u64 = 18446744073709551615;

    struct CurrentResult has key, copy, drop, store {
        result: Decimal,
        timestamp: u64,
        min_timestamp: u64,
        max_timestamp: u64,
        min_result: Decimal,
        max_result: Decimal,
        stdev: Decimal,
        range: Decimal,
        mean: Decimal,
    }

    struct Update has copy, drop, store {
        result: Decimal,
        timestamp: u64,
        oracle: address,
    }

    struct UpdateState has key, copy, drop, store {
        results: vector<Update>,
        curr_idx: u64,
    }

    struct Aggregator has key, copy, drop {
        queue: address,
        created_at: u64,
        name: String,
        authority: address,
        feed_hash: vector<u8>,
        min_sample_size: u64,
        max_staleness_seconds: u64,
        max_variance: u64,  
        min_responses: u32,
    }

    struct ViewAggregator has copy, drop {
        aggregator_address: address,
        queue: address,
        created_at: u64,
        name: String,
        authority: address,
        feed_hash: vector<u8>,
        min_sample_size: u64,
        max_staleness_seconds: u64,
        max_variance: u64,  
        min_responses: u32,
        current_result: CurrentResult,
        update_state: UpdateState,
    }


    // -- current result fields --
    
    #[view]
    public fun current_result(aggregator: Object<Aggregator>): CurrentResult acquires CurrentResult {
        let current_result_address = object::object_address(&aggregator);
        let current_result = borrow_global<CurrentResult>(current_result_address);
        *current_result
    }
    
    public fun result(self: &CurrentResult): Decimal {
        self.result
    }

    public fun min_timestamp(self: &CurrentResult): u64 {
        self.min_timestamp
    }

    public fun max_timestamp(self: &CurrentResult): u64 {
        self.max_timestamp
    }

    public fun min_result(self: &CurrentResult): Decimal {
        self.min_result
    }

    public fun max_result(self: &CurrentResult): Decimal {
        self.max_result
    }

    public fun stdev(self: &CurrentResult): Decimal {
        self.stdev
    }

    public fun range(self: &CurrentResult): Decimal {
        self.range
    }

    public fun mean(self: &CurrentResult): Decimal {
        self.mean
    }

    public fun timestamp(self: &CurrentResult): u64 {
        self.timestamp
    }

    // -- aggregator fields --
    
    public fun aggregator_exists(aggregator: Object<Aggregator>): bool {
        exists<Aggregator>(object::object_address(&aggregator))
    }

    #[view]
    public fun get_aggregator(aggregator: Object<Aggregator>): Aggregator acquires Aggregator {
        let aggregator_address = object::object_address(&aggregator);
        let aggregator = borrow_global<Aggregator>(aggregator_address);
        *aggregator
    }

    public fun queue(self: &Aggregator): address {
        self.queue
    }

    public fun created_at(self: &Aggregator): u64 {
        self.created_at
    }

    public fun name(self: &Aggregator): String {
        self.name
    }

    public fun authority(self: &Aggregator): address {
        self.authority
    }

    public fun feed_hash(self: &Aggregator): vector<u8> {
        self.feed_hash
    }

    public fun min_sample_size(self: &Aggregator): u64 {
        self.min_sample_size
    }

    public fun max_staleness_seconds(self: &Aggregator): u64 {
        self.max_staleness_seconds
    }

    public fun max_variance(self: &Aggregator): u64 {
        self.max_variance
    }

    public fun min_responses(self: &Aggregator): u32 {
        self.min_responses
    }

    #[view]
    public fun has_authority(aggregator: Object<Aggregator>, authority: address): bool acquires Aggregator {
        let aggregator = borrow_global<Aggregator>(object::object_address(&aggregator));
        aggregator.authority == authority
    }

    #[view]
    public(friend) fun get_queue(aggregator: Object<Aggregator>): Object<Queue> acquires Aggregator {
        let aggregator = borrow_global<Aggregator>(object::object_address(&aggregator));
        object::address_to_object<Queue>(aggregator.queue)
    }

    // -- write functions --

    public(friend) fun new(
        account: &signer,
        queue: address,
        name: String,
        feed_hash: vector<u8>,
        min_sample_size: u64,
        max_staleness_seconds: u64,
        max_variance: u64,
        min_responses: u32,
    ): address {
        let created_at = timestamp::now_seconds();
        let authority = signer::address_of(account);
        let aggregator = Aggregator {
            queue,
            name,
            authority,
            feed_hash,
            min_sample_size,
            max_staleness_seconds,
            max_variance,
            min_responses,
            created_at,
            
        };

        let update_state = UpdateState {
            results: vector::empty<Update>(),
            curr_idx: 0,
        };

        let current_result = CurrentResult {
            result: decimal::zero(),
            min_timestamp: 0,
            max_timestamp: 0,
            min_result: decimal::zero(),
            max_result: decimal::zero(),
            stdev: decimal::zero(),
            range: decimal::zero(),
            mean: decimal::zero(),
            timestamp: 0,
        };

        // give the authority of the aggregator to the authority
        let constructor_ref = object::create_object(authority);
        let object_signer = object::generate_signer(&constructor_ref);

        // assign the aggregator to the object
        move_to(&object_signer, aggregator);

        // assign the update state to the object
        move_to(&object_signer, update_state);

        // assign the current result to the object
        move_to(&object_signer, current_result);

        // return the address of the aggregator
        object::address_from_constructor_ref(&constructor_ref)
    }

    public(friend) fun set_authority(aggregator: Object<Aggregator>, authority: address) acquires Aggregator {
        let aggregator_address = object::object_address(&aggregator);
        let aggregator = borrow_global_mut<Aggregator>(aggregator_address);
        aggregator.authority = authority;
    }

    public(friend) fun set_configs(
        aggregator: Object<Aggregator>,
        name: String,
        min_sample_size: u64,
        max_staleness_seconds: u64,
        max_variance: u64,
        min_responses: u32,
        feed_hash: vector<u8>,
    ) acquires Aggregator {
        let aggregator_address = object::object_address(&aggregator);
        let aggregator = borrow_global_mut<Aggregator>(aggregator_address);
        aggregator.name = name;
        aggregator.min_sample_size = min_sample_size;
        aggregator.max_staleness_seconds = max_staleness_seconds;
        aggregator.max_variance = max_variance;
        aggregator.min_responses = min_responses;
        aggregator.feed_hash = feed_hash;
    }

    public(friend) fun add_result(
        aggregator: Object<Aggregator>,
        result: Decimal,
        timestamp: u64,
        oracle: address
    ) acquires Aggregator, UpdateState, CurrentResult {
        let aggregator_address = object::object_address(&aggregator);
        let now = timestamp::now_seconds();
        let update_state = borrow_global_mut<UpdateState>(aggregator_address);
        set_update(update_state, result, oracle, timestamp);
        let current_result: Option<CurrentResult> = compute_current_result(aggregator, now);
        if (option::is_some(&current_result)) {
            let new_current_result = option::extract(&mut current_result);
            
            // write the new current result to the object
            let current_result = borrow_global_mut<CurrentResult>(aggregator_address);
            
            // update the current result
            current_result.result = new_current_result.result;
            current_result.timestamp = new_current_result.timestamp;
            current_result.min_timestamp = new_current_result.min_timestamp;
            current_result.max_timestamp = new_current_result.max_timestamp;
            current_result.min_result = new_current_result.min_result;
            current_result.max_result = new_current_result.max_result;
            current_result.stdev = new_current_result.stdev;
            current_result.range = new_current_result.range;
            current_result.mean = new_current_result.mean;
        }
    }

    fun set_update(
        update_state: &mut UpdateState,
        result: Decimal,
        oracle: address,
        timestamp: u64
    ) {
        let results = &mut update_state.results;
        let last_idx = update_state.curr_idx;
        let curr_idx = (last_idx + 1) % MAX_RESULTS;

        if (vector::length(results) == 0) {
            vector::push_back(results, Update { result, timestamp, oracle });
            return
        };

        if (vector::length(results) > 0) {
            let last_result = vector::borrow(results, last_idx);
            if (timestamp < last_result.timestamp) {
                return
            };
        };
        
        if (vector::length(results) < MAX_RESULTS) {
            vector::push_back(results, Update { result, timestamp, oracle });
        } else {
            let existing_result: &mut Update = vector::borrow_mut(results, curr_idx);
            existing_result.result = result;
            existing_result.timestamp = timestamp;
            existing_result.oracle = oracle;
        };

        update_state.curr_idx = curr_idx;
    }

    #[view]
    fun compute_current_result(aggregator_object: Object<Aggregator>, now: u64): Option<CurrentResult> acquires Aggregator, UpdateState {

        let aggregator_address = object::object_address(&aggregator_object);

        let aggregator_cfg = borrow_global<Aggregator>(copy aggregator_address);
        
        let update_state = borrow_global<UpdateState>(aggregator_address);

        let updates = update_state.results;
        let update_indices = valid_update_indices(update_state, aggregator_cfg.max_staleness_seconds, now);

        if (vector::length(&update_indices) < aggregator_cfg.min_sample_size) {
            return option::none<CurrentResult>()
        };

        if (vector::length(&update_indices) == 1) {
            return option::some(CurrentResult {
                min_timestamp: vector::borrow(&updates, *vector::borrow(&update_indices, 0)).timestamp,
                max_timestamp: vector::borrow(&updates, *vector::borrow(&update_indices, 0)).timestamp,
                min_result: vector::borrow(&updates, *vector::borrow(&update_indices, 0)).result,
                max_result: vector::borrow(&updates, *vector::borrow(&update_indices, 0)).result,
                range: decimal::zero(),
                stdev: decimal::zero(),
                mean: vector::borrow(&updates, *vector::borrow(&update_indices, 0)).result,
                result: vector::borrow(&updates, *vector::borrow(&update_indices, 0)).result,
                timestamp: vector::borrow(&updates, *vector::borrow(&update_indices, 0)).timestamp,
            })
        };

        let min_result = decimal::max_value();
        let max_result = decimal::zero();
        let min_timestamp: u64 = MAX_U64;
        let max_timestamp: u64 = 0;
        let sum: u128 = 0;
        let mean: u128 = 0;
        let mean_neg: bool = false;
        let m2: u128 = 0;
        let m2_neg: bool = false;
        let count: u128 = 0;


        let idx: u64 = 0;
        while (idx < vector::length(&update_indices)) {
            let update = vector::borrow_mut(&mut updates, *vector::borrow(&update_indices, idx));
            count = count + 1;
            let value = decimal::value(&update.result);
            let value_neg = decimal::neg(&update.result);


            // Welford's online algorithm
            let (delta, delta_neg) = sub_i128(value, value_neg, mean, mean_neg);
            (mean, mean_neg) = add_i128(mean, mean_neg, delta / count, delta_neg);
            let (delta2, delta2_neg) = sub_i128(value, value_neg, mean, mean_neg);
            (m2, m2_neg) = add_i128(m2, m2_neg, delta * delta2, delta_neg != delta2_neg);

            sum = sum + value;
            min_result = decimal::min(&min_result, &update.result);
            max_result = decimal::max(&max_result, &update.result);
            min_timestamp = math64::min(min_timestamp, update.timestamp);
            max_timestamp = math64::max(max_timestamp, update.timestamp);
            idx = idx + 1;
        };

        let variance = m2 / (count - 1);
        let stdev = math128::sqrt(variance);
        let (median_result, median_timestamp) = median_result(update_state, &mut update_indices);

        option::some(CurrentResult {
            min_timestamp,
            max_timestamp,
            min_result,
            max_result,
            range: decimal::sub(&max_result, &min_result),
            stdev: decimal::new(stdev, false),
            mean: decimal::new(mean, mean_neg),
            result: median_result,
            timestamp: median_timestamp,
        })
    }

    fun add_i128(a: u128, a_neg: bool, b: u128, b_neg: bool): (u128, bool) {
        if (a_neg && b_neg) {
            return (a + b, true)
        } else if (!a_neg && b_neg) {
            if (a < b) {
                return (b - a, true)
            } else {
                return (a - b, false)
            }
        } else if (a_neg && !b_neg) {
            if (a < b) {
                return (b - a, false)
            } else {
                return (a - b, true)
            }
        } else {
            return (a + b, false)
        }
    }

    fun sub_i128(a: u128, a_neg: bool, b: u128, b_neg: bool): (u128, bool) {
        add_i128(a, a_neg, b, !b_neg)
    }

    fun valid_update_indices(update_state: &UpdateState, max_staleness: u64, now: u64): vector<u64> {
        let results = update_state.results;
        let valid_updates = vector::empty<u64>();
        let seen_oracles = vector::empty<address>();

        let idx = update_state.curr_idx;
        let remaining_max_iterations = math64::min(MAX_RESULTS, vector::length(&results));

        if (remaining_max_iterations == 0) {
            return valid_updates
        };

        loop {
            if (remaining_max_iterations == 0 || (vector::borrow(&results, idx).timestamp + max_staleness) < now) {
                break
            };

            let result = vector::borrow(&results, idx);
            let oracle = result.oracle;

            if (!vector::contains(&seen_oracles, &oracle)) {
                vector::push_back(&mut seen_oracles, oracle);
                vector::push_back(&mut valid_updates, idx);
            };

            if (idx == 0) {
                idx = vector::length(&results) - 1;
            } else {
                idx = idx - 1;
            };

            remaining_max_iterations = remaining_max_iterations - 1;
        };

        valid_updates
    }

    // select median or lower bound middle item if even (with quickselect)
    // sort the update indices in place
    fun median_result(update_state: &UpdateState, update_indices: &mut vector<u64>): (Decimal, u64) {
        let updates = &update_state.results;
        let n = vector::length(update_indices);
        let mid = n / 2;
        let lo = 0;
        let hi = n - 1;

        while (lo < hi) {
            let pivot = *vector::borrow(update_indices, hi);
            let i = lo;
            let j = lo;

            while (j < hi) {
                let j_result = vector::borrow(updates, *vector::borrow(update_indices, j)).result;
                let pivot_result = vector::borrow(updates, pivot).result;

                if (decimal::lt(&j_result, &pivot_result)) {
                    vector::swap(update_indices, i, j);
                    i = i + 1;
                };
                j = j + 1;
            };

            vector::swap(update_indices, i, hi);

            if (i == mid) {
                break
            } else if (i < mid) {
                lo = i + 1;
            } else {
                hi = i - 1;
            };
        };

        let update_result = vector::borrow(updates, *vector::borrow(update_indices, mid));
        (update_result.result, update_result.timestamp)
    }

    // -- external view functions --
    
    #[view]
    public fun view_aggregator(aggregator: Object<Aggregator>): ViewAggregator acquires Aggregator, CurrentResult, UpdateState {
        let aggregator_address = object::object_address(&aggregator);
        let aggregator = borrow_global<Aggregator>(aggregator_address);
        let current_result = *borrow_global<CurrentResult>(aggregator_address);
        let update_state = *borrow_global<UpdateState>(aggregator_address);
        ViewAggregator {
            aggregator_address,
            queue: aggregator.queue,
            created_at: aggregator.created_at,
            name: aggregator.name,
            authority: aggregator.authority,
            feed_hash: aggregator.feed_hash,
            min_sample_size: aggregator.min_sample_size,
            max_staleness_seconds: aggregator.max_staleness_seconds,
            max_variance: aggregator.max_variance,
            min_responses: aggregator.min_responses,
            current_result,
            update_state,
        }
    }

    #[view]
    public fun view_queue_aggregators(queue: Object<Queue>): vector<ViewAggregator> acquires Aggregator, CurrentResult, UpdateState {
        let aggregator_addresses = queue::view_queue_aggregators(queue);
        let aggregators = vector::empty<ViewAggregator>();
        let idx = 0;
        while (idx < vector::length(&aggregator_addresses)) {
            let aggregator = borrow_global<Aggregator>(*vector::borrow(&aggregator_addresses, idx));
            let current_result = borrow_global<CurrentResult>(*vector::borrow(&aggregator_addresses, idx));
            let update_state = borrow_global<UpdateState>(*vector::borrow(&aggregator_addresses, idx));
            vector::push_back(&mut aggregators, ViewAggregator {
                aggregator_address: *vector::borrow(&aggregator_addresses, idx),
                queue: aggregator.queue,
                created_at: aggregator.created_at,
                name: aggregator.name,
                authority: aggregator.authority,
                feed_hash: aggregator.feed_hash,
                min_sample_size: aggregator.min_sample_size,
                max_staleness_seconds: aggregator.max_staleness_seconds,
                max_variance: aggregator.max_variance,
                min_responses: aggregator.min_responses,
                current_result: *current_result,
                update_state: *update_state,
            });
            idx = idx + 1;
        };
        aggregators
    }

    #[test_only]
    public fun new_aggregator(
        account: &signer,
        queue: address,
        name: String,
        feed_hash: vector<u8>,
        min_sample_size: u64,
        max_staleness_seconds: u64,
        max_variance: u64,
        min_responses: u32,
    ): address {
        let aggregator_address = new(
            account,
            queue,
            name,
            feed_hash,
            min_sample_size,
            max_staleness_seconds,
            max_variance,
            min_responses
        );
        aggregator_address
    }

    #[test(account = @0x1)]
    public fun test_aggregator_accessors(account: signer) acquires Aggregator {
        let err = 1337;
        let queue_address = example_queue_address();
        let name = std::string::utf8(b"test_aggregator");
        let feed_hash = vector::empty<u8>();
        let min_sample_size = 10;
        let max_staleness_seconds = 1000;
        let max_variance = 100;
        let min_responses = 5;

             // create timestamp resource
        timestamp::set_time_has_started_for_testing(&account);


        let aggregator_address = new_aggregator(
            &account,
            queue_address,
            name,
            feed_hash,
            min_sample_size,
            max_staleness_seconds,
            max_variance,
            min_responses,
        );

        let aggregator = borrow_global<Aggregator>(aggregator_address);

        assert!(queue(aggregator) == queue_address, err);
        assert!(name(aggregator) == name, err);
        assert!(authority(aggregator) == std::signer::address_of(&account), err);
        assert!(feed_hash(aggregator) == feed_hash, err);
        assert!(min_sample_size(aggregator) == min_sample_size, err);
        assert!(max_staleness_seconds(aggregator) == max_staleness_seconds, err);
        assert!(max_variance(aggregator) == max_variance, err);
        assert!(min_responses(aggregator) == min_responses, err);
    }


    #[test(account = @0x1)]
    public fun test_aggregator_updates(account: signer) acquires Aggregator, UpdateState, CurrentResult {
        let err: u64 = 1337;
        let queue_address = example_queue_address();
        let name = std::string::utf8(b"test_aggregator");
        let feed_hash = vector::empty<u8>();
        let min_sample_size = 3;
        let max_staleness_seconds = 1000000;
        let max_variance = 1000000;
        let min_responses = 5;

        // create timestamp resource
        timestamp::set_time_has_started_for_testing(&account);

        let aggregator_address = new_aggregator(
            &account,
            queue_address,
            name,
            feed_hash,
            min_sample_size,
            max_staleness_seconds,
            max_variance,
            min_responses,
        );

        let aggregator_object = object::address_to_object<Aggregator>(aggregator_address);

        let oracle1 = @0x1;
        let oracle2 = @0x2;
        let oracle3 = @0x3;
        let oracle4 = @0x4;
        let oracle5 = @0x5;
        let oracle6 = @0x6;
        let oracle7 = @0x7;
        let oracle8 = @0x8;
        let oracle9 = @0x9;
        let oracle10 = @0x10;
        let oracle11 = @0x11;
        let oracle12 = @0x12;
        let oracle13 = @0x13;
        let oracle14 = @0x14;
        let oracle15 = @0x15;
        let oracle16 = @0x16;
        let oracle17 = @0x17;
        let oracle18 = @0x18;

        // add 18 results
        let result1 = decimal::new(100000000000000000, false);
        let result2 = decimal::new(123456789000000000, false);
        let result3 = decimal::new(567891234000000000, false);
        let result4 = decimal::new(789123456000000000, false);
        let result5 = decimal::new(912345678000000000, false);
        let result6 = decimal::new(345678912000000000, false);
        let result7 = decimal::new(456789123000000000, false);
        let result8 = decimal::new(567891234000000000, false);
        let result9 = decimal::new(678912345000000000, false);
        let result10 = decimal::new(789123456000000000, false);
        let result11 = decimal::new(891234567000000000, false);
        let result12 = decimal::new(912345678000000000, false);
        let result13 = decimal::new(123456789000000000, false);
        let result14 = decimal::new(234567891000000000, false);
        let result15 = decimal::new(345678912000000000, false);
        let result16 = decimal::new(456789123000000000, false);
        let result17 = decimal::new(567891234000000000, false);
        let result18 = decimal::new(678912345000000000, false);

        timestamp::update_global_time_for_test_secs(18000000);
        
        add_result(aggregator_object, result1, 1000000, oracle1);
        add_result(aggregator_object, result2, 2000000, oracle2);
        add_result(aggregator_object, result3, 3000000, oracle3);
        add_result(aggregator_object, result4, 4000000, oracle4);
        add_result(aggregator_object, result5, 5000000, oracle5);
        add_result(aggregator_object, result6, 6000000, oracle6);
        add_result(aggregator_object, result7, 7000000, oracle7);
        add_result(aggregator_object, result8, 8000000, oracle8);
        add_result(aggregator_object, result9, 9000000, oracle9);
        add_result(aggregator_object, result10, 10000000, oracle10);
        add_result(aggregator_object, result11, 11000000, oracle11);
        add_result(aggregator_object, result12, 12000000, oracle12);
        add_result(aggregator_object, result13, 13000000, oracle13);
        add_result(aggregator_object, result14, 14000000, oracle14);
        add_result(aggregator_object, result15, 15000000, oracle15);
        add_result(aggregator_object, result16, 16000000, oracle16);
        add_result(aggregator_object, result17, 17000000, oracle17);
        add_result(aggregator_object, result18, 18000000, oracle18);

        let current_result = compute_current_result(aggregator_object, 18001);
        assert!(option::is_some(&current_result), err);

        let current_result = option::extract(&mut current_result);
        let expected_mean = 582414498562500000;
        let expected_range = 788888889000000000;
        let expected_stdev = 244005876836959584;
        let tolerated_precision_err = 30;

        assert!(result(&current_result) == result3, decimal::value(&result(&current_result)) as u64);
        assert!(min_timestamp(&current_result) == 3000000, err);
        assert!(max_timestamp(&current_result) == 18000000, err);
        assert!(min_result(&current_result) == result13, decimal::value(&min_result(&current_result)) as u64);
        assert!(max_result(&current_result) == result12, decimal::value(&max_result(&current_result)) as u64);
        assert!(
            decimal::value(&stdev(&current_result))> expected_stdev - tolerated_precision_err && 
            decimal::value(&stdev(&current_result)) < expected_stdev + tolerated_precision_err, 
            decimal::value(&stdev(&current_result)) as u64,
        );
        assert!(range(&current_result) == decimal::new(expected_range, false), decimal::value(&range(&current_result)) as u64);
        assert!(mean(&current_result) == decimal::new(expected_mean, false), decimal::value(&mean(&current_result)) as u64);
    }

        #[test(account = @0x1)]
    public fun test_aggregator_updates_small(account: signer) acquires Aggregator, UpdateState, CurrentResult {
        let err: u64 = 1337;
        let queue_address = example_queue_address();
        let name = std::string::utf8(b"test_aggregator");
        let feed_hash = vector::empty<u8>();
        let min_sample_size = 1;
        let max_staleness_seconds = 1000000;
        let max_variance = 1000000;
        let min_responses = 1;
        timestamp::set_time_has_started_for_testing(&account);
        let aggregator_address = new_aggregator(
            &account,
            queue_address,
            name,
            feed_hash,
            min_sample_size,
            max_staleness_seconds,
            max_variance,
            min_responses,
        );

        let aggregator_object = object::address_to_object<Aggregator>(aggregator_address);
        let oracle1 = @0x1;
        let result1 = decimal::new(100000000000000000, false);
        timestamp::update_global_time_for_test_secs(18000000);
        add_result(aggregator_object, result1, 1000000, oracle1);
        let current_result = compute_current_result(aggregator_object, 18001);
        assert!(option::is_some(&current_result), err);
    }

    #[test_only]
    public fun example_queue_address(): address {
        @0x1
    }

}