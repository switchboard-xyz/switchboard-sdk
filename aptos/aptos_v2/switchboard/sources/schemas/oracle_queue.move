module switchboard::oracle_queue {
    use aptos_framework::timestamp;
    use switchboard::math::{SwitchboardDecimal};
    use switchboard::errors;
    use std::vector;
    use std::signer;
    
    friend switchboard::aggregator;
    friend switchboard::aggregator_init_action;
    friend switchboard::aggregator_add_job_action;
    friend switchboard::aggregator_remove_job_action;
    friend switchboard::aggregator_set_configs_action;
    friend switchboard::aggregator_open_round_action;
    friend switchboard::aggregator_save_result_action;
    friend switchboard::job_init_action;
    friend switchboard::crank_init_action;
    friend switchboard::crank_pop_action;
    friend switchboard::crank_push_action;
    friend switchboard::oracle_heartbeat_action;
    friend switchboard::oracle_init_action;
    friend switchboard::oracle_queue_init_action;
    friend switchboard::oracle_queue_set_configs_action;
    friend switchboard::lease_init_action;
    friend switchboard::oracle_wallet_init_action;

    struct OracleQueueData has key {
        data: vector<address>,
        curr_idx: u64,
        gc_idx: u64,
    }

    struct OracleQueueConfig has key {
        authority: address,
        reward: u64,
        open_round_reward: u64,
        save_reward: u64,
        save_confirmation_reward: u64,
        slashing_penalty: u64,
        slashing_enabled: bool,
        unpermissioned_feeds_enabled: bool,
        variance_tolerance_multiplier: SwitchboardDecimal,
        oracle_timeout: u64,
    }

    struct OracleQueue<phantom CoinType> has key {
        name: vector<u8>,
        metadata: vector<u8>,
        feed_probation_period: u64,
        consecutive_feed_failure_limit: u64,
        consecutive_oracle_failure_limit: u64,
        unpermissioned_vrf_enabled: bool,
        lock_lease_funding: bool,
        enable_buffer_relayers: bool,
        min_stake: u64,
        max_size: u64,
        created_at: u64,
        features: vector<bool>,
        _ebuf: vector<u8>
    }

    public(friend) fun set_config<CoinType>(
        addr: address, 
        authority: address,
        name: vector<u8>,
        metadata: vector<u8>,
        oracle_timeout: u64,
        reward: u64,
        min_stake: u64,
        slashing_enabled: bool,
        variance_tolerance_multiplier: SwitchboardDecimal,
        feed_probation_period: u64,
        consecutive_feed_failure_limit: u64,
        consecutive_oracle_failure_limit: u64,
        unpermissioned_feeds_enabled: bool,
        lock_lease_funding: bool,
        max_size: u64,
        save_confirmation_reward: u64,
        save_reward: u64,
        open_round_reward: u64,
        slashing_penalty: u64,
    ) acquires OracleQueue, OracleQueueConfig {
        let oracle_queue = borrow_global_mut<OracleQueue<CoinType>>(addr);
        let oracle_queue_config = borrow_global_mut<OracleQueueConfig>(addr);

        // queue metadata
        oracle_queue.min_stake = min_stake;
        oracle_queue.feed_probation_period = feed_probation_period;
        oracle_queue.consecutive_feed_failure_limit = consecutive_feed_failure_limit;
        oracle_queue.consecutive_oracle_failure_limit = consecutive_oracle_failure_limit;
        oracle_queue.lock_lease_funding = lock_lease_funding;
        oracle_queue.max_size = max_size;
        oracle_queue.name = name;
        oracle_queue.metadata = metadata;

        // queue configs
        oracle_queue_config.unpermissioned_feeds_enabled = unpermissioned_feeds_enabled;
        oracle_queue_config.authority = authority;
        oracle_queue_config.oracle_timeout = oracle_timeout;
        oracle_queue_config.reward = reward;
        oracle_queue_config.save_confirmation_reward = save_confirmation_reward;
        oracle_queue_config.save_reward = save_reward;
        oracle_queue_config.open_round_reward = open_round_reward;
        oracle_queue_config.slashing_penalty = slashing_penalty;
        oracle_queue_config.slashing_enabled = slashing_enabled;
        oracle_queue_config.variance_tolerance_multiplier = variance_tolerance_multiplier;
    }

    public fun exist<CoinType>(queue: address): bool {
        exists<OracleQueue<CoinType>>(queue)
    }

    public fun has_authority(addr: address, account: &signer): bool acquires OracleQueueConfig {
        let ref = borrow_global<OracleQueueConfig>(addr);
        ref.authority == signer::address_of(account)
    }

    public fun reward(self: address): u64 acquires OracleQueueConfig {
        borrow_global<OracleQueueConfig>(self).reward
    }

    public fun feed_probation_period<CoinType>(self: address): u64 acquires OracleQueue {
        borrow_global<OracleQueue<CoinType>>(self).feed_probation_period
    }

    public fun slashing_enabled<CoinType>(self: address): bool acquires OracleQueueConfig {
        borrow_global<OracleQueueConfig>(self).slashing_enabled
    }

    public fun variance_tolerance_multiplier<CoinType>(self: address): SwitchboardDecimal acquires OracleQueueConfig {
        borrow_global<OracleQueueConfig>(self).variance_tolerance_multiplier
    }

    public fun authority<CoinType>(self: address): address acquires OracleQueueConfig {
        borrow_global<OracleQueueConfig>(self).authority
    }

    public fun max_reward<CoinType>(self: address, batch_size: u64): u64 acquires OracleQueueConfig {
        borrow_global<OracleQueueConfig>(self).reward * (batch_size + 1)
    }

    public fun unpermissioned_feeds_enabled<CoinType>(self: address): bool acquires OracleQueueConfig {
        borrow_global<OracleQueueConfig>(self).unpermissioned_feeds_enabled
    }

    public fun lock_lease_funding<CoinType>(self: address): bool acquires OracleQueue {
        borrow_global<OracleQueue<CoinType>>(self).lock_lease_funding
    }

    public fun min_stake<CoinType>(self: address): u64 acquires OracleQueue {
        borrow_global<OracleQueue<CoinType>>(self).min_stake
    }

    public fun data_len(self: address): u64 acquires OracleQueueData {
        vector::length(&borrow_global<OracleQueueData>(self).data)
    }

    public(friend) fun oracle_queue_create<CoinType>(
        account: &signer, 
        authority: address,
        name: vector<u8>,
        metadata: vector<u8>,
        oracle_timeout: u64,
        reward: u64,
        min_stake: u64,
        slashing_enabled: bool,
        variance_tolerance_multiplier: SwitchboardDecimal,
        feed_probation_period: u64,
        consecutive_feed_failure_limit: u64,
        consecutive_oracle_failure_limit: u64,
        unpermissioned_feeds_enabled: bool,
        unpermissioned_vrf_enabled: bool,
        lock_lease_funding: bool,
        enable_buffer_relayers: bool,
        max_size: u64,
        data: vector<address>,
        save_confirmation_reward: u64,
        save_reward: u64,
        open_round_reward: u64,
        slashing_penalty: u64,
    ) {
        assert!(!exists<OracleQueueData>(signer::address_of(account)), errors::ResourceAlreadyExists());
        move_to(
            account, 
            OracleQueue<CoinType> {
                name,
                metadata,
                feed_probation_period,
                consecutive_feed_failure_limit,
                consecutive_oracle_failure_limit,
                unpermissioned_vrf_enabled,
                lock_lease_funding,
                enable_buffer_relayers,
                min_stake,
                max_size,
                created_at: timestamp::now_seconds(),
                features: vector::empty(),
                _ebuf: vector::empty(),
            }
        );
        move_to(
            account, 
            OracleQueueData {
                curr_idx: 0,
                gc_idx: 0,
                data,
            }
        );
        move_to(
            account, 
            OracleQueueConfig {
                authority,
                reward,
                open_round_reward,
                save_reward,
                save_confirmation_reward,
                slashing_penalty,
                slashing_enabled,
                unpermissioned_feeds_enabled,
                variance_tolerance_multiplier,
                oracle_timeout,
            }
        );
    }

    public(friend) fun next_n(self: address, batch_size: u64): vector<address> acquires OracleQueueData {
        let queue = borrow_global_mut<OracleQueueData>(self);
        let alloc = vector::empty();
        let idx = queue.curr_idx;
        let size = vector::length(&queue.data);
        let count = batch_size;
        while (count > 0) {
            let oracle_key = *vector::borrow(&queue.data, idx);
            idx = idx + 1;
            idx = idx % size;
            queue.curr_idx = idx;
            count = count - 1;
            vector::push_back(&mut alloc, oracle_key);
        };
        alloc
    }
    
    public(friend) fun next_garbage_collection_oracle(self: address): (address, u64) acquires OracleQueueData {
        let queue = borrow_global_mut<OracleQueueData>(self);
        let gc_address = *vector::borrow(&queue.data, queue.gc_idx);
        let idx = queue.gc_idx;
        queue.gc_idx = queue.gc_idx + 1;
        queue.gc_idx = queue.gc_idx % vector::length(&queue.data);
        (gc_address, idx)
    }

    public(friend) fun garbage_collect(queue_addr: address, gc_idx: u64) acquires OracleQueueData {
        let queue = borrow_global_mut<OracleQueueData>(queue_addr);
        vector::swap_remove(&mut queue.data, gc_idx);
        let new_len = vector::length(&queue.data);
        queue.curr_idx = queue.curr_idx % new_len;
        queue.gc_idx = queue.gc_idx % new_len;
    }

    public(friend) fun push_back(self: address, oracle: address) acquires OracleQueueData {
        let queue = borrow_global_mut<OracleQueueData>(self);
        vector::push_back(&mut queue.data, oracle);
    }

    /**
     * config_info allows us to grab relevant configuration data 
     * and metadata about an oracle queue. This is done because 
     * using individual accessors requires way too many loads from 
     * global storage. 
     */
    public(friend) fun configs(self: address): (
        address, // Authority
        u64,     // Reward
        u64,     // Open Round Reward
        u64,     // Reward for Save Result call
        u64,     // Reward for Save Result call with Confirmation
        u64,     // Slashing Penalty
        bool,    // Slashing enabled
        SwitchboardDecimal, // Variance tolerance
        bool,    // Unpermissioned feeds enabled
    ) acquires OracleQueueConfig {
        let queue = borrow_global<OracleQueueConfig>(self);
        (
            queue.authority,
            queue.reward,
            queue.open_round_reward,
            queue.save_reward,
            queue.save_confirmation_reward,
            queue.slashing_penalty,
            queue.slashing_enabled,
            queue.variance_tolerance_multiplier,
            queue.unpermissioned_feeds_enabled,
        )
    }
}
