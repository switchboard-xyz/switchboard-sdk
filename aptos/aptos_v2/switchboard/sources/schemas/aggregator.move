module switchboard::aggregator {
    use aptos_framework::account::{Self, SignerCapability};
    use aptos_framework::timestamp;
    use aptos_framework::block;
    use aptos_std::ed25519;
    use switchboard::serialization;
    use switchboard::job::{Self, Job};
    use switchboard::oracle_queue;
    use switchboard::math::{Self, SwitchboardDecimal};
    use switchboard::vec_utils;
    use switchboard::errors;
    use switchboard::switchboard;
    use std::hash;
    use std::option::{Self, Option};
    use std::signer; 
    use std::vector;
    use std::coin::{Self, Coin};

    friend switchboard::aggregator_init_action;
    friend switchboard::aggregator_add_job_action;
    friend switchboard::aggregator_fetch_multiple;
    friend switchboard::aggregator_remove_job_action;
    friend switchboard::aggregator_set_configs_action;
    friend switchboard::aggregator_open_round_action;
    friend switchboard::aggregator_save_result_action;
    friend switchboard::aggregator_lock_action;
    friend switchboard::crank_push_action;
    friend switchboard::crank_pop_action;
    friend switchboard::lease_init_action;
    friend switchboard::lease_extend_action;
    friend switchboard::lease_withdraw_action;

    // Aggregator Round Data
    struct LatestConfirmedRound {}
    struct CurrentRound {}
    struct AggregatorRound<phantom T> has key, store, copy, drop {
        // Maintains the current update count
        id: u128,
        // Maintains the time that the round was opened at.
        round_open_timestamp: u64,
        // Maintain the blockheight at the time that the round was opened
        round_open_block_height: u64,
        // Maintains the current median of all successful round responses.
        result: SwitchboardDecimal,
        // Standard deviation of the accepted results in the round.
        std_deviation: SwitchboardDecimal,
        // Maintains the minimum node response this round.
        min_response: SwitchboardDecimal,
        // Maintains the maximum node response this round.
        max_response: SwitchboardDecimal,
        // Pubkeys of the oracles fulfilling this round.
        oracle_keys: vector<address>,
        // Represents all successful node responses this round. `NaN` if empty.
        medians: vector<Option<SwitchboardDecimal>>,
        // Payouts so far in a given round
        current_payout: vector<SwitchboardDecimal>,
        // could do specific error codes
        errors_fulfilled: vector<bool>,
        // Maintains the number of successful responses received from nodes.
        // Nodes can submit one successful response per round.
        num_success: u64,
        num_error: u64,
        // Maintains whether or not the round is closed
        is_closed: bool,
        // Maintains the round close timestamp
        round_confirmed_timestamp: u64,
    }

    fun default_round<T>(): AggregatorRound<T> {
        
        AggregatorRound<T> {
            id: 0,
            round_open_timestamp: 0,
            round_open_block_height: block::get_current_block_height(),
            result: math::zero(),
            std_deviation: math::zero(),
            min_response: math::zero(),
            max_response: math::zero(),
            oracle_keys: vector::empty(),
            medians: vector::empty(),
            errors_fulfilled: vector::empty(),
            num_error: 0,
            num_success: 0,
            is_closed: false,
            round_confirmed_timestamp: 0,
            current_payout: vector::empty(),
        }
    }

    struct Aggregator has key, store, drop {
        
        // Aggregator account signer cap
        signer_cap: SignerCapability,

        // Configs
        authority: address,
        name: vector<u8>,
        metadata: vector<u8>,

        // Aggregator data that's fairly fixed
        created_at: u64,
        is_locked: bool,
        _ebuf: vector<u8>,
        features: vector<bool>,
    }

    // Frequently used configs 
    struct AggregatorConfig has key {
        queue_addr: address,
        batch_size: u64,
        min_oracle_results: u64,
        min_update_delay_seconds: u64,
        history_limit: u64,
        variance_threshold: SwitchboardDecimal, 
        force_report_period: u64,
        min_job_results: u64,
        expiration: u64,
        crank_addr: address,
        crank_disabled: bool,
        crank_row_count: u64,
        next_allowed_update_time: u64,
        consecutive_failure_count: u64,
        start_after: u64,
    }

    // Configuation items that are only used on the Oracle side
    struct AggregatorResultsConfig has key {
        variance_threshold: SwitchboardDecimal,
        force_report_period: u64,
        min_job_results: u64,
        expiration: u64,
    }

    struct AggregatorReadConfig has key {
        read_charge: u64,
        reward_escrow: address,
        read_whitelist: vector<address>,
        limit_reads_to_whitelist: bool,
    }

    struct AggregatorJobData has key {
        job_keys: vector<address>,
        job_weights: vector<u8>,
        jobs_checksum: vector<u8>,
    }

    struct AggregatorHistoryData has key {
        history: vector<AggregatorHistoryRow>,
        history_write_idx: u64,
    }

    struct AggregatorHistoryRow has drop, copy, store {
        value: SwitchboardDecimal,
        timestamp: u64,
        round_id: u128,
    }

    struct AggregatorConfigParams has drop, copy {
        addr: address,
        name: vector<u8>,
        metadata: vector<u8>,
        queue_addr: address,
        crank_addr: address,
        batch_size: u64,
        min_oracle_results: u64,
        min_job_results: u64,
        min_update_delay_seconds: u64,
        start_after: u64,
        variance_threshold: SwitchboardDecimal,
        force_report_period: u64,
        expiration: u64,
        disable_crank: bool,
        history_limit: u64,
        read_charge: u64,
        reward_escrow: address,
        read_whitelist: vector<address>,
        limit_reads_to_whitelist: bool,
        authority: address,
    }

    struct FeedRelay has key {
        oracle_keys: vector<vector<u8>>, 
        authority: address,
    }

    struct AggregatorFull has key {
        addr: address,
        authority: address,
        name: vector<u8>,
        metadata: vector<u8>,
        queue_addr: address,
        batch_size: u64,
        min_oracle_results: u64,
        min_job_results: u64,
        min_update_delay_seconds: u64,
        start_after: u64,
        variance_threshold: SwitchboardDecimal, 
        force_report_period: u64,
        expiration: u64,
        read_charge: u64,
        reward_escrow: address,
        read_whitelist: vector<address>,
        crank_disabled: bool,
        history_limit: u64,
        limit_reads_to_whitelist: bool,
        next_allowed_update_time: u64,
        consecutive_failure_count: u64,
        crank_addr: address,
        latest_confirmed_round: AggregatorRound<LatestConfirmedRound>,
        current_round: AggregatorRound<CurrentRound>,
        job_keys: vector<address>,
        job_weights: vector<u8>,
        jobs_checksum: vector<u8>,
        history: vector<AggregatorHistoryRow>,
        history_write_idx: u64,
        created_at: u64,
        is_locked: bool,
        crank_row_count: u64,
        _ebuf: vector<u8>,
        features: vector<bool>,
    }

    public fun addr_from_conf(conf: &AggregatorConfigParams): address {
        conf.addr
    }

    public fun queue_from_conf(conf: &AggregatorConfigParams): address {
        conf.queue_addr
    }

    public fun authority_from_conf(conf: &AggregatorConfigParams): address {
        conf.authority
    }

    public fun history_limit_from_conf(conf: &AggregatorConfigParams): u64 {
        conf.history_limit
    }
    
    public fun batch_size_from_conf(conf: &AggregatorConfigParams): u64 {
        conf.batch_size
    }

    public fun min_oracle_results_from_conf(conf: &AggregatorConfigParams): u64 {
        conf.min_oracle_results
    }

    public fun min_update_delay_seconds_from_conf(conf: &AggregatorConfigParams): u64 {
        conf.min_update_delay_seconds
    }

    public(friend) fun new_config(
        addr: address,
        name: vector<u8>,
        metadata: vector<u8>,
        queue_addr: address,
        crank_addr: address,
        batch_size: u64,
        min_oracle_results: u64,
        min_job_results: u64,
        min_update_delay_seconds: u64,
        start_after: u64,
        variance_threshold: SwitchboardDecimal,
        force_report_period: u64,
        expiration: u64,
        disable_crank: bool,
        history_limit: u64,
        read_charge: u64,
        reward_escrow: address,
        read_whitelist: vector<address>,
        limit_reads_to_whitelist: bool,
        authority: address,
    ): AggregatorConfigParams {
        AggregatorConfigParams {
            addr,
            name,
            metadata,
            queue_addr,
            crank_addr,
            batch_size,
            min_oracle_results,
            min_job_results,
            min_update_delay_seconds,
            start_after,
            variance_threshold,
            force_report_period,
            expiration,
            disable_crank,
            history_limit,
            read_charge,
            reward_escrow,
            read_whitelist,
            limit_reads_to_whitelist,
            authority,
        }
    }

    public fun exist(addr: address): bool {
        exists<Aggregator>(addr)
    }

    public(friend) fun fetch_full(
        addr: address
    ): AggregatorFull acquires Aggregator, AggregatorConfig, AggregatorReadConfig, AggregatorJobData, AggregatorHistoryData, AggregatorRound {
        let agg = borrow_global<Aggregator>(addr);
        let config = borrow_global<AggregatorConfig>(addr);
        let read_config = borrow_global<AggregatorReadConfig>(addr);
        let job_data = borrow_global<AggregatorJobData>(addr);
        let history_data = borrow_global<AggregatorHistoryData>(addr);
        let latest_confirmed_round = borrow_global<AggregatorRound<LatestConfirmedRound>>(addr);
        let current_round = borrow_global<AggregatorRound<CurrentRound>>(addr);

        AggregatorFull {
            addr: addr,
            authority: agg.authority,
            name: agg.name,
            metadata: agg.metadata,
            queue_addr: config.queue_addr,
            batch_size: config.batch_size,
            min_oracle_results: config.min_oracle_results,
            min_job_results: config.min_job_results,
            min_update_delay_seconds: config.min_update_delay_seconds,
            start_after: config.start_after,
            variance_threshold: config.variance_threshold,
            force_report_period: config.force_report_period,
            expiration: config.expiration,
            read_charge: read_config.read_charge,
            reward_escrow: read_config.reward_escrow,
            read_whitelist: read_config.read_whitelist,
            crank_disabled: config.crank_disabled,
            history_limit: config.history_limit,
            limit_reads_to_whitelist: read_config.limit_reads_to_whitelist,
            next_allowed_update_time: config.next_allowed_update_time,
            consecutive_failure_count: config.consecutive_failure_count,
            crank_addr: config.crank_addr,
            latest_confirmed_round: *latest_confirmed_round,
            current_round: *current_round,
            job_keys: job_data.job_keys,
            job_weights: job_data.job_weights,
            jobs_checksum: job_data.jobs_checksum,
            history: history_data.history,
            history_write_idx: history_data.history_write_idx,
            created_at: agg.created_at,
            is_locked: agg.is_locked,
            crank_row_count: config.crank_row_count,
            _ebuf: agg._ebuf,
            features: agg.features,
        }
    }

    public fun has_authority(addr: address, account: &signer): bool acquires Aggregator {
        let ref = borrow_global<Aggregator>(addr);
        ref.authority == signer::address_of(account)
    }

    public fun buy_latest_value<CoinType>(
        account: &signer, 
        addr: address, 
        fee: Coin<CoinType>
    ): SwitchboardDecimal acquires AggregatorConfig, AggregatorReadConfig, AggregatorRound {
        let aggregator_config = borrow_global<AggregatorConfig>(addr);
        let aggregator_read_config = borrow_global<AggregatorReadConfig>(addr);
        let fee_value: u64 = coin::value(&fee);
        assert!(oracle_queue::exist<CoinType>(aggregator_config.queue_addr), errors::QueueNotFound());
        if (aggregator_read_config.limit_reads_to_whitelist) {
            assert!(vector::contains(&aggregator_read_config.read_whitelist, &signer::address_of(account)), errors::PermissionDenied());
        } else {
            assert!(
                fee_value == aggregator_read_config.read_charge ||
                vector::contains(&aggregator_read_config.read_whitelist, &signer::address_of(account)), 
                errors::InvalidArgument()
            );
        };
        coin::deposit(aggregator_read_config.reward_escrow, fee);
        let latest_confirmed_round = borrow_global<AggregatorRound<LatestConfirmedRound>>(addr);
        switchboard::emit_aggregator_read_event(addr, fee_value, latest_confirmed_round.result);
        latest_confirmed_round.result
    }

    public fun buy_latest_round<CoinType>(account: &signer, addr: address, fee: Coin<CoinType>): (
        SwitchboardDecimal, /* Result */
        u64,                /* Round Confirmed Timestamp */
        SwitchboardDecimal, /* Standard Deviation of Oracle Responses */
        SwitchboardDecimal, /* Min Oracle Response */
        SwitchboardDecimal, /* Max Oracle Response */
    ) acquires AggregatorConfig, AggregatorReadConfig, AggregatorRound {
        let aggregator_config = borrow_global_mut<AggregatorConfig>(addr);
        let aggregator_read_config = borrow_global_mut<AggregatorReadConfig>(addr);
        let fee_value: u64 = coin::value(&fee);
        assert!(oracle_queue::exist<CoinType>(aggregator_config.queue_addr), errors::QueueNotFound());
        if (aggregator_read_config.limit_reads_to_whitelist) {
            assert!(vector::contains(&aggregator_read_config.read_whitelist, &signer::address_of(account)), errors::PermissionDenied());
        } else {
            assert!(
                fee_value == aggregator_read_config.read_charge ||
                vector::contains(&aggregator_read_config.read_whitelist, &signer::address_of(account)), 
                errors::InvalidArgument()
            );
        };
        coin::deposit(aggregator_read_config.reward_escrow, fee);
        let latest_confirmed_round = borrow_global<AggregatorRound<LatestConfirmedRound>>(addr);
        switchboard::emit_aggregator_read_event(addr, fee_value, latest_confirmed_round.result);
        (
            latest_confirmed_round.result,
            latest_confirmed_round.round_confirmed_timestamp,
            latest_confirmed_round.std_deviation,
            latest_confirmed_round.min_response,
            latest_confirmed_round.max_response,
        )
    }

    public fun latest_value(addr: address): SwitchboardDecimal acquires AggregatorRound, AggregatorReadConfig {
        let aggregator_read_config = borrow_global_mut<AggregatorReadConfig>(addr);
        assert!(aggregator_read_config.read_charge == 0 && !aggregator_read_config.limit_reads_to_whitelist, errors::PermissionDenied());
        let latest_confirmed_round = borrow_global<AggregatorRound<LatestConfirmedRound>>(addr);
        switchboard::emit_aggregator_read_event(addr, 0, latest_confirmed_round.result);
        latest_confirmed_round.result
    }


    public fun latest_value_in_threshold(addr: address, max_confidence_interval: &SwitchboardDecimal): (SwitchboardDecimal, bool) acquires AggregatorRound, AggregatorReadConfig {
        let aggregator_read_config = borrow_global_mut<AggregatorReadConfig>(addr);
        assert!(aggregator_read_config.read_charge == 0 && !aggregator_read_config.limit_reads_to_whitelist, errors::PermissionDenied());
        let latest_confirmed_round = borrow_global<AggregatorRound<LatestConfirmedRound>>(addr);
        let uwm = vec_utils::unwrap(&latest_confirmed_round.medians);
        let std_deviation = math::std_deviation(&uwm, &latest_confirmed_round.result);
        let is_within_bound = math::gt(&std_deviation, max_confidence_interval);
        switchboard::emit_aggregator_read_event(addr, 0, latest_confirmed_round.result);
        (borrow_global<AggregatorRound<LatestConfirmedRound>>(addr).result, is_within_bound)
    }


    public fun latest_round(addr: address): (
        SwitchboardDecimal, /* Result */
        u64,                /* Round Confirmed Timestamp */
        SwitchboardDecimal, /* Standard Deviation of Oracle Responses */
        SwitchboardDecimal, /* Min Oracle Response */
        SwitchboardDecimal, /* Max Oracle Response */
    ) acquires AggregatorRound, AggregatorReadConfig {
        let aggregator = borrow_global_mut<AggregatorReadConfig>(addr);
        assert!(aggregator.read_charge == 0 && !aggregator.limit_reads_to_whitelist, errors::PermissionDenied());
        let latest_confirmed_round = borrow_global<AggregatorRound<LatestConfirmedRound>>(addr);
        switchboard::emit_aggregator_read_event(addr, 0, latest_confirmed_round.result);
        (
            latest_confirmed_round.result,
            latest_confirmed_round.round_confirmed_timestamp,
            latest_confirmed_round.std_deviation,
            latest_confirmed_round.min_response,
            latest_confirmed_round.max_response,
        )
    }

    // GETTERS

    public fun latest_round_timestamp(addr: address): u64 acquires AggregatorRound {
        let latest_confirmed_round = borrow_global<AggregatorRound<LatestConfirmedRound>>(addr);
        latest_confirmed_round.round_confirmed_timestamp
    }

    public fun latest_round_open_timestamp(addr: address): u64 acquires AggregatorRound {
        let latest_confirmed_round = borrow_global<AggregatorRound<LatestConfirmedRound>>(addr);
        latest_confirmed_round.round_open_timestamp
    }

    public fun lastest_round_min_response(addr: address): SwitchboardDecimal acquires AggregatorRound {
        let latest_confirmed_round = borrow_global<AggregatorRound<LatestConfirmedRound>>(addr);
        latest_confirmed_round.min_response
    }

    public fun lastest_round_max_response(addr: address): SwitchboardDecimal acquires AggregatorRound {
        let latest_confirmed_round = borrow_global<AggregatorRound<LatestConfirmedRound>>(addr);
        latest_confirmed_round.max_response
    }

    public fun authority(addr: address): address acquires Aggregator {
        let aggregator = borrow_global<Aggregator>(addr);
        aggregator.authority
    }

    public fun is_locked(addr: address): bool acquires Aggregator {
        let aggregator = borrow_global<Aggregator>(addr);
        aggregator.is_locked
    }

    public fun read_charge(addr: address): u64 acquires AggregatorReadConfig {
        let aggregator = borrow_global<AggregatorReadConfig>(addr);
        aggregator.read_charge
    }

    public fun next_allowed_timestamp(addr: address): u64 acquires AggregatorConfig {
        let aggregator = borrow_global<AggregatorConfig>(addr);
        aggregator.next_allowed_update_time
    }

    public fun job_keys(addr: address): vector<address> acquires AggregatorJobData {
        borrow_global<AggregatorJobData>(addr).job_keys
    }

    public fun min_oracle_results(addr: address): u64 acquires AggregatorConfig {
        borrow_global<AggregatorConfig>(addr).min_oracle_results
    }

    public fun crank_addr(addr: address): address acquires AggregatorConfig {
        borrow_global<AggregatorConfig>(addr).crank_addr
    }

    public fun crank_disabled(addr: address): bool acquires AggregatorConfig {
        borrow_global<AggregatorConfig>(addr).crank_disabled
    }

    public(friend) fun crank_row_count(self: address): u64 acquires AggregatorConfig {
        borrow_global<AggregatorConfig>(self).crank_row_count
    }

    public fun current_round_num_success(addr: address): u64 acquires AggregatorRound {
        let current_round = borrow_global<AggregatorRound<CurrentRound>>(addr);
        current_round.num_success
    }

    public fun current_round_open_timestamp(addr: address): u64 acquires AggregatorRound {
        let current_round = borrow_global<AggregatorRound<CurrentRound>>(addr);
        current_round.round_open_timestamp
    }

    public fun current_round_num_error(addr: address): u64 acquires AggregatorRound {
        let current_round = borrow_global<AggregatorRound<CurrentRound>>(addr);
        current_round.num_error
    }

    public fun curent_round_oracle_key_at_idx(addr: address, idx: u64): address acquires AggregatorRound {
        let current_round = borrow_global<AggregatorRound<CurrentRound>>(addr);
        *vector::borrow(&current_round.oracle_keys, idx)
    }

    public fun curent_round_median_at_idx(addr: address, idx: u64): SwitchboardDecimal acquires AggregatorRound {
        let current_round = borrow_global<AggregatorRound<CurrentRound>>(addr);
        let median = vector::borrow(&current_round.medians, idx);
        *option::borrow<SwitchboardDecimal>(median)
    }
    
    public fun current_round_std_dev(addr: address): SwitchboardDecimal acquires AggregatorRound {
        let current_round = borrow_global<AggregatorRound<CurrentRound>>(addr);
        current_round.std_deviation
    }

    public fun current_round_result(addr: address): SwitchboardDecimal acquires AggregatorRound {
        let current_round = borrow_global<AggregatorRound<CurrentRound>>(addr);
        current_round.result
    }

    public fun is_median_fulfilled(addr: address, idx: u64): bool acquires AggregatorRound {
        let current_round = borrow_global<AggregatorRound<CurrentRound>>(addr);
        let val = vector::borrow(&current_round.medians, idx);
        option::is_some(val)
    }

    public fun is_error_fulfilled(addr: address, idx: u64): bool acquires AggregatorRound {
        let current_round = borrow_global<AggregatorRound<CurrentRound>>(addr);
        *vector::borrow(&current_round.errors_fulfilled, idx)
    }

    public fun configs(self: address): (
        address, // Queue Address
        u64,     // Batch Size
        u64,     // Min Oracle Results
    ) acquires AggregatorConfig {
        let aggregator = borrow_global<AggregatorConfig>(self);
        (
            aggregator.queue_addr,
            aggregator.batch_size,
            aggregator.min_oracle_results,
        )
    }

    public fun batch_size(self: address): u64 acquires AggregatorConfig {
        borrow_global<AggregatorConfig>(self).batch_size
    }
    
    public fun queue_addr(addr: address): address acquires AggregatorConfig {
        borrow_global<AggregatorConfig>(addr).queue_addr
    }

    public fun history_limit(self: address): u64 acquires AggregatorConfig {
        borrow_global<AggregatorConfig>(self).history_limit
    }
    
    public fun can_open_round(addr: address): bool acquires AggregatorConfig {
        let ref = borrow_global<AggregatorConfig>(addr);
        timestamp::now_seconds() >= ref.start_after &&
        timestamp::now_seconds() >= ref.next_allowed_update_time
    }

    public fun is_jobs_checksum_equal(addr: address, vec: &vector<u8>): bool acquires AggregatorJobData {
        let checksum = borrow_global<AggregatorJobData>(addr).jobs_checksum; // copy
        &checksum == vec
    }

    // Get latest value
    public(friend) fun latest_value_internal(addr: address): SwitchboardDecimal acquires AggregatorRound {
        borrow_global<AggregatorRound<LatestConfirmedRound>>(addr).result
    }

    /**
     * can_save_result determines whether or not an oracle result can be saved to an aggregator
     * returns 0 if successful
     */
    public(friend) fun can_save_result( 
        aggregator_addr: address,
        oracle_addr: address,
        oracle_idx: u64,
        jobs_checksum: &vector<u8>
    ): u64 acquires AggregatorJobData, AggregatorRound {
        let aggregator = borrow_global<AggregatorJobData>(aggregator_addr);
        let current_round = borrow_global<AggregatorRound<CurrentRound>>(aggregator_addr);

        // ensure that the correct oracle is responding
        let cr_oracle_addr = vector::borrow(&current_round.oracle_keys, oracle_idx);
        if (&oracle_addr != cr_oracle_addr) {
            return errors::OracleMismatch()
        };

        // verify jobs checksum in case of rpc issue
        let checksum = aggregator.jobs_checksum;
        if (&checksum != jobs_checksum) {
            return errors::JobsChecksumMismatch()
        };

        // ensure that no result has already been marked for this oracle
        let median_fulfilled = option::is_some(vector::borrow(&current_round.medians, oracle_idx));
        let error_fulfilled = *vector::borrow(&current_round.errors_fulfilled, oracle_idx);
        
        if (median_fulfilled || error_fulfilled) {
            return errors::OracleAlreadyResponded()
        };

        // no error
        0       
    }

    /**
     * current_round_info pulls data about the current round for the save result action.
     * This is done because individual accessors have been extremely expensive in gas terms. 
     */
    public(friend) fun current_round_info(addr: address): (
        SwitchboardDecimal, // current_result
        SwitchboardDecimal, // current_round_std_dev 
        vector<Option<SwitchboardDecimal>>, // medians
        vector<bool>, // errors 
        vector<address>, // oracles responding
    ) acquires AggregatorRound {
        let current_round = borrow_global<AggregatorRound<CurrentRound>>(addr);
        (
            current_round.result,
            current_round.std_deviation,
            current_round.medians,
            current_round.errors_fulfilled,
            current_round.oracle_keys,
        )
    }

    // set feed relay info for a feed
    public entry fun set_feed_relay(
        account: signer, 
        aggregator_addr: address, 
        authority: address, 
        oracle_keys: vector<vector<u8>>
    ) acquires Aggregator, FeedRelay {
        assert!(has_authority(aggregator_addr, &account), errors::PermissionDenied());
        if (!exists<FeedRelay>(aggregator_addr)) {
            let aggregator_acct = get_aggregator_account(aggregator_addr);
            move_to(&aggregator_acct, FeedRelay {
                authority,
                oracle_keys
            });
        } else {
            let feed_relay = borrow_global_mut<FeedRelay>(aggregator_addr);
            feed_relay.oracle_keys = oracle_keys;
            feed_relay.authority = authority;
        };
    }

    public entry fun set_feed_relay_oracle_keys(
        account: signer, 
        aggregator_addr: address, 
        oracle_keys: vector<vector<u8>>
    ) acquires Aggregator, FeedRelay {
        let feed_relay = borrow_global_mut<FeedRelay>(aggregator_addr);
        assert!(
            feed_relay.authority == signer::address_of(&account) || 
            has_authority(aggregator_addr, &account),
            errors::PermissionDenied()
        );
        feed_relay.oracle_keys = oracle_keys;
    }


    // Update Aggregator with oracle keys from FeedRelay
    #[legacy_entry_fun]
    public entry fun relay_value(
        addr: address, 
        updates: &mut vector<vector<u8>>
    ) acquires AggregatorRound, AggregatorConfig, AggregatorJobData, FeedRelay {
        assert!(exists<FeedRelay>(addr), errors::FeedRelayNotFound());

        // wipe current round oracle keys - to avoid anachronic / unwanted updates until open round
        {
            borrow_global_mut<AggregatorRound<CurrentRound>>(addr).oracle_keys = vector::empty();
        };

        let latest_confirmed_round = borrow_global_mut<AggregatorRound<LatestConfirmedRound>>(addr);
        let job_checksum = borrow_global<AggregatorJobData>(addr).jobs_checksum;
        let last_round_confirmed_timestamp = latest_confirmed_round.round_confirmed_timestamp;
        let updates_length = vector::length(updates);
        let (_queue_addr, _batch_size, min_oracle_results) = configs(addr);
        let force_report_period = borrow_global<AggregatorConfig>(addr).force_report_period;
        let feed_relay = borrow_global<FeedRelay>(addr);
        let i = 0;
        let min = math::zero();
        let max = math::zero();
        let medians = vector::empty<SwitchboardDecimal>();
        while (i < updates_length) {
            let sb_update = vector::borrow_mut(updates, i);
            i = i + 1;
            let (
                value,             // SwitchboardDecimal
                min_value,         // SwitchboardDecimal
                max_value,         // SwitchboardDecimal
                timestamp_seconds, // u64,
                aggregator_addr,   // aggregator address
                checksum,          // jobs checksum
                _oracle_addr,      // oracle address
                oracle_public_key, // oracle public_key
                signature,         // message signature
                message,           // message
            ) = serialization::read_update(sb_update);

            assert!(job_checksum == checksum, errors::JobsChecksumMismatch());

            // validate that this oracle can make updates
            assert!(vector::contains(&feed_relay.oracle_keys, &oracle_public_key), errors::OracleMismatch());
            let public_key = ed25519::new_unvalidated_public_key_from_bytes(oracle_public_key);
            serialization::validate(message, signature, public_key);

            // here we at least know that oracle_addr signed this update
            // we want to make sure that it's actually meant for this feed
            assert!(aggregator_addr == addr, errors::FeedRelayIncorrectAggregator());

            // check that the timestamp is valid - don't punish old timestamps if within threshold
            assert!(timestamp_seconds >= timestamp::now_seconds() - force_report_period, errors::InvalidArgument());

            // ignore values that fall within acceptable timestamp range, but are technically stale
            if (timestamp_seconds < last_round_confirmed_timestamp) {
                continue
            };

            vector::push_back(&mut medians, value);
            if (i == 1) {
                min = min_value;
                max = max_value;
            } else {
                if (math::gt(&min, &min_value)) {
                    min = min_value;
                };
                if (math::lt(&max, &max_value)) {
                    max = max_value;
                };
            };
        };
        
        // if we met the threshold of fresh updates to trigger a new result (but within the staleness threshold)
        // then override latest round
        let successes = vector::length(&medians);
        if (successes >= min_oracle_results) {
            let wrapped_medians = {
                let i = 0;
                let vec = vector::empty<Option<SwitchboardDecimal>>();
                while (i < successes) {
                    vector::push_back(&mut vec, option::some(*vector::borrow(&medians, i)));
                    i = i + 1;
                };
                vec
            };

            // Update latest round
            latest_confirmed_round.id = latest_confirmed_round.id + 1;
            latest_confirmed_round.round_open_timestamp = timestamp::now_seconds();
            latest_confirmed_round.round_open_block_height = block::get_current_block_height();
            latest_confirmed_round.result = math::median(&mut medians);
            latest_confirmed_round.std_deviation = math::std_deviation(&medians, &latest_confirmed_round.result);
            latest_confirmed_round.min_response = min;
            latest_confirmed_round.max_response = max;
            latest_confirmed_round.oracle_keys = vector::empty();
            latest_confirmed_round.medians = wrapped_medians;
            latest_confirmed_round.errors_fulfilled = vector::empty();
            latest_confirmed_round.num_success = successes;
            latest_confirmed_round.num_error = 0;
            latest_confirmed_round.is_closed = true;
            latest_confirmed_round.round_confirmed_timestamp = timestamp::now_seconds();
        }
    }

    /**
     * apply_open_round_simulate allows us to return apply the crank row count
     * change and next allowed update time change while also determining 
     * rescheduling and next update time (if unsuccessful) used by crank pop
     */
    public(friend) fun apply_open_round_simulate(
        aggregator_addr: address, 
        simulation_result: u64,
        jitter: u64,
    ): (
        u64,  // next timestamp
        bool, // reschedule
    ) acquires AggregatorConfig {
        let aggregator_data = borrow_global_mut<AggregatorConfig>(aggregator_addr);
        if (simulation_result == 0 || simulation_result == errors::AggregatorIllegalRoundOpenCall()) { 
            (aggregator_data.next_allowed_update_time, true)
        } else if (simulation_result == errors::AggregatorQueueNotReady()) {

            // shift back one update interval (with jitter)
            let jitter = (timestamp::now_seconds() + jitter) % 5;
            aggregator_data.next_allowed_update_time = timestamp::now_seconds() + aggregator_data.min_update_delay_seconds + jitter;
            (aggregator_data.next_allowed_update_time, true)
        } else {

            // this is happening in a crank pop - remove from crank while we determine rescheduling
            aggregator_data.crank_row_count = aggregator_data.crank_row_count - 1;
            
            // don't reschedule 
            (aggregator_data.next_allowed_update_time, false)
        }
    }

    public(friend) fun aggregator_create(
        account: &signer, 
        signer_cap: SignerCapability,
        params: &AggregatorConfigParams,
    ) {
        move_to(
            account, 
            Aggregator {
                signer_cap,

                // Configs
                authority: params.authority,
                name: params.name,
                metadata: params.metadata,

                // Aggregator data that's fairly fixed
                created_at: timestamp::now_seconds(),
                is_locked: false,
                _ebuf: vector::empty(),
                features: vector::empty(),
            }
        );

        move_to(
            account, 
            AggregatorConfig {
                queue_addr: params.queue_addr,
                batch_size: params.batch_size,
                min_oracle_results: params.min_oracle_results,
                min_update_delay_seconds: params.min_update_delay_seconds,
                history_limit: params.history_limit,
                crank_addr: params.crank_addr,
                crank_disabled: params.disable_crank,
                crank_row_count: 0,
                next_allowed_update_time: 0,
                consecutive_failure_count: 0,
                start_after: params.start_after,
                variance_threshold: params.variance_threshold,
                force_report_period: params.force_report_period,
                min_job_results: params.min_job_results,
                expiration: params.expiration,
            }
        );

        move_to(
            account, 
            AggregatorReadConfig {
                read_charge: params.read_charge,
                reward_escrow: params.reward_escrow,
                read_whitelist: params.read_whitelist,
                limit_reads_to_whitelist: params.limit_reads_to_whitelist,
            }
        );

        move_to(
            account, 
            AggregatorJobData {
                job_keys: vector::empty(),
                job_weights: vector::empty(),
                jobs_checksum: vector::empty(),
            }
        );

        move_to(
            account, 
            AggregatorHistoryData {
                history: vector::empty(),
                history_write_idx: 0,
            }
        );

        move_to(
            account,
            AggregatorResultsConfig {
                variance_threshold: params.variance_threshold,
                force_report_period: params.force_report_period,
                min_job_results: params.min_job_results,
                expiration: params.expiration,
            }
        );
        
        move_to(account, default_round<LatestConfirmedRound>());
        move_to(account, default_round<CurrentRound>());
    }

    public(friend) fun get_aggregator_account(
        addr: address, 
    ): signer acquires Aggregator {
        let aggregator = borrow_global<Aggregator>(addr);
        let aggregator_acct = account::create_signer_with_capability(&aggregator.signer_cap);
        aggregator_acct
    }

    public(friend) fun set_config(
        params: &AggregatorConfigParams
    ) acquires 
        Aggregator, 
        AggregatorConfig,
        AggregatorReadConfig, 
        AggregatorResultsConfig,
        AggregatorHistoryData {
        let aggregator = borrow_global_mut<Aggregator>(params.addr);
        let aggregator_config = borrow_global_mut<AggregatorConfig>(params.addr);
        let aggregator_read_config = borrow_global_mut<AggregatorReadConfig>(params.addr);
        let aggregator_results_config = borrow_global_mut<AggregatorResultsConfig>(params.addr);
        let aggregator_history_data = borrow_global_mut<AggregatorHistoryData>(params.addr);

        aggregator.name = params.name;
        aggregator.metadata = params.metadata;
        aggregator.authority = params.authority;

        aggregator_results_config.min_job_results = params.min_job_results;
        aggregator_results_config.variance_threshold = params.variance_threshold;
        aggregator_results_config.force_report_period = params.force_report_period;
        aggregator_results_config.expiration = params.expiration;

        aggregator_config.crank_disabled = params.disable_crank;
        aggregator_config.crank_addr = params.crank_addr;
        aggregator_config.start_after = params.start_after;

        aggregator_config.queue_addr = params.queue_addr;
        aggregator_config.batch_size = params.batch_size;
        aggregator_config.min_oracle_results = params.min_oracle_results;
        aggregator_config.min_update_delay_seconds = params.min_update_delay_seconds;
        aggregator_config.variance_threshold = params.variance_threshold;
        aggregator_config.force_report_period = params.force_report_period;


        aggregator_read_config.read_whitelist = params.read_whitelist;
        aggregator_read_config.read_charge = params.read_charge;
        aggregator_read_config.reward_escrow = params.reward_escrow;
        
        // if change in history length, reset history
        if (params.history_limit != aggregator_config.history_limit) {
            aggregator_history_data.history = vector::empty();
            aggregator_history_data.history_write_idx = 0;
            aggregator_config.history_limit = params.history_limit;
        };
    }

    public(friend) fun set_crank(addr: address, crank_addr: address) acquires AggregatorConfig {
        let aggregator = borrow_global_mut<AggregatorConfig>(addr);
        aggregator.crank_addr = crank_addr;
    }

    public(friend) fun add_crank_row_count(self: address) acquires AggregatorConfig {
        let aggregator = borrow_global_mut<AggregatorConfig>(self);
        aggregator.crank_row_count = aggregator.crank_row_count + 1;
    }

    public(friend) fun sub_crank_row_count(self: address) acquires AggregatorConfig {
        let aggregator = borrow_global_mut<AggregatorConfig>(self);
        aggregator.crank_row_count = aggregator.crank_row_count - 1;
    }

    public(friend) fun add_job(addr: address, job: &Job, weight: u8) acquires AggregatorJobData {
        let aggregator_job_data = borrow_global_mut<AggregatorJobData>(addr);
        vector::push_back(&mut aggregator_job_data.job_keys, job::addr(job));
        vector::push_back(&mut aggregator_job_data.job_weights, weight);
        let checksum = vector::empty();
        let i = 0;
        while (i < vector::length(&aggregator_job_data.job_keys)) {
            let job_key = vector::borrow(&aggregator_job_data.job_keys, i);
            vector::append(&mut checksum, job::hash(*job_key));
            checksum = hash::sha3_256(checksum);
            i = i + 1;
        };
        aggregator_job_data.jobs_checksum = checksum;
    }

    public(friend) fun remove_job(addr: address, job: address) acquires AggregatorJobData {
        let aggregator_job_data = borrow_global_mut<AggregatorJobData>(addr);
        let (is_in, idx) = vector::index_of(&aggregator_job_data.job_keys, &job);
        assert!(is_in, errors::InvalidArgument());
        vector::swap_remove(&mut aggregator_job_data.job_keys, idx);
        vector::swap_remove(&mut aggregator_job_data.job_weights, idx);
        let checksum = vector::empty();
        let i = 0;
        while (i < vector::length(&aggregator_job_data.job_keys)) {
            let job_key = vector::borrow(&aggregator_job_data.job_keys, i);
            vector::append(&mut checksum, job::hash(*job_key));
            checksum = hash::sha3_256(checksum);
            i = i + 1;
        };
        aggregator_job_data.jobs_checksum = checksum;
    }

    public(friend) fun apply_oracle_error(addr: address, oracle_idx: u64) acquires AggregatorRound {
        let current_round = borrow_global_mut<AggregatorRound<CurrentRound>>(addr);
        current_round.num_error = current_round.num_error + 1;
        let val_ref = vector::borrow_mut(&mut current_round.errors_fulfilled, oracle_idx);
        *val_ref = true
    }

    public(friend) fun push_back_next_allowed_update_time(
        self: address, 
        jitter: u64
    ) acquires AggregatorConfig {
        let aggregator = borrow_global_mut<AggregatorConfig>(self);
        let jitter = (timestamp::now_seconds() + jitter) % 5;
        aggregator.next_allowed_update_time = timestamp::now_seconds() + aggregator.min_update_delay_seconds + jitter;
    }

    public(friend) fun lock(self: address) acquires Aggregator {
        let aggregator = borrow_global_mut<Aggregator>(self);
        aggregator.is_locked = true;
    }

    public(friend) fun open_round(
        self: address, 
        jitter: u64, 
        oracle_keys: &vector<address>
    ): u64 acquires AggregatorConfig, AggregatorRound {
        let aggregator = borrow_global_mut<AggregatorConfig>(self);

        let next_id = {
            let latest_round = borrow_global_mut<AggregatorRound<LatestConfirmedRound>>(self);
            latest_round.is_closed = true; // mark latest round as closed and get id 
            latest_round.id + 1
        };
        
        let current_round = borrow_global_mut<AggregatorRound<CurrentRound>>(self);

        if (!current_round.is_closed && current_round.num_success < aggregator.min_oracle_results)
        {
            aggregator.consecutive_failure_count =  aggregator.consecutive_failure_count + 1;
        } else {
            aggregator.consecutive_failure_count = 0;
        };

        let size = aggregator.batch_size;
        current_round.id = next_id;
        current_round.round_open_timestamp = timestamp::now_seconds();
        // current_round.result = math::zero(); 
        // current_round.std_deviation = math::zero();
        current_round.num_success = 0;
        current_round.num_error = 0;
        current_round.is_closed = false;
        current_round.oracle_keys = *oracle_keys;
        current_round.round_confirmed_timestamp = 0;
        current_round.medians = vec_utils::new_sized(size, option::none());
        current_round.errors_fulfilled = vec_utils::new_sized(size, false);
        let jitter = (timestamp::now_seconds() + jitter) % 5;
        aggregator.next_allowed_update_time = timestamp::now_seconds() + aggregator.min_update_delay_seconds + jitter;
        aggregator.next_allowed_update_time
    }
    
    public(friend) fun save_result(
        aggregator_addr: address, 
        oracle_idx: u64, 
        value: &SwitchboardDecimal,
        min_response: &SwitchboardDecimal,
        max_response: &SwitchboardDecimal,
    ): bool acquires AggregatorConfig, AggregatorRound {
        let aggregator_config = borrow_global<AggregatorConfig>(aggregator_addr);

        let current_round = {
            let round = borrow_global_mut<AggregatorRound<CurrentRound>>(aggregator_addr);
            let val_ref = vector::borrow_mut(&mut round.medians, oracle_idx);
            *val_ref = option::some(*value);
            round.num_success = round.num_success + 1;

            if (round.num_success == 1) {
                round.min_response = *min_response;
                round.max_response = *max_response;
            } else {
                if (math::gt(&round.min_response, min_response)) {
                    round.min_response = *min_response;
                };
                if (math::lt(&round.max_response, max_response)) {
                    round.max_response = *max_response;
                };
            };

            if (round.num_success >= aggregator_config.min_oracle_results) {
                let uwm = vec_utils::unwrap(&round.medians);
                round.result = math::median(&mut uwm);
                round.std_deviation = math::std_deviation(&uwm, &round.result);
            };
            
            *round
        };

        let latest_confirmed_round = borrow_global_mut<AggregatorRound<LatestConfirmedRound>>(aggregator_addr);

        if (current_round.num_success >= aggregator_config.min_oracle_results) {

            // map all fields from current_round to latest_confirmed_round
            // could have embedded the object to do a copy avoiding this
            latest_confirmed_round.id = current_round.id;
            latest_confirmed_round.round_open_timestamp = current_round.round_open_timestamp;
            latest_confirmed_round.round_open_block_height = current_round.round_open_block_height;
            latest_confirmed_round.result = current_round.result;
            latest_confirmed_round.std_deviation = current_round.std_deviation;
            latest_confirmed_round.min_response = current_round.min_response;
            latest_confirmed_round.max_response = current_round.max_response;
            latest_confirmed_round.oracle_keys = current_round.oracle_keys;
            latest_confirmed_round.medians = current_round.medians;
            latest_confirmed_round.errors_fulfilled = current_round.errors_fulfilled;
            latest_confirmed_round.num_success = current_round.num_success;
            latest_confirmed_round.num_error = current_round.num_error;
            latest_confirmed_round.is_closed = current_round.is_closed;
            latest_confirmed_round.round_confirmed_timestamp = timestamp::now_seconds();

            // update history if history_limit set
            // if (aggregator_config.history_limit > 0) {
            //     let aggregator_history = borrow_global_mut<AggregatorHistoryData>(aggregator_addr);

            //     let new_row = AggregatorHistoryRow {
            //         timestamp: timestamp::now_seconds(),
            //         round_id: current_round.id,
            //         value: latest_confirmed_round.result
            //     };
            //     if (vector::length(&aggregator_history.history) < aggregator_config.history_limit) {
            //         vector::push_back(&mut aggregator_history.history, new_row);
            //     } else {
            //         *vector::borrow_mut(&mut aggregator_history.history, aggregator_history.history_write_idx) = new_row;
            //     };

            //     // update history write
            //     aggregator_history.history_write_idx = (aggregator_history.history_write_idx + 1) % aggregator_config.history_limit;
            // };

            return true
        };

        false
    }

    #[test_only]
    public entry fun new_test(account: &signer, value: u128, dec: u8, neg: bool) {
        let cap = account::create_test_signer_cap(signer::address_of(account));
        move_to(
            account, 
            Aggregator {
                signer_cap: cap,

                // Configs
                authority: signer::address_of(account),
                name: b"Switchboard Aggregator",
                metadata: b"",

                // Aggregator data that's fairly fixed
                created_at: timestamp::now_seconds(),
                is_locked: false,
                _ebuf: vector::empty(),
                features: vector::empty(),
            }
        );
        
        move_to(
            account, 
            AggregatorConfig {
                queue_addr: @0x51,
                batch_size: 1,
                min_oracle_results: 1,
                min_update_delay_seconds: 5,
                history_limit: 0,
                crank_addr: @0x5,
                crank_disabled: false,
                crank_row_count: 0,
                next_allowed_update_time: 0,
                consecutive_failure_count: 0,
                start_after: 0,
                variance_threshold: math::zero(),
                force_report_period: 0,
                min_job_results: 1,
                expiration: 0,
            }
        );
        move_to(
            account, 
            AggregatorReadConfig {
                read_charge: 0,
                reward_escrow: @0x55,
                read_whitelist: vector::empty(),
                limit_reads_to_whitelist: false,
            }
        );
        move_to(
            account, 
            AggregatorJobData {
                job_keys: vector::empty(),
                job_weights: vector::empty(),
                jobs_checksum: vector::empty(),
            }
        );
        move_to(
            account, 
            AggregatorHistoryData {
                history: vector::empty(),
                history_write_idx: 0,
            }
        );
        move_to(account, AggregatorRound<LatestConfirmedRound> {
            id: 0,
            round_open_timestamp: 0,
            round_open_block_height: block::get_current_block_height(),
            result: math::new(value, dec, neg),
            std_deviation: math::zero(),
            min_response: math::zero(),
            max_response: math::zero(),
            oracle_keys: vector::empty(),
            medians: vector::empty(),
            errors_fulfilled: vector::empty(),
            num_error: 0,
            num_success: 0,
            is_closed: false,
            round_confirmed_timestamp: 0,
            current_payout: vector::empty(),
        });
        move_to(
            account,
            AggregatorResultsConfig {
                variance_threshold: math::zero(),
                force_report_period: 0,
                min_job_results: 1,
                expiration: 0,
            }
        );
        move_to(account, default_round<CurrentRound>());
    }

    #[test_only]
    public entry fun update_value(account: &signer, value: u128, dec: u8, neg: bool) acquires AggregatorRound {
        let latest_confirmed_round = borrow_global_mut<AggregatorRound<LatestConfirmedRound>>(signer::address_of(account));
        latest_confirmed_round.result = math::new(value, dec, neg);
    }

    #[test_only]
    public entry fun update_open_timestamp(account: &signer, timestamp: u64) acquires AggregatorRound {
        let latest_confirmed_round = borrow_global_mut<AggregatorRound<LatestConfirmedRound>>(signer::address_of(account));
        latest_confirmed_round.round_open_timestamp = timestamp;
    }

    #[test_only]
    public entry fun update_confirmed_timestamp(account: &signer, timestamp: u64) acquires AggregatorRound {
        let latest_confirmed_round = borrow_global_mut<AggregatorRound<LatestConfirmedRound>>(signer::address_of(account));
        latest_confirmed_round.round_confirmed_timestamp = timestamp;
    }
}
