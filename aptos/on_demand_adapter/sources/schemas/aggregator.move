module switchboard_adapter::aggregator {
    use aptos_framework::account::{Self, SignerCapability};
    use aptos_framework::timestamp;
    use aptos_framework::block;
    use aptos_std::ed25519;
    use switchboard_adapter::serialization;
    use switchboard_adapter::math::{Self, SwitchboardDecimal};
    use switchboard_adapter::vec_utils;
    use switchboard_adapter::errors;
    use std::option::{Self, Option};
    use std::signer; 
    use std::vector;
    use std::coin::{Self, Coin};

    // New On-demand Dependencies
    use switchboard::aggregator::{
        Self as on_demand_aggregator, 
        Aggregator as OnDemandAggregator, 
        CurrentResult
    };
    use switchboard::decimal::{Self, Decimal};
    use aptos_framework::object::{Self, Object};
    
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

    /**
     * ON DEMAND
     * Check if an on-demand aggregator exists
     * @param addr: address of the aggregator
     * @return bool - whether the aggregator exists
     */
    public fun exist(addr: address): bool {
        let aggregator: Object<OnDemandAggregator> = object::address_to_object<OnDemandAggregator>(addr);
        on_demand_aggregator::aggregator_exists(aggregator)
    }

    /**
     * ON DEMAND
     * Check if the account has authority over the aggregator
     * @param addr: address of the aggregator
     * @param account: the account to check
     * @return bool - whether the account has authority
     */
    public fun has_authority(addr: address, account: &signer): bool {
        let aggregator: Object<OnDemandAggregator> = object::address_to_object<OnDemandAggregator>(addr);

        // check if the account has authority
        on_demand_aggregator::has_authority(
            aggregator, 
            signer::address_of(account)
        )
    }
    
    /**
     * ON DEMAND
     * Get the latest value for a feed
     * @param addr: address of the aggregator
     * @return SwitchboardDecimal - the latest value
     */
    public fun latest_value(addr: address): SwitchboardDecimal {
        let aggregator: Object<OnDemandAggregator> = object::address_to_object<OnDemandAggregator>(addr);

        // Get the latest update info for the feed
        let current_result: CurrentResult = on_demand_aggregator::current_result(aggregator);

        // get the current result
        let result: Decimal = on_demand_aggregator::result(&current_result);
        
        // result, create a new SwitchboardDecimal - scale down value from 18 decimals to 9
        let (value, neg) = decimal::unpack(result);
        let value_sbd = math::new(value / 1000000000, 9, neg);

        // get the timestamp
        let timestamp_seconds = on_demand_aggregator::timestamp(&current_result);
        
        // check if the timestamp is old
        let current_time = timestamp::now_seconds();
        let is_old = current_time - timestamp_seconds > 120;

        // error out if the data is older than 2 minutes
        assert!(!is_old, errors::PermissionDenied());

        // get the latest value and whether it's within the bounds
        value_sbd
    }

    /**
     * ON DEMAND
     * Get the latest value for a feed and check if the stdev of the reported values is within a certain threshold
     * @param addr: address of the aggregator
     * @param max_confidence_interval: the maximum confidence interval
     * @return (SwitchboardDecimal, bool) - the latest value and whether it's within the bounds
     */
    public fun latest_value_in_threshold(addr: address, max_confidence_interval: &SwitchboardDecimal): (SwitchboardDecimal, bool) {

        let aggregator: Object<OnDemandAggregator> = object::address_to_object<OnDemandAggregator>(addr);

        // Get the latest update info for the feed
        let current_result: CurrentResult = on_demand_aggregator::current_result(aggregator);

        // get the current result
        let result: Decimal = on_demand_aggregator::result(&current_result);
        let stdev = on_demand_aggregator::stdev(&current_result);
        
        // result, create a new SwitchboardDecimal - scale down value from 18 decimals to 9
        let (value, neg) = decimal::unpack(result);
        let value_sbd = math::new(value / 1000000000, 9, neg);

        // stdev
        let (std_dev, neg) = decimal::unpack(stdev);
        let std_deviation = math::new(std_dev / 1000000000, 9, neg);
        let is_within_bound = math::gt(&std_deviation, max_confidence_interval);

        // get the latest value and whether it's within the bounds
        (value_sbd, is_within_bound)
    }


    /**
     * On Demand Aggregator - Latest Round
     * @param addr: address of the aggregator
     * @return (SwitchboardDecimal, u64, SwitchboardDecimal, SwitchboardDecimal, SwitchboardDecimal) - the latest round data
     */
    public fun latest_round(addr: address): (
        SwitchboardDecimal, /* Result */
        u64,                /* Round Confirmed Timestamp */
        SwitchboardDecimal, /* Standard Deviation of Oracle Responses */
        SwitchboardDecimal, /* Min Oracle Response */
        SwitchboardDecimal, /* Max Oracle Response */
    ) {

        let aggregator: Object<OnDemandAggregator> = object::address_to_object<OnDemandAggregator>(addr);

        // Get the latest update info for the feed
        let current_result: CurrentResult = on_demand_aggregator::current_result(aggregator);

        // get the current result
        let timestamp_seconds = on_demand_aggregator::timestamp(&current_result);
        let result: Decimal = on_demand_aggregator::result(&current_result);
        let stdev = on_demand_aggregator::stdev(&current_result);
        let min_response: Decimal = on_demand_aggregator::min_result(&current_result);
        let max_response: Decimal = on_demand_aggregator::max_result(&current_result);

        // result, create a new SwitchboardDecimal - scale down value from 18 decimals to 9
        let (value, neg) = decimal::unpack(result);
        let value_sbd = math::new(value / 1000000000, 9, neg);

        // stdev
        let (std_dev, neg) = decimal::unpack(stdev);
        let std_dev_sbd = math::new(std_dev / 1000000000, 9, neg);

        // min_response
        let (min, neg) = decimal::unpack(min_response);
        let min_sbd = math::new(min / 1000000000, 9, neg);

        // max_response
        let (max, neg) = decimal::unpack(max_response);
        let max_sbd = math::new(max / 1000000000, 9, neg);

        // supply the new on-demand data
        (
            value_sbd,
            timestamp_seconds,
            std_dev_sbd,
            min_sbd,
            max_sbd,
        )
    }

    /**
     * Get the current aggregator authority
     * @param addr: address of the aggregator
     * @return address - the authority address
     */
    public fun authority(addr: address): address {
        let aggregator: Object<OnDemandAggregator> = object::address_to_object<OnDemandAggregator>(addr);
        let aggregator_value = on_demand_aggregator::get_aggregator(aggregator);
        on_demand_aggregator::authority(&aggregator_value)
    }

    // UNSUPPORTED FUNCTIONS

    // DISABLED
    public fun buy_latest_value<CoinType>(
        account: &signer, 
        addr: address, 
        fee: Coin<CoinType>
    ): SwitchboardDecimal {
        assert!(false, errors::AggregatorLocked());
        coin::deposit(signer::address_of(account), fee);
        math::zero()
    }

    // DISABLED
    public fun buy_latest_round<CoinType>(account: &signer, addr: address, fee: Coin<CoinType>): (
        SwitchboardDecimal, /* Result */
        u64,                /* Round Confirmed Timestamp */
        SwitchboardDecimal, /* Standard Deviation of Oracle Responses */
        SwitchboardDecimal, /* Min Oracle Response */
        SwitchboardDecimal, /* Max Oracle Response */
    ) {
        assert!(false, errors::AggregatorLocked());
        coin::deposit(signer::address_of(account), fee);
        (
            math::zero(),
            0,
            math::zero(),
            math::zero(),
            math::zero(),
        )
    }

    // DISABLED
    public fun latest_round_timestamp(addr: address): u64 {
        assert!(false, errors::AggregatorLocked());
        0
    }

    // DISABLED
    public fun latest_round_open_timestamp(addr: address): u64 {
        assert!(false, errors::AggregatorLocked());
        0
    }

    // DISABLED
    public fun lastest_round_min_response(addr: address): SwitchboardDecimal {
        assert!(false, errors::AggregatorLocked());
        math::zero()
    }

    // DISABLED
    public fun lastest_round_max_response(addr: address): SwitchboardDecimal {
        assert!(false, errors::AggregatorLocked());
        math::zero()
    }

    // DISABLED
    public fun is_locked(addr: address): bool {
        assert!(false, errors::AggregatorLocked());
        true
    }

    // DISABLED
    public fun read_charge(addr: address): u64 {
        assert!(false, errors::AggregatorLocked());
        0
    }

    // DISABLED
    public fun next_allowed_timestamp(addr: address): u64 {
        assert!(false, errors::AggregatorLocked());
        0
    }

    // DISABLED
    public fun addr_from_conf(conf: &AggregatorConfigParams): address {
        assert!(false, errors::AggregatorLocked());
        @0x0
    }

    // DISABLED
    public fun queue_from_conf(conf: &AggregatorConfigParams): address {
        assert!(false, errors::AggregatorLocked());
        @0x0
    }

    // DISABLED
    public fun authority_from_conf(conf: &AggregatorConfigParams): address {
        assert!(false, errors::AggregatorLocked());
        conf.authority
    }

    // DISABLED
    public fun history_limit_from_conf(conf: &AggregatorConfigParams): u64 {
        assert!(false, errors::AggregatorLocked());
        0
    }
    
    // DISABLED
    public fun batch_size_from_conf(conf: &AggregatorConfigParams): u64 {
        assert!(false, errors::AggregatorLocked());
        0
    }

    // DISABLED
    public fun min_oracle_results_from_conf(conf: &AggregatorConfigParams): u64 {
        assert!(false, errors::AggregatorLocked());
        0
    }

    // DISABLED
    public fun min_update_delay_seconds_from_conf(conf: &AggregatorConfigParams): u64 {
        assert!(false, errors::AggregatorLocked());
        0
    }

    // DISABLED
    public fun job_keys(addr: address): vector<address> {
        assert!(false, errors::AggregatorLocked());
        vector::empty()
    }

    // DISABLED
    public fun min_oracle_results(addr: address): u64 {
        assert!(false, errors::AggregatorLocked());
        0
    }

    // DISABLED
    public fun crank_addr(addr: address): address {
        assert!(false, errors::AggregatorLocked());
        @0x0
    }

    // DISABLED
    public fun crank_disabled(addr: address): bool {
        assert!(false, errors::AggregatorLocked());
        false
    }

    // DISABLED
    public fun current_round_num_success(addr: address): u64 {
        assert!(false, errors::AggregatorLocked());
        0
    }

    // DISABLED
    public fun current_round_open_timestamp(addr: address): u64 {
        assert!(false, errors::AggregatorLocked());
        0
    }

    // DISABLED
    public fun current_round_num_error(addr: address): u64 {
        assert!(false, errors::AggregatorLocked());
        0
    }

    // DISABLED
    public fun curent_round_oracle_key_at_idx(addr: address, idx: u64): address {
        assert!(false, errors::AggregatorLocked());
        @0x0
    }

    // DISABLED
    public fun curent_round_median_at_idx(addr: address, idx: u64): SwitchboardDecimal {
        assert!(false, errors::AggregatorLocked());
        math::zero()
    }
    
    // DISABLED
    public fun current_round_std_dev(addr: address): SwitchboardDecimal {
        assert!(false, errors::AggregatorLocked());
        math::zero()
    }

    // DISABLED
    public fun current_round_result(addr: address): SwitchboardDecimal {
        assert!(false, errors::AggregatorLocked());
        math::zero()
    }

    // DISABLED
    public fun is_median_fulfilled(addr: address, idx: u64): bool {
        assert!(false, errors::AggregatorLocked());
        false
    }

    // DISABLED
    public fun is_error_fulfilled(addr: address, idx: u64): bool {
        assert!(false, errors::AggregatorLocked());
        false
    }

    // DISABLED
    public fun configs(self: address): (
        address, // Queue Address
        u64,     // Batch Size
        u64,     // Min Oracle Results
    ) {
        assert!(false, errors::AggregatorLocked());
        (
            @0x0,
            0,
            0,
        )
    }

    // DISABLED
    public fun batch_size(self: address): u64 {
        assert!(false, errors::AggregatorLocked());
        0
    }
    
    // DISABLED
    public fun queue_addr(addr: address): address {
        assert!(false, errors::AggregatorLocked());
        @0x0
    }

    // DISABLED
    public fun history_limit(self: address): u64 {
        assert!(false, errors::AggregatorLocked());
        0
    }
    
    // DISABLED
    public fun can_open_round(addr: address): bool {
        assert!(false, errors::AggregatorLocked());
        false
    }

    // DISABLED
    public fun is_jobs_checksum_equal(addr: address, vec: &vector<u8>): bool {
        assert!(false, errors::AggregatorLocked());
        false
    }

    // DISABLED
    public entry fun set_feed_relay(
        account: signer, 
        aggregator_addr: address, 
        authority: address, 
        oracle_keys: vector<vector<u8>>
    ) {
        assert!(false, errors::AggregatorLocked());
    }

    // DISABLED
    public entry fun set_feed_relay_oracle_keys(
        account: signer, 
        aggregator_addr: address, 
        oracle_keys: vector<vector<u8>>
    ) {
        assert!(false, errors::AggregatorLocked());
    }

    // DISABLED
    #[legacy_entry_fun]
    public entry fun relay_value(
        addr: address, 
        updates: &mut vector<vector<u8>>
    ) {
        assert!(false, errors::AggregatorLocked());
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
        borrow_global_mut<AggregatorRound<LatestConfirmedRound>>(signer::address_of(account)).result = math::new(value, dec, neg);
    }

    #[test_only]
    public entry fun update_open_timestamp(account: &signer, timestamp: u64) acquires AggregatorRound {
        borrow_global_mut<AggregatorRound<LatestConfirmedRound>>(signer::address_of(account)).round_open_timestamp = timestamp;
    }

    #[test_only]
    public entry fun update_confirmed_timestamp(account: &signer, timestamp: u64) acquires AggregatorRound {
        borrow_global_mut<AggregatorRound<LatestConfirmedRound>>(signer::address_of(account)).round_confirmed_timestamp = timestamp;
    }
}
