module switchboard::events {

    use switchboard::math::{SwitchboardDecimal};

    struct AggregatorInitEvent has drop, store {
        aggregator_address: address,
    }

    struct AggregatorUpdateEvent has drop, store {
        aggregator_address: address,
        old_value: SwitchboardDecimal,
        new_value: SwitchboardDecimal,
    }

    struct AggregatorSaveResultEvent has drop, store {
        aggregator_address: address,
        oracle_key: address,
        value: SwitchboardDecimal,
    }

    struct AggregatorOpenRoundEvent has drop, store {
        aggregator_address: address,
        oracle_keys: vector<address>,
        job_keys: vector<address>,
    }

    struct AggregatorReadEvent has drop, store {
        aggregator_address: address,
        cost: u64,
        result: SwitchboardDecimal,
    }

    struct AggregatorCrankEvictionEvent has drop, store {
        crank_address: address,
        aggregator_address: address,
        reason: u64,
        timestamp: u64,
    }

    struct OracleRewardEvent has drop, store {
        aggregator_address: address,
        oracle_address: address,
        amount: u64,
        timestamp: u64,
    }

    struct OracleWithdrawEvent has drop, store {
        oracle_address: address,
        destination_wallet: address,
        previous_amount: u64,
        new_amount: u64,
        timestamp: u64,
    }

    struct OracleSlashEvent has drop, store {
        aggregator_address: address, 
        oracle_address: address,
        amount: u64,
        timestamp: u64,
    }

    struct LeaseWithdrawEvent has drop, store {
        lease_address: address,
        destination_wallet: address,
        previous_amount: u64,
        new_amount: u64,
        timestamp: u64,
    }

    struct LeaseFundEvent has drop, store {
        lease_address: address,
        funder: address,
        amount: u64,
        timestamp: u64,
    }

    struct ProbationBrokenEvent has drop, store {
        aggregator_address: address,
        queue_address: address,
        timestamp: u64,
    }

    struct FeedPermissionRevokedEvent has drop, store {
        aggregator_address: address,
        timestamp: u64,
    }

    struct GarbageCollectFailureEvent has drop, store {
        queue_address: address,
    }

    struct OracleBootedEvent has drop, store {
        queue_address: address,
        oracle_address: address,
    }

    struct CrankLeaseInsufficientFundsEvent has drop, store {
        aggregator_address: address,
    }

    struct CrankPopExpectedFailureEvent has drop, store {
        aggregator_address: address,
    }

    public fun new_aggregator_init_event(aggregator_address: address): AggregatorInitEvent {
        AggregatorInitEvent {
            aggregator_address
        }
    }

    public fun new_aggregator_update_event(aggregator_address: address, old_value: SwitchboardDecimal, new_value: SwitchboardDecimal): AggregatorUpdateEvent {
        AggregatorUpdateEvent {
            aggregator_address,
            old_value,
            new_value
        }
    }

    public fun new_aggregator_save_result_event(aggregator_address: address, oracle_key: address, value: SwitchboardDecimal): AggregatorSaveResultEvent {
        AggregatorSaveResultEvent {
            aggregator_address,
            oracle_key,
            value,
        }
    }

    public fun new_aggregator_open_round_event(aggregator_address: address, job_keys: vector<address>, oracle_keys: vector<address>): AggregatorOpenRoundEvent {
        AggregatorOpenRoundEvent {
            aggregator_address,
            oracle_keys,
            job_keys,
        }
    }

    public fun new_aggregator_crank_eviction_event(
        crank_address: address,
        aggregator_address: address,
        reason: u64,
        timestamp: u64,
    ): AggregatorCrankEvictionEvent {
        AggregatorCrankEvictionEvent {
            crank_address,
            aggregator_address,
            reason,
            timestamp
        }
    }

    public fun new_oracle_reward_event(
        aggregator_address: address,
        oracle_address: address,
        amount: u64,
        timestamp: u64,
    ): OracleRewardEvent {
        OracleRewardEvent {
            aggregator_address,
            oracle_address,
            amount,
            timestamp
        }
    }

    public fun new_oracle_withdraw_event(
        oracle_address: address,
        destination_wallet: address,
        previous_amount: u64,
        new_amount: u64,
        timestamp: u64,
    ): OracleWithdrawEvent {
        OracleWithdrawEvent {
            oracle_address,
            destination_wallet,
            previous_amount,
            new_amount,
            timestamp,
        }
    }

    public fun new_oracle_slash_event(
        aggregator_address: address, 
        oracle_address: address,
        amount: u64,
        timestamp: u64,
    ): OracleSlashEvent {
        OracleSlashEvent {
            aggregator_address,
            oracle_address,
            amount,
            timestamp,
        }
    }

    public fun new_lease_withdraw_event(
        lease_address: address,
        destination_wallet: address,
        previous_amount: u64,
        new_amount: u64,
        timestamp: u64,
    ): LeaseWithdrawEvent {
        LeaseWithdrawEvent {
            lease_address,
            destination_wallet,
            previous_amount,
            new_amount,
            timestamp,
        }
    }

    public fun new_lease_fund_event(
        lease_address: address,
        funder: address,
        amount: u64,
        timestamp: u64,
    ): LeaseFundEvent {
        LeaseFundEvent {
            lease_address,
            funder,
            amount,
            timestamp,
        }
    }

    public fun new_probation_broker_event(
        aggregator_address: address,
        queue_address: address,
        timestamp: u64,
    ): ProbationBrokenEvent {
        ProbationBrokenEvent {
            aggregator_address,
            queue_address,
            timestamp
        }
    }

    public fun new_feed_permission_revoked_event(
        aggregator_address: address,
        timestamp: u64,
    ): FeedPermissionRevokedEvent {
        FeedPermissionRevokedEvent {
            aggregator_address,
            timestamp
        }
    }

    public fun new_garbage_collection_failure_event(
        queue_address: address,
    ): GarbageCollectFailureEvent {
        GarbageCollectFailureEvent {
            queue_address
        }
    }

    public fun new_oracle_booted_event(
        queue_address: address,
        oracle_address: address,
    ): OracleBootedEvent {
        OracleBootedEvent {
            queue_address,
            oracle_address,
        }
    }

    public fun new_crank_lease_insufficient_funds_event(
        aggregator_address: address,
    ): CrankLeaseInsufficientFundsEvent {
        CrankLeaseInsufficientFundsEvent {
            aggregator_address,
        }
    }

    public fun new_crank_pop_expected_failure_event( 
        aggregator_address: address,
    ): CrankPopExpectedFailureEvent {
        CrankPopExpectedFailureEvent {
            aggregator_address,
        }
    }

    public fun new_aggregator_read_event(aggregator_address: address, cost: u64, result: SwitchboardDecimal): AggregatorReadEvent {
        AggregatorReadEvent {
            aggregator_address,
            cost,
            result
        }
    }
}
