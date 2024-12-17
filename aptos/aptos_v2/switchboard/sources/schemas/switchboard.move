module switchboard::switchboard {
    use switchboard::errors;
    use switchboard::events;
    use switchboard::math::{SwitchboardDecimal};
    use switchboard::permission::{Self, Permission};
    use aptos_framework::account;
    use aptos_framework::event::{Self, EventHandle};
    use aptos_std::table::{Self, Table};
    use std::signer;
    use std::vector;
    
    friend switchboard::aggregator;
    friend switchboard::switchboard_init_action;
    friend switchboard::aggregator_init_action;
    friend switchboard::permission_init_action;
    friend switchboard::permission_set_action;
    friend switchboard::aggregator_set_configs_action;
    friend switchboard::aggregator_open_round_action;
    friend switchboard::aggregator_save_result_action;
    friend switchboard::crank_pop_action;
    friend switchboard::oracle_heartbeat_action;
    friend switchboard::lease_withdraw_action;
    friend switchboard::lease_extend_action;
    friend switchboard::lease_init_action;
    friend switchboard::oracle_wallet_withdraw_action;

    // SWITCHBOARD STATE
    struct State has key {
        
        // PDAs
        permissions: Table<address, Permission>,

        // AUTHORITIES
        aggregator_authorities: Table<address, vector<address>>,

        // CORE EVENTS
        aggregator_update_events: EventHandle<events::AggregatorUpdateEvent>,
        aggregator_open_round_events: EventHandle<events::AggregatorOpenRoundEvent>,
        aggregator_init_events: EventHandle<events::AggregatorInitEvent>,
    }

    // ADDITIONAL SWITCHBOARD EVENT
    struct SwitchboardEvents has key {
        aggregator_save_result_events: EventHandle<events::AggregatorSaveResultEvent>,
        aggregator_crank_eviction_events: EventHandle<events::AggregatorCrankEvictionEvent>,

        oracle_booted_events: EventHandle<events::OracleBootedEvent>,
        oracle_reward_events: EventHandle<events::OracleRewardEvent>,
        oracle_withdraw_events: EventHandle<events::OracleWithdrawEvent>,
        oracle_slash_events: EventHandle<events::OracleSlashEvent>,

        lease_withdraw_events: EventHandle<events::LeaseWithdrawEvent>,
        lease_fund_events: EventHandle<events::LeaseFundEvent>,

        feed_permission_revoked_events: EventHandle<events::FeedPermissionRevokedEvent>,

        garbage_collection_failure_events: EventHandle<events::GarbageCollectFailureEvent>,

        crank_lease_insufficient_funds_events: EventHandle<events::CrankLeaseInsufficientFundsEvent>,
        crank_pop_expected_failure_events: EventHandle<events::CrankPopExpectedFailureEvent>,
    }

    struct SwitchboardReadEvents has key {
        aggregator_read_events: EventHandle<events::AggregatorReadEvent>,
    }
    
    public fun switchboard_events_exists(): bool {
        exists<SwitchboardEvents>(@switchboard)
    }

    public fun switchboard_read_events_exists(): bool {
        exists<SwitchboardReadEvents>(@switchboard)
    }

    public fun exist(addr: address): bool {
        exists<State>(addr)
    }

    public(friend) fun state_create(state: &signer) {
        assert!(!exists<State>(signer::address_of(state)), errors::StateAlreadyExists());
        move_to(state, State {
            permissions: table::new(),
            aggregator_authorities: table::new(),
            aggregator_update_events: account::new_event_handle(state),
            aggregator_open_round_events: account::new_event_handle(state),
            aggregator_init_events: account::new_event_handle(state),
        });
    }

    public(friend) fun switchboard_events_create(state: &signer) {
        assert!(!exists<SwitchboardEvents>(signer::address_of(state)), errors::SwitchboardEventsAlreadyExist());
        move_to(state, SwitchboardEvents {
            aggregator_save_result_events: account::new_event_handle(state),
            aggregator_crank_eviction_events: account::new_event_handle(state),
            oracle_booted_events: account::new_event_handle(state),
            oracle_reward_events: account::new_event_handle(state),
            oracle_withdraw_events: account::new_event_handle(state),
            oracle_slash_events: account::new_event_handle(state),
            lease_withdraw_events: account::new_event_handle(state),
            lease_fund_events: account::new_event_handle(state),
            feed_permission_revoked_events: account::new_event_handle(state),
            garbage_collection_failure_events: account::new_event_handle(state),
            crank_lease_insufficient_funds_events: account::new_event_handle(state),
            crank_pop_expected_failure_events: account::new_event_handle(state),
        });
    }

    public(friend) fun switchboard_read_event_create(state: &signer) {
        assert!(!exists<SwitchboardReadEvents>(signer::address_of(state)), errors::SwitchboardEventsAlreadyExist());
        move_to(state, SwitchboardReadEvents {
            aggregator_read_events: account::new_event_handle(state),
        });
    }

    public(friend) fun permission_exists(permission: address): bool acquires State {
        let state = borrow_global<State>(@switchboard);
        table::contains(&state.permissions, permission)
    }

    public(friend) fun permission_get(permission: address): Permission acquires State {
        let state = borrow_global<State>(@switchboard);
        *table::borrow(&state.permissions, permission)
    }
    
    public(friend) fun permission_has_authority(pkey: address, account: &signer): bool acquires State {
        let ref = permission_get(pkey);
        permission::authority(&ref) == signer::address_of(account)
    }

    public(friend) fun permission_create(permission: &Permission) acquires State {
        let state = borrow_global_mut<State>(@switchboard);
        table::add(&mut state.permissions, permission::key_from_permission(permission), *permission);
    }

    public(friend) fun permission_set(permission: &Permission) acquires State {
        let state = borrow_global_mut<State>(@switchboard);
        *table::borrow_mut(&mut state.permissions, permission::key_from_permission(permission)) = *permission;
    }

    // For feed tracking
    public(friend) fun aggregator_authority_set(aggregator_address: address, new_authority: address, current_authority: address) acquires State {
        let state = borrow_global_mut<State>(@switchboard);

        // remove address from previous authority if there is one
        if (table::contains(&state.aggregator_authorities, current_authority)) {

            // todo: check if this copies or not
            let (found, aggregator_idx) = vector::index_of(table::borrow(&state.aggregator_authorities, current_authority), &aggregator_address);
            if (found) {
                vector::swap_remove(table::borrow_mut(&mut state.aggregator_authorities, current_authority), aggregator_idx);
            }
        };

        // add address to new owner if there is one 
        if (table::contains(&state.aggregator_authorities, new_authority)) {
            vector::push_back(table::borrow_mut(&mut state.aggregator_authorities, new_authority), aggregator_address);

        // otherwise make one
        } else {
            table::add(&mut state.aggregator_authorities, new_authority, vector::singleton(aggregator_address))
        }

    }

    // CORE EVENTS
    public(friend) fun emit_aggregator_update_event(aggregator_address: address, old_value: SwitchboardDecimal, new_value: SwitchboardDecimal) acquires State {
        let state = borrow_global_mut<State>(@switchboard);
        event::emit_event<events::AggregatorUpdateEvent>(
            &mut state.aggregator_update_events,
            events::new_aggregator_update_event(aggregator_address, old_value, new_value)
        );
    }

    public(friend) fun emit_aggregator_open_round_event(aggregator_address: address, job_keys: vector<address>, oracle_keys: vector<address>) acquires State {
        let state = borrow_global_mut<State>(@switchboard);
        event::emit_event<events::AggregatorOpenRoundEvent>(
            &mut state.aggregator_open_round_events,
            events::new_aggregator_open_round_event(aggregator_address, job_keys, oracle_keys)
        );
    }

    public(friend) fun emit_aggregator_init_event(
        aggregator_address: address
    ) acquires State {
        let state = borrow_global_mut<State>(@switchboard);
        event::emit_event<events::AggregatorInitEvent>(
            &mut state.aggregator_init_events,
            events::new_aggregator_init_event(aggregator_address)
        );
    }

    // ADDITIONAL EVENTS
    public(friend) fun emit_aggregator_save_result_event(aggregator_address: address,  oracle_key: address, value: SwitchboardDecimal) acquires SwitchboardEvents {
        if (!exists<SwitchboardEvents>(@switchboard)) {
            return
        };
        let switchboard_events = borrow_global_mut<SwitchboardEvents>(@switchboard);
        event::emit_event<events::AggregatorSaveResultEvent>(
            &mut switchboard_events.aggregator_save_result_events,
            events::new_aggregator_save_result_event(aggregator_address, oracle_key, value)
        );
    }

    public(friend) fun emit_aggregator_crank_eviction_event(
        crank_address: address,
        aggregator_address: address,
        reason: u64,
        timestamp: u64,
    ) acquires SwitchboardEvents {
        if (!exists<SwitchboardEvents>(@switchboard)) {
            return
        };
        let switchboard_events = borrow_global_mut<SwitchboardEvents>(@switchboard);
        event::emit_event<events::AggregatorCrankEvictionEvent>(
            &mut switchboard_events.aggregator_crank_eviction_events,
            events::new_aggregator_crank_eviction_event(  
                crank_address,
                aggregator_address,
                reason,
                timestamp,
            )
        );
    }

    public(friend) fun emit_oracle_reward_event(
        aggregator_address: address,
        oracle_address: address,
        amount: u64,
        timestamp: u64,
    ) acquires SwitchboardEvents {
        if (!exists<SwitchboardEvents>(@switchboard)) {
            return
        };
        let switchboard_events = borrow_global_mut<SwitchboardEvents>(@switchboard);
        event::emit_event<events::OracleRewardEvent>(
            &mut switchboard_events.oracle_reward_events,
            events::new_oracle_reward_event(  
                aggregator_address,
                oracle_address,
                amount,
                timestamp,
            )
        );
    }

    public(friend) fun emit_oracle_withdraw_event(
        oracle_address: address,
        destination_wallet: address,
        previous_amount: u64,
        new_amount: u64,
        timestamp: u64,
    ) acquires SwitchboardEvents {
        if (!exists<SwitchboardEvents>(@switchboard)) {
            return
        };
        let switchboard_events = borrow_global_mut<SwitchboardEvents>(@switchboard);
        event::emit_event<events::OracleWithdrawEvent>(
            &mut switchboard_events.oracle_withdraw_events,
            events::new_oracle_withdraw_event(  
                oracle_address,
                destination_wallet,
                previous_amount,
                new_amount,
                timestamp,
            )
        );
    }

    public(friend) fun emit_oracle_slash_event(
        aggregator_address: address, 
        oracle_address: address,
        amount: u64,
        timestamp: u64,
    ) acquires SwitchboardEvents {
        if (!exists<SwitchboardEvents>(@switchboard)) {
            return
        };
        let switchboard_events = borrow_global_mut<SwitchboardEvents>(@switchboard);
        event::emit_event<events::OracleSlashEvent>(
            &mut switchboard_events.oracle_slash_events,
            events::new_oracle_slash_event(  
                aggregator_address,
                oracle_address,
                amount,
                timestamp,
            )
        );
    }

    public(friend) fun emit_lease_withdraw_event(
        lease_address: address,
        destination_wallet: address,
        previous_amount: u64,
        new_amount: u64,
        timestamp: u64,
    ) acquires SwitchboardEvents {
        if (!exists<SwitchboardEvents>(@switchboard)) {
            return
        };
        let switchboard_events = borrow_global_mut<SwitchboardEvents>(@switchboard);
        event::emit_event<events::LeaseWithdrawEvent>(
            &mut switchboard_events.lease_withdraw_events,
            events::new_lease_withdraw_event(  
                lease_address,
                destination_wallet,
                previous_amount,
                new_amount,
                timestamp,
            )
        );
    }

    public(friend) fun emit_lease_fund_event(
        lease_address: address,
        funder: address,
        amount: u64,
        timestamp: u64,
    ) acquires SwitchboardEvents {
        if (!exists<SwitchboardEvents>(@switchboard)) {
            return
        };
        let switchboard_events = borrow_global_mut<SwitchboardEvents>(@switchboard);
        event::emit_event<events::LeaseFundEvent>(
            &mut switchboard_events.lease_fund_events,
            events::new_lease_fund_event(  
                lease_address,
                funder,
                amount,
                timestamp,
            )
        );
    }

    public(friend) fun emit_feed_permission_revoked_event(
        aggregator_address: address,
        timestamp: u64,
    ) acquires SwitchboardEvents {
        if (!exists<SwitchboardEvents>(@switchboard)) {
            return
        };
        let switchboard_events = borrow_global_mut<SwitchboardEvents>(@switchboard);
        event::emit_event<events::FeedPermissionRevokedEvent>(
            &mut switchboard_events.feed_permission_revoked_events,
            events::new_feed_permission_revoked_event(  
                aggregator_address,
                timestamp
            )
        );
    }

    public(friend) fun emit_garbage_collection_failure_event(
        queue_address: address,
    ) acquires SwitchboardEvents {
        if (!exists<SwitchboardEvents>(@switchboard)) {
            return
        };
        let switchboard_events = borrow_global_mut<SwitchboardEvents>(@switchboard);
        event::emit_event<events::GarbageCollectFailureEvent>(
            &mut switchboard_events.garbage_collection_failure_events,
            events::new_garbage_collection_failure_event(  
               queue_address
            )
        );
    }

    public(friend) fun emit_oracle_booted_event(
        queue_address: address,
        oracle_address: address,
    ) acquires SwitchboardEvents {
        if (!exists<SwitchboardEvents>(@switchboard)) {
            return
        };
        let switchboard_events = borrow_global_mut<SwitchboardEvents>(@switchboard);
        event::emit_event<events::OracleBootedEvent>(
            &mut switchboard_events.oracle_booted_events,
            events::new_oracle_booted_event(  
                queue_address,
                oracle_address,
            )
        );
    }

    public(friend) fun emit_crank_lease_insufficient_funds_event(
        aggregator_address: address,
    ) acquires SwitchboardEvents {
        if (!exists<SwitchboardEvents>(@switchboard)) {
            return
        };
        let switchboard_events = borrow_global_mut<SwitchboardEvents>(@switchboard);
        event::emit_event<events::CrankLeaseInsufficientFundsEvent>(
            &mut switchboard_events.crank_lease_insufficient_funds_events,
            events::new_crank_lease_insufficient_funds_event(  
                aggregator_address,
            )
        );
    }

    public(friend) fun emit_crank_pop_expected_failure_event( 
        aggregator_address: address,
    ) acquires SwitchboardEvents {
        if (!exists<SwitchboardEvents>(@switchboard)) {
            return
        };
        let switchboard_events = borrow_global_mut<SwitchboardEvents>(@switchboard);
        event::emit_event<events::CrankPopExpectedFailureEvent>(
            &mut switchboard_events.crank_pop_expected_failure_events,
            events::new_crank_pop_expected_failure_event(  
                aggregator_address,
            )
        );
    }

    public(friend) fun emit_aggregator_read_event(
        aggregator_address: address,
        cost: u64,
        value: SwitchboardDecimal,
    ) acquires SwitchboardReadEvents {
        if (!switchboard_read_events_exists()) {
            return
        };
        let switchboard_events = borrow_global_mut<SwitchboardReadEvents>(@switchboard);
        event::emit_event<events::AggregatorReadEvent>(
            &mut switchboard_events.aggregator_read_events,
            events::new_aggregator_read_event(  
                aggregator_address,
                cost,
                value,
            )
        );
    }
}
