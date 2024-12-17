module switchboard::oracle {
    use aptos_framework::account::{Self, SignerCapability};
    use aptos_framework::timestamp;
    use std::signer;
    use std::vector;

    // AGGREGATOR ACTIONS
    friend switchboard::aggregator_open_round_action;
    friend switchboard::aggregator_save_result_action;
    
    // ORACLE ACTIONS
    friend switchboard::oracle_heartbeat_action;
    friend switchboard::oracle_init_action;
    friend switchboard::create_oracle_action;
    friend switchboard::oracle_set_configs_action;

    // STAKING WALLET ACTIONS
    friend switchboard::oracle_wallet_init_action;
    friend switchboard::oracle_wallet_contribute_action;

    struct OracleConfig has key {
        authority: address,
        queue_addr: address,
    }

    struct OracleData has key {
        num_rows: u8,
        last_heartbeat: u64
    }

    struct Oracle has key {

        name: vector<u8>,
        metadata: vector<u8>,

        // Oracle account signer cap
        signer_cap: SignerCapability,
        features: vector<bool>,

        // Oracle Data
        created_at: u64,

        _ebuf: vector<u8>,
    }

    struct OracleMetrics has key {
        consecutive_success: u64,
        consecutive_error: u64,
        consecutive_disagreement: u64,
        consecutive_late_response: u64,
        consecutive_failure: u64,
        total_success: u128,
        total_error: u128,
        total_disagreement: u128,
        total_late_response: u128,
    }

    // oracle response types
    public fun OracleResponseDisagreement(): u8 { 0 }
    public fun OracleResponseSuccess(): u8 { 1 } 
    public fun OracleResponseError(): u8 { 2 }
    public fun OracleResponseNoResponse(): u8 { 3 }


    public fun queue_addr(self: address): address acquires OracleConfig {
        borrow_global<OracleConfig>(self).queue_addr
    }

    public fun authority(self: address): address acquires OracleConfig {
        borrow_global<OracleConfig>(self).authority
    }

    public fun is_expired(self: address, expiration_period: u64): bool acquires OracleData {
        let hb_timestamp = borrow_global<OracleData>(self).last_heartbeat;
        timestamp::now_seconds() - hb_timestamp > expiration_period
    }

    public fun num_rows(self: address): u8 acquires OracleData {
        borrow_global<OracleData>(self).num_rows
    }

    public fun has_authority(addr: address, account: &signer): bool acquires OracleConfig {
        let ref = borrow_global<OracleConfig>(addr);
        ref.authority == signer::address_of(account)
    }

    public fun exist(addr: address): bool {
        exists<Oracle>(addr)
    }

    public(friend) fun oracle_create(
        account: &signer,        
        signer_cap: SignerCapability,
        name: vector<u8>,
        metadata: vector<u8>,
        authority: address,
        queue_addr: address,
    ) {
        move_to(
            account, 
            Oracle {
                name,
                metadata,
                signer_cap,
                created_at: timestamp::now_seconds(),
                features: vector::empty(),
                _ebuf: vector::empty(),
            }
        );

        move_to(
            account, 
            OracleConfig {
                authority,
                queue_addr,
            }
        );

        move_to(
            account, 
            OracleData {
                num_rows: 0,
                last_heartbeat: 0,
            }
        );

        move_to(
            account, 
            OracleMetrics {
                consecutive_success: 0,
                consecutive_error: 0,
                consecutive_disagreement: 0,
                consecutive_late_response: 0,
                consecutive_failure: 0,
                total_success: 0,
                total_error: 0,
                total_disagreement: 0,
                total_late_response: 0,
            }
        );
    }

    public(friend) fun get_oracle_account(
        addr: address, 
    ): signer acquires Oracle {
        let oracle = borrow_global<Oracle>(addr);
        let oracle_acct = account::create_signer_with_capability(&oracle.signer_cap);
        oracle_acct
    }

    public(friend) fun set_configs(
        self: address,         
        name: vector<u8>,
        metadata: vector<u8>,
        oracle_authority: address,
        queue_addr: address,
    ) acquires Oracle, OracleConfig {
        let oracle = borrow_global_mut<Oracle>(self);
        let oracle_config = borrow_global_mut<OracleConfig>(self);
        oracle.name = name;
        oracle.metadata = metadata;
        oracle_config.authority = oracle_authority;
        oracle_config.queue_addr = queue_addr;
    }

    public(friend) fun increment_num_rows(self: address) acquires OracleData {
        let oracle = borrow_global_mut<OracleData>(self);
        oracle.num_rows = oracle.num_rows + 1;
    }

    public(friend) fun decrement_num_rows(self: address) acquires OracleData {
        let oracle = borrow_global_mut<OracleData>(self);
        oracle.num_rows = oracle.num_rows - 1;
    }

    public(friend) fun heartbeat(
        oracle_addr: address,
    ) acquires OracleData {
        let oracle = borrow_global_mut<OracleData>(oracle_addr);
        oracle.last_heartbeat = timestamp::now_seconds();
    }

    public(friend) fun update_reputation(oracle_addr: address, response_type: u8) acquires OracleMetrics {
        let metrics = borrow_global_mut<OracleMetrics>(oracle_addr);
        if (response_type == OracleResponseSuccess()) {
            metrics.consecutive_success =
                metrics.consecutive_success + 1;
            metrics.total_success = metrics.total_success + 1;
            metrics.consecutive_failure = 0;
            metrics.consecutive_error = 0;
            metrics.consecutive_disagreement = 0;
            metrics.consecutive_late_response = 0;
        } else if (response_type == OracleResponseError()) {
            metrics.consecutive_error =
                metrics.consecutive_error + 1;
            metrics.total_error = metrics.total_error + 1;
            metrics.consecutive_failure =
                metrics.consecutive_failure + 1;
            metrics.consecutive_success = 0;
            metrics.consecutive_disagreement = 0;
            metrics.consecutive_late_response = 0;
        } else if (response_type == OracleResponseDisagreement()) {
            metrics.consecutive_disagreement = 
                metrics.consecutive_disagreement + 1;
            metrics.total_disagreement =
                metrics.total_disagreement + 1;
            metrics.consecutive_failure =
                metrics.consecutive_failure + 1;
            metrics.consecutive_success = 0;
            metrics.consecutive_error = 0;
            metrics.consecutive_late_response = 0;
        } else if (response_type == OracleResponseNoResponse()) {
            metrics.consecutive_late_response = 
                metrics.consecutive_late_response + 1;
            metrics.total_late_response =
                metrics.total_late_response + 1;
            metrics.consecutive_failure =
                metrics.consecutive_failure + 1;
            metrics.consecutive_success = 0;
            metrics.consecutive_error = 0;
            metrics.consecutive_disagreement = 0;
        };
    }
}
