module switchboard::oracle_heartbeat_action {
    use switchboard::errors;
    use switchboard::oracle;
    use switchboard::oracle_queue;
    use switchboard::permission;
    use switchboard::switchboard;

    struct OracleHeartbeatParams has copy, drop {
        oracle_addr: address,
    }

    fun validate_and_actuate<CoinType>(account: &signer, params: &OracleHeartbeatParams) {

        // VALIDATE
        assert!(oracle::exist(params.oracle_addr), errors::OracleNotFound());
        assert!(oracle::has_authority(params.oracle_addr, account), errors::InvalidAuthority());
        let queue_addr = oracle::queue_addr(params.oracle_addr);
        let authority = oracle_queue::authority<CoinType>(queue_addr);
        let pkey = permission::key(&authority, &queue_addr, &params.oracle_addr);
        let p = switchboard::permission_get(pkey);
        assert!(permission::has(&p, permission::PERMIT_ORACLE_HEARTBEAT()), errors::PermissionDenied());

        // ACTUATE
        oracle::heartbeat(params.oracle_addr);
        if (oracle::num_rows(params.oracle_addr) == 0) {
            oracle_queue::push_back(queue_addr, params.oracle_addr);
            oracle::increment_num_rows(params.oracle_addr)
        };
        let (gc_oracle, gc_idx) = oracle_queue::next_garbage_collection_oracle(queue_addr);
        if (oracle::is_expired(gc_oracle, 180)) {
            oracle::decrement_num_rows(gc_oracle);
            oracle_queue::garbage_collect(queue_addr, gc_idx);
            switchboard::emit_oracle_booted_event(queue_addr, gc_oracle);
        }
    }

    public fun run_internal<CoinType>(account: &signer, oracle_addr: address) {
        let params = OracleHeartbeatParams {
            oracle_addr,
        };
        validate_and_actuate<CoinType>(account, &params);
    }

    public entry fun run<CoinType>(account: signer, oracle_addr: address) {
        let params = OracleHeartbeatParams {
            oracle_addr,
        };
        validate_and_actuate<CoinType>(&account, &params);
    }
}
