module switchboard::oracle_save_result_action {
    use std::vector;
    use switchboard::aggregator_save_result_action;
    use switchboard::oracle_heartbeat_action;
    
    // initialize aggregator for user
    public entry fun run<CoinType>(
        account: signer,
        oracle_addr: address,
        aggregator_addr: vector<address>,
        oracle_idx: vector<u64>,
        error: vector<bool>,
        value_num: vector<u128>,
        value_scale_factor: vector<u8>, // scale factor
        value_neg: vector<bool>,
        jobs_checksum: vector<vector<u8>>,
        min_response_num: vector<u128>,
        min_response_scale_factor: vector<u8>,
        min_response_neg: vector<bool>,
        max_response_num: vector<u128>,
        max_response_scale_factor: vector<u8>,
        max_response_neg: vector<bool>,
    ) {   
        oracle_heartbeat_action::run_internal<CoinType>(&account, oracle_addr);
        let len = vector::length(&aggregator_addr);
        let i = 0;
        while (i < len) {
            aggregator_save_result_action::run_internal<CoinType>(
                &account, 
                oracle_addr,
                *vector::borrow(&aggregator_addr, i),
                *vector::borrow(&oracle_idx, i),
                *vector::borrow(&error, i),
                *vector::borrow(&value_num, i),
                *vector::borrow(&value_scale_factor, i),
                *vector::borrow(&value_neg, i),
                *vector::borrow(&jobs_checksum, i),
                *vector::borrow(&min_response_num, i),
                *vector::borrow(&min_response_scale_factor, i),
                *vector::borrow(&min_response_neg, i),
                *vector::borrow(&max_response_num, i),
                *vector::borrow(&max_response_scale_factor, i),
                *vector::borrow(&max_response_neg, i),
            );
            i = i + 1;
        };
    }    
}
