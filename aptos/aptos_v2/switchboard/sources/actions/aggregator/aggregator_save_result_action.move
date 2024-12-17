module switchboard::aggregator_save_result_action {
    use switchboard::aggregator;
    use switchboard::errors;
    use switchboard::math;
    use switchboard::oracle;
    use switchboard::oracle_queue;
    use switchboard::switchboard;
    
    // initialize aggregator for user
    public entry fun run<CoinType>(
        account: signer,
        oracle_addr: address,
        aggregator_addr: address,
        oracle_idx: u64,
        error: bool,
        value_num: u128,
        value_scale_factor: u8, // scale factor
        value_neg: bool,
        jobs_checksum: vector<u8>,
        min_response_num: u128,
        min_response_scale_factor: u8,
        min_response_neg: bool,
        max_response_num: u128,
        max_response_scale_factor: u8,
        max_response_neg: bool,
    ) {   
        run_internal<CoinType>(
            &account,
            oracle_addr,
            aggregator_addr,
            oracle_idx,
            error,
            value_num,
            value_scale_factor,
            value_neg,
            jobs_checksum,
            min_response_num,
            min_response_scale_factor,
            min_response_neg,
            max_response_num,
            max_response_scale_factor,
            max_response_neg,
        );
    }

    public fun run_internal<CoinType>(
        account: &signer,
        oracle_addr: address,
        aggregator_addr: address,
        oracle_idx: u64,
        error: bool,
        value_num: u128,
        value_scale_factor: u8, // scale factor
        value_neg: bool,
        jobs_checksum: vector<u8>,
        min_response_num: u128,
        min_response_scale_factor: u8,
        min_response_neg: bool,
        max_response_num: u128,
        max_response_scale_factor: u8,
        max_response_neg: bool,
    ) {   

        let value = math::new(value_num, value_scale_factor, value_neg);
        let min_response = math::new(min_response_num, min_response_scale_factor, min_response_neg);
        let max_response = math::new(max_response_num, max_response_scale_factor, max_response_neg);

        /////
        // VALIDATE
        //

        assert!(oracle::exist(oracle_addr), errors::OracleNotFound());
        assert!(aggregator::exist(aggregator_addr), errors::AggregatorNotFound());
        assert!(oracle::has_authority(oracle_addr, account), errors::InvalidAuthority());

        let last_aggregator_value = aggregator::latest_value_internal(aggregator_addr);

        // Get relevant fields from config
        let (
            queue_addr,
            batch_size,
            _min_oracle_results,
        ) = aggregator::configs(aggregator_addr);

        assert!(oracle_idx < batch_size, errors::InvalidArgument());

        // ensure that we're using the queue's CoinType
        assert!(oracle_queue::exist<CoinType>(queue_addr), errors::QueueNotFound());

        // checks if correct oracle is responding
        // checks if job checksums match (in the case of incorrect RPC response)
        // checks if result has already been marked for this oracle
        let save_error_code = aggregator::can_save_result(
            aggregator_addr,
            oracle_addr,
            oracle_idx, 
            &jobs_checksum,
        );

        assert!(save_error_code == 0, save_error_code);

        //////
        // ACTUATE
        //

        let is_confirmed = false;        
        if (error) {

            // add error to aggregator
            aggregator::apply_oracle_error(aggregator_addr, oracle_idx);

        } else {
           is_confirmed = aggregator::save_result(
                aggregator_addr, 
                oracle_idx, 
                &value, 
                &min_response, 
                &max_response
            );
        };
        
        // emit the save result event regardless of success
        switchboard::emit_aggregator_save_result_event(
            aggregator_addr,
            oracle_addr,
            value
        );

        // if the save_result updates the aggregator result, emit update as well
        if (is_confirmed) {

            // emit update value
            switchboard::emit_aggregator_update_event(
                aggregator_addr,
                last_aggregator_value,
                value,
            );
        };
    }    
}
