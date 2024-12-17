module switchboard::create_feed_action {
    use switchboard::aggregator_init_action;
    use switchboard::aggregator_add_job_action;
    use switchboard::job_init_action;
    use switchboard::lease_init_action;
    use switchboard::crank_push_action;
    use switchboard::oracle_queue;
    use switchboard::permission_init_action;
    use switchboard::permission_set_action;
    use switchboard::permission;
    use aptos_framework::account;
    use std::signer;
    use std::vector;
    use std::bcs;

    public entry fun run<CoinType>(
        account: signer,
        authority: address,

        // Aggregator
        name: vector<u8>,
        metadata: vector<u8>,
        queue_addr: address,
        batch_size: u64,
        min_oracle_results: u64,
        min_job_results: u64,
        min_update_delay_seconds: u64,
        start_after: u64,
        variance_threshold_value: u128, 
        variance_threshold_scale: u8, 
        force_report_period: u64,
        expiration: u64,
        disable_crank: bool,
        history_size: u64,
        read_charge: u64,
        reward_escrow: address,
        read_whitelist: vector<address>,
        limit_reads_to_whitelist: bool,

        // Lease
        load_amount: u64,

        // Job 1 
        job_1_name: vector<u8>,
        job_1_metadata: vector<u8>,
        job_1_data: vector<u8>,
        job_1_weight: u8,


        // Job 2
        job_2_name: vector<u8>,
        job_2_metadata: vector<u8>,
        job_2_data: vector<u8>,
        job_2_weight: u8, 


        // Job 3
        job_3_name: vector<u8>,
        job_3_metadata: vector<u8>,
        job_3_data: vector<u8>,
        job_3_weight: u8,


        // Job 4
        job_4_name: vector<u8>,
        job_4_metadata: vector<u8>,
        job_4_data: vector<u8>,
        job_4_weight: u8, 

        // Job 5
        job_5_name: vector<u8>,
        job_5_metadata: vector<u8>,
        job_5_data: vector<u8>,
        job_5_weight: u8,

        // Job 6
        job_6_name: vector<u8>,
        job_6_metadata: vector<u8>,
        job_6_data: vector<u8>,
        job_6_weight: u8,

         // Job 7
        job_7_name: vector<u8>,
        job_7_metadata: vector<u8>,
        job_7_data: vector<u8>,
        job_7_weight: u8,

        // Job 8
        job_8_name: vector<u8>,
        job_8_metadata: vector<u8>,
        job_8_data: vector<u8>,
        job_8_weight: u8,

        // Crank Push
        crank_addr: address,

        // Seed 
        seed: address,
    ) {

        let bcs_seed = bcs::to_bytes(&seed);

        let aggregator_addr = account::create_resource_address(&signer::address_of(&account), copy bcs_seed);

        // Initialize Aggregator
        aggregator_init_action::run<CoinType>(
            &account,
            name,
            metadata,
            queue_addr,
            crank_addr,
            batch_size,
            min_oracle_results,
            min_job_results,
            min_update_delay_seconds,
            start_after,
            variance_threshold_value,
            variance_threshold_scale,
            force_report_period,
            expiration,
            disable_crank,
            history_size,
            read_charge,
            reward_escrow,
            read_whitelist,
            limit_reads_to_whitelist,
            authority,
            seed,
        );

        // Initialize Lease for Aggregator
        lease_init_action::run<CoinType>(
            &account,
            aggregator_addr,
            queue_addr, 
            authority, 
            load_amount,
        );

        // Create and Add Jobs (if they exist)
        if (vector::length<u8>(&job_1_data) > 0) {

            let job_seed = copy bcs_seed;
            vector::push_back(&mut job_seed, 1);
            
            let (resource, _signer_cap) = account::create_resource_account(&account, job_seed);
            job_init_action::run(
                &resource,
                job_1_name,
                job_1_metadata,
                authority,
                job_1_data
            );

            aggregator_add_job_action::run(
                &account,
                aggregator_addr,
                signer::address_of(&resource), 
                job_1_weight,
            );
        };

        if (vector::length<u8>(&job_2_data) > 0) {

            let job_seed = copy bcs_seed;
            vector::push_back(&mut job_seed, 2);
            let (resource, _signer_cap) = account::create_resource_account(&account, job_seed);
            job_init_action::run(
                &resource,
                job_2_name,
                job_2_metadata,
                authority,
                job_2_data
            );

            aggregator_add_job_action::run(
                &account,
                aggregator_addr,
                signer::address_of(&resource), 
                job_2_weight,
            );
        };

        if (vector::length<u8>(&job_3_data) > 0) {

            let job_seed = copy bcs_seed;
            vector::push_back(&mut job_seed, 3);
            let (resource, _signer_cap) = account::create_resource_account(&account, job_seed);
            job_init_action::run(
                &resource,
                job_3_name,
                job_3_metadata,
                authority,
                job_3_data
            );

            aggregator_add_job_action::run(
                &account,
                aggregator_addr,
                signer::address_of(&resource), 
                job_3_weight,
            );
        };

        if (vector::length<u8>(&job_4_data) > 0) {

            let job_seed = copy bcs_seed;
            vector::push_back(&mut job_seed, 4);
            let (resource, _signer_cap) = account::create_resource_account(&account, job_seed);
            job_init_action::run(
                &resource,
                job_4_name,
                job_4_metadata,
                authority,
                job_4_data
            );

            aggregator_add_job_action::run(
                &account,
                aggregator_addr,
                signer::address_of(&resource), 
                job_4_weight,
            );
        };

        if (vector::length<u8>(&job_5_data) > 0) {

            let job_seed = copy bcs_seed;
            vector::push_back(&mut job_seed, 5);
            let (resource, _signer_cap) = account::create_resource_account(&account, job_seed);
            job_init_action::run(
                &resource,
                job_5_name,
                job_5_metadata,
                authority,
                job_5_data
            );   

            aggregator_add_job_action::run(
                &account,
                aggregator_addr,
                signer::address_of(&resource), 
                job_5_weight,
            );
        };

        if (vector::length<u8>(&job_6_data) > 0) {

            let job_seed = copy bcs_seed;
            vector::push_back(&mut job_seed, 6);
            let (resource, _signer_cap) = account::create_resource_account(&account, job_seed);
            job_init_action::run(
                &resource,
                job_6_name,
                job_6_metadata,
                authority,
                job_6_data
            );

            aggregator_add_job_action::run(
                &account,
                aggregator_addr,
                signer::address_of(&resource), 
                job_6_weight,
            ); 
        };

        if (vector::length<u8>(&job_7_data) > 0) {

            let job_seed = copy bcs_seed;
            vector::push_back(&mut job_seed, 7);
            let (resource, _signer_cap) = account::create_resource_account(&account, job_seed);
            job_init_action::run(
                &resource,
                job_7_name,
                job_7_metadata,
                authority,
                job_7_data
            );   

            aggregator_add_job_action::run(
                &account,
                aggregator_addr,
                signer::address_of(&resource), 
                job_7_weight,
            );
        };

        if (vector::length<u8>(&job_8_data) > 0) {
            
            let job_seed = copy bcs_seed;
            vector::push_back(&mut job_seed, 8);
            let (resource, _signer_cap) = account::create_resource_account(&account, job_seed);
            job_init_action::run(
                &resource,
                job_8_name,
                job_8_metadata,
                authority,
                job_8_data
            );

            aggregator_add_job_action::run(
                &account,
                aggregator_addr,
                signer::address_of(&resource), 
                job_8_weight,
            ); 
        };

        // get the authority from queue_addr
        let queue_authority = oracle_queue::authority<CoinType>(queue_addr);

        // create permission
        permission_init_action::run(
            &account,
            queue_authority,
            queue_addr,
            aggregator_addr,
        );

        // allow heartbeat permission
        if (queue_authority == signer::address_of(&account)) {
            permission_set_action::run(
                &account,
                queue_authority,
                queue_addr,
                aggregator_addr,
                permission::PERMIT_ORACLE_QUEUE_USAGE(),
                true,
            );
        };

        if (!disable_crank) {
            crank_push_action::run<CoinType>(
                &account,
                crank_addr, 
                aggregator_addr,
            );
        }
    }
}