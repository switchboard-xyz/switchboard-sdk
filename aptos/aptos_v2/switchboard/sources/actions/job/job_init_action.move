module switchboard::job_init_action {
    use std::signer;
    use switchboard::job;
    use switchboard::errors;

    struct JobConfigParams has drop, copy {
        name: vector<u8>,
        metadata: vector<u8>,
        authority: address,
        data: vector<u8>
    }

    public fun validate(account: &signer, _params: &JobConfigParams) {
        assert!(!job::exist(signer::address_of(account)), errors::JobAlreadyExists());
    }

    fun actuate(account: &signer, params: &JobConfigParams) {
        let job = job::new(
            signer::address_of(account),
            params.name,
            params.metadata,
            params.authority,
            params.data
        );

        // Add the job to switchboard state
        job::job_create(account, job);    
    }

    // initialize aggregator for user
    public entry fun run(
        account: &signer,
        name: vector<u8>,
        metadata: vector<u8>,
        authority: address,
        data: vector<u8>
    ) {   

        // generate params
        let params = JobConfigParams {
            name,
            metadata,
            authority,
            data
        };

        validate(account, &params);
        actuate(account, &params);
    }    
}
