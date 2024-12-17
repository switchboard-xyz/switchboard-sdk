module switchboard::aggregator_add_job_action {
    use switchboard::aggregator;
    use switchboard::errors;
    use switchboard::job;

    struct AggregatorAddJobParams has copy, drop {
        aggregator_addr: address,
        job_addr: address,
        weight: u8,
    }
    
    public fun validate(account: &signer, params: &AggregatorAddJobParams) {
        assert!(aggregator::exist(params.aggregator_addr), errors::AggregatorNotFound());
        assert!(job::exist(params.job_addr), errors::JobNotFound());
        assert!(aggregator::has_authority(params.aggregator_addr, account), errors::InvalidAuthority());
        assert!(!aggregator::is_locked(params.aggregator_addr), errors::AggregatorLocked());
        assert!(params.weight > 0, errors::InvalidArgument());
    }

    fun actuate(_account: &signer, params: &AggregatorAddJobParams) {
        let job = job::job_get(params.job_addr);
        aggregator::add_job(params.aggregator_addr, &job, params.weight);    
        job::add_ref_count(params.job_addr);
    }

    
    // initialize aggregator for user
    public entry fun run(
        account: &signer,
        aggregator_addr: address, 
        job_addr: address, 
        weight: u8
    ) {   
        
        // sender will be the authority
        let params = AggregatorAddJobParams { aggregator_addr, job_addr, weight };
    
        validate(account, &params);
        actuate(account, &params);
    }    
}
