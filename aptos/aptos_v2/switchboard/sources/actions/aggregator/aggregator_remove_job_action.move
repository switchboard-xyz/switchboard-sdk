module switchboard::aggregator_remove_job_action {
    use switchboard::aggregator;
    use switchboard::errors;
    use switchboard::job;

    struct AggregatorRemoveJobParams has copy, drop {
        aggregator_addr: address,
        job_addr: address,
    }

    public fun validate(account: &signer, params: &AggregatorRemoveJobParams) {
        assert!(aggregator::exist(params.aggregator_addr), errors::AggregatorNotFound());
        assert!(job::exist(params.job_addr), errors::JobNotFound());
        assert!(!aggregator::is_locked(params.aggregator_addr), errors::AggregatorLocked());
        assert!(aggregator::has_authority(params.aggregator_addr, account), errors::InvalidAuthority());
    }

    fun actuate(_account: &signer, params: &AggregatorRemoveJobParams) {
        aggregator::remove_job(params.aggregator_addr, params.job_addr);
        job::sub_ref_count(params.job_addr);
    }

    public entry fun run(account: signer, aggregator_addr: address, job_addr: address) {
        let params = AggregatorRemoveJobParams { aggregator_addr, job_addr };
        validate(&account, &params);
        actuate(&account, &params);
    }

}
