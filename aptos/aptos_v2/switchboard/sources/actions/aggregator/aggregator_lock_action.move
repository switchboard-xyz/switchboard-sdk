module switchboard::aggregator_lock_action {
    use switchboard::aggregator;
    use switchboard::errors;

    struct AggregatorLockParams has copy, drop {
        aggregator_addr: address,
    }
    
    public fun validate(account: &signer, params: &AggregatorLockParams) {
        assert!(aggregator::exist(params.aggregator_addr), errors::AggregatorNotFound());
        assert!(aggregator::has_authority(params.aggregator_addr, account), errors::InvalidAuthority());
    }

    fun actuate(_account: &signer, params: &AggregatorLockParams) {
        aggregator::lock(params.aggregator_addr);    
    }

    // initialize aggregator for user
    public entry fun run(
        account: &signer,
        aggregator_addr: address,
    ) {   
        
        // sender will be the authority
        let params = AggregatorLockParams { aggregator_addr };
        validate(account, &params);
        actuate(account, &params);
    }    
}
