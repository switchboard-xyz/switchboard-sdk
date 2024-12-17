module switchboard::lease_set_authority_action {
    use switchboard::aggregator;
    use switchboard::escrow;
    use switchboard::errors;
    use std::signer;

    struct LeaseSetAuthorityparams has drop {
        aggregator_addr: address,
        queue_addr: address,
        authority: address,
    }
    
    public fun validate<CoinType>(account: &signer, params: &LeaseSetAuthorityparams) {
        assert!(escrow::exist<CoinType>(params.aggregator_addr, params.queue_addr), errors::LeaseNotFound());
        assert!(escrow::authority<CoinType>(params.aggregator_addr, params.queue_addr) == signer::address_of(account), errors::InvalidAuthority());
    }

    fun actuate<CoinType>(_account: &signer, params: &LeaseSetAuthorityparams) {
        let queue_addr = aggregator::queue_addr(params.aggregator_addr);

        escrow::set_authority<CoinType>(params.aggregator_addr, queue_addr, params.authority);
    }

    public entry fun run<CoinType>(
        account: signer, 
        aggregator_addr: address,
        queue_addr: address,
        authority: address, 
    ) {
        let params = LeaseSetAuthorityparams { 
            aggregator_addr,
            queue_addr,
            authority,
        };
        validate<CoinType>(&account, &params);
        actuate<CoinType>(&account, &params);
    }
}
