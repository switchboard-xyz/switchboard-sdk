module switchboard::crank_init_action {
    use switchboard::crank;
    use switchboard::errors;
    use switchboard::oracle_queue;
    use std::signer;

    struct CrankInitParams has drop {
        queue_addr: address,
    }
    
    public fun validate<CoinType>(account: &signer, params: &CrankInitParams) {
        assert!(oracle_queue::exist<CoinType>(params.queue_addr), errors::QueueNotFound());
        assert!(!crank::exist(signer::address_of(account)), errors::CrankAlreadyExists());
    }

    fun actuate<CoinType>(account: &signer, params: &CrankInitParams) {
        let crank = crank::new(
            params.queue_addr
        );
        crank::crank_create(account, crank);
    }

    public entry fun run<CoinType>(account: signer, queue_addr: address) {
        let params = CrankInitParams { queue_addr };
        validate<CoinType>(&account, &params);
        actuate<CoinType>(&account, &params);
    }

}
