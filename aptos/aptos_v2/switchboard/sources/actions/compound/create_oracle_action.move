// should be run by the queue authority
module switchboard::create_oracle_action {
    use switchboard::oracle_init_action;
    use switchboard::oracle_wallet_init_action;
    use switchboard::oracle_wallet_contribute_action;
    use switchboard::permission_init_action;
    use switchboard::permission_set_action;
    use switchboard::permission;
    use switchboard::oracle_queue;
    use aptos_framework::account;
    use std::signer;
    use std::bcs;

    public entry fun run<CoinType>(
        account: signer,
        authority: address,
        name: vector<u8>,
        metadata: vector<u8>,
        queue_addr: address,
        seed: address,
    ) {
        let oracle_addr = account::create_resource_address(&signer::address_of(&account), bcs::to_bytes(&seed));

        // initialize the oracle
        oracle_init_action::run<CoinType>(
            &account, 
            name,
            metadata,
            authority,
            queue_addr,
            seed,
        );

        // initialize the wallet 
        oracle_wallet_init_action::run<CoinType>(
            &account,
            oracle_addr,
            queue_addr,
            authority,
        );

        // fund the wallet with the minimum
        oracle_wallet_contribute_action::run<CoinType>(
            &account,
            oracle_addr,
            queue_addr,
            oracle_queue::min_stake<CoinType>(queue_addr),
        );

        // get the authority from queue_addr
        let queue_authority = oracle_queue::authority<CoinType>(queue_addr);

        // create permission
        permission_init_action::run(
            &account,
            queue_authority,
            queue_addr,
            oracle_addr,
        );

        // allow heartbeat permission
        if (queue_authority == signer::address_of(&account)) {
            permission_set_action::run(
                &account,
                queue_authority,
                queue_addr,
                oracle_addr,
                permission::PERMIT_ORACLE_HEARTBEAT(),
                true,
            );
        }
    }
}