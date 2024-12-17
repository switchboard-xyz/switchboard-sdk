module switchboard::escrow {
    use std::coin::{Self, Coin};
    use std::vector;
    use std::signer;
    use aptos_framework::timestamp;
    use aptos_std::table::{Self, Table};

    friend switchboard::aggregator;
    friend switchboard::lease_init_action;
    friend switchboard::lease_extend_action;
    friend switchboard::lease_withdraw_action;
    friend switchboard::lease_set_authority_action;
    friend switchboard::aggregator_init_action;
    friend switchboard::aggregator_save_result_action;
    friend switchboard::aggregator_open_round_action;
    friend switchboard::crank_pop_action;
    friend switchboard::crank_push_action;
    friend switchboard::oracle_wallet_init_action;
    friend switchboard::oracle_wallet_contribute_action;
    friend switchboard::oracle_wallet_withdraw_action;

    struct EscrowManager<phantom CoinType> has key {

        // mapping of queue to Escrow<CoinType>
        escrows: Table<address, Escrow<CoinType>>,
    }

    struct Escrow<phantom CoinType> has store {
        created_at: u64,
        // This signer may withdraw funds from a escrow at any time. Optional
        authority: address,
        // escrow
        escrow: Coin<CoinType>,
        features: vector<bool>,
        _ebuf: vector<u8>,
    }

    public fun authority<CoinType>(addr: address, queue_addr: address): address acquires EscrowManager {
        let escrow_manager = borrow_global<EscrowManager<CoinType>>(addr);
        let escrow = table::borrow(&escrow_manager.escrows, queue_addr);
        escrow.authority
    }

    public fun exist<CoinType>(addr: address, queue_addr: address): bool acquires EscrowManager {
        exists<EscrowManager<CoinType>>(addr) && 
        table::contains(&borrow_global<EscrowManager<CoinType>>(addr).escrows, queue_addr)
    }

    public fun balance<CoinType>(addr: address, queue_addr: address): u64 acquires EscrowManager {
        let escrow_manager = borrow_global<EscrowManager<CoinType>>(addr);
        let escrow = table::borrow(&escrow_manager.escrows, queue_addr);
        coin::value(&escrow.escrow)
    }

    public(friend) fun new<CoinType>(
        authority: address,
        escrow: Coin<CoinType>
    ): Escrow<CoinType> {
        Escrow {
            authority,
            escrow,
            created_at: timestamp::now_seconds(),
            features: vector::empty(),
            _ebuf: vector::empty(),
        }
    }

    public(friend) fun set_authority<CoinType>(
        addr: address, 
        queue_addr: address, 
        authority: address
    ) acquires EscrowManager {
        let escrow_manager = borrow_global_mut<EscrowManager<CoinType>>(addr);
        let escrow = table::borrow_mut(&mut escrow_manager.escrows, queue_addr);
        escrow.authority = authority;
    }

    public(friend) fun withdraw<CoinType>(addr: address, queue_addr: address, amount: u64): Coin<CoinType> acquires EscrowManager {
        let escrow_manager = borrow_global_mut<EscrowManager<CoinType>>(addr);
        let escrow_container = table::borrow_mut(&mut escrow_manager.escrows, queue_addr);
        let balance = coin::value(&escrow_container.escrow);
        let amount = if (amount < balance) amount else balance;
        coin::extract<CoinType>(&mut escrow_container.escrow, amount)
    }

    // add to the escrow escrow
    public(friend) fun deposit<CoinType>(addr: address, queue_addr: address, coin: Coin<CoinType>) acquires EscrowManager {
        let escrow_manager = borrow_global_mut<EscrowManager<CoinType>>(addr);
        let escrow = table::borrow_mut(&mut escrow_manager.escrows, queue_addr);
        coin::merge(&mut escrow.escrow, coin);
    }

    public(friend) fun create<CoinType>(
        account: &signer,
        queue_addr: address, 
        escrow: Escrow<CoinType>
    ) acquires EscrowManager {
        create_escrow_manager_if_not_exists<CoinType>(account);
        let escrow_manager = borrow_global_mut<EscrowManager<CoinType>>(signer::address_of(account));
        table::add(&mut escrow_manager.escrows, queue_addr, escrow); 
    }

    fun create_escrow_manager_if_not_exists<CoinType>(account: &signer) {
        if (!exists<EscrowManager<CoinType>>(signer::address_of(account))) {
            move_to(account, EscrowManager<CoinType> {
                escrows: table::new(),
            });
        };
    }
}
