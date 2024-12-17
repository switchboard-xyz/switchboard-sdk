module switchboard::permission {
    use std::bit_vector::{Self, BitVector};
    use std::vector;
    use std::bcs;
    use aptos_framework::account;
    use aptos_framework::timestamp;

    friend switchboard::aggregator_open_round_action;
    friend switchboard::oracle_heartbeat_action;
    friend switchboard::permission_set_action;
    friend switchboard::permission_init_action;
    friend switchboard::switchboard;

    struct Permission has store, copy, drop {
        authority: address,
        permissions: BitVector,
        granter: address,
        grantee: address,
        created_at: u64,
        updated_at: u64,
        features: vector<bool>,
        _ebuf: vector<u8>,
    }

    public fun PERMIT_ORACLE_HEARTBEAT(): u64 { 0 }
    public fun PERMIT_ORACLE_QUEUE_USAGE(): u64 { 1 }
    public fun PERMIT_VRF_REQUESTS(): u64 { 2 }
    

    public fun key_from_permission(permission: &Permission): address {
        let key = b"Permission";
        vector::append(&mut key, bcs::to_bytes(&permission.granter));
        vector::append(&mut key, bcs::to_bytes(&permission.grantee));
        account::create_resource_address(&permission.authority, key)
    }

    public fun key(
        authority: &address, 
        granter: &address, 
        grantee: &address
    ): address {
        let key = b"Permission";
        vector::append(&mut key, bcs::to_bytes(granter));
        vector::append(&mut key, bcs::to_bytes(grantee));
        account::create_resource_address(authority, key)
    }

    public fun authority(permission: &Permission): address {
        permission.authority
    }

    public fun set(permission: &mut Permission, code: u64) {
        bit_vector::set(&mut permission.permissions, code);
        permission.updated_at = timestamp::now_seconds();
    }

    public fun unset(permission: &mut Permission, code: u64) {
        bit_vector::unset(&mut permission.permissions, code);
        permission.updated_at = timestamp::now_seconds();
    }

    public fun has(permission: &Permission, code: u64): bool{
        bit_vector::is_index_set(&permission.permissions, code)
    }

    public fun seconds_since_last_update(permission: &Permission): u64 {
        timestamp::now_seconds() - permission.updated_at
    }

    public(friend) fun new(authority: address, granter: address, grantee: address): Permission {
        Permission {
           authority,
           permissions: bit_vector::new(32),
           granter,
           grantee,
           created_at: timestamp::now_seconds(),
           updated_at: timestamp::now_seconds(),
           features: vector::empty(),
           _ebuf: vector::empty(),
        }
    }
}
