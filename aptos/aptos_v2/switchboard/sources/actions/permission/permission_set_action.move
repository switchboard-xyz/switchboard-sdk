module switchboard::permission_set_action {
    use switchboard::permission;
    use switchboard::errors;
    use switchboard::switchboard;
    use std::signer;

    struct PermissionSetParams has drop, copy {
        authority: address,
        granter: address,
        grantee: address,
        permission: u64,
        enable: bool
    }

    public fun validate(account: &signer, params: &PermissionSetParams) {
        let pkey = permission::key(
            &params.authority,
            &params.granter,
            &params.grantee,
        );
        assert!(switchboard::exist(@switchboard), errors::StateNotFound());
        let p = switchboard::permission_get(pkey);
        assert!(permission::authority(&p) == signer::address_of(account), errors::InvalidAuthority());
    }

    fun actuate(_account: &signer, params: &PermissionSetParams) {
        let pkey = permission::key(
            &params.authority,
            &params.granter,
            &params.grantee,
        );
        let p = switchboard::permission_get(pkey);
        if (params.enable) {
            permission::set(&mut p, params.permission);
        } else {
            permission::unset(&mut p, params.permission);
        };
        switchboard::permission_set(&p);
    }

    public entry fun run(
        account: &signer,
        authority: address,
        granter: address,
        grantee: address,
        permission: u64,
        enable: bool
    ) {   

        let params = PermissionSetParams {
            authority,
            granter,
            grantee,
            permission,
            enable
        };

        validate(account, &params);
        actuate(account, &params);
    }    
}
