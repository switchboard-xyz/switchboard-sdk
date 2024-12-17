module switchboard::permission_init_action {
    use switchboard::errors;
    use switchboard::switchboard;
    use switchboard::permission;

    struct PermissionInitParams has drop, copy {
        authority: address,
        granter: address,
        grantee: address,
    }

    public fun validate(_account: &signer, params: &PermissionInitParams) {
        let key = permission::key(
            &params.authority,
            &params.granter,
            &params.grantee,
        );
        assert!(!switchboard::permission_exists(key), errors::PermissionAlreadyExists());
        assert!(switchboard::exist(@switchboard), errors::StateNotFound());
    }

    fun actuate(_account: &signer, params: &PermissionInitParams) {
        let p = permission::new(
            params.authority,
            params.granter,
            params.grantee,
        );
        switchboard::permission_create(&p);
    }

    public entry fun run(
        account: &signer,
        authority: address,
        granter: address,
        grantee: address,
    ) {   
        let params = PermissionInitParams {
            authority,
            granter,
            grantee,
        };

        validate(account, &params);
        actuate(account, &params);
    }    
}
