module switchboard::oracle_queue_init_action {
    use switchboard::errors;
    use switchboard::math::{Self, SwitchboardDecimal};
    use switchboard::oracle_queue;
    use std::signer;
    use std::vector;

    struct OracleQueueInitParams has copy, drop {
        authority: address,
        name: vector<u8>,
        metadata: vector<u8>,
        oracle_timeout: u64,
        reward: u64,
        min_stake: u64,
        slashing_enabled: bool,
        variance_tolerance_multiplier: SwitchboardDecimal,
        feed_probation_period: u64,
        consecutive_feed_failure_limit: u64,
        consecutive_oracle_failure_limit: u64,
        unpermissioned_feeds_enabled: bool,
        unpermissioned_vrf_enabled: bool,
        lock_lease_funding: bool,
        enable_buffer_relayers: bool,
        max_size: u64,
        data: vector<address>,
        save_confirmation_reward: u64,
        save_reward: u64,
        open_round_reward: u64,
        slashing_penalty: u64
    }

    public fun validate<CoinType>(account: &signer, _params: &OracleQueueInitParams) {
        assert!(!oracle_queue::exist<CoinType>(signer::address_of(account)), errors::QueueAlreadyExists());
    }

    fun actuate<CoinType>(account: &signer, params: &OracleQueueInitParams) {
        oracle_queue::oracle_queue_create<CoinType>(
            account,
            params.authority,
            params.name,
            params.metadata,
            params.oracle_timeout,
            params.reward,
            params.min_stake,
            params.slashing_enabled,
            params.variance_tolerance_multiplier,
            params.feed_probation_period,
            params.consecutive_feed_failure_limit,
            params.consecutive_oracle_failure_limit,
            params.unpermissioned_feeds_enabled,
            params.unpermissioned_vrf_enabled,
            params.lock_lease_funding,
            params.enable_buffer_relayers,
            params.max_size,
            params.data,
            params.save_confirmation_reward,
            params.save_reward,
            params.open_round_reward,
            params.slashing_penalty,
        );
    }

    public entry fun run<CoinType>(
        account: signer, 
        authority: address,
        name: vector<u8>,
        metadata: vector<u8>,
        oracle_timeout: u64,
        reward: u64,
        min_stake: u64,
        slashing_enabled: bool,
        variance_tolerance_multiplier_value: u128,
        variance_tolerance_multiplier_scale: u8,
        feed_probation_period: u64,
        consecutive_feed_failure_limit: u64,
        consecutive_oracle_failure_limit: u64,
        unpermissioned_feeds_enabled: bool,
        unpermissioned_vrf_enabled: bool,
        lock_lease_funding: bool,
        enable_buffer_relayers: bool,
        max_size: u64,
        save_confirmation_reward: u64,
        save_reward: u64,
        open_round_reward: u64,
        slashing_penalty: u64,
    ) {

        // enforce one OracleQueue resource per address
        let params = OracleQueueInitParams {
            authority,
            name,
            metadata,
            oracle_timeout,
            reward,
            min_stake,
            slashing_enabled,
            variance_tolerance_multiplier: math::new(variance_tolerance_multiplier_value, variance_tolerance_multiplier_scale, false),
            feed_probation_period,
            consecutive_feed_failure_limit,
            consecutive_oracle_failure_limit,
            unpermissioned_feeds_enabled,
            unpermissioned_vrf_enabled,
            lock_lease_funding,
            enable_buffer_relayers,
            max_size,
            data: vector::empty(),
            save_confirmation_reward,
            save_reward,
            open_round_reward,
            slashing_penalty,
        };

        validate<CoinType>(&account, &params);
        actuate<CoinType>(&account, &params);
    }
}
