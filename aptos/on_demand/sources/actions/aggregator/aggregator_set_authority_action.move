module switchboard::aggregator_set_authority_action {
    use std::signer;
    use aptos_framework::object::{Self, Object};
    use switchboard::aggregator::{Self, Aggregator};
    use switchboard::errors;
    use aptos_std::event;

    #[event]
    struct AggregatorAuthorityUpdated has drop, store {
        aggregator: address,
        existing_authority: address,
        new_authority: address,
    }

    struct AggregatorSetAuthorityParams {
        aggregator: Object<Aggregator>,
        authority: address,
    }
    
    fun params(
        aggregator: Object<Aggregator>,
        authority: address,
    ): AggregatorSetAuthorityParams {
        AggregatorSetAuthorityParams {
            aggregator,
            authority,
        }
    }

    public fun validate(
        account: &signer,
        params: &AggregatorSetAuthorityParams,
    ) {
        assert!(aggregator::aggregator_exists(params.aggregator), errors::aggregator_does_not_exist());
        assert!(object::owner(params.aggregator) == signer::address_of(account), errors::invalid_authority());
        assert!(aggregator::has_authority(params.aggregator, signer::address_of(account)), errors::invalid_authority());
    }

    fun actuate(
        account: &signer,
        params: AggregatorSetAuthorityParams,
    ) {
        let aggregator_state = aggregator::get_aggregator(params.aggregator);
        let existing_authority = aggregator::authority(&aggregator_state);
        let AggregatorSetAuthorityParams {
            aggregator,
            authority,
        } = params;
        aggregator::set_authority(copy aggregator, copy authority);
        object::transfer(account, aggregator, authority);
        event::emit(AggregatorAuthorityUpdated {
            aggregator: object::object_address(&aggregator),
            existing_authority,
            new_authority: authority,
        });
    }

    public entry fun run(
        account: &signer,
        aggregator: Object<Aggregator>,
        authority: address,
    ) {
      
      let params = params(
          aggregator,
          authority,
      );

      validate(account, &params);
      actuate(account, params);
    }
}