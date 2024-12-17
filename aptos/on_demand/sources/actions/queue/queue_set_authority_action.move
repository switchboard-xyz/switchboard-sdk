module switchboard::queue_set_authority_action {
    use std::signer;
    use aptos_framework::object::{Self, Object};
    use switchboard::queue::{Self, Queue};
    use switchboard::errors;
    use aptos_std::event;

    #[event]
    struct QueueAuthorityUpdated has drop, store {
        queue: address,
        existing_authority: address,
        new_authority: address,
    }

    struct QueueSetAuthorityParams {
        queue: Object<Queue>,
        authority: address,
    }

    fun params(
        queue: Object<Queue>,
        authority: address,
    ): QueueSetAuthorityParams {
        QueueSetAuthorityParams {
            queue,
            authority,
        }
    }

    public fun validate(
        account: &signer,
        params: &QueueSetAuthorityParams,
    ) {
        assert!(queue::queue_exists(params.queue), errors::queue_does_not_exist());
        assert!(queue::has_authority(params.queue, signer::address_of(account)), errors::invalid_authority());
    }

    fun actuate(
        params: QueueSetAuthorityParams,
    ) {
        let existing_authority = queue::authority(params.queue);
        let QueueSetAuthorityParams {
            queue,
            authority,
        } = params;
        queue::set_authority(queue, authority);
        event::emit(QueueAuthorityUpdated {
            queue: object::object_address(&queue),
            existing_authority,
            new_authority: authority,
        });
    }

    public entry fun run(
        account: &signer,
        queue: Object<Queue>,
        authority: address,
    ) {
      
      let params = params(
          queue,
          authority,
      );

      validate(account, &params);
      actuate(params);
    }
}