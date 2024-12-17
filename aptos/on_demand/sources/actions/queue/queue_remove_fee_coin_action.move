module switchboard::queue_remove_fee_coin_action {
    use std::signer;
    use std::type_info::{Self, TypeInfo};
    use aptos_framework::object::{Self, Object};
    use switchboard::queue::{Self, Queue};
    use switchboard::errors;
    use aptos_std::event;

    #[event]
    struct QueueFeeTypeRemoved has drop, store {
        queue: address,
        fee_type: TypeInfo,
    }
    struct RemoveFeeCoinParams {
        queue: Object<Queue>,
        type_info: TypeInfo,
    }

    fun params(
        queue: Object<Queue>,
        fee_coin: TypeInfo,
    ): RemoveFeeCoinParams {
        RemoveFeeCoinParams {
            queue,
            type_info: fee_coin,
        }
    }

    public fun validate(
        account: &signer,
        params: &RemoveFeeCoinParams,
    ) {
        assert!(queue::queue_exists(params.queue), errors::queue_does_not_exist());
        assert!(queue::has_authority(params.queue, signer::address_of(account)), errors::invalid_authority());
        assert!(queue::fee_coin_exists(params.queue, &params.type_info), errors::invalid_fee_type());
    }

    fun actuate(
        params: RemoveFeeCoinParams,
    ) {
        let RemoveFeeCoinParams {
            queue,
            type_info,
        } = params;
        queue::remove_fee_type(queue, type_info);
        event::emit(QueueFeeTypeRemoved {
            queue: object::object_address(&queue),
            fee_type: type_info,
        });
    }

    public entry fun run<CoinType>(
        account: &signer,
        queue: Object<Queue>,
    ) {
      
      let fee_coin = type_info::type_of<CoinType>();
      let params = params(
          queue,
          fee_coin,
      );

      validate(account, &params);
      actuate(params);
    }
}