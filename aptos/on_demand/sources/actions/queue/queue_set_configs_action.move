module switchboard::queue_set_configs_action {
    use std::signer;
    use std::string::String;
    use aptos_std::event;
    use aptos_framework::object::{Self, Object};
    use switchboard::queue::{Self, Queue};
    use switchboard::errors;

    #[event]
    struct QueueConfigsUpdated has drop, store {
        queue: address,
        name: String,
        fee: u64,
        fee_recipient: address,
        min_attestations: u64,
        oracle_validity_length: u64,
    }

    struct QueueSetConfigsParams {
      queue: Object<Queue>,
      name: String,
      fee: u64,
      fee_recipient: address,
      min_attestations: u64,
      oracle_validity_length_seconds: u64,
    }

    fun params(
      queue: Object<Queue>,
      name: String,
      fee: u64,
      fee_recipient: address,
      min_attestations: u64,
      oracle_validity_length_seconds: u64,
    ): QueueSetConfigsParams {
      QueueSetConfigsParams {
        queue,
        name,
        fee,
        fee_recipient,
        min_attestations,
        oracle_validity_length_seconds,
      }
    }

    public fun validate(
      account: &signer,
      params: &QueueSetConfigsParams,
    ) {
        assert!(queue::queue_exists(params.queue), errors::queue_does_not_exist());
        assert!(queue::has_authority(params.queue, signer::address_of(account)), errors::invalid_authority());
    }

    fun actuate(
      params: QueueSetConfigsParams,
    ) {
      let QueueSetConfigsParams {
        queue,
        name,
        fee,
        fee_recipient,
        min_attestations,
        oracle_validity_length_seconds,
      } = params;
      queue::set_configs(
        queue,
        name,
        fee,
        fee_recipient,
        min_attestations,
        oracle_validity_length_seconds,
      );
      event::emit(QueueConfigsUpdated {
        queue: object::object_address(&queue),
        name,
        fee,
        fee_recipient,
        min_attestations,
        oracle_validity_length: oracle_validity_length_seconds,
      });
    }

    public entry fun run(
      account: &signer,
      queue: Object<Queue>,
      name: String,
      fee: u64,
      fee_recipient: address,
      min_attestations: u64,
      oracle_validity_length_seconds: u64,
    ) {
      
      let params = params(
        queue,
        name,
        fee,
        fee_recipient,
        min_attestations,
        oracle_validity_length_seconds,
      );

      validate(account, &params);
      actuate(params);
    }

}