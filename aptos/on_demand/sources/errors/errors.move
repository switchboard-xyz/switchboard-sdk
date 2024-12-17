module switchboard::errors {

  // Object authority doesn't match the expected authority
  const EINVALID_AUTHORITY: u64 = 1;

  // Fee type is not accepted on this action
  const EINVALID_FEE_TYPE: u64 = 2;

  // Invalid length
  const EINVALID_LENGTH: u64 = 3;

  // Invalid expiration time
  const EINVALID_EXPIRATION_TIME: u64 = 4;

  // Invalid queue
  const EINVALID_QUEUE: u64 = 5;

  // Invalid oracle
  const EINVALID_ORACLE: u64 = 6;

  // Invalid validity length
  const EINVALID_VALIDITY_LENGTH: u64 = 7;

  // Invalid timestamp
  const EINVALID_TIMESTAMP: u64 = 8;

  // Invalid max staleness seconds
  const EINVALID_MAX_STALENESS_SECONDS: u64 = 9;

  // Invalid min sample size
  const EINVALID_MIN_SAMPLE_SIZE: u64 = 10;

  // Invalid min responses
  const EINVALID_MIN_RESPONSES: u64 = 11;

  // Invalid Fee
  const EINVALID_FEE: u64 = 12;

  // Aggregator does not exist
  const EAGGREGATOR_DOES_NOT_EXIST: u64 = 13;

  // Queue does not exist
  const EQUEUE_DOES_NOT_EXIST: u64 = 14;

  // Oracle expired
  const EORACLE_EXPIRED: u64 = 15;

  // Guardian expired
  const EGUARDIAN_EXPIRED: u64 = 16;

  // Invalid Guardian
  const EINVALID_GUARDIAN: u64 = 17;

  // Invalid guardian validity length
  const EINVALID_GUARDIAN_VALIDITY_LENGTH: u64 = 18;

  // Invalid oracle validity length
  const EINVALID_ORACLE_VALIDITY_LENGTH: u64 = 19;

  // Oracle does not exist
  const EORACLE_DOES_NOT_EXIST: u64 = 20;

  // Guardian does not exist
  const EGUARDIAN_DOES_NOT_EXIST: u64 = 21;

  // ---- Functions ----

  public fun invalid_authority(): u64 {
    EINVALID_AUTHORITY
  }

  public fun invalid_fee_type(): u64 {
    EINVALID_FEE_TYPE
  }

  public fun invalid_length(): u64 {
    EINVALID_LENGTH
  }

  public fun invalid_expiration_time(): u64 {
    EINVALID_EXPIRATION_TIME
  }

  public fun invalid_queue(): u64 {
    EINVALID_QUEUE
  }

  public fun invalid_oracle(): u64 {
    EINVALID_ORACLE
  }

  public fun invalid_validity_length(): u64 {
    EINVALID_VALIDITY_LENGTH
  }

  public fun invalid_timestamp(): u64 {
    EINVALID_TIMESTAMP
  }

  public fun invalid_max_staleness_seconds(): u64 {
    EINVALID_MAX_STALENESS_SECONDS
  }

  public fun invalid_min_sample_size(): u64 {
    EINVALID_MIN_SAMPLE_SIZE
  }

  public fun invalid_min_responses(): u64 {
    EINVALID_MIN_RESPONSES
  }

  public fun invalid_fee(): u64 {
    EINVALID_FEE
  }

  public fun aggregator_does_not_exist(): u64 {
    EAGGREGATOR_DOES_NOT_EXIST
  }

  public fun queue_does_not_exist(): u64 {
    EQUEUE_DOES_NOT_EXIST
  }

  public fun oracle_expired(): u64 {
    EORACLE_EXPIRED
  }

  public fun guardian_expired(): u64 {
    EGUARDIAN_EXPIRED
  }

  public fun invalid_guardian(): u64 {
    EINVALID_GUARDIAN
  }

  public fun invalid_guardian_validity_length(): u64 {
    EINVALID_GUARDIAN_VALIDITY_LENGTH
  }

  public fun invalid_oracle_validity_length(): u64 {
    EINVALID_ORACLE_VALIDITY_LENGTH
  }

  public fun oracle_does_not_exist(): u64 {
    EORACLE_DOES_NOT_EXIST
  }

  public fun guardian_does_not_exist(): u64 {
    EGUARDIAN_DOES_NOT_EXIST
  }
}
