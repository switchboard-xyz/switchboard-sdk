#!/bin/bash

# Replace it with the network your contract lives on
NETWORK=devnet
# Replace it with your contract address
CONTRACT_ADDRESS=0x465e420630570b780bd8bfc25bfadf444e98594357c488fe397a1142a7b11ffa
# Replace it with your module name; every .move file except move script has module_address::module_name {}

# Define an array
declare -a MODULE_NAMES=(
  "aggregator"
  "queue"
  "oracle"
  "state"
  "aggregator_init_action"
  "aggregator_set_authority_action"
  "aggregator_set_configs_action"
  "aggregator_submit_results_action"
  "oracle_attest_action"
  "oracle_init_action"
  "guardian_queue_init_action"
  "oracle_queue_init_action"
  "queue_add_fee_coin_action"
  "queue_remove_fee_coin_action"
  "queue_override_oracle_action"
  "queue_set_authority_action"
  "queue_set_configs_action"
)

# Ensure the output directory exists
mkdir -p ./scripts/ts/abis

# Loop through the array and run the command with each element
for MODULE_NAME in "${MODULE_NAMES[@]}"
do
  # Save the ABI to a TypeScript file
  echo "export const ABI = $(curl -s https://fullnode.$NETWORK.aptoslabs.com/v1/accounts/$CONTRACT_ADDRESS/module/$MODULE_NAME | sed -n 's/.*\"abi\":\({.*}\).*}$/\1/p') as const;" > ./scripts/ts/abis/$NETWORK.$MODULE_NAME.abi.ts
  echo "Generated ABI for module: $MODULE_NAME"
done
