#!/bin/bash

# check that DEPLOYER_ADDRESS is set
if [ -z "$DEPLOYER_ADDRESS" ]; then
  echo "DEPLOYER_ADDRESS is not set. Please set DEPLOYER_ADDRESS to the address of the deployer account."
  exit 1
fi

# check that DEPLOYER ADDRESS follows pattern 0x[64 characters]
# deployer address should be a valid aptos address and should start with 0x
if [[ ! $DEPLOYER_ADDRESS =~ ^0x[0-9a-fA-F]{64}$ ]]; then
  echo "DEPLOYER_ADDRESS is not a valid address. Please set DEPLOYER_ADDRESS to the address of the deployer account."
  exit 1
fi

aptos move compile --named-addresses switchboard=$DEPLOYER_ADDRESS
aptos move deploy-object --address-name switchboard