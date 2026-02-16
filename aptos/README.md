# Aptos On-Demand Integration

**DISCLAIMER: SWITCHBOARD ON-DEMAND FOR APTOS IS CURRENTLY UNDERGOING AUDIT. USE AT YOUR OWN RISK.**

# Switchboard On-Demand Integration Guide

This guide covers the setup and use of Switchboard data feeds within your project, using the `Aggregator` module for updating feeds and integrating `Switchboard` in Move.

## Active Deployments

The Switchboard On-Demand service is currently deployed on the following networks:

- Mainnet: [0xfea54925b5ac1912331e2e62049849b37842efaea298118b66f85a59057752b8](https://explorer.aptoslabs.com/object/0xfea54925b5ac1912331e2e62049849b37842efaea298118b66f85a59057752b8/modules/code/aggregator?network=mainnet)
- Testnet: [0x81fc6bbc64b7968e631b2a5b3a88652f91a617534e3755efab2f572858a30989](https://explorer.aptoslabs.com/object/0x4fc1809ffb3c5ada6b4e885d4dbdbeb70cbdd99cbc0c8485965d95c2eab90935/modules/code/aggregator?network=testnet)

## Typescript-SDK Installation

To use Switchboard On-Demand, add the following dependencies to your project:

### NPM

```bash
npm install @switchboard-xyz/aptos-sdk --save
```

### Bun

```bash
bun add @switchboard-xyz/aptos-sdk
```

### PNPM

```bash
pnpm add @switchboard-xyz/aptos-sdk
```

## Adding Switchboard to Move Code

To integrate Switchboard with Move, add the following dependencies to Move.toml:

```toml
[dependencies.Switchboard]
git = "https://github.com/switchboard-xyz/aptos.git"
subdir = "on_demand/"
rev = "mainnet" # testnet or mainnet
```

## Example Move Code for Using Switchboard Values

In the example.move module, use the Aggregator and CurrentResult types to access the latest feed data.

```move
module example::switchboard_example {
    use aptos_framework::aptos_coin::AptosCoin;
    use aptos_framework::object::{Self, Object};
    use switchboard::aggregator::{Self, Aggregator, CurrentResult};
    use switchboard::decimal::Decimal;
    use switchboard::update_action;

    public entry fun my_function(account: &signer, update_data: vector<vector<u8>>) {

        // Update the feed with the provided data
        update_action::run<AptosCoin>(account, update_data);

        /**
        * You can use the following code to remove and run switchboard updates from the update_data vector,
        * keeping only non-switchboard byte vectors:
        *
        * update_action::extract_and_run<AptosCoin>(account, &mut update_data);
        */

        // Get the feed object
        let aggregator: address = @0xSomeFeedAddress;
        let aggregator: Object<Aggregator> = object::address_to_object<Aggregator>(aggregator);

        // Get the latest update info for the feed
        let current_result: CurrentResult = aggregator::current_result(aggregator);

        // Access various result properties
        let result: Decimal = aggregator::result(&current_result);              // Update result
        let (result_u128, result_neg) = decimal::unpack(result);                // Unpack result
        let timestamp_seconds = aggregator::timestamp(&current_result);         // Timestamp in seconds

        // Other properties you can use from the current result
        let min_timestamp: u64 = aggregator::min_timestamp(&current_result);    // Oldest valid timestamp used
        let max_timestamp: u64 = aggregator::max_timestamp(&current_result);    // Latest valid timestamp used
        let range: Decimal = aggregator::range(&current_result);                // Range of results
        let mean: Decimal = aggregator::mean(&current_result);                  // Average (mean)
        let stdev: Decimal = aggregator::stdev(&current_result);                // Standard deviation

        // Use the computed result as needed...
    }
}
```

Once dependencies are configured, updated aggregators can be referenced easily.

This implementation allows you to read and utilize Switchboard data feeds within Move. If you have any questions or need further assistance, please contact the Switchboard team.

## Creating an Aggregator and Sending Transactions

Building a feed in Switchboard can be done using the Typescript SDK, or it can be done with the [Switchboard Web App](https://ondemand.switchboard.xyz/aptos/mainnet). Visit our [docs](https://docs.switchboard.xyz/quick-start) for more on designing and creating feeds.

### Building Feeds in Typescript [optional]

```typescript
import {
  CrossbarClient,
  SwitchboardClient,
  Aggregator,
  ON_DEMAND_MAINNET_QUEUE_KEY,
  ON_DEMAND_TESTNET_QUEUE_KEY,
} from "@switchboard-xyz/aptos-sdk";

// get the aptos client
const config = new AptosConfig({
  network: Network.MAINNET, // network a necessary param / if not passed in, full node url is required
});
const aptos = new Aptos(config);

// create a SwitchboardClient using the aptos client
const client = new SwitchboardClient(aptos);

// for initial testing and development, you can use the public
// https://crossbar.switchboard.xyz instance of crossbar
const crossbar = new CrossbarClient("https://crossbar.switchboard.xyz");

// ... define some jobs ...

const queue = isMainnet
  ? ON_DEMAND_MAINNET_QUEUE_KEY
  : ON_DEMAND_TESTNET_QUEUE_KEY;

// Store some job definition
const { feedHash } = await crossbarClient.store(queue, jobs);

// try creating a feed
const feedName = "BTC/USDT";

// Require only one oracle response needed
const minSampleSize = 1;

// Allow update data to be up to 60 seconds old
const maxStalenessSeconds = 60;

// If jobs diverge more than 1%, don't allow the feed to produce a valid update
const maxVariance = 1e9;

// Require only 1 job response
const minJobResponses = 1;

//==========================================================
// Feed Initialization On-Chain
//==========================================================

// ... get the account object for your signer with relevant key / address ...

// get the signer address
const signerAddress = account.accountAddress.toString();

const aggregatorInitTx = await Aggregator.initTx(client, signerAddress, {
  name: feedName,
  minSampleSize,
  maxStalenessSeconds,
  maxVariance,
  feedHash,
  minResponses,
});

const res = await aptos.signAndSubmitTransaction({
  signer: account,
  transaction: aggregatorInitTx,
});

const result = await aptos.waitForTransaction({
  transactionHash: res.hash,
  options: {
    timeoutSecs: 30,
    checkSuccess: true,
  },
});

// Log the transaction results
console.log(result);
```

## Updating Feeds

With Switchboard On-Demand, passing the PTB (proof-to-be) into the feed update method handles the update automatically.

```typescript
const aggregator = new Aggregator(sb, aggregatorId);

// Fetch and log the oracle responses
const { updates } = await aggregator.fetchUpdate(signerAddress);

// Create a transaction to run the feed update
const updateTx = await switchboardClient.aptos.transaction.build.simple({
  sender: singerAddress,
  data: {
    function: `${exampleAddress}::switchboard_example::my_function`,
    functionArguments: [updates],
  },
});

// Sign and submit the transaction
const res = await aptos.signAndSubmitTransaction({
  signer: account,
  transaction: updateTx,
});

// Wait for the transaction to complete
const result = await aptos.waitForTransaction({
  transactionHash: res.hash,
  options: {
    timeoutSecs: 30,
    checkSuccess: true,
  },
});

// Log the transaction results
console.log(result);
```

Note: Ensure the Switchboard Aggregator update is the first action in your PTB or occurs before referencing the feed update.
