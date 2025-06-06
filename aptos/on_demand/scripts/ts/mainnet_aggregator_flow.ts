import {
  Aggregator,
  Queue,
  Oracle,
  SwitchboardClient,
  axiosAptosClient,
  waitForTx,
  OracleData,
} from "@switchboard-xyz/aptos-sdk";
import {
  Account,
  Aptos,
  AptosConfig,
  Network,
  Ed25519PrivateKey,
  Ed25519Account,
  PrivateKey,
  PrivateKeyVariants,
  APTOS_COIN,
} from "@aptos-labs/ts-sdk";
import * as fs from "fs";
import * as YAML from "yaml";

// ==============================================================================
// Setup Signer and account
// ==============================================================================
const parsedYaml = YAML.parse(fs.readFileSync("./.aptos/config.yaml", "utf8"));
const privateKey = PrivateKey.formatPrivateKey(
  parsedYaml!.profiles!.default!.private_key!,
  PrivateKeyVariants.Ed25519
);
const pk = new Ed25519PrivateKey(privateKey);
const signer = parsedYaml!.profiles!.default!.account!;

const account = new Ed25519Account({
  privateKey: pk,
  address: signer,
});

// ==============================================================================
// Setup Aptos RPC
// ==============================================================================

const config = new AptosConfig({
  network: Network.MAINNET,
  client: { provider: axiosAptosClient },
});
const aptos = new Aptos(config);

const client = new SwitchboardClient(aptos);
const { switchboardAddress, oracleQueue } = await client.fetchState();

const queue = new Queue(client, oracleQueue);
console.log(await queue.loadData());

console.log("Switchboard address:", switchboardAddress);

// // ================================================================================================
// // Initialization and Logging
// // ================================================================================================

// const aggregatorInitTx = await Aggregator.initTx(client, signer, {
//   name: "BTC/USD",
//   minSampleSize: 1,
//   maxStalenessSeconds: 60,
//   maxVariance: 1e9,
//   feedHash:
//     "0x937efd0ba38a4db89364ea2c07de8873e443955b893ba5bcb2edaa611fb13a78",
//   minResponses: 1,
//   oracleQueue,
// });
// const res = await aptos.signAndSubmitTransaction({
//   signer: account,
//   transaction: aggregatorInitTx,
// });
// const result = await waitForTx(aptos, res.hash);

// //================================================================================================
// // Get aggregator id
// //================================================================================================

// const aggregatorAddress =
//   "address" in result.changes[0] ? result.changes[0].address : undefined;

// // const aggregatorAddress =
// //   "0x78372b4f49d210bb96108cca5683fd3c0111d4240d50c3d5ffb3f7bb22246832";

// if (!aggregatorAddress) {
//   throw new Error("Failed to initialize aggregator");
// }

// console.log("Aggregator address:", aggregatorAddress);

// // wait 2 seconds for the transaction to be finalized
// await new Promise((r) => setTimeout(r, 2000));

// //================================================================================================
// // Fetch the aggregator ix
// //================================================================================================

const aggregatorAddress =
  "0x9d7c5ee3e21c670a83cd783811daa8eec80bae1b23799d50f1f7c5e7ecfe11f4";

const aggregator = new Aggregator(client, aggregatorAddress);

console.log("aggregator", await aggregator.loadData());

const { responses, updates, updateTx } = await aggregator.fetchUpdate(signer);

console.log("Aggregator responses:", responses);

if (!updateTx) {
  console.log("No updates to submit");
  process.exit(0);
}

// run the first transaction
// const tx = transactions[0];
const tx = updateTx;
const resTx = await aptos.signAndSubmitTransaction({
  signer: account,
  transaction: updateTx,
  feePayer: account,
});
const resultTx = await waitForTx(aptos, resTx.hash);

console.log("Transaction result:", resultTx);
