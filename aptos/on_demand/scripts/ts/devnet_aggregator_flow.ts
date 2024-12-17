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
  network: Network.DEVNET,
  client: { provider: axiosAptosClient },
});
const aptos = new Aptos(config);

const client = new SwitchboardClient(aptos);
const { switchboardAddress, oracleQueue } = await client.fetchState();

console.log("Switchboard address:", switchboardAddress);

const queue = new Queue(client, oracleQueue);
console.log(await queue.loadOracles());


// ================================================================================================
// Initialization and Logging
// ================================================================================================

const aggregatorInitTx = await Aggregator.initTx(client, signer, {
  name: "BTC/USD",
  minSampleSize: 1,
  maxStalenessSeconds: 60,
  maxVariance: 1e9,
  feedHash:
    "0x558be89a28d20c32f4cd427dd0dc05229ffefb8c17396124d0bbb0e5efd0a04f",
  minResponses: 1,
  oracleQueue,
});
const res = await aptos.signAndSubmitTransaction({
  signer: account,
  transaction: aggregatorInitTx,
});
const result = await waitForTx(aptos, res.hash);

//================================================================================================
// Get aggregator id
//================================================================================================

const aggregatorAddress =
  "address" in result.changes[0] ? result.changes[0].address : undefined;

// const aggregatorAddress =
//   "0x6efd87f0f123a0b42de249592aabf61ca7abe2bf705fe6902cb51d093355e39f";

if (!aggregatorAddress) {
  throw new Error("Failed to initialize aggregator");
}

console.log("Aggregator address:", aggregatorAddress);

// wait 2 seconds for the transaction to be finalized
await new Promise((r) => setTimeout(r, 2000));

//================================================================================================
// Fetch the aggregator ix
//================================================================================================


const aggregator = new Aggregator(client, aggregatorAddress);

console.log("aggregator", await aggregator.loadData());

const { responses, updates, updateTx } = await aggregator.fetchUpdate(
  signer
);

console.log("Aggregator responses:", responses);

// run the transaction
const tx = updateTx;
const resTx = await aptos.signAndSubmitTransaction({
  signer: account,
  transaction: tx,
  feePayer: account,
});
const resultTx = await waitForTx(aptos, resTx.hash);

console.log("Transaction result:", resultTx);

