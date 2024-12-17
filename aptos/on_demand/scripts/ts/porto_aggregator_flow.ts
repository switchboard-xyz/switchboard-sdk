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
  network: Network.CUSTOM,
  fullnode: "https://aptos.testnet.porto.movementlabs.xyz/v1",
  client: { provider: axiosAptosClient },
});

const aptos = new Aptos(config);
const client = new SwitchboardClient(aptos, "porto");

const { switchboardAddress, oracleQueue } = await client.fetchState();

console.log("Switchboard address:", switchboardAddress);

// ================================================================================================
// Initialization and Logging
// ================================================================================================

const aggregatorInitTx = await Aggregator.initTx(client, signer, {
  name: "BTC and USD",
  minSampleSize: 1,
  maxStalenessSeconds: 60,
  maxVariance: 1e9,
  feedHash:
    "0x558be89a28d20c32f4cd427dd0dc05229ffefb8c17396124d0bbb0e5efd0a04f",
  minResponses: 1,
  oracleQueue,
});

console.log("Aggregator init tx");

const [aggTransactionResponse] = await aptos.transaction.simulate.simple({
  signerPublicKey: account.publicKey,
  transaction: aggregatorInitTx,
  feePayerPublicKey: account.publicKey,
});

console.log("User transaction response:", aggregatorInitTx);


const res = await aptos.signAndSubmitTransaction({
  signer: account,
  transaction: aggregatorInitTx,
});

console.log("Aggregator init tx result");
const result = await waitForTx(aptos, res.hash);

console.log(result);

//================================================================================================
// Get aggregator id
//================================================================================================

// const aggregatorAddress =
//   "address" in result.changes[0] ? result.changes[0].address : undefined;

const aggregatorAddress =
  "0x4db24a4f0c2bf8b892c299b494937be223cfafc159a6841bbe8305761e1881a";

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

const [userTransactionResponse] = await aptos.transaction.simulate.simple({
  signerPublicKey: account.publicKey,
  transaction: updateTx,
  feePayerPublicKey: account.publicKey,
});

console.log("User transaction response:", userTransactionResponse);

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

