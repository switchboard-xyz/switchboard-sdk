import {
  Queue,
  SwitchboardClient,
  axiosAptosClient,
  waitForTx,
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
} from "@aptos-labs/ts-sdk";
import * as fs from "fs";
import * as YAML from "yaml";
import {
  ON_DEMAND_DEVNET_GUARDIAN_QUEUE,
  ON_DEMAND_DEVNET_QUEUE,
} from "@switchboard-xyz/on-demand";

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
  client: { 
    provider: axiosAptosClient 
  },
});

const aptos = new Aptos(config);

const client = new SwitchboardClient(aptos, "porto");
const { switchboardAddress } = await client.fetchState();

console.log("Switchboard address:", switchboardAddress);

// ==============================================================================
// Initialize the guardian queue
// ==============================================================================

// const queueInitTx = await Queue.initTx(client, signer, {
//   queueKey: ON_DEMAND_DEVNET_GUARDIAN_QUEUE.toBuffer().toString("hex"),
//   authority: signer,
//   name: "Testnet Guardian Queue",
//   fee: 0,
//   feeRecipient: signer,
//   minAttestations: 3,
//   oracleValidityLength: 60 * 60 * 24 * 365 * 5,
//   isGuardianQueue: true,
//   switchboardAddress,
// });

// console.log(queueInitTx);

// const txResponse = await aptos.signAndSubmitTransaction({
//   signer: account,
//   transaction: queueInitTx,
// });
// const result = await waitForTx(aptos, txResponse.hash);

// ==============================================================================
// Get the Queue address from the result
// ==============================================================================

// const guardianQueueAddress =
//   "address" in result.changes[0] ? result.changes[0].address : undefined;

const guardianQueueAddress =
  "0x63be642227d71ba56713d2cdb2efd13de0fb7d9783761e4d7f18327c7336f994";

if (!guardianQueueAddress) {
  throw new Error("Queue address not found in the transaction result");
}
console.log("Guardian Queue address:", guardianQueueAddress);

// ==============================================================================
// Load the Queue
// ==============================================================================
const guardianQueue = new Queue(client, guardianQueueAddress);
console.log(await guardianQueue.loadData());

//==============================================================================
// Initialize the Oracle Queue
//==============================================================================

const oracleQueueInitTx = await Queue.initTx(client, signer, {
  queueKey: ON_DEMAND_DEVNET_QUEUE.toBuffer().toString("hex"),
  authority: signer,
  name: "Testnet Oracle Queue",
  fee: 0,
  feeRecipient: signer,
  minAttestations: 3,
  oracleValidityLength: 60 * 60 * 24 * 7,
  isGuardianQueue: false,
  guardianQueue: guardianQueueAddress,
  switchboardAddress,
});

// ==============================================================================
// Sign and submit the transaction
// ==============================================================================
const txResponseOracleQueue = await aptos.signAndSubmitTransaction({
  signer: account,
  transaction: oracleQueueInitTx,
});

const resultOracleQueue = await waitForTx(aptos, txResponseOracleQueue.hash);

// ==============================================================================
// Get the Queue address from the result
// ==============================================================================
const oracleQueueAddress =
  "address" in resultOracleQueue.changes[0]
    ? resultOracleQueue.changes[0].address
    : undefined;

// const oracleQueueAddress =
//   "0x4e445d2968329979b34df2606a65e962ad15a540885fdcbb19eaf7a2a5bf4a22";

if (!oracleQueueAddress) {
  throw new Error("Queue address not found in the transaction result");
}
console.log("Oracle Queue address:", oracleQueueAddress);
const oracleQueue = new Queue(client, oracleQueueAddress);
console.log(await oracleQueue.loadData());
