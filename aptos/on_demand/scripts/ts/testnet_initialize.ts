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

console.log({ signer, account: account.publicKey.toString() });

// ==============================================================================
// Setup Aptos RPC
// ==============================================================================

const config = new AptosConfig({
  network: Network.TESTNET,
  client: { provider: axiosAptosClient },
});
const aptos = new Aptos(config);

const client = new SwitchboardClient(aptos);
const { switchboardAddress } = await client.fetchState();

console.log("Switchboard address:", switchboardAddress);

// ==============================================================================
// Initialize the guardian queue
// ==============================================================================

const queueInitTx = await Queue.initTx(client, signer, {
  queueKey: ON_DEMAND_DEVNET_GUARDIAN_QUEUE.toBuffer().toString("hex"),
  authority: signer,
  name: "Testnet Guardian Queue",
  fee: 0,
  feeRecipient: signer,
  minAttestations: 3,
  oracleValidityLength: 60 * 60 * 24 * 365 * 5,
  isGuardianQueue: true,
  switchboardAddress,
});

console.log(queueInitTx);

const txResponse = await aptos.signAndSubmitTransaction({
  signer: account,
  transaction: queueInitTx,
});
const result = await waitForTx(aptos, txResponse.hash);

// ==============================================================================
// Get the Queue address from the result
// ==============================================================================

const guardianQueueAddress =
  "address" in result.changes[0] ? result.changes[0].address : undefined;

// const guardianQueueAddress =
//   "0x672f8ac54c11dc0b1cc544d30438b3c77d179fb9671974d74c881b3b38ae9f27";

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
//   "0xbe884a65eb035824dca9a12b212584b50efb618ad5a8b9d2b951a8df599773c4";

if (!oracleQueueAddress) {
  throw new Error("Queue address not found in the transaction result");
}
console.log("Oracle Queue address:", oracleQueueAddress);
const oracleQueue = new Queue(client, oracleQueueAddress);
console.log(await oracleQueue.loadData());
