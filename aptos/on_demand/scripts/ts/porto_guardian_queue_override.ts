import {
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
} from "@aptos-labs/ts-sdk";
import * as fs from "fs";
import * as YAML from "yaml";
import {
  ON_DEMAND_DEVNET_GUARDIAN_QUEUE,
  ON_DEMAND_DEVNET_QUEUE,
  getDefaultDevnetGuardianQueue,
  getDefaultDevnetQueue,
  getDefaultQueue,
  Oracle as SolanaOracle,
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



const doTheThing = async () => {

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

  //================================================================================================
  // Initialization and Logging
  //================================================================================================

  const aptosOracleQueue = oracleQueue;
  const queue = new Queue(client, aptosOracleQueue);
  const allOracles: OracleData[] = await queue.loadOracles();

  //================================================================================================
  // Initialize Oracles
  //================================================================================================

  // Load all the oracles on the solana queue
  const solanaQueue = await getDefaultDevnetGuardianQueue();
  const solanaOracleKeys = await solanaQueue.fetchOracleKeys();
  const solanaOracles = await SolanaOracle.loadMany(
    solanaQueue.program,
    solanaOracleKeys
  ).then((oracles) => {
    oracles.forEach((o, i) => {
      o.pubkey = solanaOracleKeys[i];
    });
    return oracles;
  });

  // Initialize the oracles
  console.log(
    "Initializing/Updating Solana Oracles, oracles:",
    solanaOracles.length
  );

  let oracleInits = 0;
  let oracleUpdates = 0;

  for (const oracle of solanaOracles) {
    if (allOracles.find((o) => o.oracleKey === oracle.pubkey.toBase58())) {
      const o = allOracles.find((o) => o.oracleKey === oracle.pubkey.toBase58());
      if (
        o &&
        o.secp256k1Key ===
          `0x${Buffer.from(oracle.enclave.secp256K1Signer).toString("hex")}` &&
        o.expirationTime > Date.now() / 1000
      ) {
        console.log("Oracle already initialized");
        continue;
      } else if (o) {
        console.log("Oracle found, updating", oracle.pubkey.toBase58());
        oracleUpdates++;
        try {
          const tx = await queue.overrideOracleTx(signer, {
            oracle: o.address,
            secp256k1Key: oracle.enclave.secp256K1Signer,
            mrEnclave: oracle.enclave.mrEnclave,
            expirationTime: Date.now() + 60 * 60 * 24 * 7,
          });
          const res = await aptos.signAndSubmitTransaction({
            signer: account,
            transaction: tx,
          });
          const result = await waitForTx(aptos, res.hash);
          console.log("override result:", result);
        } catch (e) {
          console.error(e);
        }
      }
    } else {
      console.log("Oracle not found, initializing", oracle.pubkey.toBase58());
      oracleInits++;
      try {
        const tx = await Oracle.initTx(client, signer, {
          oracleQueue: aptosOracleQueue,
          oracleKey: oracle.pubkey.toBuffer().toString("hex"),
          switchboardAddress,
        });
        const res = await aptos.signAndSubmitTransaction({
          signer: account,
          transaction: tx,
        });
        const result = await waitForTx(aptos, res.hash);
        console.log("initialize result:", result);
      } catch (e) {
        console.error(e);
      }
    }
    
    if (oracleInits === 0 && oracleUpdates == 0) {
      console.log(
        "synced"
      );
    }
  }
};

while (true) {
  await doTheThing();
}