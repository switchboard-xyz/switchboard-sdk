import {
  AptosClient,
  AptosAccount,
  FaucetClient,
  HexString,
  CoinClient,
} from "aptos";
import {
  OracleQueueAccount,
  CrankAccount,
  generateResourceAccountAddress,
  createOracle,
  sendAptosTx,
} from "@switchboard-xyz/aptos.js";;
import * as YAML from "yaml";
import * as fs from "fs";

const getAllOpenRounds = async (
  client: AptosClient,
  switchboardAddress: HexString
) => {
  let page = 0;
  const cap = 25;
  const events = [];
  // eslint-disable-next-line no-constant-condition
  while (true) {
    const result = await client.getEventsByEventHandle(
      switchboardAddress,
      `${switchboardAddress.hex()}::switchboard::State`,
      "aggregator_open_round_events",
      { start: page * cap, limit: cap }
    );
    events.push(...result);
    if (result.length === 25) page += 1;
    else return events;
  }
};

const NODE_URL = "https://fullnode.mainnet.aptoslabs.com/v1";
// const NODE_URL =
//   "https://7023b384-9d18-4480-ba7a-bb629d724ae9:881b272ea3154b9dbb64b0bfe3878c9f@aptos-mainnet.nodereal.io/v1";
//const NODE_URL = "http://0.0.0.0:8080/v1";

// TODO: MAKE THIS THE DEPLOYER ADDRESS
const SWITCHBOARD_ADDRESS =
  "0x7d7e436f0b2aafde60774efb26ccc432cf881b677aca7faaf2a01879bd19fb8"; // (localnet)

// TODO: MAKE THIS THE AUTHORITY THAT WILL OWN THE ORACLE
const QUEUE_ADDRESS =
  "0xc887072e37f17f9cc7afc0a00e2b283775d703c610acca3997cb26e74bc53f3b"; // "0x34e2eead0aefbc3d0af13c0522be94b002658f4bef8e0740a21086d22236ad77"; // (localnet)
/*
  CREATE 1 ORACLE AND WRITE OUT THE KEY
 */

(async () => {
  const client = new AptosClient(NODE_URL);

  let funder;

  // if file extension ends with yaml
  try {
    const parsedYaml = YAML.parse(
      fs.readFileSync("../.aptos/config.yaml", "utf8")
    );
    funder = new AptosAccount(
      HexString.ensure(parsedYaml.profiles.default.private_key).toUint8Array()
    );
  } catch (e) {
    console.log(e);
  }

  if (!funder) {
    throw new Error("Could not get funder account.");
  }

  const results = await getAllOpenRounds(
    client,
    HexString.ensure(SWITCHBOARD_ADDRESS)
  );
  console.log(results.length);
})();
