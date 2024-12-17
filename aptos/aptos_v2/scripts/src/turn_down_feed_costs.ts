import { AptosClient, AptosAccount, HexString } from "aptos";
import {
  Permission,
  SwitchboardPermission,
  AggregatorAccount,
  CrankAccount,
  LeaseAccount,
} from "@switchboard-xyz/aptos.js";
import * as YAML from "yaml";
import * as fs from "fs";
import Big from "big.js";

const NODE_URL =
  "https://aptos-api.rpcpool.com/8f545350616dc47e67cfb35dc857/v1";
// const NODE_URL = "https://fullnode.testnet.aptoslabs.com/v1";
// const NODE_URL = "http://0.0.0.0:8080/v1";

// TODO: MAKE THIS THE DEPLOYER ADDRESS
const SWITCHBOARD_ADDRESS =
  "0x7d7e436f0b2aafde60774efb26ccc432cf881b677aca7faaf2a01879bd19fb8";

// TODO: MAKE THIS THE AUTHORITY THAT WILL OWN THE ORACLE
// const QUEUE_ADDRESS =
// "0x11fbd91e4a718066891f37958f0b68d10e720f2edf8d57854fb20c299a119a8c";
const QUEUE_ADDRESS =
  "0xc887072e37f17f9cc7afc0a00e2b283775d703c610acca3997cb26e74bc53f3b";

const feeds = [
  "0xdc7f6fbc4efe2995e1e37b9f73d113085e4ee3597d47210a2933ad3bf5b78774",
  "0x7b5f536d201280a10d33d8c2202a1892b1dd8247aecfef7762ea8e7565eac7b6",
  "0x5af65afeeab555f8b742ce7fc2c539a5cb6a6fb2a6e6d96bc1b075fb28067808",
  "0xdc1045b4d9fd1f4221fc2f91b2090d88483ba9745f29cf2d96574611204659a5",
  "0xb8f20223af69dcbc33d29e8555e46d031915fc38cb1a4fff5d5167a1e08e8367",
];

(async () => {
  const client = new AptosClient(NODE_URL);

  let funder;

  // if file extension ends with yaml
  try {
    const parsedYaml = YAML.parse(
      fs.readFileSync("../.aptos/config.yaml", "utf8")
    );
    funder = new AptosAccount(
      HexString.ensure(
        parsedYaml!.profiles!.queue_authority!.private_key!
      ).toUint8Array()
    );
  } catch (e) {
    console.log(e);
  }

  if (!funder) {
    throw new Error("Could not get funder account.");
  }

  // push to crank
  for (let feed of feeds) {
    try {
      const aggregator = new AggregatorAccount(
        client,
        feed,
        SWITCHBOARD_ADDRESS
      );
      const aggregatorData = await aggregator.loadData();
      await aggregator.setConfig(funder, {
        varianceThreshold: new Big("2"), // 2% variance threshold
        minUpdateDelaySeconds: 300, // 5 minutes
        forceReportPeriod: 3600, // 1 hour
      });
      console.log(`${feed} done`);
    } catch (e) {
      console.log(e);
    }
  }
})();