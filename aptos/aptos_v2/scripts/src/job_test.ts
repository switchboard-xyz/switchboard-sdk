import { AptosClient, AptosAccount, HexString } from "aptos";
import {
  createFeed,
  generateResourceAccountAddress,
  bcsAddressToBytes,
  JobAccount,
} from "@switchboard-xyz/aptos.js";
import * as YAML from "yaml";
import * as fs from "fs";
import Big from "big.js";
import { usdtBinance, usdtBitstamp, usdtKraken } from "./usdt";

const NODE_URL = "https://rpc.ankr.com/http/aptos_testnet/v1";

const SWITCHBOARD_ADDRESS =
  "0xb91d3fef0eeb4e685dc85e739c7d3e2968784945be4424e92e2f86e2418bf271";

const QUEUE_ADDRESS =
  "0xc887072e37f17f9cc7afc0a00e2b283775d703c610acca3997cb26e74bc53f3b";

(async () => {
  const client = new AptosClient(NODE_URL);

  const parsedYaml = YAML.parse(
    fs.readFileSync("../../.aptos/config.yaml", "utf8")
  );
  let funder: AptosAccount = new AptosAccount(
    HexString.ensure(
      parsedYaml!.profiles!.queue_authority!.private_key!
    ).toUint8Array()
  );
  const [job, sig] = await JobAccount.init(
    client,
    funder,
    {
      name: "NAME",
      metadata: "META",
      authority: funder.address(),
      data: "BUFFER",
    },
    SWITCHBOARD_ADDRESS
  );
  console.log(sig);
})();
