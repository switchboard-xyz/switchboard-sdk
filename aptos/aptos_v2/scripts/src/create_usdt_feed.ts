import { AptosClient, AptosAccount, HexString } from "aptos";
import {
  createFeed,
  generateResourceAccountAddress,
  bcsAddressToBytes,
} from "@switchboard-xyz/aptos.js";
import * as YAML from "yaml";
import * as fs from "fs";
import Big from "big.js";
import { usdtBinance, usdtBitstamp, usdtKraken } from "./usdt";

const NODE_URL = "https://fullnode.mainnet.aptoslabs.com/v1";
const NODE_URL_TESTNET = "https://fullnode.testnet.aptoslabs.com/v1";

const SWITCHBOARD_ADDRESS_MAINNET =
  "0x7d7e436f0b2aafde60774efb26ccc432cf881b677aca7faaf2a01879bd19fb8";

const SWITCHBOARD_ADDRESS_TESTNET =
  "0x7d7e436f0b2aafde60774efb26ccc432cf881b677aca7faaf2a01879bd19fb8";

const QUEUE_ADDRESS =
  "0x11fbd91e4a718066891f37958f0b68d10e720f2edf8d57854fb20c299a119a8c";

const CRANK_ADDRESS =
  "0xbc9576fedda51d33e8129b5f122ef4707c2079dfb11cd836e86adcb168cbd473";

const SWITCHBOARD_ADDRESS = SWITCHBOARD_ADDRESS_MAINNET;

(async () => {
  const client = new AptosClient(NODE_URL_TESTNET);

  let funder: AptosAccount | undefined;

  // if file extension ends with yaml
  try {
    const parsedYaml = YAML.parse(
      fs.readFileSync(".aptos/config.yaml", "utf8")
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

  if (
    funder.address().toString() !==
    "0xca62eccbbdb22b5de18165d0bdf2d7127569b91498f0a7f6944028793cef8137"
  ) {
    throw new Error(
      "The funder account is not the queue authority. It should be 0xca62eccbbdb22b5de18165d0bdf2d7127569b91498f0a7f6944028793cef8137."
    );
  }

  const FEED_SEED = "0x7"; // formerly chuck's

  const FEED_KEY_1 = generateResourceAccountAddress(
    funder.address(),
    bcsAddressToBytes(HexString.ensure(FEED_SEED))
  );

  try {
    const [aggregator, createFeedTx] = await createFeed(
      client,
      funder,
      {
        name: "USDT/USD",
        authority: funder.address(),
        queueAddress: QUEUE_ADDRESS,
        batchSize: 1,
        minJobResults: 2,
        minOracleResults: 1,
        minUpdateDelaySeconds: 5,
        varianceThreshold: new Big(0),
        coinType: "0x1::aptos_coin::AptosCoin",
        crankAddress: CRANK_ADDRESS,
        initialLoadAmount: 0,
        seed: FEED_SEED,
        jobs: [
          {
            name: "USDT/USD binance",
            metadata: "binance",
            authority: funder.address().hex(),
            data: usdtBinance.toString("base64"),
            weight: 1,
          },
          {
            name: "USDT/USD kraken",
            metadata: "kraken",
            authority: funder.address().hex(),
            data: usdtKraken.toString("base64"),
            weight: 1,
          },
          {
            name: "USDT/USD Bitstamp",
            metadata: "bitstamp",
            authority: funder.address().hex(),
            data: usdtBitstamp.toString("base64"),
            weight: 1,
          },
        ],
      },
      SWITCHBOARD_ADDRESS
    );

    console.log("made usdt feed", FEED_KEY_1);
    console.log("made usdt feed tx", createFeedTx);
  } catch (e) {
    console.log(`couldn't make usdt feed`, e);
  }
})();
