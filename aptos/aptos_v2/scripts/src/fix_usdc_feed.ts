import { AptosClient, AptosAccount, HexString } from "aptos";
import {
  Permission,
  SwitchboardPermission,
  AggregatorAccount,
  CrankAccount,
  LeaseAccount,
  JobAccount,
  OracleJob,
  sendAptosTx,
} from "@switchboard-xyz/aptos.js";
import * as YAML from "yaml";
import * as fs from "fs";
import Big from "big.js";

const NODE_URL =
  "https://aptos-api.rpcpool.com/8f545350616dc47e67cfb35dc857/v1";
// const NODE_URL =
//   "https://aptos-testnet.blastapi.io/a181a45f-807f-4ca3-9e93-d8dcf4743dba/v1";
// const NODE_URL = "http://0.0.0.0:8080/v1";

// Job Init Gas APT
const OCTAS_PER_APT = 10 ** 8;

// add job + init job + extra bump for gas - amounts taken from testnet
const GAS_USED = 0.0016365 + 0.00069 + 0.01;

const TRANSFER_AMOUNT = GAS_USED * OCTAS_PER_APT;

// TODO: MAKE THIS THE DEPLOYER ADDRESS
const SWITCHBOARD_ADDRESS =
  "0x7d7e436f0b2aafde60774efb26ccc432cf881b677aca7faaf2a01879bd19fb8";

const feed =
  "0xdc1045b4d9fd1f4221fc2f91b2090d88483ba9745f29cf2d96574611204659a5";

const transfer = async (
  client: AptosClient,
  from: AptosAccount,
  to: AptosAccount,
  amount: number
) => {
  const payload = {
    type: "entry_function_payload",
    function: "0x1::aptos_account::transfer",
    type_arguments: [],
    arguments: [to.address().hex(), amount],
  };
  await sendAptosTx(
    client,
    from,
    payload.function,
    payload.arguments,
    payload.type_arguments
  );
};

(async () => {
  const client = new AptosClient(NODE_URL);

  let funder: AptosAccount | undefined;

  // if file extension ends with yaml
  try {
    const parsedYaml = YAML.parse(
      fs.readFileSync("../../switchboard/.aptos/config.yaml", "utf8")
    );
    funder = new AptosAccount(
      HexString.ensure(
        parsedYaml!.profiles!.default!.private_key!
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
      "The funder account is not the owner. It should be 0xca62eccbbdb22b5de18165d0bdf2d7127569b91498f0a7f6944028793cef8137."
    );
  }

  /**
   * Add Coinbase
   */
  try {
    const jobAcct = new AptosAccount();
    await transfer(client, funder, jobAcct, TRANSFER_AMOUNT);
    console.log(
      `transfer done, ${jobAcct.address()} ${jobAcct.signingKey.secretKey.toString()}`
    );
    const aggregator = new AggregatorAccount(client, feed, SWITCHBOARD_ADDRESS);
    const serializedJob = Buffer.from(
      OracleJob.encodeDelimited(
        OracleJob.create({
          tasks: [
            {
              httpTask: {
                url: "https://api.coinbase.com/v2/prices/USDC-USD/spot",
              },
            },
            {
              jsonParseTask: {
                path: "$.data.amount",
              },
            },
            {
              boundTask: {
                lowerBoundValue: "0.98",
                upperBoundValue: "1.02",
              },
            },
          ],
        })
      ).finish()
    );
    const [job, jobSig] = await JobAccount.init(
      client,
      jobAcct,
      {
        name: "Coinbase (bound) USDC/USD",
        metadata: "coinbase",
        authority: funder.address().toString(),
        data: serializedJob.toString("base64"),
      },
      aggregator.switchboardAddress
    );
    console.log(`Job Address (${job.address}): ${job.address}`);
    console.log(`Job Signature (${job.address}): ${jobSig}`);
    const addJobSig = await aggregator.addJob(funder, {
      job: job.address,
    });
    console.log(`coinbase done, ${addJobSig}`);
  } catch (e) {
    console.log(e);
  }

  /**
   * Add Kraken
   */
  try {
    const jobAcct = new AptosAccount();
    await transfer(client, funder, jobAcct, TRANSFER_AMOUNT);
    console.log(
      `transfer done, ${jobAcct.address()} ${jobAcct.signingKey.secretKey.toString()}`
    );
    const aggregator = new AggregatorAccount(client, feed, SWITCHBOARD_ADDRESS);
    const serializedJob = Buffer.from(
      OracleJob.encodeDelimited(
        OracleJob.create({
          tasks: [
            {
              httpTask: {
                url: "https://api.kraken.com/0/public/Ticker?pair=USDCUSD",
              },
            },
            {
              medianTask: {
                tasks: [
                  {
                    jsonParseTask: {
                      path: "$.result.USDCUSD.a[0]",
                    },
                  },
                  {
                    jsonParseTask: {
                      path: "$.result.USDCUSD.b[0]",
                    },
                  },
                  {
                    jsonParseTask: {
                      path: "$.result.USDCUSD.c[0]",
                    },
                  },
                ],
              },
            },
            {
              boundTask: {
                lowerBoundValue: "0.98",
                upperBoundValue: "1.02",
              },
            },
          ],
        })
      ).finish()
    );
    const [job, jobSig] = await JobAccount.init(
      client,
      jobAcct,
      {
        name: "Kraken (bound) USDC/USD",
        metadata: "kraken",
        authority: funder.address(),
        data: serializedJob.toString("base64"),
      },
      aggregator.switchboardAddress
    );
    console.log(`Job Address (${job.address}): ${job.address}`);
    console.log(`Job Signature (${job.address}): ${jobSig}`);
    const addJobSig = await aggregator.addJob(funder, {
      job: job.address,
    });
    console.log(`kraken done, ${addJobSig}}`);
  } catch (e) {
    console.log(e);
  }

  /**
   * Add Bitstamp
   */
  try {
    const jobAcct = new AptosAccount();
    await transfer(client, funder, jobAcct, 10 ** 8);
    console.log(
      `transfer done, ${jobAcct.address()} ${jobAcct.signingKey.secretKey.toString()}`
    );
    const aggregator = new AggregatorAccount(client, feed, SWITCHBOARD_ADDRESS);
    const serializedJob = Buffer.from(
      OracleJob.encodeDelimited(
        OracleJob.create({
          tasks: [
            {
              httpTask: {
                url: "https://www.bitstamp.net/api/v2/ticker/usdcusd",
              },
            },
            {
              medianTask: {
                tasks: [
                  {
                    jsonParseTask: {
                      path: "$.ask",
                    },
                  },
                  {
                    jsonParseTask: {
                      path: "$.bid",
                    },
                  },
                  {
                    jsonParseTask: {
                      path: "$.last",
                    },
                  },
                ],
              },
            },
            {
              boundTask: {
                lowerBoundValue: "0.98",
                upperBoundValue: "1.02",
              },
            },
          ],
        })
      ).finish()
    );
    const [job, jobSig] = await JobAccount.init(
      client,
      jobAcct,
      {
        name: "Bitstamp (bound) USDC/USD",
        metadata: "bitstamp",
        authority: funder.address(),
        data: serializedJob.toString("base64"),
      },
      aggregator.switchboardAddress
    );
    console.log(`Job Address (${job.address}): ${job.address}`);
    console.log(`Job Signature (${job.address}): ${jobSig}`);
    const addJobSig = await aggregator.addJob(funder, {
      job: job.address,
    });
    console.log(`bitstamp done, ${addJobSig}}`);
  } catch (e) {
    console.log(e);
  }

  /**
   * Add Binance
   */
  try {
    const jobAcct = new AptosAccount();
    await transfer(client, funder, jobAcct, TRANSFER_AMOUNT); //.01 apt
    console.log(
      `transfer done, ${jobAcct.address()} ${jobAcct.signingKey.secretKey.toString()}`
    );
    const aggregator = new AggregatorAccount(client, feed, SWITCHBOARD_ADDRESS);
    const serializedJob = Buffer.from(
      OracleJob.encodeDelimited(
        OracleJob.create({
          tasks: [
            {
              httpTask: {
                url: "https://www.binance.us/api/v3/ticker/price?symbol=USDCUSD",
              },
            },
            {
              jsonParseTask: {
                path: "$.price",
              },
            },
            {
              boundTask: {
                lowerBoundValue: "0.98",
                upperBoundValue: "1.02",
              },
            },
          ],
        })
      ).finish()
    );
    const [job, jobSig] = await JobAccount.init(
      client,
      jobAcct,
      {
        name: "Binance US (bound) USDC/USD",
        metadata: "Binance",
        authority: funder.address(),
        data: serializedJob.toString("base64"),
      },
      aggregator.switchboardAddress
    );
    console.log(`Job Address (${job.address}): ${job.address}`);
    console.log(`Job Signature (${job.address}): ${jobSig}`);
    const addJobSig = await aggregator.addJob(funder, {
      job: job.address,
    });
    console.log(`binance done, ${addJobSig}}`);
  } catch (e) {
    console.log(e);
  }

  /**
   * Remove all existing jobs from USDC/USD
   */
  for (let jobAddress of [
    "0x388f6f5c55d7b8786e53774141cb1e9915854197433d9f4e9c4bbc1aafdb6a81",
    "0x20dbd751c5c8cd739e4f9b73cf9bd2c9eb92dd28a53361da4d161cacdb73e702",
    "0x835ae972444150f02711d64e9733e507f70c731257f1cc6a5bc7241b4163ed3f",
    "0x78c83c3158416b3fa040fc7295b04ea2209706820503f1fdcaaf4186cfc1ae4e",
  ]) {
    try {
      // Remove Job from Aggregator
      const tx = await sendAptosTx(
        client,
        funder,
        `${SWITCHBOARD_ADDRESS}::aggregator_remove_job_action::run`,
        [HexString.ensure(feed).hex(), HexString.ensure(jobAddress).hex()]
      );
      console.log(`Removed job ${jobAddress} from aggregator - tx hash: ${tx}`);
    } catch (e) {
      console.log(e);
    }
  }
})();
