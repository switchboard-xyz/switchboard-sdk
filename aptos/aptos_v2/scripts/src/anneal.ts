import { AptosClient, AptosAccount, HexString, CoinClient } from "aptos";
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
import * as utils from "./jobs";
import Big from "big.js";
import yargs = require("yargs/yargs");

const feeds = [
  // {
  // name: "BTC/USD",
  // addr: "0xdc7f6fbc4efe2995e1e37b9f73d113085e4ee3597d47210a2933ad3bf5b78774",
  // testAddr:
  // "0xc07d068fe17f67a85147702359b5819d226591307a5bb54139794f8931327e88",
  // jobs: [
  // utils.binanceComJob("BTCUSDT"),
  // utils.bitfinexJob("tBTCUSD"),
  // utils.coinbaseJob("BTC-USD"),
  // utils.krakenJob("XBTUSDT"),
  // utils.huobiJob("btcusdt"),
  // utils.mexcJob("BTC_USDT"),
  // ],
  // },
  // {
  // name: "ETH/USD",
  // addr: "0x7b5f536d201280a10d33d8c2202a1892b1dd8247aecfef7762ea8e7565eac7b6",
  // testAddr:
  // "0xcaccdee7954db165b1b0923a619b58808937dbf4afb19091fdbb7f0584f41da1",
  // jobs: [
  // utils.binanceComJob("ETHUSDT"),
  // utils.bitfinexJob("tETHUSD"),
  // utils.coinbaseJob("ETH-USD"),
  // utils.krakenJob("ETHUSDT"),
  // utils.huobiJob("ethusdt"),
  // utils.mexcJob("ETH_USDT"),
  // ],
  // },
  // {
  // name: "SOL/USD",
  // addr: "0x5af65afeeab555f8b742ce7fc2c539a5cb6a6fb2a6e6d96bc1b075fb28067808",
  // testAddr:
  // "0xe2677e5bd7473c13b7a8849d463b0b920b9bb02e98bbb6e638992dcf02394688",
  // jobs: [
  // utils.binanceComJob("SOLUSDT"),
  // utils.bitfinexJob("tSOLUSD"),
  // utils.coinbaseJob("SOL-USD"),
  // utils.krakenJob("SOLUSDT"),
  // utils.huobiJob("solusdt"),
  // utils.mexcJob("SOL_USDT"),
  // ],
  // },
  // {
  // name: "APT/USD",
  // addr: "0xb8f20223af69dcbc33d29e8555e46d031915fc38cb1a4fff5d5167a1e08e8367",
  // testAddr:
  // "0x7ac62190ba57b945975146f3d8725430ad3821391070b965b38206fe4cec9fd5",
  // jobs: [
  // utils.binanceComJob("APTUSDT"),
  // utils.bitfinexJob("tAPTUSD"),
  // utils.coinbaseJob("APT-USD"),
  // utils.krakenJob("APTUSDT"),
  // utils.huobiJob("aptusdt"),
  // utils.mexcJob("APT_USDT"),
  // ],
  // },
  // {
  // name: "USDC/USD",
  // addr: "0xdc1045b4d9fd1f4221fc2f91b2090d88483ba9745f29cf2d96574611204659a5",
  // testAddr:
  // "0x1f7b23e6d81fa2102b2e994d2e54d26d116426c7dda5417925265f7b46f50c73",
  // jobs: [
  // utils.appendBounds(utils.binanceComJob("USDCUSDT"), 0.95, 1.05),
  // utils.appendBounds(utils.bitfinexJob("tUSDCUSD"), 0.95, 1.05),
  // utils.appendBounds(utils.coinbaseJob("USDC-USD"), 0.95, 1.05),
  // utils.appendBounds(utils.krakenJob("USDCUSDT"), 0.95, 1.05),
  // utils.appendBounds(utils.huobiJob("usdcusdt"), 0.95, 1.05),
  // utils.appendBounds(utils.mexcJob("USDC_USDT"), 0.95, 1.05),
  // ],
  // },
  {
    name: "USDT/USD",
    addr: "0x94a63284ace90c56f6326de5d62e867d915370a7d059bf974b39aefc699b8baa",
    testAddr: "",
    jobs: [
      utils.appendBounds(utils.binanceComJob("USDTUSD"), 0.95, 1.05),
      utils.appendBounds(utils.bitfinexJob("tUSDTUSD"), 0.95, 1.05),
      utils.appendBounds(utils.coinbaseJob("USDT-USD"), 0.95, 1.05),
      utils.appendBounds(utils.krakenJob("USDT/USD"), 0.95, 1.05),
      // https://api.huobi.pro/market/tickers
      utils.appendBounds(utils.huobiJob("usdtusd"), 0.95, 1.05),
      utils.appendBounds(utils.mexcJob("USDT_USD"), 0.95, 1.05),
      utils.appendBounds(utils.gateIoJob("USDT_USD"), 0.95, 1.05),
    ],
  },
  // {
  // name: "CAKE/USD",
  // addr: "0x4531f956f68ccf05ab29a1db5e73f7c828af5f42f2018b3a74bf934e81fef80f",
  // testAddr: "",
  // jobs: [
  // utils.binanceComJob("CAKEUSDT"),
  // utils.bitfinexJob("tCAKEUSD"),
  // utils.coinbaseJob("CAKE-USD"),
  // utils.krakenJob("CAKEUSDT"),
  // utils.huobiJob("cakeusdt"),
  // utils.mexcJob("CAKE_USDT"),
  // ],
  // },
];

let argv = yargs(process.argv).options({
  mainnet: {
    type: "boolean",
    describe: "",
    demand: false,
    default: false,
  },
}).argv as any;

const MAINNET_NODE_URL =
  "https://switchbo-switchbo-66c4.mainnet.aptos.rpcpool.com/8f545350616dc47e67cfb35dc857/v1";
const TEST_NODE_URL = "https://rpc.ankr.com/http/aptos_testnet/v1";

// Job Init Gas APT
const OCTAS_PER_APT = 10 ** 8;

// add job + init job + extra bump for gas - amounts taken from testnet
const GAS_USED = 0.0016365 + 0.00069 + 0.01;

const TRANSFER_AMOUNT = GAS_USED * OCTAS_PER_APT;

const SWITCHBOARD_ADDRESS =
  "0x7d7e436f0b2aafde60774efb26ccc432cf881b677aca7faaf2a01879bd19fb8";
const SWITCHBOARD_TEST_ADDRESS =
  "0xb91d3fef0eeb4e685dc85e739c7d3e2968784945be4424e92e2f86e2418bf271";
const funderStr =
  "0xca62eccbbdb22b5de18165d0bdf2d7127569b91498f0a7f6944028793cef8137";

const transfer = async (
  client: AptosClient,
  from: AptosAccount,
  to: AptosAccount,
  amount: number
) => {
  const coinClient = new CoinClient(client);
  console.log(`From: ${await coinClient.checkBalance(from)}`);
  let txnHash = await coinClient.transfer(from, to, amount, {
    createReceiverIfMissing: true,
    gasUnitPrice: BigInt(1000),
  }); // <:!:section_5
  console.log(await client.waitForTransaction(txnHash));
  return txnHash;
};

export async function simulateAndRun(
  client: AptosClient,
  user: AptosAccount,
  txn: any,
  maxGasPrice: number = 3000
) {
  let txnRequest = await client.generateTransaction(user.address(), txn as any);

  const simulation = (
    await client.simulateTransaction(user, txnRequest, {
      estimateGasUnitPrice: true,
      estimateMaxGasAmount: true, // @ts-ignore
      estimatePrioritizedGasUnitPrice: true,
    })
  )[0];

  if (Number(simulation.gas_unit_price) > maxGasPrice) {
    throw Error(
      `Estimated gas price from simulation ${simulation.gas_unit_price} above maximum (${maxGasPrice}).`
    );
  }

  if (simulation.success === false) {
    throw new Error(simulation.vm_status);
  }

  /* eslint-disable no-constant-condition */
  while (true) {
    try {
      txnRequest = await client.generateTransaction(
        user.address(),
        txn as any,
        {
          gas_unit_price: simulation.gas_unit_price,
        }
      );
      const signedTxn = await client.signTransaction(user, txnRequest);
      const transactionRes = await client.submitTransaction(signedTxn);
      await client.waitForTransaction(transactionRes.hash);
      return transactionRes.hash;
    } catch (e) {
      console.log(e);
    }
  }
  return null;
}

async function jobInit(
  client,
  job: OracleJob,
  funder
): Promise<[JobAccount, string]> {
  const jobAcct = new AptosAccount();
  return await JobAccount.init(
    client,
    jobAcct,
    {
      name: "",
      metadata: "",
      authority: funder.address().toString(),
      data: utils.jobSerializeB64(job),
    },
    SWITCHBOARD_ADDRESS
  );
}

async function jobDeserialize(job): Promise<OracleJob> {
  const jobbuf = Buffer.from(
    Buffer.from((await job.loadData()).data).toString("utf8"),
    "base64"
  );
  return OracleJob.decodeDelimited(jobbuf);
}

(async () => {
  let PID = SWITCHBOARD_ADDRESS;
  let addrPath = "addr";
  let NODE_URL = MAINNET_NODE_URL;
  if (!argv.mainnet) {
    console.log("TESTNET");
    PID = SWITCHBOARD_TEST_ADDRESS;
    addrPath = "testAddr";
    NODE_URL = TEST_NODE_URL;
  } else {
    console.log("MAINNET");
  }
  const client = new AptosClient(NODE_URL);
  const coinClient = new CoinClient(client);
  let funder: AptosAccount | undefined = undefined;
  const parsedYaml = YAML.parse(fs.readFileSync("../.aptos/config.yaml", "utf8"));
  funder = new AptosAccount(
    HexString.ensure(
      parsedYaml!.profiles!.queue_authority!.private_key!
    ).toUint8Array()
  );
  if (funder!.address().toString() !== funderStr) {
    throw new Error(`Funder should be ${funderStr}.`);
  }
  const devauth = new AptosAccount(
    HexString.ensure(
      parsedYaml!.profiles!.devnet_auth!.private_key!
    ).toUint8Array()
  );
  let feedAuth = funder;
  if (!argv.mainnet) {
    feedAuth = devauth;
  }

  for (const feedMeta of feeds) {
    console.log(`Feed: ${feedMeta.name}`);
    console.log(`Feed: ${feedMeta[addrPath]}`);
    console.log(`Feed: ${addrPath}`);
    const aggregator = new AggregatorAccount(client, feedMeta[addrPath], PID);
    const aggData = await aggregator.loadData();
    console.log(utils.bufString(aggData.name));
    const currentHashes = await Promise.all(
      aggData.jobKeys.map(async (jobKey) => {
        const job = new JobAccount(client, jobKey, PID);
        const jobInner = await jobDeserialize(job);
        const jobHash = utils.jobHash(jobInner);
        return jobHash;
      })
    );
    const annealHashes = feedMeta.jobs.map((j) => utils.jobHash(j));
    for (const idx in annealHashes) {
      if (!currentHashes.includes(annealHashes[idx])) {
        console.log(`Adding job ${idx}`);
        // make job account and add to feed
        const jobKey = new AptosAccount();
        // DO TRANSFER
        await transfer(client, funder, jobKey, OCTAS_PER_APT * 0.3);
        console.log(`Job: ${await coinClient.checkBalance(jobKey)}`);
        const [_, jobInitTx] = await JobAccount.initTx(
          client,
          jobKey.address(),
          {
            name: "",
            metadata: "",
            authority: funder.address(),
            data: utils.jobSerializeB64(feedMeta.jobs[idx]),
          },
          PID
        );
        await simulateAndRun(client, jobKey, jobInitTx);
        while (true) {
          try {
            await aggregator.addJob(feedAuth, { job: jobKey.address() });
            break;
          } catch (e) {
            console.log(e);
          }
        }
      }
    }
    for (const idx in currentHashes) {
      if (!annealHashes.includes(currentHashes[idx])) {
        console.log(`Removing job ${idx}`);
        // remove job from feed
        await simulateAndRun(
          client,
          feedAuth,
          await aggregator.removeJobTx({ job: aggData.jobKeys[idx] })
        );
      }
    }
  }
})();