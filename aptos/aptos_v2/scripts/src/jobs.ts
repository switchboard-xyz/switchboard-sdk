import { AptosClient, AptosAccount, HexString } from "aptos";
import {
  Permission,
  SwitchboardPermission,
  AggregatorAccount,
  CrankAccount,
  LeaseAccount,
  JobAccount,
  sendAptosTx,
} from "@switchboard-xyz/aptos.js";
import { OracleJob } from "@switchboard-xyz/common";
import * as YAML from "yaml";
import * as fs from "fs";
import Big from "big.js";
import { createHash } from "crypto";

const BTC = "BTC";

export function bufString(buf: Uint8Array): string {
  return Buffer.from(buf).toString("utf8").replace(/\0/g, "");
}

export function sha256Hash(data: string): string {
  const hash = createHash("sha256");
  hash.update(data);
  return hash.digest("hex");
}

export function jobHash(data: OracleJob): string {
  return sha256Hash(JSON.stringify(data));
}

export function jobSerialize(data: OracleJob): Buffer {
  return Buffer.from(OracleJob.encodeDelimited(data).finish());
}

export function jobSerializeB64(data: OracleJob): string {
  return jobSerialize(data).toString("base64");
}

export function appendBounds(job, lower, upper) {
  let bounds = OracleJob.Task.create({
    boundTask: {
      upperBoundValue: `${upper}`,
      // onExceedsUpperBoundValue: upper,
      lowerBoundValue: `${lower}`,
      // onExceedsLowerBoundValue: lower,
    },
  });
  job.tasks.push(bounds);
  return job;
}

export function binanceComJob(pair: string): OracleJob {
  return OracleJob.fromObject({
    tasks: [
      {
        httpTask: {
          url: `https://www.binance.com/api/v3/ticker/price?symbol=${pair}`,
        },
      },
      {
        jsonParseTask: {
          path: "$.price",
        },
      },
    ],
  });
}

export function bitfinexJob(pair: string): OracleJob {
  return OracleJob.fromObject({
    tasks: [
      {
        httpTask: {
          url: `https://api-pub.bitfinex.com/v2/tickers?symbols=${pair}`,
        },
      },
      {
        medianTask: {
          tasks: [
            {
              jsonParseTask: {
                path: "$[0][1]",
              },
            },
            {
              jsonParseTask: {
                path: "$[0][3]",
              },
            },
            {
              jsonParseTask: {
                path: "$[0][7]",
              },
            },
          ],
        },
      },
    ],
  });
}

export function coinbaseJob(pair: string): OracleJob {
  return OracleJob.fromObject({
    tasks: [
      {
        httpTask: {
          url: `https://api.coinbase.com/v2/prices/${pair}/spot`,
        },
      },
      {
        jsonParseTask: {
          path: "$.data.amount",
        },
      },
    ],
  });
}

export function coinbaseWsJob(pair: string): OracleJob {
  return OracleJob.fromObject({
    tasks: [
      {
        websocketTask: {
          url: "wss://ws-feed.pro.coinbase.com",
          subscription: `{"type":"subscribe","product_ids":["${pair}"],"channels":["ticker",{"name":"ticker","product_ids":["${pair}"]}]}`,
          maxDataAgeSeconds: 60,
          filter: `$[?(@.type == 'ticker' && @.product_id == '${pair}')]`,
        },
      },
      {
        jsonParseTask: {
          path: "$.price",
        },
      },
    ],
  });
}

export function krakenJob(pair: string): OracleJob {
  return OracleJob.fromObject({
    tasks: [
      {
        httpTask: {
          url: `https://api.kraken.com/0/public/Ticker?pair=${pair}`,
        },
      },
      {
        medianTask: {
          tasks: [
            {
              jsonParseTask: {
                path: `$.result.${pair}.a[0]`,
              },
            },
            {
              jsonParseTask: {
                path: `$.result.${pair}.b[0]`,
              },
            },
            {
              jsonParseTask: {
                path: `$.result.${pair}.c[0]`,
              },
            },
          ],
        },
      },
    ],
  });
}

export function huobiJob(pair: string): OracleJob {
  return OracleJob.fromObject({
    tasks: [
      {
        httpTask: {
          url: `https://api.huobi.pro/market/detail/merged?symbol=${pair}`,
        },
      },
      {
        medianTask: {
          tasks: [
            {
              jsonParseTask: {
                path: "$.tick.bid[0]",
              },
            },
            {
              jsonParseTask: {
                path: "$.tick.ask[0]",
              },
            },
          ],
        },
      },
      // {
      // multiplyTask: {
      // aggregatorPubkey: "ETAaeeuQBwsh9mC2gCov9WdhJENZuffRMXY2HgjCcSL9",
      // },
      // },
    ],
  });
}

export function kucoinJob(pair: string): OracleJob {
  return OracleJob.fromObject({
    tasks: [
      {
        httpTask: {
          url: `https://api.kucoin.com/api/v1/market/orderbook/level1?symbol=${pair}`,
        },
      },
      {
        jsonParseTask: {
          path: "$.data.price",
        },
      },
      // {
      // multiplyTask: {
      // aggregatorPubkey: "ETAaeeuQBwsh9mC2gCov9WdhJENZuffRMXY2HgjCcSL9",
      // },
      // },
    ],
  });
}

export function mexcJob(pair: string): OracleJob {
  return OracleJob.fromObject({
    tasks: [
      {
        httpTask: {
          url: `https://www.mexc.com/open/api/v2/market/ticker?symbol=${pair}`,
        },
      },
      {
        medianTask: {
          tasks: [
            {
              jsonParseTask: {
                path: "$.data[0].ask",
              },
            },
            {
              jsonParseTask: {
                path: "$.data[0].bid",
              },
            },
            {
              jsonParseTask: {
                path: "$.data[0].last",
              },
            },
          ],
        },
      },
      // {
      // multiplyTask: {
      // aggregatorPubkey: "ETAaeeuQBwsh9mC2gCov9WdhJENZuffRMXY2HgjCcSL9",
      // },
      // },
    ],
  });
}

export function gateIoJob(pair: string): OracleJob {
  return OracleJob.fromObject({
    tasks: [
      {
        httpTask: {
          url: `https://api.gateio.ws/api/v4/spot/tickers?currency_pair=${pair}`,
        },
      },
      {
        medianTask: {
          tasks: [
            {
              jsonParseTask: {
                path: "$[0].lowest_ask",
              },
            },
            {
              jsonParseTask: {
                path: "$[0].highest_bid",
              },
            },
            {
              jsonParseTask: {
                path: "[0]$.last",
              },
            },
          ],
        },
      },
      // {
      // multiplyTask: {
      // aggregatorPubkey: "ETAaeeuQBwsh9mC2gCov9WdhJENZuffRMXY2HgjCcSL9",
      // },
      // },
    ],
  });
}

export function buildOkexTask(
  pair: String,
  maxDataAgeSeconds: number = 15
): OracleJob {
  const tasks: Array<any> = [
    {
      websocketTask: {
        url: "wss://ws.okex.com:8443/ws/v5/public",
        subscription: JSON.stringify({
          op: "subscribe",
          args: [{ channel: "tickers", instId: pair }],
        }),
        maxDataAgeSeconds: maxDataAgeSeconds,
        filter:
          "$[?(" +
          `@.event != 'subscribe' && ` +
          `@.arg.channel == 'tickers' && ` +
          `@.arg.instId == '${pair}' && ` +
          `@.data[0].instType == 'SPOT' && ` +
          `@.data[0].instId == '${pair}')]`,
      },
    },
    {
      medianTask: {
        tasks: [
          {
            jsonParseTask: {
              path: "$.data[0].bidPx",
            },
          },
          {
            jsonParseTask: {
              path: "$.data[0].askPx",
            },
          },
          {
            jsonParseTask: {
              path: "$.data[0].last",
            },
          },
        ],
      },
    },
  ];
  if (pair.toLowerCase().endsWith("usdt")) {
    tasks.push({
      multiplyTask: {
        aggregatorPubkey: "ETAaeeuQBwsh9mC2gCov9WdhJENZuffRMXY2HgjCcSL9",
      },
    });
  }
  return OracleJob.fromObject({ tasks });
}

// console.log(jobHash(bitfinexJob("BTCUSD")));
// console.log(jobHash(bitfinexJob("BTCUSD")));