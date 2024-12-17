import { OracleJob } from "@switchboard-xyz/aptos.js";

// Binance
export const cakeBinance = Buffer.from(
  OracleJob.encodeDelimited(
    OracleJob.create({
      tasks: [
        {
          httpTask: {
            url: "https://www.binance.com/api/v3/ticker/price?symbol=CAKEUSDT",
          },
        },
        { jsonParseTask: { path: "$.price" } },
        {
          multiplyTask: {
            aggregatorPubkey: "ETAaeeuQBwsh9mC2gCov9WdhJENZuffRMXY2HgjCcSL9",
          },
        },
      ],
    })
  ).finish()
);

// Gate
export const cakeGate = Buffer.from(
  OracleJob.encodeDelimited(
    OracleJob.create({
      tasks: [
        {
          httpTask: {
            url: "https://api.gateio.ws/api/v4/spot/tickers?currency_pair=CAKE_USDT",
          },
        },
        {
          medianTask: {
            tasks: [
              { jsonParseTask: { path: "$[0].lowest_ask" } },
              { jsonParseTask: { path: "$[0].highest_bid" } },
              { jsonParseTask: { path: "$[0].last" } },
            ],
          },
        },
        {
          multiplyTask: {
            aggregatorPubkey: "ETAaeeuQBwsh9mC2gCov9WdhJENZuffRMXY2HgjCcSL9",
          },
        },
      ],
    })
  ).finish()
);

// Huobi
export const cakeHuobi = Buffer.from(
  OracleJob.encodeDelimited(
    OracleJob.create({
      tasks: [
        {
          httpTask: {
            url: "https://api.huobi.pro/market/detail/merged?symbol=cakeusdt",
          },
        },
        {
          medianTask: {
            tasks: [
              { jsonParseTask: { path: "$.tick.bid[0]" } },
              { jsonParseTask: { path: "$.tick.ask[0]" } },
            ],
          },
        },
        {
          multiplyTask: {
            aggregatorPubkey: "ETAaeeuQBwsh9mC2gCov9WdhJENZuffRMXY2HgjCcSL9",
          },
        },
      ],
    })
  ).finish()
);

// KuCoin
export const cakeKuCoin = Buffer.from(
  OracleJob.encodeDelimited(
    OracleJob.create({
      tasks: [
        {
          httpTask: {
            url: "https://api.kucoin.com/api/v1/market/orderbook/level1?symbol=CAKE-USDT",
          },
        },
        { jsonParseTask: { path: "$.data.price" } },
        {
          multiplyTask: {
            aggregatorPubkey: "ETAaeeuQBwsh9mC2gCov9WdhJENZuffRMXY2HgjCcSL9",
          },
        },
      ],
    })
  ).finish()
);

// Mexc
export const cakeMexc = Buffer.from(
  OracleJob.encodeDelimited(
    OracleJob.create({
      tasks: [
        {
          httpTask: {
            url: "https://www.mexc.com/open/api/v2/market/ticker?symbol=CAKE_USDT",
          },
        },
        {
          medianTask: {
            tasks: [
              { jsonParseTask: { path: "$.data[0].ask" } },
              { jsonParseTask: { path: "$.data[0].bid" } },
              { jsonParseTask: { path: "$.data[0].last" } },
            ],
          },
        },
        {
          multiplyTask: {
            aggregatorPubkey: "ETAaeeuQBwsh9mC2gCov9WdhJENZuffRMXY2HgjCcSL9",
          },
        },
      ],
    })
  ).finish()
);
