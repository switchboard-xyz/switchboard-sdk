import { OracleFeed, OracleFeedUtils } from '../src/index.js';

import { expect } from 'chai';

const json = {
  name: 'btcv2',
  jobs: [
    {
      tasks: [
        {
          httpTask: {
            url: 'https://www.binance.com/api/v3/ticker/price?symbol=BTCUSDT',
            method: 'METHOD_GET',
          },
        },
        {
          jsonParseTask: {
            path: '$.price',
            aggregationMethod: 'NONE',
          },
        },
      ],
    },
  ],
  minOracleSamples: 1,
  minJobResponses: 1,
  maxJobRangePct: '3',
};
const oracleFeed = OracleFeed.fromObject(json);

describe('OracleFeed Tests', () => {
  describe('JSON', () => {
    it('OracleJob.toJSON', () => {
      expect(oracleFeed.toJSON()).deep.equal(json);
    });
  });

  it('OracleFeedUtils.normalizeOracleFeed', () => {
    const normalized = OracleFeedUtils.normalizeOracleFeed(json);
    expect(normalized.toJSON()).deep.equal(oracleFeed.toJSON());
  });
});
