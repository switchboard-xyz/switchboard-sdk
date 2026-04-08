import type { IOracleFeed, IOracleJob } from '../src/index.js';
import {
  CrossbarClient,
  serializeOracleJob,
} from '../src/index.js';
import { deserializeOracleFeed } from '../src/utils/oracle-feed.js';

import { PublicKey } from '@solana/web3.js';
import { expect } from 'chai';

const job: IOracleJob = {
  tasks: [{ valueTask: { value: 123 } }],
};
const oracleFeed: IOracleFeed = {
  jobs: [job],
};

describe('CrossbarClient Tests', () => {
  const client = CrossbarClient.default(/* verbose= */ true);
  let storedFeedId: string;
  let storedCid: string;

  beforeAll(async () => {
    const { cid, feedId } = await client.storeOracleFeed(oracleFeed);
    storedCid = cid;
    storedFeedId = feedId;
  }, 10_000);

  test('Storing an oracle feed.', async () => {
    expect(storedCid).to.be.a('string').and.not.empty;
    expect(storedFeedId).to.match(/^0x[0-9a-f]+$/i);
  });

  test('Fetching from a feed hash (with `0x` prefix).', async () => {
    const response = await client.fetchOracleFeed(storedFeedId);
    const decodedFeed = deserializeOracleFeed(Buffer.from(response.data, 'base64'));

    expect(response.cid).to.equal(storedCid);
    expect(response.version).to.be.a('string').and.not.empty;
    expect(decodedFeed.jobs).to.have.lengthOf(1);
    expect(serializeOracleJob(decodedFeed.jobs[0]!).toString('hex')).to.equal(
      serializeOracleJob(job).toString('hex')
    );
  });

  test('Fetching from a feed hash (without `0x` prefix).', async () => {
    const response = await client.fetchOracleFeed(storedFeedId.replace(/^0x/i, ''));
    const decodedFeed = deserializeOracleFeed(Buffer.from(response.data, 'base64'));

    expect(response.cid).to.equal(storedCid);
    expect(decodedFeed.jobs).to.have.lengthOf(1);
    expect(serializeOracleJob(decodedFeed.jobs[0]!).toString('hex')).to.equal(
      serializeOracleJob(job).toString('hex')
    );
  });

  test('Fetching Solana updates.', async () => {
    const network = 'devnet';
    const devnetProgramId = new PublicKey(
      'Aio4gaXjXzJNVLtzwtNVmSqGKpANtXhybbkhtAC94ji2'
    );

    const feeds = [
      // BTC Price Feed
      // https://ondemand.switchboard.xyz/solana/devnet/feed/FAmE211gC241L5YTAssfUT3h2dHcUygVGFbz2hHBKNWG
      'FAmE211gC241L5YTAssfUT3h2dHcUygVGFbz2hHBKNWG',
      // sui test
      // https://ondemand.switchboard.xyz/solana/devnet/feed/7XcCuTUtpw7ZvfohHvXfCp2GTG5RYmNAYMMdSdSMreC5
      '7XcCuTUtpw7ZvfohHvXfCp2GTG5RYmNAYMMdSdSMreC5',
    ];
    const numSignatures = 2;

    const updates = await client.fetchSolanaUpdates(
      network,
      feeds,
      /* payer= */ 'nXsE22JSmWYk7f4KtfjXVqCvGuaVXntdSbCKzdumzFv',
      numSignatures
    );

    expect(updates).to.be.an('array');
    expect(updates).to.have.lengthOf(feeds.length);

    updates.forEach(update => {
      expect(update).to.have.property('success').to.eq(true);
      expect(update)
        .to.have.property('pullIxns')
        .that.is.an('array')
        .with.lengthOf(2);
      expect(update.pullIxns[1])
        .to.have.property('programId')
        .that.is.instanceOf(PublicKey);
      expect(update.pullIxns[1].programId.toBase58()).to.equal(
        devnetProgramId.toBase58()
      );

      update.responses.forEach(response => {
        expect(response).to.have.property('oracle');
        expect(response).to.have.property('result');
      });

      expect(update).to.have.property('lookupTables').that.is.an('array');
    });
  }, 10_000);

  test('Simulating Solana feeds.', async () => {
    const network = 'devnet';
    const feeds = [
      'AXRydnjDeWUgR5VGFFqtzYv52u2MHqFCYcsHsnEgCD15',
      '7yQ4ae7XH4tdiXXbzdsycUjFEWAhonVXLcR1vreqGT3s',
    ];

    const simulationResults = await client.simulateSolanaFeeds(network, feeds);

    expect(simulationResults).to.be.an('array');
    expect(simulationResults).to.have.lengthOf(feeds.length);

    simulationResults.forEach(result => {
      expect(result)
        .to.have.property('feed')
        .that.is.a('string')
        .and.oneOf(feeds);
      expect(result).to.have.property('feedHash').that.is.a('string');
      expect(result)
        .to.have.property('results')
        .that.is.an('array')
        .with.length.greaterThan(0);
      const results = result.results ?? [];
      expect(results).to.have.length.greaterThan(0);
      results
        .map(value => Number(value))
        .forEach(val => expect(Number.isFinite(val)).to.be.true);
    });
  });

  test('Simulating from a feed hash (without `0x` prefix).', async () => {
    const feedhHashes = [
      'e2ba292a366ff6138ea8b66b12e49e74243816ad4edd333884acedcd0e0c2e9d',
    ];
    const results = await client.simulateFeeds(feedhHashes);
    expect(results).to.be.an('array');
    expect(results).to.have.lengthOf(feedhHashes.length);

    results.forEach(result => {
      expect(result)
        .to.have.property('feedHash')
        .that.is.a('string')
        .and.oneOf(feedhHashes);
      expect(result)
        .to.have.property('results')
        .that.is.an('array')
        .with.length.greaterThan(0);
      const results = result.results ?? [];
      expect(results).to.have.length.greaterThan(0);
      results
        .map(value => Number(value))
        .forEach(val => expect(Number.isFinite(val)).to.be.true);
    });
  });
});
