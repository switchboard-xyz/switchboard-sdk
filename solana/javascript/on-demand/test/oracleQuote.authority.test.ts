import {
  FEED_AUTHORITY_UPDATE_OPCODE,
  MAX_AUTHORITY_FEED_IDS,
  OracleQuote,
  QUOTE_PROGRAM_ID,
} from '../src/classes/oracleQuote.ts';

import * as web3 from '@solana/web3.js';
import assert from 'node:assert/strict';

function run(): void {
  const authority = new web3.PublicKey(new Uint8Array(32).fill(5));
  const payer = new web3.PublicKey(new Uint8Array(32).fill(3));
  const feeds = [
    {
      feedHash: Buffer.from(new Uint8Array(32).fill(2)),
      value: 42n,
    },
    {
      feedHash: Buffer.from(new Uint8Array(32).fill(1)),
      value: -7n,
      minOracleSamples: 9,
    },
  ];

  const payload = OracleQuote.buildAuthorityQuotePayload(feeds, 99);
  const decoded = OracleQuote.decode(payload);
  assert.equal(decoded.sourceScheme, 'authority');
  assert.equal(decoded.signatures.length, 0);
  assert.equal(decoded.oracleIdxs.length, 0);
  assert.equal(decoded.slot, 99);
  assert.equal(decoded.version, 1);
  assert.equal(decoded.feeds.length, 2);
  assert.equal(decoded.feeds[0].minOracleSamples, 1);
  assert.equal(decoded.feeds[1].minOracleSamples, 9);
  assert.deepEqual(decoded.signedSlothash, Buffer.alloc(32));

  const feedHashes = feeds.map(feed => feed.feedHash);
  const reversedFeedHashes = [...feedHashes].reverse();
  const [derivedQuote] = OracleQuote.deriveAuthorityQuotePubkey(
    authority,
    feedHashes
  );
  const [derivedQuoteReversed] = OracleQuote.deriveAuthorityQuotePubkey(
    authority,
    reversedFeedHashes
  );
  assert.notEqual(derivedQuote.toBase58(), derivedQuoteReversed.toBase58());

  const instruction = OracleQuote.buildFeedAuthorityUpdateInstruction({
    authority,
    payer,
    payload,
  });
  assert.equal(instruction.programId.toBase58(), QUOTE_PROGRAM_ID.toBase58());
  assert.equal(instruction.data[0], FEED_AUTHORITY_UPDATE_OPCODE);
  assert.equal(instruction.keys.length, 5);
  assert.equal(instruction.keys[0].pubkey.toBase58(), derivedQuote.toBase58());
  assert.equal(instruction.keys[1].isSigner, true);
  assert.equal(instruction.keys[3].isSigner, true);

  assert.throws(() =>
    OracleQuote.buildFeedAuthorityUpdateInstruction({
      authority,
      payer,
      payload,
      quoteAccount: new web3.PublicKey(new Uint8Array(32).fill(8)),
    })
  );

  const tooManyFeeds = Array.from(
    { length: MAX_AUTHORITY_FEED_IDS + 1 },
    (_, i) => ({
      feedHash: Buffer.from(new Uint8Array(32).fill(i + 1)),
      value: BigInt(i),
    })
  );
  assert.throws(() => OracleQuote.buildAuthorityQuotePayload(tooManyFeeds, 1));

  const duplicateFeed = Buffer.from(new Uint8Array(32).fill(9));
  assert.throws(() =>
    OracleQuote.buildAuthorityQuotePayload(
      [
        { feedHash: duplicateFeed, value: 1n },
        { feedHash: duplicateFeed, value: 2n },
      ],
      1
    )
  );
}

run();
console.log('oracleQuote authority-update smoke test passed');
