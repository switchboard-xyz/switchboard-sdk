import { PullFeed } from '../src/accounts/pullFeed.ts';
import { Queue } from '../src/accounts/queue.ts';

import type { Program } from '@coral-xyz/anchor-31';
import { BN, web3 } from '@coral-xyz/anchor-31';
import { LegacyCrossbarClient } from '@switchboard-xyz/common-legacy';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';

const ON_DEMAND_PROGRAM_ID_HEX =
  '0673bd46f2e47e04f12bd92fb731968ecd9d9757c274da87476f465c040c6573';
const PLACEHOLDER_DISCRIMINATOR = Buffer.from('0102030405060708', 'hex');
const ON_DEMAND_PROGRAM_ID = new web3.PublicKey(
  Buffer.from(ON_DEMAND_PROGRAM_ID_HEX, 'hex')
);
const PAYER = web3.Keypair.generate().publicKey;
const QUEUE = web3.Keypair.generate().publicKey;
const FEED = web3.Keypair.generate().publicKey;
const ORACLE = web3.Keypair.generate().publicKey;
const FEED_HASH = Buffer.from(new Uint8Array(32).fill(7));

function anchorDiscriminator(name: string): Buffer {
  return createHash('sha256')
    .update(`global:${name}`)
    .digest()
    .subarray(0, 8);
}

function instructionWithDiscriminator(
  name: string,
  keys: web3.AccountMeta[] = []
): web3.TransactionInstruction {
  return new web3.TransactionInstruction({
    programId: ON_DEMAND_PROGRAM_ID,
    keys,
    data: Buffer.concat([anchorDiscriminator(name), Buffer.alloc(8)]),
  });
}

function assertDiscriminator(
  instruction: web3.TransactionInstruction,
  expectedName: string
): void {
  const discriminator = instruction.data.subarray(0, 8);
  assert.deepEqual(discriminator, anchorDiscriminator(expectedName));
  assert.notDeepEqual(discriminator, PLACEHOLDER_DISCRIMINATOR);
}

function createMockProgram(): {
  program: Program;
  calls: { legacy: number; consensus: number; svm: number };
} {
  const calls = { legacy: 0, consensus: 0, svm: 0 };
  const program = {
    programId: ON_DEMAND_PROGRAM_ID,
    provider: { publicKey: PAYER },
    instruction: {
      pullFeedSubmitResponseConsensus: (
        _data: unknown,
        context: { remainingAccounts?: web3.AccountMeta[] }
      ) => {
        calls.consensus += 1;
        return instructionWithDiscriminator(
          'pull_feed_submit_response_consensus',
          context.remainingAccounts
        );
      },
      pullFeedSubmitResponse: (
        _data: unknown,
        context: { remainingAccounts?: web3.AccountMeta[] }
      ) => {
        calls.legacy += 1;
        return instructionWithDiscriminator(
          'pull_feed_submit_response',
          context.remainingAccounts
        );
      },
      pullFeedSubmitResponseSvm: () => {
        calls.svm += 1;
        throw new Error('SVM submit path should not be used for Solana feeds');
      },
    },
  } as unknown as Program;

  return { program, calls };
}

function gatewayOracleResponse() {
  return {
    eth_address: Buffer.alloc(20, 8).toString('hex'),
    signature: Buffer.alloc(64, 9).toString('base64'),
    checksum: Buffer.alloc(32, 10).toString('base64'),
    recovery_id: 1,
    oracle_pubkey: ORACLE.toBuffer().toString('hex'),
    errors: [],
    feed_responses: [],
  };
}

function feedEvalResponse() {
  return {
    queue_pubkey: QUEUE.toBuffer().toString('hex'),
    oracle_pubkey: ORACLE.toBuffer().toString('hex'),
    success_value: '42000000000000000000',
    signature: Buffer.alloc(64, 9).toString('base64'),
    recovery_id: 1,
  };
}

async function testFetchUpdateManyUsesConsensusDiscriminator(): Promise<void> {
  const { program, calls } = createMockProgram();
  const feed = new PullFeed(program, FEED);
  feed.data = {
    queue: QUEUE,
    maxVariance: new BN(0),
    minResponses: 1,
    feedHash: FEED_HASH,
  } as any;

  const originalLegacyFetch = LegacyCrossbarClient.prototype.fetch;
  const originalFetchSignaturesConsensus = Queue.fetchSignaturesConsensus;

  try {
    LegacyCrossbarClient.prototype.fetch = async () => ({ jobs: [] });
    Queue.fetchSignaturesConsensus = (async () => ({
      oracle_responses: [gatewayOracleResponse()],
      median_responses: [
        {
          feed_hash: FEED_HASH.toString('hex'),
          value: '42000000000000000000',
        },
      ],
      slot: 150,
    })) as typeof Queue.fetchSignaturesConsensus;

    const [instructions] = await PullFeed.fetchUpdateManyIx(program, {
      feeds: [feed],
      numSignatures: 1,
      payer: PAYER,
      crossbarClient: { crossbarUrl: 'http://localhost' } as any,
    });

    const submitIx = instructions.find(instruction =>
      instruction.programId.equals(ON_DEMAND_PROGRAM_ID)
    );
    assert.ok(submitIx);
    assert.equal(calls.consensus, 1);
    assert.equal(calls.legacy, 0);
    assertDiscriminator(submitIx, 'pull_feed_submit_response_consensus');
  } finally {
    LegacyCrossbarClient.prototype.fetch = originalLegacyFetch;
    Queue.fetchSignaturesConsensus = originalFetchSignaturesConsensus;
  }
}

function testLowerLevelSubmitHelperUsesLegacyDiscriminator(): void {
  const { program, calls } = createMockProgram();
  const feed = new PullFeed(program, FEED);
  const instruction = feed.getSolanaSubmitSignaturesIx({
    resps: [feedEvalResponse() as any],
    offsets: [0],
    slot: new BN(150),
    payer: PAYER,
  });

  assert.equal(calls.legacy, 1);
  assert.equal(calls.consensus, 0);
  assertDiscriminator(instruction, 'pull_feed_submit_response');
}

async function run(): Promise<void> {
  await testFetchUpdateManyUsesConsensusDiscriminator();
  testLowerLevelSubmitHelperUsesLegacyDiscriminator();
}

run()
  .then(() => console.log('pullFeed discriminator smoke test passed'))
  .catch(error => {
    console.error(error);
    process.exitCode = 1;
  });
