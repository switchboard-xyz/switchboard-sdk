import { IxFromHex } from '../src/utils/instructions.js';

import { expect } from 'chai';
import { Buffer } from 'buffer';

const ON_DEMAND_PROGRAM_ID_HEX =
  '0673bd46f2e47e04f12bd92fb731968ecd9d9757c274da87476f465c040c6573';
const CONSENSUS_DISCRIMINATOR = 'ef7c27b893de10f8';
const PLACEHOLDER_DISCRIMINATOR = '0102030405060708';

// Production Crossbar returned this malformed instruction before the fix.
// It decodes as a Solana instruction, but the inner Anchor discriminator is a placeholder.
const BAD_PRODUCTION_PULL_IX_HEX =
  '0673bd46f2e47e04f12bd92fb731968ecd9d9757c274da87476f465c040c6573050000000000000089e0fecf1c1b3a11e77b9d1048192288ae1d8ada0e19ff95d737c4e5afb583f70001d284bd424eb258f1f502c95ff334245b64af7df6b14435d1ea46d10fb3ac68b6000086807068432f186a147cf0b13a30067d386204ea9d6c8b04743ac2ef010b075200007752c55e8b0a7079ad51975736764e45f56d61ebd15aa96d55f7ca86d5b5e387010100000000000000000000000000000000000000000000000000000000000000000000310000000000000001020304050607081d3c620d5670b1b26d0d3c7c27cb75a5187d99b8ccf8850d5b79d573b81bff7c030000009600000000';

const VALID_CONSENSUS_PULL_IX_HEX = serializedInstructionHex({
  programId: ON_DEMAND_PROGRAM_ID_HEX,
  keys: [
    {
      pubkey:
        '89e0fecf1c1b3a11e77b9d1048192288ae1d8ada0e19ff95d737c4e5afb583f7',
      isSigner: false,
      isWritable: true,
    },
  ],
  data:
    CONSENSUS_DISCRIMINATOR +
    '9600000000000000' +
    '01000000' +
    '2a000000000000000000000000000000',
});

const LEGACY_PULL_IX = {
  keys: [
    {
      pubkey:
        'acf6ac33e6e7ac61fc2753cb2b47461aa25aecf4309b48fd5aec01c9d898d391',
      isSigner: false,
      isWritable: true,
    },
    {
      pubkey:
        '86807068432f186a147cf0b13a30067d386204ea9d6c8b04743ac2ef010b0752',
      isSigner: false,
      isWritable: false,
    },
  ],
  programId: ON_DEMAND_PROGRAM_ID_HEX,
  data: '9616d7a68f5d3089728139110000000001000000761474bfb3cb6c060000000000000000ac95d3645aa39944c6c61ab056d05c27ae359ae45c25108946c7b6fe6e555e0d7ef033918d2ae6887af8bf1d44bd815f5d1a4772e46ffec2d01d01af896476ce0100',
};

function u64Le(value: number): Buffer {
  const buffer = Buffer.alloc(8);
  buffer.writeBigUInt64LE(BigInt(value));
  return buffer;
}

function serializedInstructionHex(params: {
  programId: string;
  keys: Array<{ pubkey: string; isSigner: boolean; isWritable: boolean }>;
  data: string;
}) {
  const accountBytes = params.keys.flatMap(key => [
    Buffer.from(key.pubkey, 'hex'),
    Buffer.from([key.isSigner ? 1 : 0]),
    Buffer.from([key.isWritable ? 1 : 0]),
  ]);
  const data = Buffer.from(params.data, 'hex');
  return Buffer.concat([
    Buffer.from(params.programId, 'hex'),
    u64Le(params.keys.length),
    ...accountBytes,
    u64Le(data.length),
    data,
  ]).toString('hex');
}

describe('Instruction decoding', () => {
  test('Decodes valid serialized consensus pullIxns entries.', () => {
    const ix = IxFromHex(VALID_CONSENSUS_PULL_IX_HEX);

    expect(ix.programId.toBuffer().toString('hex')).to.equal(
      ON_DEMAND_PROGRAM_ID_HEX
    );
    expect(ix.keys).to.have.lengthOf(1);
    expect(ix.keys[0].pubkey.toBuffer().toString('hex')).to.equal(
      '89e0fecf1c1b3a11e77b9d1048192288ae1d8ada0e19ff95d737c4e5afb583f7'
    );
    expect(ix.keys[0].isSigner).to.equal(false);
    expect(ix.keys[0].isWritable).to.equal(true);
    expect(ix.data.subarray(0, 8).toString('hex')).to.equal(
      CONSENSUS_DISCRIMINATOR
    );
    expect(ix.data.subarray(0, 8).toString('hex')).not.to.equal(
      VALID_CONSENSUS_PULL_IX_HEX.slice(0, 16)
    );
  });

  test('Documents the bad production placeholder discriminator fixture.', () => {
    const ix = IxFromHex(BAD_PRODUCTION_PULL_IX_HEX);

    expect(ix.programId.toBuffer().toString('hex')).to.equal(
      ON_DEMAND_PROGRAM_ID_HEX
    );
    expect(ix.data.subarray(0, 8).toString('hex')).to.equal(
      PLACEHOLDER_DISCRIMINATOR
    );
    expect(ix.data.subarray(0, 8).toString('hex')).not.to.equal(
      CONSENSUS_DISCRIMINATOR
    );
  });

  test('Decodes legacy object-form pullIxns entries.', () => {
    const ix = IxFromHex(LEGACY_PULL_IX);

    expect(ix.programId.toBuffer().toString('hex')).to.equal(
      LEGACY_PULL_IX.programId
    );
    expect(ix.keys).to.have.lengthOf(2);
    expect(ix.keys[1].pubkey.toBuffer().toString('hex')).to.equal(
      LEGACY_PULL_IX.keys[1].pubkey
    );
    expect(ix.data.toString('hex')).to.equal(LEGACY_PULL_IX.data);
  });

  test('Rejects truncated serialized instructions.', () => {
    expect(() => IxFromHex(VALID_CONSENSUS_PULL_IX_HEX.slice(0, -2))).to.throw(
      'Truncated serialized instruction'
    );
  });
});
