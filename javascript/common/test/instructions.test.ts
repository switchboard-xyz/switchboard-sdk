import { IxFromHex } from '../src/utils/instructions.js';

import { expect } from 'chai';

const LIVE_PULL_IX_HEX =
  '0673bd46f2e47e04f12bd92fb731968ecd9d9757c274da87476f465c040c6573050000000000000089e0fecf1c1b3a11e77b9d1048192288ae1d8ada0e19ff95d737c4e5afb583f70001d284bd424eb258f1f502c95ff334245b64af7df6b14435d1ea46d10fb3ac68b6000086807068432f186a147cf0b13a30067d386204ea9d6c8b04743ac2ef010b075200007752c55e8b0a7079ad51975736764e45f56d61ebd15aa96d55f7ca86d5b5e387010100000000000000000000000000000000000000000000000000000000000000000000310000000000000001020304050607081d3c620d5670b1b26d0d3c7c27cb75a5187d99b8ccf8850d5b79d573b81bff7c030000009600000000';

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
  programId:
    '0673bd46f2e47e04f12bd92fb731968ecd9d9757c274da87476f465c040c6573',
  data: '9616d7a68f5d3089728139110000000001000000761474bfb3cb6c060000000000000000ac95d3645aa39944c6c61ab056d05c27ae359ae45c25108946c7b6fe6e555e0d7ef033918d2ae6887af8bf1d44bd815f5d1a4772e46ffec2d01d01af896476ce0100',
};

describe('Instruction decoding', () => {
  test('Decodes current serialized pullIxns entries.', () => {
    const ix = IxFromHex(LIVE_PULL_IX_HEX);

    expect(ix.programId.toBuffer().toString('hex')).to.equal(
      '0673bd46f2e47e04f12bd92fb731968ecd9d9757c274da87476f465c040c6573'
    );
    expect(ix.keys).to.have.lengthOf(5);
    expect(ix.keys[0].pubkey.toBuffer().toString('hex')).to.equal(
      '89e0fecf1c1b3a11e77b9d1048192288ae1d8ada0e19ff95d737c4e5afb583f7'
    );
    expect(ix.keys[0].isSigner).to.equal(false);
    expect(ix.keys[0].isWritable).to.equal(true);
    expect(ix.keys[3].isSigner).to.equal(true);
    expect(ix.data.length).to.be.greaterThan(0);
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
    expect(() => IxFromHex(LIVE_PULL_IX_HEX.slice(0, -2))).to.throw(
      'Truncated serialized instruction'
    );
  });
});
