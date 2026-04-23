import { Randomness } from '../src/accounts/randomness.ts';
import { getLutKey, getLutSigner } from '../src/utils/lookupTable.ts';

import { BN, web3 } from '@coral-xyz/anchor-31';
import assert from 'node:assert/strict';

function run(): void {
  const programId = new web3.PublicKey(new Uint8Array(32).fill(1));
  const randomness = new web3.PublicKey(new Uint8Array(32).fill(2));
  const recipient = new web3.PublicKey(new Uint8Array(32).fill(3));
  const lutSlot = new BN(12345);
  const lutSigner = getLutSigner(programId, randomness);
  const lut = getLutKey(lutSigner, lutSlot);
  let calls = 0;

  const program = {
    programId,
    instruction: {
      randomnessCloseLut(
        params: { lutSlot: BN },
        context: {
          accounts: {
            randomness: web3.PublicKey;
            lut: web3.PublicKey;
            lutSigner: web3.PublicKey;
            recipient: web3.PublicKey;
            addressLookupTableProgram: web3.PublicKey;
          };
        }
      ) {
        calls += 1;
        assert.equal(params.lutSlot.toString(), lutSlot.toString());
        assert.equal(context.accounts.randomness.toBase58(), randomness.toBase58());
        assert.equal(context.accounts.lut.toBase58(), lut.toBase58());
        assert.equal(context.accounts.lutSigner.toBase58(), lutSigner.toBase58());
        assert.equal(context.accounts.recipient.toBase58(), recipient.toBase58());
        assert.equal(
          context.accounts.addressLookupTableProgram.toBase58(),
          web3.AddressLookupTableProgram.programId.toBase58()
        );
        return new web3.TransactionInstruction({
          programId,
          keys: [],
          data: Buffer.alloc(0),
        });
      },
    },
  };

  Randomness.closeLutIx(program as any, {
    randomness,
    lutSlot,
    recipient,
  });

  const instance = new Randomness(program as any, randomness);
  instance.loadData = async () => {
    throw new Error('closeLutIx must not load randomness data');
  };
  instance.closeLutIx({ lutSlot, recipient });

  assert.equal(calls, 2);
}

run();
console.log('randomness close-LUT instruction smoke test passed');
