import {
  SOL_NATIVE_MINT,
  SPL_ASSOCIATED_TOKEN_ACCOUNT_PROGRAM_ID,
  SPL_TOKEN_PROGRAM_ID,
} from '../constants.js';
import { getAssociatedTokenAddress, getNodePayer } from '../utils/index.js';
import { getLutSigner } from '../utils/lookupTable.js';

import { Queue } from './queue.js';
import { State } from './state.js';

import type { BN, Program } from '@coral-xyz/anchor-30';
import { web3 } from '@coral-xyz/anchor-30';
import { Buffer } from 'buffer';

/**
 *  A map of LUTs to their public keys.
 *
 *  Users can initialize to compact all oracle and feed keys they use into a single
 *  account, and then use the LUT to load all tx keys efficiently.
 */
export class LutMap {
  /**
   *  The public key of the LUT map account.
   */
  static async keyFromSeed(
    program: Program,
    queue: web3.PublicKey,
    authority: web3.PublicKey
  ): Promise<web3.PublicKey> {
    const [lut] = web3.PublicKey.findProgramAddressSync(
      [
        Buffer.from('LutMapAccountData'),
        queue.toBuffer(),
        authority.toBuffer(),
      ],
      program.programId
    );
    return lut;
  }

  /**
   * Creating a LUT map account will allow a user or protocol to easy manage
   * and associate a common account grouping for their feeds to reduce the
   * total number of transaction bytes taken by Switchboard.
   * This will maximize the flexibility users have in their instructions.
   *
   * @param program - The program that owns the LUT map account.
   * @param queue - The queue account that the LUT map is associated with.
   * @param slot - The slot that the LUT map is associated with.
   * @returns A promise that resolves to the LUT map and the transaction signature.
   */
  static async create(
    program: Program,
    queue: web3.PublicKey,
    slot: BN
  ): Promise<[LutMap, string]> {
    const payer = getNodePayer(program);
    const lutKey = await LutMap.keyFromSeed(program, queue, payer.publicKey);
    const sig = await program.rpc.lutMapInit(
      { slot },
      {
        accounts: {
          lutMap: lutKey,
          queue: queue,
          payer: payer.publicKey,
          authority: payer.publicKey,
          systemProgram: web3.SystemProgram.programId,
        },
        signers: [payer],
      }
    );
    return [new LutMap(program, lutKey), sig];
  }

  constructor(
    readonly program: Program,
    readonly pubkey: web3.PublicKey
  ) {}

  async queueLutExtendIx(params: {
    queue: web3.PublicKey;
    newKey: web3.PublicKey;
    payer: web3.PublicKey;
  }): Promise<web3.TransactionInstruction> {
    const payer = getNodePayer(this.program);
    const queueAccount = new Queue(this.program, params.queue);
    const queueData = await queueAccount.loadData();
    const lutKey = await LutMap.keyFromSeed(
      this.program,
      params.queue,
      payer.publicKey
    );
    const lutSigner = getLutSigner(this.program.programId, params.queue);
    const ix = await this.program.instruction.queueLutExtend(
      { newKey: params.newKey },
      {
        accounts: {
          queue: params.queue,
          authority: queueData.authority,
          lutSigner,
          lut: lutKey,
          addressLookupTableProgram: web3.AddressLookupTableProgram.programId,
          payer: payer.publicKey,
          systemProgram: web3.SystemProgram.programId,
        },
      }
    );
    return ix;
  }

  /**
   *  Loads the data for this {@linkcode LutMap} account from on chain.
   *
   *  @returns A promise that resolves to the data.
   *  @throws if the account does not exist.
   */
  async loadData() {
    return await this.program.account['lutMapAccountData'].fetch(this.pubkey);
  }

  async loadLut(): Promise<[web3.PublicKey, web3.AddressLookupTableState]> {
    const data = await this.loadData();
    const lutKey = data.lut;
    const lutAccountInfo =
      await this.program.provider.connection.getAccountInfo(lutKey);
    const lutData = web3.AddressLookupTableAccount.deserialize(
      lutAccountInfo!.data
    );
    return [lutKey, lutData];
  }

  async syncLut(feeds: web3.PublicKey[]): Promise<void> {
    const wrapperData = await this.loadData();
    const queueKey = wrapperData.queue;
    const queue = new Queue(this.program, queueKey);
    const queueData = await queue.loadData();
    const oracles = queueData.oracleKeys.slice(0, queueData.oracleKeysLen);
    const neededLutAccounts: web3.PublicKey[] = [];
    neededLutAccounts.push(queueKey);
    neededLutAccounts.push(SOL_NATIVE_MINT);
    neededLutAccounts.push(SPL_TOKEN_PROGRAM_ID);
    neededLutAccounts.push(SPL_ASSOCIATED_TOKEN_ACCOUNT_PROGRAM_ID);
    neededLutAccounts.push(State.keyFromSeed(this.program));
    for (const oracle of oracles) {
      for (const feed of feeds) {
        const [statsKey] = web3.PublicKey.findProgramAddressSync(
          [Buffer.from('OracleFeedStats'), feed.toBuffer(), oracle.toBuffer()],
          this.program.programId
        );
        const feedRewardEscrow = await getAssociatedTokenAddress(
          SOL_NATIVE_MINT,
          feed
        );
        neededLutAccounts.push(statsKey);
        neededLutAccounts.push(feed);
        neededLutAccounts.push(oracle);
        neededLutAccounts.push(feedRewardEscrow);
      }
    }
    // TODO: do anneal here
  }
}
