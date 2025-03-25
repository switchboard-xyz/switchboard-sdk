import { getNodePayer } from '../utils/index.js';

import { Queue } from './queue.js';

import type { BN, Program } from '@coral-xyz/anchor-30';
import { web3 } from '@coral-xyz/anchor-30';
import { Buffer } from 'buffer';

export interface StateData {
  bump: number;
  testOnlyDisableMrEnclaveCheck: boolean;
  enableStaking: boolean;
  // padding1
  authority: web3.PublicKey;
  guardianQueue: web3.PublicKey;
  // reserved1
  epochLength: BN;
  // reserved2
  switchMint: web3.PublicKey;
  sgxAdvisories: Uint16Array;
  advisoriesLen: number;
  // padding2
  flatRewardCutPercentage: number;
  enableSlashing: boolean;
  // padding3
  lutSlot: BN;
  baseReward: number;
  // padding4
  subsidyAmount: BN;
  // ebuf6
  // ebuf5
  // ebuf4
  // ebuf3
  // ebuf2
  costWhitelist: web3.PublicKey[];
}

/**
 *  Abstraction around the Switchboard-On-Demand State account
 *
 *  This account is used to store the state data for a given program.
 */
export class State {
  public pubkey: web3.PublicKey;

  /**
   * Derives a state PDA (Program Derived Address) from the program.
   *
   * @param {Program} program - The Anchor program instance.
   * @returns {web3.PublicKey} The derived state account's public key.
   */
  static keyFromSeed(program: Program): web3.PublicKey {
    const [state] = web3.PublicKey.findProgramAddressSync(
      [Buffer.from('STATE')],
      program.programId
    );
    return state;
  }

  /**
   * Initializes the state account.
   *
   * @param {Program} program - The Anchor program instance.
   * @returns {Promise<[State, string]>} A promise that resolves to the state account and the transaction signature.
   */
  static async create(program: Program): Promise<[State, string]> {
    const payer = getNodePayer(program);
    const sig = await program.rpc.stateInit(
      {},
      {
        accounts: {
          state: State.keyFromSeed(program),
          payer: payer.publicKey,
          systemProgram: web3.SystemProgram.programId,
        },
        signers: [payer],
      }
    );

    return [new State(program), sig];
  }

  /**
   * Constructs a `State` instance.
   *
   * @param {Program} program - The Anchor program instance.
   */
  constructor(readonly program: Program) {
    const pubkey = State.keyFromSeed(program);
    this.pubkey = pubkey;
  }

  /**
   * Set program-wide configurations.
   *
   * @param {object} params - The configuration parameters.
   * @param {web3.PublicKey} [params.guardianQueue] - The guardian queue account.
   * @param {web3.PublicKey} [params.newAuthority] - The new authority account.
   * @param {BN} [params.minQuoteVerifyVotes] - The minimum number of votes required to verify a quote.
   * @param {number} [params.permitAdvisory] - The permit advisory value.
   * @param {number} [params.denyAdvisory] - The deny advisory value.
   * @param {boolean} [params.testOnlyDisableMrEnclaveCheck] - A flag to disable MrEnclave check for testing purposes.
   * @param {web3.PublicKey} [params.switchMint] - The switch mint account.
   * @returns {Promise<web3.TransactionInstruction>} A promise that resolves to the transaction instruction.
   */
  async setConfigsIx(params: {
    guardianQueue?: web3.PublicKey;
    newAuthority?: web3.PublicKey;
    minQuoteVerifyVotes?: BN;
    permitAdvisory?: number;
    denyAdvisory?: number;
    testOnlyDisableMrEnclaveCheck?: boolean;
    subsidyAmount?: BN;
    switchMint?: web3.PublicKey;
    addCostWl?: web3.PublicKey;
    rmCostWl?: web3.PublicKey;
  }): Promise<web3.TransactionInstruction> {
    const state = await this.loadData();
    const queue = params.guardianQueue ?? state.guardianQueue;
    const payer = getNodePayer(this.program);
    const testOnlyDisableMrEnclaveCheck =
      params.testOnlyDisableMrEnclaveCheck ??
      state.testOnlyDisableMrEnclaveCheck;
    const ix = await this.program.instruction.stateSetConfigs(
      {
        newAuthority: params.newAuthority ?? state.authority,
        testOnlyDisableMrEnclaveCheck: testOnlyDisableMrEnclaveCheck ? 1 : 0,
        addAdvisory: params.permitAdvisory,
        rmAdvisory: params.denyAdvisory,
        lutSlot: state.lutSlot,
        subsidyAmount: params.subsidyAmount ?? state.subsidyAmount,
        switchMint: params.switchMint ?? state.switchMint,
        authority: params.newAuthority ?? state.authority,
        addCostWl: params.addCostWl ?? web3.PublicKey.default,
        rmCostWl: params.rmCostWl ?? web3.PublicKey.default,
      },
      {
        accounts: {
          state: this.pubkey,
          authority: state.authority,
          queue,
          payer: payer.publicKey,
          systemProgram: web3.SystemProgram.programId,
        },
      }
    );
    return ix;
  }

  /**
   * Register a guardian with the global guardian queue.
   *
   * @param {object} params - The parameters object.
   * @param {PublicKey} params.guardian - The guardian account.
   * @returns {Promise<TransactionInstruction>} A promise that resolves to the transaction instruction.
   */
  async registerGuardianIx(params: {
    guardian: web3.PublicKey;
  }): Promise<web3.TransactionInstruction> {
    const state = await this.loadData();
    const payer = getNodePayer(this.program);
    const ix = await this.program.instruction.guardianRegister(
      {},
      {
        accounts: {
          oracle: params.guardian,
          state: this.pubkey,
          guardianQueue: state.guardianQueue,
          authority: state.authority,
        },
        signers: [payer],
      }
    );
    return ix;
  }

  /**
   * Unregister a guardian from the global guardian queue.
   *
   * @param {object} params - The parameters object.
   * @param {web3.PublicKey} params.guardian - The guardian account.
   * @returns {Promise<web3.TransactionInstruction>} A promise that resolves to the transaction instruction.
   */
  async unregisterGuardianIx(params: {
    guardian: web3.PublicKey;
  }): Promise<web3.TransactionInstruction> {
    const state = await this.loadData();
    const guardianQueue = new Queue(this.program, state.guardianQueue);
    const queueData = await guardianQueue.loadData();
    const idx = queueData.oracleKeys.findIndex(key =>
      key.equals(params.guardian)
    );
    const payer = getNodePayer(this.program);
    const ix = await this.program.instruction.guardianUnregister(
      { idx },
      {
        accounts: {
          oracle: params.guardian,
          state: this.pubkey,
          guardianQueue: state.guardianQueue,
          authority: state.authority,
        },
        signers: [payer],
      }
    );
    return ix;
  }

  /**
   *  Loads the state data from on chain.
   *
   *  @returns A promise that resolves to the state data.
   *  @throws if the state account does not exist.
   */
  async loadData(): Promise<StateData> {
    return await this.program.account['state'].fetch(this.pubkey);
  }

  /**
   *  Loads the state data from on chain.
   *
   *  @returns A promise that resolves to the state data.
   *  @throws if the state account does not exist.
   */
  static async loadData(program: Program) {
    return await new State(program).loadData();
  }
}
