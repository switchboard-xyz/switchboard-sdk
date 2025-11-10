import { SOL_NATIVE_MINT, SPL_TOKEN_PROGRAM_ID } from '../constants.js';
import * as spl from '../utils/index.js';
import { getLutKey, getLutSigner } from '../utils/lookupTable.js';

import { State } from './state.js';

import type { Program } from '@coral-xyz/anchor-31';
import { BN, web3 } from '@coral-xyz/anchor-31';
import { Buffer } from 'buffer';

export interface OracleAccountData {
  enclave: {
    enclaveSigner: web3.PublicKey;
    mrEnclave: Uint8Array;
    verificationStatus: number;
    // padding1
    verificationTimestamp: BN;
    validUntil: BN;
    quoteRegistry: Uint8Array;
    registryKey: Uint8Array;
    secp256K1Signer: Uint8Array;
    lastEd25519Signer: web3.PublicKey;
    lastSecp256K1Signer: Uint8Array;
    lastRotateSlot: BN;
    guardianApprovers: web3.PublicKey[];
    guardianApproversLen: number;
    // padding2
    stagingEd25519Signer: web3.PublicKey;
    stagingSecp256K1Signer: Uint8Array;
    ethSigner: Uint8Array;
    // ebuf4
    // ebuf3
    // ebuf2
    // ebuf1
  };
  authority: web3.PublicKey;
  queue: web3.PublicKey;
  createdAt: BN;
  lastHeartbeat: BN;
  secpAuthority: Uint8Array;
  gatewayUri: Uint8Array;
  permissions: BN;
  isOnQueue: boolean;
  // padding1
  lutSlot: BN;
  lastRewardEpoch: BN;
  operator: web3.PublicKey;
  // ebuf3
  // ebuf2
  // ebuf1
}

/**
 *  This class represents an oracle account on chain.
 */
export class Oracle {
  lut: web3.AddressLookupTableAccount | null;

  constructor(
    readonly program: Program,
    readonly pubkey: web3.PublicKey
  ) {
    this.lut = null;
  }

  /**
   * Creates a new oracle account. linked to the specified queue.
   * After creation the oracle still must receive run approval and verify their
   * enclave measurement.
   * @param program - The program that owns the oracle account.
   * @param params.queue - The queue that the oracle will be linked to.
   * @returns A promise that resolves to a tuple containing the oracle account
   * and the transaction signature.
   *
   */
  static async create(
    program: Program,
    params: {
      queue: web3.PublicKey;
    }
  ): Promise<[Oracle, web3.TransactionInstruction[], web3.Keypair]> {
    const stateKey = State.keyFromSeed(program);
    const state = await State.loadData(program);
    const payer = spl.getNodePayer(program);
    const oracle = web3.Keypair.generate();
    const oracleStats = web3.PublicKey.findProgramAddressSync(
      [Buffer.from('OracleStats'), oracle.publicKey.toBuffer()],
      program.programId
    )[0];
    const lutSigner = getLutSigner(program.programId, oracle.publicKey);
    const recentSlot = await program.provider.connection.getSlot('finalized');
    const lutKey = getLutKey(lutSigner, recentSlot);

    const ix = await program.instruction.oracleInit(
      {
        recentSlot: new BN(recentSlot.toString()),
        authority: payer.publicKey,
        queue: params.queue,
        secpAuthority: null,
      },
      {
        accounts: {
          oracle: oracle.publicKey,
          oracleStats,
          authority: payer.publicKey,
          programState: stateKey,
          payer: payer.publicKey,
          systemProgram: web3.SystemProgram.programId,
          tokenProgram: SPL_TOKEN_PROGRAM_ID,
          tokenMint: SOL_NATIVE_MINT,
          lutSigner: lutSigner,
          lut: lutKey,
          addressLookupTableProgram: web3.AddressLookupTableProgram.programId,
          switchMint: state.switchMint,
          wsolVault: spl.getAssociatedTokenAddressSync(
            SOL_NATIVE_MINT,
            oracle.publicKey
          ),
          switchVault: spl.getAssociatedTokenAddressSync(
            state.switchMint,
            oracle.publicKey
          ),
        },
      }
    );

    return [new Oracle(program, oracle.publicKey), [ix], oracle];
  }

  /**
   * Creates a new oracle account for SVM chains (non-solana). linked to the specified queue.
   * After creation the oracle still must receive run approval and verify their
   * enclave measurement.
   * @param program - The program that owns the oracle account.
   * @param params.queue - The queue that the oracle will be linked to.
   * @returns A promise that resolves to a tuple containing the oracle account
   * and the transaction signature.
   *
   */
  static async createSVM(
    program: Program,
    params: {
      queue: web3.PublicKey;
      sourceOracleKey: web3.PublicKey;
    }
  ): Promise<[Oracle, web3.TransactionInstruction[]]> {
    const stateKey = State.keyFromSeed(program);
    const state = await State.loadData(program);
    const payer = spl.getNodePayer(program);
    // Generate the queue PDA for the given source queue key
    const [oracle] = web3.PublicKey.findProgramAddressSync(
      [
        Buffer.from('Oracle'),
        params.queue.toBuffer(),
        params.sourceOracleKey.toBuffer(),
      ],
      program.programId
    );
    const oracleStats = web3.PublicKey.findProgramAddressSync(
      [Buffer.from('OracleStats'), oracle.toBuffer()],
      program.programId
    )[0];
    const lutSigner = getLutSigner(program.programId, oracle);
    const recentSlot = await program.provider.connection.getSlot('finalized');
    const lutKey = getLutKey(lutSigner, recentSlot);

    const ix = program.instruction.oracleInitSvm(
      {
        recentSlot: new BN(recentSlot.toString()),
        authority: payer.publicKey,
        queue: params.queue,
        secpAuthority: null,
        sourceOracleKey: params.sourceOracleKey,
      },
      {
        accounts: {
          oracle: oracle,
          oracleStats,
          authority: payer.publicKey,
          programState: stateKey,
          payer: payer.publicKey,
          systemProgram: web3.SystemProgram.programId,
          tokenProgram: SPL_TOKEN_PROGRAM_ID,
          tokenMint: SOL_NATIVE_MINT,
          lutSigner: lutSigner,
          lut: lutKey,
          addressLookupTableProgram: web3.AddressLookupTableProgram.programId,
          switchMint: state.switchMint,
          wsolVault: spl.getAssociatedTokenAddressSync(
            SOL_NATIVE_MINT,
            oracle,
            true
          ),
          switchVault: spl.getAssociatedTokenAddressSync(
            state.switchMint,
            oracle,
            true
          ),
        },
      }
    );

    return [new Oracle(program, oracle), [ix]];
  }

  /**
   * TODO: wrap this one up with the gateway bridge oracle fn
   * @param params
   * @returns
   */
  // static async quoteVerifySvmIx(
  //   program: Program,
  //   params: {
  //     chain?: string; // Unused atm
  //     network?: 'mainnet' | 'mainnet-beta' | 'testnet' | 'devnet';
  //     queue: web3.PublicKey; // Solana queue
  //     attestee: web3.PublicKey; // Solana attestee
  //     attester: web3.PublicKey; // Solana attester guardian we're requesting from
  //   }
  // ): Promise<web3.TransactionInstruction> {
  // const [queuePDA, queueBump] = PublicKey.findProgramAddressSync(
  //   [Buffer.from("Queue"), params.queue.toBuffer()],
  //   program.programId
  // );

  // timestamp handled by bridge fn
  // mrEnclave handled by bridge fn
  // secp256k1Key handled by bridge fn
  // slot has to be handled by us I think
  // signature has to be handled by bridge fn
  // recoveryId has to be handled by bridge fn

  // guardian key & oracle key

  // source oracle key handled by us:

  // source oracle queue key handled by us:

  // source guardian queue key handled by us:

  // const ix = await program.instruction.guardianQuoteVerifySvm(
  //   {
  //     timestamp: new anchor.BN(params.timestamp),
  //     mrEnclave: params.mrEnclave, // 32-byte array
  //     _reserved1: params._reserved1, // 32-bit unsigned integer
  //     secp256k1Key: params.secp256k1Key, // 64-byte array
  //     slot: new anchor.BN(params.slot), // Slot as u64
  //     signature: params.signature, // 64-byte array
  //     recoveryId: params.recoveryId, // u8
  //     sourceOracleKey: params.sourceOracleKey, // Pubkey of source oracle
  //     sourceOracleQueueKey: params.sourceOracleQueueKey, // Pubkey of oracle queue
  //     sourceGuardianQueueKey: params.sourceGuardianQueueKey, // Pubkey of guardian queue
  //     oracleBump: params.oracleBump, // Bump for oracle PDA
  //     oracleQueueBump: params.oracleQueueBump, // Bump for oracle queue PDA
  //     guardianQueueBump: params.guardianQueueBump, // Bump for guardian queue PDA
  //   },
  //   {
  //     accounts: {
  //       guardian: guardianAccountLoader, // AccountLoader for OracleAccountData
  //       oracle: oracleAccountLoader, // AccountLoader for OracleAccountData
  //       oracleStats: oracleStatsAccountLoader, // AccountLoader for OracleStatsAccountData
  //       payer: payer.publicKey, // Signer for transaction
  //       systemProgram: SystemProgram.programId, // System program ID
  //       oracleQueue: oracleQueueAccountLoader, // AccountLoader for QueueAccountData
  //       guardianQueue: guardianQueueAccountLoader, // AccountLoader for QueueAccountData
  //       state: stateAccountLoader, // AccountLoader for State
  //       recentSlothashes: anchor.web3.SYSVAR_SLOT_HASHES_PUBKEY, // Sysvar slot hashes
  //       lutSigner: lutSignerAccount, // AccountInfo for lut signer
  //       lut: lutAccount, // AccountInfo for lut (lookup table)
  //       programState: programStateAccountLoader, // AccountLoader for State
  //     },
  //     signers: [payer], // Add payer as the signer for the instruction
  //   }
  // );

  //   throw new Error('Quote verify SVM not implemented yet.');
  // }

  async findSolanaOracleFromPDA(): Promise<{
    oracleData: OracleAccountData;
    oracle: web3.PublicKey;
  }> {
    const oracleData = await this.loadData();
    const isMainnet = oracleData.queue.equals(spl.ON_DEMAND_MAINNET_QUEUE_PDA);
    const queue = await spl.getQueue({
      program: this.program,
      queueAddress: spl.getDefaultQueueAddress(isMainnet),
    });
    const solanaOracles = await queue.fetchOracleKeys();
    for (const oracle of solanaOracles) {
      const [oraclePDA] = web3.PublicKey.findProgramAddressSync(
        [Buffer.from('Oracle'), oracleData.queue.toBuffer(), oracle.toBuffer()],
        this.program.programId
      );
      if (oraclePDA.equals(this.pubkey)) {
        return {
          oracleData: await new Oracle(queue.program, oracle).loadData(),
          oracle,
        };
      }
    }
    throw new Error(`Solana Oracle not found for ${this.pubkey.toBase58()}`);
  }

  async setConfigsIx(params: {
    authority: web3.PublicKey;
  }): Promise<web3.TransactionInstruction> {
    const ix = await this.program.instruction.oracleSetConfigs(
      {
        authority: params.authority,
        newSecpAuthority: null,
      },
      {
        accounts: {
          oracle: this.pubkey,
          authority: params.authority,
        },
      }
    );
    return ix;
  }

  /**
   *  Loads the oracle data for this {@linkcode Oracle} account from on chain.
   *
   *  @returns A promise that resolves to the oracle data.
   *  @throws if the oracle account does not exist.
   */
  async loadData(): Promise<OracleAccountData> {
    return await Oracle.loadData(this.program, this.pubkey);
  }

  async fetchGateway(): Promise<string> {
    const data = await this.loadData();
    const gw = Buffer.from(data.gatewayUri).toString();
    return gw.replace(/\0+$/, '');
  }

  /**
   *  Loads the oracle data for this {@linkcode Oracle} account from on chain.
   *
   *  @returns A promise that resolves to the oracle data.
   *  @throws if the oracle account does not exist.
   */
  static async loadData(
    program: Program,
    pubkey: web3.PublicKey
  ): Promise<OracleAccountData> {
    return await program.account['oracleAccountData'].fetch(pubkey);
  }

  /**
   * Loads the oracle data for a list of {@linkcode Oracle} accounts from on chain.
   *
   * @param program - The program that owns the oracle accounts.
   * @param keys - The public keys of the oracle accounts to load.
   * @returns A promise that resolves to an array of oracle data.
   * @throws if any of the oracle accounts do not exist.
   */
  static async loadMany(
    program: Program,
    keys: web3.PublicKey[]
  ): Promise<(OracleAccountData | null)[]> {
    return await program.account['oracleAccountData'].fetchMultiple(keys);
  }

  /**
   * Loads the oracle data and checks if the oracle is verified.
   *
   * @returns A promise that resolves to a tuple containing a boolean indicating
   * if the oracle is verified and the expiration time of the verification.
   * @throws if the oracle account does not exist.
   */
  async verificationStatus(): Promise<[boolean, number]> {
    const data = await this.loadData();
    const now = new BN(Date.now() / 1000);
    const status = data.enclave.verificationStatus;
    const expiration = data.enclave.validUntil;
    return [status === 4 && now.lt(expiration), expiration.toNumber()];
  }

  /**
   * Get the pubkey of the stats account for this oracle.
   * @returns A promise that resolves to the pubkey of the stats account.
   */
  statsKey(): web3.PublicKey {
    return web3.PublicKey.findProgramAddressSync(
      [Buffer.from('OracleStats'), this.pubkey.toBuffer()],
      this.program.programId
    )[0];
  }

  async loadLookupTableKey(): Promise<web3.PublicKey> {
    const data = await this.loadData();
    return this.lookupTableKey(data);
  }

  public lookupTableKey(data: { lutSlot: number | BN }): web3.PublicKey {
    const lutSigner = getLutSigner(this.program.programId, this.pubkey);
    return getLutKey(lutSigner, data.lutSlot);
  }

  async loadLookupTable(): Promise<web3.AddressLookupTableAccount> {
    if (this.lut !== null && this.lut !== undefined) return this.lut;

    const lutKey = await this.loadLookupTableKey();
    const accnt =
      await this.program.provider.connection.getAddressLookupTable(lutKey);
    this.lut = accnt.value!;
    return this.lut!;
  }

  async setOperatorIx(params: {
    operator: web3.PublicKey;
  }): Promise<web3.TransactionInstruction> {
    const data = await this.loadData();
    const ix = await this.program.instruction.oracleSetOperator(
      {},
      {
        accounts: {
          oracle: this.pubkey,
          operator: params.operator,
          authority: data.authority,
        },
      }
    );
    return ix;
  }
}
