import { OracleQuote, QUOTE_PROGRAM_ID } from '../classes/oracleQuote.js';
import {
  SOL_NATIVE_MINT,
  SPL_ASSOCIATED_TOKEN_ACCOUNT_PROGRAM_ID,
  SPL_SYSVAR_INSTRUCTIONS_ID,
  SPL_SYSVAR_SLOT_HASHES_ID,
  SPL_TOKEN_PROGRAM_ID,
} from '../constants.js';
import { Ed25519InstructionUtils } from '../instruction-utils/ed25519-instruction-utils.js';
import type { FetchSignaturesConsensusResponse } from '../oracle-interfaces/gateway.js';
import type { FeedRequest } from '../oracle-interfaces/gateway.js';
import { Gateway } from '../oracle-interfaces/gateway.js';
import {
  getAssociatedTokenAddress,
  getNodePayer,
  isMainnetConnection,
} from '../utils/index.js';
import { getLutKey, getLutSigner } from '../utils/lookupTable.js';

import { Oracle } from './oracle.js';
import type { SwitchboardPermission } from './permission.js';
import { Permission } from './permission.js';
import { State } from './state.js';

import type { Program } from '@coral-xyz/anchor-31';
import { BN, web3 } from '@coral-xyz/anchor-31';
import type { IOracleFeed } from '@switchboard-xyz/common';
import {
  CrossbarClient,
  CrossbarNetwork,
  FeedHash,
} from '@switchboard-xyz/common';
import axios from 'axios';
import { Buffer } from 'buffer';

/**
 * Health response from gateway/oracle test endpoints
 */
interface HealthResponse {
  chain: string;
  disable_heartbeats: boolean;
  ed25519_pubkey: string;
  enable_gateway: number;
  enable_guardian: number;
  enable_pull_oracle: number;
  enable_push_oracle: number;
  gateway_advertise_interval: number;
  gateway_ingress: string;
  heartbeat_interval: number;
  key_rotate_interval: number;
  mr_enclave: string;
  network_id: string;
  oracle_authority: string;
  oracle_ingress: string;
  pagerduty_api_key: string;
  payer_secret: string;
  payer_secret_filepath: string;
  guardian_oracle: string;
  pull_oracle: string;
  push_oracle: string;
  push_queue: string;
  routine_max_jitter: number;
  rpc_url: string;
  secp256k1_pubkey: string;
  switchboard_on_demand_program_id: string;
  version_filepath: string;
  version: string;
  wss_rpc_url: string;
  known_oracles: string;
  system_time: number;
  restricted: boolean;
  api_key_service_url: string;
}

/**
 * On-chain queue account data structure
 *
 * The queue account is the core configuration for a set of oracle operators.
 * It defines which oracles are authorized to sign data and various security
 * parameters for the oracle network.
 *
 * @interface QueueAccountData
 */
export interface QueueAccountData {
  /** Authority that can modify the queue configuration */
  authority: web3.PublicKey;
  /** Intel SGX enclave measurements for TEE verification */
  mrEnclaves: Uint8Array[];
  /** Public keys of authorized oracle operators */
  oracleKeys: web3.PublicKey[];
  /** Maximum age for TEE quote verification */
  maxQuoteVerificationAge: BN;
  /** Last heartbeat timestamp from authority */
  lastHeartbeat: BN;
  /** Timeout period for oracle nodes */
  nodeTimeout: BN;
  /** Minimum stake required for oracle operators */
  oracleMinStake: BN;
  /** Time after which authority can override without permission */
  allowAuthorityOverrideAfter: BN;
  /** Number of valid enclave measurements */
  mrEnclavesLen: number;
  /** Number of registered oracle keys */
  oracleKeysLen: number;
  /** Reward amount for oracle operators */
  reward: number;
  /** Current oracle index for round-robin selection */
  currIdx: number;
  /** Garbage collection index */
  gcIdx: number;
  /** Whether authority heartbeat permission is required */
  requireAuthorityHeartbeatPermission: boolean;
  /** Whether authority verify permission is required */
  requireAuthorityVerifyPermission: boolean;
  /** Whether usage permissions are enforced */
  requireUsagePermissions: boolean;
  /** PDA bump for the queue signer account */
  signerBump: number;
  /** Token mint for rewards and fees */
  mint: web3.PublicKey;
  /** Slot when the lookup table was last updated */
  lutSlot: BN;
  /** Whether subsidies are allowed for this queue */
  allowSubsidies: boolean;
  /** Network configuration node (NCN) account */
  ncn: web3.PublicKey;
  /** Oracle fee proportion in basis points (e.g., 5000 = 50%) */
  oracleFeeProportionBps: number;
}

/**
 * Queue account management for Switchboard On-Demand
 *
 * The Queue class is the primary interface for interacting with oracle operators
 * in the Switchboard network. It manages:
 *
 * - Oracle operator authorization and verification
 * - Quote fetching and signature verification
 * - Address lookup table management
 * - Gateway interactions for data retrieval
 *
 * ## Key Features
 *
 * - **Oracle Management**: Track and verify authorized oracle signers
 * - **Quote Operations**: Fetch signed data quotes from oracle operators
 * - **LUT Optimization**: Automatic address lookup table management
 * - **Network Detection**: Automatic mainnet/devnet queue selection
 *
 * @example
 * ```typescript
 * import * as sb from '@switchboard-xyz/on-demand';
 *
 * // Initialize program and connection
 * const program = anchor.workspace.SwitchboardOnDemand;
 * const connection = new Connection("https://api.devnet.solana.com");
 *
 * // Load the default queue for your network
 * const queue = await Queue.loadDefault(program);
 *
 * // Set up gateway and crossbar
 * const crossbar = sb.CrossbarClient.default();
 * const gateway = await queue.fetchGatewayFromCrossbar(crossbar);
 *
 * // Fetch a quote for specific feeds (BTC/USD and ETH/USD)
 * const feedHashes = [
 *   '0xef0d8b6fcd0104e3e75096912fc8e1e432893da4f18faedaacca7e5875da620f', // BTC/USD
 *   '0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef'  // ETH/USD
 * ];
 *
 * const sigVerifyIx = await queue.fetchQuoteIx(
 *   gateway,
 *   crossbar,
 *   feedHashes,
 *   {
 *     numSignatures: 3, // Require 3 oracle signatures for consensus
 *     variableOverrides: {},
 *     instructionIdx: 0
 *   }
 * );
 *
 * // Build and send transaction
 * const tx = await sb.asV0Tx({
 *   connection,
 *   ixs: [sigVerifyIx, yourBusinessLogicIx],
 *   signers: [payer],
 *   computeUnitPrice: 200_000,
 *   computeUnitLimitMultiple: 1.3,
 * });
 *
 * await connection.sendTransaction(tx, {
 *   preflightCommitment: "processed",
 * });
 * ```
 *
 * @class Queue
 */
export class Queue {
  private data: QueueAccountData | null = null;
  private lookupTable: web3.AddressLookupTableAccount | null = null;
  private lookupTableRefreshTime: number = 0;
  private network: CrossbarNetwork | null = null; // Cache network detection
  static readonly DEFAULT_DEVNET_KEY: web3.PublicKey = new web3.PublicKey(
    'EYiAmGSdsQTuCw413V5BzaruWuCCSDgTPtBGvLkXHbe7'
  );
  static readonly DEFAULT_MAINNET_KEY: web3.PublicKey = new web3.PublicKey(
    'A43DyUGA7s8eXPxqEjJY6EBu1KKbNgfxF8h17VAHn13w'
  );

  /**
   * Loads the default queue for the current network
   *
   * Automatically detects whether you're on mainnet or devnet and loads
   * the appropriate default queue. This is the recommended way to get
   * started with Switchboard On-Demand.
   *
   * @param {Program} program - Anchor program instance
   * @returns {Promise<Queue>} The default queue for your network
   *
   * @example
   * ```typescript
   * const queue = await Queue.loadDefault(program);
   * console.log('Using queue:', queue.pubkey.toBase58());
   * ```
   */
  static async loadDefault(program: Program): Promise<Queue> {
    try {
      const queue = new Queue(program, Queue.DEFAULT_MAINNET_KEY);
      await queue.loadData();
      return queue;
    } catch {
      // do nothing
    }
    const queue = new Queue(program, Queue.DEFAULT_DEVNET_KEY);
    return queue;
  }

  /**
   * Fetches a gateway with the latest/majority version from Crossbar
   *
   * @param crossbar - CrossbarClient instance
   * @returns Promise<Gateway> - Gateway instance with the latest version
   */
  async fetchGatewayByLatestVersion(
    crossbar: CrossbarClient
  ): Promise<Gateway> {
    // Detect and cache network if not already set
    if (this.network === null) {
      const isMainnet = await isMainnetConnection(
        this.program.provider.connection
      );
      this.network = isMainnet
        ? CrossbarNetwork.SolanaMainnet
        : CrossbarNetwork.SolanaDevnet;
    }

    // Set the crossbar network based on detected/cached network
    crossbar.setNetwork(this.network);

    const gatewayUrls = await crossbar.fetchGateways(
      this.network === CrossbarNetwork.SolanaMainnet ? 'mainnet' : 'devnet'
    );
    const gatewayHealths: Array<{
      url: string;
      health: HealthResponse;
      version: string;
    }> = [];

    // Check health of all gateways
    for (const gatewayUrl of gatewayUrls) {
      try {
        const healthUrl = `${gatewayUrl}/gateway/api/v1/test`;
        const response = await axios.get<HealthResponse>(healthUrl);
        const health = response.data;
        gatewayHealths.push({
          url: gatewayUrl,
          health,
          version: health.version || 'unknown',
        });
      } catch (error) {
        console.warn(
          `Failed to fetch health for gateway ${gatewayUrl}:`,
          error
        );
      }
    }

    // Find majority version
    const versionCounts = new Map<string, number>();
    gatewayHealths.forEach(({ version }) => {
      versionCounts.set(version, (versionCounts.get(version) || 0) + 1);
    });

    let majorityVersion = 'unknown';
    let maxCount = 0;
    for (const [version, count] of versionCounts.entries()) {
      if (count > maxCount) {
        maxCount = count;
        majorityVersion = version;
      }
    }

    // Find all gateways with majority version
    const majorityGateways = gatewayHealths.filter(
      g => g.version === majorityVersion
    );
    if (majorityGateways.length === 0) {
      throw new Error('No healthy gateways found');
    }

    // Randomly select from majority version gateways
    const randomIndex = Math.floor(Math.random() * majorityGateways.length);
    const selectedGateway = majorityGateways[randomIndex];
    const gateway = new Gateway(selectedGateway.url);
    return gateway;
  }

  /**
   * Fetches a random oracle from the queue
   *
   * This method queries all oracles on the queue and randomly selects one.
   *
   * @returns {Promise<Oracle>} Randomly selected oracle instance from the queue
   * @throws {Error} If no oracles are found on the queue
   *
   * @example
   * ```typescript
   * const queue = new Queue(program, queuePubkey);
   * const oracle = await queue.fetchOracleByLatestVersion();
   * console.log('Selected oracle:', oracle.pubkey.toBase58());
   * ```
   */
  async fetchOracleByLatestVersion(): Promise<Oracle> {
    // Fetch all oracle keys from the queue (internally calls loadData)
    const oracleKeys = await this.fetchOracleKeys();
    if (oracleKeys.length === 0) {
      throw new Error('No oracles found on queue');
    }

    // Randomly select an oracle from the queue
    const randomIndex = Math.floor(Math.random() * oracleKeys.length);
    const selectedOracleKey = oracleKeys[randomIndex];
    return new Oracle(this.program, selectedOracleKey);
  }

  /**
   * Creates a new queue account
   *
   * @param {Program} program - Anchor program instance
   * @param {Object} params - Queue configuration parameters
   * @returns {Promise<[Queue, web3.Keypair, web3.TransactionInstruction]>}
   *          Tuple of [Queue instance, keypair, creation instruction]
   */
  static async createIx(
    program: Program,
    params: {
      allowAuthorityOverrideAfter?: number;
      requireAuthorityHeartbeatPermission?: boolean;
      requireUsagePermission?: boolean;
      maxQuoteVerificationAge?: number;
      reward?: number;
      nodeTimeout?: number;
      lutSlot?: number;
    }
  ): Promise<[Queue, web3.Keypair, web3.TransactionInstruction]> {
    const queue = web3.Keypair.generate();
    const allowAuthorityOverrideAfter =
      params.allowAuthorityOverrideAfter ?? 60 * 60;
    const requireAuthorityHeartbeatPermission =
      params.requireAuthorityHeartbeatPermission ?? true;
    const requireUsagePermission = params.requireUsagePermission ?? false;
    const maxQuoteVerificationAge =
      params.maxQuoteVerificationAge ?? 60 * 60 * 24 * 7;
    const reward = params.reward ?? 1000000;
    const nodeTimeout = params.nodeTimeout ?? 300;
    const payer = getNodePayer(program);
    // Prepare accounts for the transaction
    const lutSigner = getLutSigner(program.programId, queue.publicKey);
    const recentSlot =
      params.lutSlot ??
      (await program.provider.connection.getSlot('finalized'));
    const lutKey = getLutKey(lutSigner, recentSlot);

    const ix = await program.instruction.queueInit(
      {
        allowAuthorityOverrideAfter,
        requireAuthorityHeartbeatPermission,
        requireUsagePermission,
        maxQuoteVerificationAge,
        reward,
        nodeTimeout,
        recentSlot: new BN(recentSlot),
      },
      {
        accounts: {
          queue: queue.publicKey,
          queueEscrow: await getAssociatedTokenAddress(
            SOL_NATIVE_MINT,
            queue.publicKey
          ),
          authority: payer.publicKey,
          payer: payer.publicKey,
          systemProgram: web3.SystemProgram.programId,
          tokenProgram: SPL_TOKEN_PROGRAM_ID,
          nativeMint: SOL_NATIVE_MINT,
          programState: State.keyFromSeed(program),
          lutSigner: lutSigner,
          lut: lutKey,
          addressLookupTableProgram: web3.AddressLookupTableProgram.programId,
          associatedTokenProgram: SPL_ASSOCIATED_TOKEN_ACCOUNT_PROGRAM_ID,
        },
        signers: [payer, queue],
      }
    );
    return [new Queue(program, queue.publicKey), queue, ix];
  }

  /**
   * Creates a new instance of the `Queue` account with a PDA for SVM (non-solana) chains.
   * @param program The anchor program instance.
   * @param params The initialization parameters for the queue.
   * @returns
   */
  static async createIxSVM(
    program: Program,
    params: {
      sourceQueueKey: web3.PublicKey;
      allowAuthorityOverrideAfter?: number;
      requireAuthorityHeartbeatPermission?: boolean;
      requireUsagePermission?: boolean;
      maxQuoteVerificationAge?: number;
      reward?: number;
      nodeTimeout?: number;
      lutSlot?: number;
    }
  ): Promise<[Queue, web3.TransactionInstruction]> {
    // Generate the queue PDA for the given source queue key
    const [queue] = web3.PublicKey.findProgramAddressSync(
      [Buffer.from('Queue'), params.sourceQueueKey.toBuffer()],
      program.programId
    );
    const allowAuthorityOverrideAfter =
      params.allowAuthorityOverrideAfter ?? 60 * 60;
    const requireAuthorityHeartbeatPermission =
      params.requireAuthorityHeartbeatPermission ?? true;
    const requireUsagePermission = params.requireUsagePermission ?? false;
    const maxQuoteVerificationAge =
      params.maxQuoteVerificationAge ?? 60 * 60 * 24 * 7;
    const reward = params.reward ?? 1000000;
    const nodeTimeout = params.nodeTimeout ?? 300;
    const payer = getNodePayer(program);
    // Prepare accounts for the transaction
    const lutSigner = getLutSigner(program.programId, queue);
    const recentSlot =
      params.lutSlot ??
      (await program.provider.connection.getSlot('finalized'));
    const lutKey = getLutKey(lutSigner, recentSlot);

    const ix = program.instruction.queueInitSvm(
      {
        allowAuthorityOverrideAfter,
        requireAuthorityHeartbeatPermission,
        requireUsagePermission,
        maxQuoteVerificationAge,
        reward,
        nodeTimeout,
        recentSlot: new BN(recentSlot),
        sourceQueueKey: params.sourceQueueKey,
      },
      {
        accounts: {
          queue: queue,
          queueEscrow: await getAssociatedTokenAddress(
            SOL_NATIVE_MINT,
            queue,
            true
          ),
          authority: payer.publicKey,
          payer: payer.publicKey,
          systemProgram: web3.SystemProgram.programId,
          tokenProgram: SPL_TOKEN_PROGRAM_ID,
          nativeMint: SOL_NATIVE_MINT,
          programState: State.keyFromSeed(program),
          lutSigner: lutSigner,
          lut: lutKey,
          addressLookupTableProgram: web3.AddressLookupTableProgram.programId,
          associatedTokenProgram: SPL_ASSOCIATED_TOKEN_ACCOUNT_PROGRAM_ID,
        },
        signers: [payer],
      }
    );
    return [new Queue(program, queue), ix];
  }

  /**
   * Add an Oracle to a queue and set permissions
   * @param program
   * @param params
   */
  async overrideSVM(params: {
    oracle: web3.PublicKey;
    secp256k1Signer: Buffer;
    maxQuoteVerificationAge: number;
    mrEnclave: Buffer;
    slot: number;
  }) {
    const stateKey = State.keyFromSeed(this.program);
    const { authority } = await this.loadData();

    const ix = this.program.instruction.queueOverrideSvm(
      {
        secp256K1Signer: Array.from(params.secp256k1Signer),
        maxQuoteVerificationAge: new BN(params.maxQuoteVerificationAge),
        mrEnclave: params.mrEnclave,
        slot: new BN(params.slot),
      },
      {
        accounts: {
          queue: this.pubkey,
          oracle: params.oracle,
          authority,
          state: stateKey,
        },
      }
    );
    return ix;
  }

  /**
   * Static method to fetch oracle signatures using the consensus mechanism
   *
   * This convenience method creates a Queue instance and fetches signed oracle responses
   * for multiple feed configurations. Gateway is automatically fetched from CrossbarClient.
   *
   * @param {Program} program - Anchor program instance
   * @param {web3.PublicKey} params.queue - Public key of the queue account
   * @param {CrossbarClient} [params.crossbarClient] - Optional CrossbarClient instance (defaults to CrossbarClient.default())
   * @param {FeedRequest[]} params.feedConfigs - Array of feed configurations to fetch signatures for
   * @param {boolean} [params.useTimestamp] - Whether to use timestamp in the signature
   * @param {number} [params.numSignatures] - Number of oracle signatures to fetch
   * @param {boolean} [params.useEd25519] - Whether to use Ed25519 signatures (default: false for Secp256k1)
   * @param {Record<string, string>} [params.variableOverrides] - Optional variable overrides for job execution
   * @returns {Promise<FetchSignaturesConsensusResponse>} A promise that resolves to the consensus response with oracle signatures
   * @throws {Error} If the request fails or no signatures are available
   */
  static async fetchSignaturesConsensus(
    program: Program,
    params: {
      crossbarClient?: CrossbarClient;
      queue: web3.PublicKey;
      feedConfigs: FeedRequest[];
      useTimestamp?: boolean;
      numSignatures?: number;
      useEd25519?: boolean;
      variableOverrides?: Record<string, string>;
    }
  ): Promise<FetchSignaturesConsensusResponse> {
    const queueAccount = new Queue(program, params.queue!);
    return queueAccount.fetchSignaturesConsensus({
      crossbarClient: params.crossbarClient,
      feedConfigs: params.feedConfigs,
      useTimestamp: params.useTimestamp,
      numSignatures: params.numSignatures,
      useEd25519: params.useEd25519,
      variableOverrides: params.variableOverrides,
    });
  }

  /**
   *  Constructs a `OnDemandQueue` instance.
   *
   *  @param program The Anchor program instance.
   *  @param pubkey The public key of the queue account.
   */
  constructor(
    readonly program: Program,
    readonly pubkey: web3.PublicKey
  ) {
    if (this.pubkey === undefined) {
      throw new Error('NoPubkeyProvided');
    }
  }

  /**
   * Manually set the network for this queue
   *
   * This method allows you to explicitly set the network type, which
   * will be used by methods that need network-specific configuration
   * (e.g., crossbar gateway selection).
   *
   * @param {CrossbarNetwork} network - The network to use (SolanaMainnet or SolanaDevnet)
   *
   * @example
   * ```typescript
   * const queue = new Queue(program, queuePubkey);
   * queue.setNetwork(CrossbarNetwork.SolanaMainnet);
   * ```
   */
  setNetwork(network: CrossbarNetwork): void {
    this.network = network;
  }

  /**
   * Get the cached network type
   *
   * @returns {CrossbarNetwork | null} The network (SolanaMainnet, SolanaDevnet, or null if not yet determined)
   */
  getNetwork(): CrossbarNetwork | null {
    return this.network;
  }

  /**
   *  Loads the queue data from on chain and returns the listed oracle keys.
   *
   *  @returns A promise that resolves to an array of oracle public keys.
   */
  async fetchOracleKeys(): Promise<web3.PublicKey[]> {
    const data = await this.loadData();
    const oracles = data.oracleKeys.slice(0, data.oracleKeysLen);
    return oracles;
  }

  /**
   * Fetches oracle signatures using the consensus mechanism
   *
   * This method retrieves signed oracle responses for multiple feed configurations
   * using the consensus endpoint. Gateway is automatically fetched from CrossbarClient.
   *
   * @param {CrossbarClient} [params.crossbarClient] - Optional CrossbarClient instance (defaults to CrossbarClient.default())
   * @param {FeedRequest[]} params.feedConfigs - Array of feed configurations to fetch signatures for
   * @param {boolean} [params.useTimestamp] - Whether to use timestamp in the signature
   * @param {number} [params.numSignatures] - Number of oracle signatures to fetch
   * @param {boolean} [params.useEd25519] - Whether to use Ed25519 signatures (default: false for Secp256k1)
   * @param {Record<string, string>} [params.variableOverrides] - Optional variable overrides for job execution
   * @returns {Promise<FetchSignaturesConsensusResponse>} A promise that resolves to the consensus response with oracle signatures
   * @throws {Error} If the request fails or no signatures are available
   */
  async fetchSignaturesConsensus(params: {
    crossbarClient?: CrossbarClient;
    feedConfigs: FeedRequest[];
    useTimestamp?: boolean;
    numSignatures?: number;
    useEd25519?: boolean;
    variableOverrides?: Record<string, string>;
  }): Promise<FetchSignaturesConsensusResponse> {
    const crossbarClient = params.crossbarClient ?? CrossbarClient.default();

    // Detect and cache network if not already set
    if (this.network === null) {
      const isMainnet = await isMainnetConnection(
        this.program.provider.connection
      );
      this.network = isMainnet
        ? CrossbarNetwork.SolanaMainnet
        : CrossbarNetwork.SolanaDevnet;
    }

    // Set the crossbar network based on detected/cached network
    crossbarClient.setNetwork(this.network);

    const gateway = await crossbarClient.fetchGateway();

    return await gateway.fetchSignaturesConsensus({
      feedConfigs: params.feedConfigs,
      useTimestamp: params.useTimestamp,
      numSignatures: params.numSignatures,
      useEd25519: params.useEd25519,
      variableOverrides: params.variableOverrides,
    });
  }

  /**
   *  Loads the queue data for this {@linkcode Queue} account from on chain.
   *
   *  @returns A promise that resolves to the queue data.
   *  @throws if the queue account does not exist.
   */
  static loadData(
    program: Program,
    pubkey: web3.PublicKey
  ): Promise<QueueAccountData> {
    return program.account['queueAccountData'].fetch(pubkey);
  }

  /**
   *  Loads the queue data for this {@linkcode Queue} account from on chain.
   *
   *  @returns A promise that resolves to the queue data.
   *  @throws if the queue account does not exist.
   */
  async loadData(): Promise<QueueAccountData> {
    if (this.data === null || this.data === undefined) {
      this.data = await Queue.loadData(this.program, this.pubkey);
    }
    return this.data;
  }

  /**
   *  Adds a new MR enclave to the queue.
   *  This will allow the queue to accept signatures from the given MR enclave.
   *  @param mrEnclave The MR enclave to add.
   *  @returns A promise that resolves to the transaction instruction.
   *  @throws if the request fails.
   *  @throws if the MR enclave is already added.
   *  @throws if the MR enclave is invalid.
   *  @throws if the MR enclave is not a valid length.
   */
  async addMrEnclaveIx(params: {
    mrEnclave: Uint8Array;
  }): Promise<web3.TransactionInstruction> {
    const stateKey = State.keyFromSeed(this.program);
    const state = await State.loadData(this.program);
    const programAuthority = state.authority;
    const { authority } = await this.loadData();
    const ix = await this.program.instruction.queueAddMrEnclave(
      { mrEnclave: params.mrEnclave },
      {
        accounts: {
          queue: this.pubkey,
          authority,
          programAuthority,
          state: stateKey,
        },
      }
    );
    return ix;
  }

  /**
   *  Removes an MR enclave from the queue.
   *  This will prevent the queue from accepting signatures from the given MR enclave.
   *  @param mrEnclave The MR enclave to remove.
   *  @returns A promise that resolves to the transaction instruction.
   *  @throws if the request fails.
   *  @throws if the MR enclave is not present.
   */
  async rmMrEnclaveIx(params: {
    mrEnclave: Uint8Array;
  }): Promise<web3.TransactionInstruction> {
    const stateKey = State.keyFromSeed(this.program);
    const state = await State.loadData(this.program);
    const programAuthority = state.authority;
    const { authority } = await this.loadData();
    const ix = await this.program.instruction.queueRemoveMrEnclave(
      { mrEnclave: params.mrEnclave },
      {
        accounts: {
          queue: this.pubkey,
          authority,
          programAuthority,
          state: stateKey,
        },
      }
    );
    return ix;
  }

  /**
   * Sets the queue configurations.
   * @param params.authority The new authority for the queue.
   * @param params.reward The new reward for the queue.
   * @param params.nodeTimeout The new node timeout for the queue.
   * @param params.oracleFeeProportionBps The oracle fee proportion in basis points (e.g., 5000 = 50%).
   * @returns A promise that resolves to the transaction instruction.
   */
  async setConfigsIx(params: {
    authority?: web3.PublicKey;
    reward?: number;
    nodeTimeout?: number;
    oracleFeeProportionBps?: number;
  }): Promise<web3.TransactionInstruction> {
    const data = await this.loadData();
    const stateKey = State.keyFromSeed(this.program);
    const nodeTimeout = params.nodeTimeout ? new BN(params.nodeTimeout) : null;
    const ix = await this.program.instruction.queueSetConfigs(
      {
        authority: params.authority ?? null,
        reward: params.reward ?? null,
        nodeTimeout: nodeTimeout,
        oracleFeeProportionBps: params.oracleFeeProportionBps ?? null,
      },
      {
        accounts: {
          queue: this.pubkey,
          authority: data.authority,
          state: stateKey,
        },
      }
    );
    return ix;
  }

  async setNcnIx(params: {
    ncn: web3.PublicKey;
  }): Promise<web3.TransactionInstruction> {
    const data = await this.loadData();
    const authority = data.authority;
    const state = State.keyFromSeed(this.program);
    return this.program.instruction.queueSetNcn(
      {},
      {
        accounts: {
          queue: this.pubkey,
          authority,
          state,
          ncn: params.ncn,
        },
      }
    );
  }

  async setVaultIx(params: {
    vault: web3.PublicKey;
    enable: boolean;
  }): Promise<web3.TransactionInstruction> {
    const data = await this.loadData();
    const authority = data.authority;
    const state = State.keyFromSeed(this.program);
    const ncn = data.ncn;
    return this.program.instruction.queueSetVault(
      {
        enable: params.enable,
      },
      {
        accounts: {
          queue: this.pubkey,
          authority,
          state,
          ncn,
          vault: params.vault,
        },
      }
    );
  }

  async allowSubsidyIx(params: {
    enable: boolean;
  }): Promise<web3.TransactionInstruction> {
    const data = await this.loadData();
    const authority = data.authority;
    const state = State.keyFromSeed(this.program);
    return this.program.instruction.queueAllowSubsidies(
      {
        allowSubsidies: params.enable,
      },
      {
        accounts: {
          queue: this.pubkey,
          authority,
          state,
        },
      }
    );
  }

  /**
   * Sets the oracle permission on the queue.
   * @param params.oracle The oracle to set the permission for.
   * @param params.permission The permission to set.
   * @param params.enabled Whether the permission is enabled.
   * @returns A promise that resolves to the transaction instruction   */
  async setOraclePermissionIx(params: {
    oracle: web3.PublicKey;
    permission: SwitchboardPermission;
    enable: boolean;
  }): Promise<web3.TransactionInstruction> {
    const data = await this.loadData();
    return Permission.setIx(this.program, {
      authority: data.authority,
      grantee: params.oracle,
      granter: this.pubkey,
      permission: params.permission,
      enable: params.enable,
    });
  }

  /**
   *  Removes all MR enclaves from the queue.
   *  @returns A promise that resolves to an array of transaction instructions.
   *  @throws if the request fails.
   */
  async rmAllMrEnclaveIxs(): Promise<Array<web3.TransactionInstruction>> {
    const { mrEnclaves, mrEnclavesLen } = await this.loadData();
    const activeEnclaves = mrEnclaves.slice(0, mrEnclavesLen);
    const ixs: Array<web3.TransactionInstruction> = [];
    for (const mrEnclave of activeEnclaves) {
      ixs.push(await this.rmMrEnclaveIx({ mrEnclave }));
    }
    return ixs;
  }

  /**
   * Get the PDA for the queue (SVM chains that are not solana)
   * @returns Queue PDA Pubkey
   */
  queuePDA(): web3.PublicKey {
    return Queue.queuePDA(this.program, this.pubkey);
  }

  /**
   * Get the PDA for the queue (SVM chains that are not solana)
   * @param program Anchor program
   * @param pubkey Queue pubkey
   * @returns Queue PDA Pubkey
   */
  static queuePDA(program: Program, pubkey: web3.PublicKey): web3.PublicKey {
    const [queuePDA] = web3.PublicKey.findProgramAddressSync(
      [Buffer.from('Queue'), pubkey.toBuffer()],
      program.programId
    );
    return queuePDA;
  }

  // auto refresh lookup table if it is older than 5 minutes
  async loadLookupTable(): Promise<web3.AddressLookupTableAccount> {
    const now = Date.now();
    if (this.lookupTable && now - this.lookupTableRefreshTime < 5 * 60 * 1000) {
      return this.lookupTable;
    }
    const data = await this.loadData();
    const lutSigner = getLutSigner(this.program.programId, this.pubkey);
    const lutKey = getLutKey(lutSigner, data.lutSlot);
    const accnt =
      await this.program.provider.connection.getAddressLookupTable(lutKey);
    this.lookupTable = accnt.value;
    return accnt.value!;
  }

  /**
   * Fetches oracle quote and creates verification instruction
   *
   * This is the primary method for fetching oracle data in the quote approach.
   * It retrieves signed price data from oracle operators and creates the
   * instruction to verify signatures on-chain.
   *
   * ## Key Features
   * - **Aggregated Data**: Fetches multiple feeds in a single request
   * - **Consensus Verification**: Requires specified number of oracle signatures
   * - **Ed25519 Signatures**: Uses efficient signature verification on-chain
   * - **Cost Effective**: ~90% lower costs compared to individual feed updates
   * - **Production Ready**: Handles error cases and validation
   * - **Feed Format Flexibility**: Accepts either feed hashes (strings) or OracleFeed objects
   *
   * ## Security Considerations
   * - Always use multiple signatures for high-value operations
   * - Validate feed hashes match your expected data sources
   * - Consider oracle staking and reputation when choosing consensus levels
   *
   * @param {Gateway} gateway - Gateway instance for oracle communication
   * @param {CrossbarClient} crossbar - Crossbar client for data routing
   * @param {string[] | IOracleFeed[]} feedHashesOrFeeds - Array of feed hashes (hex strings) or array of OracleFeed objects (max 16 feeds)
   * @param {number} numSignatures - Number of oracle signatures required (default: 1, max 255)
   * @param {number} instructionIdx - Instruction index for Ed25519 program (default: 0)
   * @returns {Promise<web3.TransactionInstruction>}
   *          Ed25519 signature verification instruction ready for transaction
   *
   * @throws {Error} When no oracle responses are available
   * @throws {Error} When oracle index is out of bounds (>= 255)
   * @throws {Error} When too many feeds requested (> 16)
   * @throws {Error} When feed responses are missing from oracle
   *
   * @since 2.14.0
   * @see {@link fetchUpdateBundleIx} - Deprecated equivalent method
   * @see {@link Gateway.fetchQuote} - Gateway method for raw quote data
   *
   * @example
   * ```typescript
   * // Basic usage with single feed hash
   * const btcFeedHash = '0xef0d8b6fcd0104e3e75096912fc8e1e432893da4f18faedaacca7e5875da620f';
   * const sigVerifyIx = await queue.fetchQuoteIx(
   *   crossbar,
   *   [btcFeedHash],
   *   {
   *     numSignatures: 1, // Single oracle signature
   *     variableOverrides: {},
   *     instructionIdx: 0
   *   }
   * );
   *
   * // Using OracleFeed objects
   * const btcFeed: IOracleFeed = {
   *   name: 'BTC/USD Price Feed',
   *   jobs: [btcJob1, btcJob2],
   *   minOracleSamples: 3,
   *   // ... other feed properties
   * };
   *
   * const ethFeed: IOracleFeed = {
   *   name: 'ETH/USD Price Feed',
   *   jobs: [ethJob1, ethJob2],
   *   minOracleSamples: 3,
   * };
   *
   * const feedsIx = await queue.fetchQuoteIx(
   *   crossbar,
   *   [btcFeed, ethFeed],
   *   {
   *     numSignatures: 3,
   *     variableOverrides: {},
   *     instructionIdx: 0
   *   }
   * );
   *
   * // Multi-feed quote with higher consensus
   * const feedHashes = [
   *   '0xef0d8b6fcd0104e3e75096912fc8e1e432893da4f18faedaacca7e5875da620f', // BTC/USD
   *   '0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef', // ETH/USD
   *   '0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890'  // SOL/USD
   * ];
   *
   * const multiQuoteIx = await queue.fetchQuoteIx(
   *   crossbar,
   *   feedHashes,
   *   {
   *     numSignatures: 5, // Require 5 oracle signatures for high-value operations
   *     variableOverrides: {},
   *     instructionIdx: 1  // Instruction index for multiple Ed25519 instructions
   *   }
   * );
   *
   * // Use in your transaction with proper error handling
   * try {
   *   const tx = await asV0Tx({
   *     connection,
   *     ixs: [sigVerifyIx, yourBusinessLogicIx],
   *     signers: [payer],
   *     computeUnitPrice: 200_000,
   *     computeUnitLimitMultiple: 1.3,
   *   });
   *
   *   const txSignature = await connection.sendTransaction(tx, {
   *     preflightCommitment: "processed",
   *   });
   *
   *   console.log('Transaction confirmed:', txSignature);
   * } catch (error) {
   *   console.error('Quote fetch failed:', error);
   * }
   * ```
   */
  async fetchQuoteIx(
    crossbar: CrossbarClient,
    feedHashesOrFeeds: string[] | IOracleFeed[],
    configs?: {
      variableOverrides?: Record<string, string>;
      numSignatures?: number;
      instructionIdx?: number;
    }
  ): Promise<web3.TransactionInstruction> {
    const config = configs ?? {
      variableOverrides: {},
      numSignatures: 1,
      instructionIdx: 0,
    };

    // Detect and cache network if not already set
    if (this.network === null) {
      const isMainnet = await isMainnetConnection(
        this.program.provider.connection
      );
      this.network = isMainnet
        ? CrossbarNetwork.SolanaMainnet
        : CrossbarNetwork.SolanaDevnet;
    }

    // Set the crossbar network based on detected/cached network
    crossbar.setNetwork(this.network);

    // Fetch gateway from crossbar
    let gateway: Gateway;
    try {
      gateway = await crossbar.fetchGateway();
    } catch (error) {
      throw new Error(
        `Failed to fetch gateway from crossbar: ${error instanceof Error ? error.message : 'Unknown error'}`
      );
    }

    if (feedHashesOrFeeds.length === 0) {
      throw new Error('At least one feed hash or OracleFeed must be provided');
    }

    let response: FetchSignaturesConsensusResponse;

    // Check if first element is a string (feed hash) or object (OracleFeed)
    if (typeof feedHashesOrFeeds[0] === 'string') {
      // Input is an array of feed hashes - use fetchQuote which fetches from IPFS
      const feedHashes = feedHashesOrFeeds as string[];
      response = await gateway.fetchQuote(
        crossbar,
        feedHashes,
        config.numSignatures ?? 1,
        config.variableOverrides
      );
    } else {
      // Input is an array of OracleFeeds - use fetchSignaturesConsensus directly
      const feeds = feedHashesOrFeeds as IOracleFeed[];
      feeds.forEach(feed => {
        if (!feed.jobs || feed.jobs.length === 0) {
          throw new Error(
            `OracleFeed "${feed.name || 'unnamed'}" must contain at least one job`
          );
        }
      });

      response = await gateway.fetchSignaturesConsensus({
        feedConfigs: feeds.map(feed => ({ feed })),
        useTimestamp: false,
        numSignatures: config.numSignatures,
        useEd25519: true,
        variableOverrides: config.variableOverrides,
      });
    }

    // Check if oracle_responses is empty
    if (!response.oracle_responses || response.oracle_responses.length === 0) {
      throw new Error('No oracle responses available for creating signatures');
    }

    // Convert to ED25519 signatures - supports variable length messages!
    const ed25519Signatures = response.oracle_responses.map(
      (oracleResponse, index) => {
        let pubkeyHex = oracleResponse.ed25519_enclave_signer;

        // If ed25519_enclave_signer is 64 bytes (128 hex chars), extract the first 32 bytes for Ed25519 pubkey
        if (pubkeyHex && pubkeyHex.length === 128) {
          pubkeyHex = pubkeyHex.substring(0, 64); // First 32 bytes (64 hex chars)
        }

        // Check that we have feed responses
        if (
          !oracleResponse.feed_responses ||
          oracleResponse.feed_responses.length === 0
        ) {
          throw new Error(`Oracle response ${index} missing feed_responses`);
        }

        // Reconstruct the actual signed message for NEW SIGNING SCHEME
        // NEW FORMAT: signed_slothash + feed_infos (for ALL feeds)
        // NOTE: recent_slot, version, and oracle_idx are NO LONGER part of the signed message
        // Oracle indices are now appended to the ED25519 instruction data, not signed

        // Use response-level recent_hash for slothash
        const recentHashPubkey = new web3.PublicKey(response.recent_hash);
        const signedSlothash = Buffer.from(recentHashPubkey.toBytes()); // 32 bytes

        // Validate oracle index bounds (prevent access violation in Rust)
        // Note: We don't have queue data here, but we can validate reasonable bounds
        if (oracleResponse.oracle_idx >= 255) {
          throw new Error(
            `Oracle index out of bounds: ${oracleResponse.oracle_idx} (maximum supported is 254)`
          );
        }

        // Validate feed count limit (must match Rust limit)
        if (oracleResponse.feed_responses.length > 16) {
          throw new Error(
            `Too many feeds in oracle response: ${oracleResponse.feed_responses.length} feeds but maximum is 16`
          );
        }

        // Build feed infos for ALL feeds (not just the first one)
        const feedInfoBuffers: Buffer[] = [];
        for (const feedResponse of oracleResponse.feed_responses) {
          // Feed info: feed_id + value + min_oracle_samples
          const feedHash = Buffer.from(feedResponse.feed_hash, 'hex'); // 32 bytes

          // Convert success_value to 16-byte representation (little-endian i128)
          const successValue = BigInt(feedResponse.success_value);
          const valueBytes = Buffer.alloc(16);
          for (let i = 0; i < 16; i++) {
            const shift = BigInt(i) * BigInt(8);
            const mask = BigInt(0xff);
            valueBytes[i] = Number((successValue >> shift) & mask);
          }

          const minOracleSamples = Buffer.from([
            feedResponse.min_oracle_samples!,
          ]); // 1 byte

          // Concatenate this feed's info: feed_id + value + min_oracle_samples
          feedInfoBuffers.push(
            Buffer.concat([
              feedHash, // 32 bytes (feed_id)
              valueBytes, // 16 bytes (feed value as i128 LE)
              minOracleSamples, // 1 byte (min_oracle_samples)
            ])
          );
        }

        // NEW SIGNED MESSAGE FORMAT: signed_slothash + (feed_info1 + feed_info2 + ...)
        // Oracle index is NOT included in the signed message - it goes to instruction data
        const actualMessage = Buffer.concat([
          signedSlothash, // 32 bytes (slothash from response)
          ...feedInfoBuffers, // All feed infos (49 bytes each)
        ]);

        return {
          pubkey: Buffer.from(pubkeyHex, 'hex'), // Use Ed25519 pubkey (32 bytes)!
          signature: Buffer.from(oracleResponse.signature, 'base64'), // Use oracle-level signature for consensus!
          message: actualMessage, // Use reconstructed Ed25519 message (variable length)!
          oracleIdx: oracleResponse.oracle_idx, // Use actual oracle index from response
        };
      }
    );

    let slot = response.slot;
    if (typeof response.slot !== 'number') {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      slot = (response.slot as any).toNumber();
    }
    const ed25519Instruction = Ed25519InstructionUtils.buildEd25519Instruction(
      ed25519Signatures,
      config.instructionIdx ?? 0,
      slot,
      0 // version 0 for Ed25519 v0 scheme
    );
    return ed25519Instruction;
  }

  /**
   * Creates instructions for managed oracle updates using the new quote program
   *
   * This method generates instructions to call the verified_update method in the
   * quote program (PID: orac1eFjzWL5R3RbbdMV68K9H6TaCVVcL6LjvQQWAbz).
   * It creates both the Ed25519 signature verification instruction and the
   * quote program instruction that verifies and stores the oracle data.
   *
   * The oracle account is automatically derived from the feed hashes using the
   * canonical derivation logic. Gateway is automatically fetched and cached.
   *
   * ## Key Features
   * - **Managed Updates**: Automatically handles oracle account creation and updates
   * - **Verification**: Uses the quote program's verified_update for secure oracle data storage
   * - **Ed25519 Signatures**: Leverages efficient signature verification
   * - **Account Management**: Handles oracle account initialization if needed
   * - **Gateway Caching**: Automatically fetches and caches gateway for subsequent calls
   * - **Feed Format Flexibility**: Accepts either feed hashes (strings) or OracleFeed objects
   *
   * @param {Gateway} gateway - Gateway instance for oracle communication
   * @param {CrossbarClient} crossbar - Crossbar client for data routing
   * @param {string[] | IOracleFeed[]} feedHashesOrFeeds - Array of feed hashes (hex strings) or array of OracleFeed objects (max 16 feeds)
   * @param {object} configs - Configuration object with optional parameters
   * @param {Record<string, string>} [configs.variableOverrides] - Variable overrides for feed processing
   * @param {number} [configs.numSignatures] - Number of oracle signatures required (default: 1)
   * @param {number} [configs.instructionIdx] - Instruction index for Ed25519 program (default: 0)
   * @param {web3.PublicKey} [configs.payer] - Payer for oracle account creation (default: program provider payer)
   * @param {web3.PublicKey} [configs.programId] - Optional program ID for oracle account derived owner (default: QUOTE_PROGRAM_ID)
   * @returns {Promise<web3.TransactionInstruction[]>}
   *          Array of instructions: [Ed25519 verification, quote program verified_update]
   *
   * @throws {Error} When no oracle responses are available
   * @throws {Error} When oracle index is out of bounds (>= 255)
   * @throws {Error} When too many feeds requested (> 16)
   * @throws {Error} When feed responses are missing from oracle
   *
   * @example
   * ```typescript
   * // Using feed hashes
   * const btcFeedHash = '0xef0d8b6fcd0104e3e75096912fc8e1e432893da4f18faedaacca7e5875da620f';
   *
   * // Create the instructions (oracle account is derived automatically)
   * const instructions = await queue.fetchManagedUpdateIxs(
   *   crossbar,
   *   [btcFeedHash],
   *   {
   *     numSignatures: 3, // Require 3 oracle signatures for consensus
   *     variableOverrides: {},
   *     instructionIdx: 0,
   *     payer: myWallet.publicKey,
   *     programId: customQuoteProgramId // Optional: use custom program ID for oracle derivation
   *   }
   * );
   *
   * // Using OracleFeed objects
   * const btcFeed: IOracleFeed = {
   *   name: 'BTC/USD Price Feed',
   *   jobs: [btcJob1, btcJob2],
   *   minOracleSamples: 3,
   *   // ... other feed properties
   * };
   *
   * const ethFeed: IOracleFeed = {
   *   name: 'ETH/USD Price Feed',
   *   jobs: [ethJob1, ethJob2],
   *   minOracleSamples: 3,
   * };
   *
   * const instructionsFromFeeds = await queue.fetchManagedUpdateIxs(
   *   crossbar,
   *   [btcFeed, ethFeed],
   *   {
   *     numSignatures: 3,
   *     variableOverrides: {},
   *     instructionIdx: 0,
   *     payer: myWallet.publicKey
   *   }
   * );
   *
   * // Build transaction with managed update instructions
   * const tx = await asV0Tx({
   *   connection,
   *   ixs: [...instructions, yourBusinessLogicIx],
   *   signers: [payer],
   *   computeUnitPrice: 200_000,
   *   computeUnitLimitMultiple: 1.3,
   * });
   *
   * await connection.sendTransaction(tx);
   * ```
   */
  async fetchManagedUpdateIxs(
    crossbar: CrossbarClient,
    feedHashesOrFeeds: string[] | IOracleFeed[],
    configs?: {
      variableOverrides?: Record<string, string>;
      numSignatures?: number;
      instructionIdx?: number;
      payer?: web3.PublicKey;
    }
  ): Promise<web3.TransactionInstruction[]> {
    const config = configs ?? {
      variableOverrides: {},
      numSignatures: 1,
      instructionIdx: 0,
    };

    // Detect and cache network if not already set
    if (this.network === null) {
      const isMainnet = await isMainnetConnection(
        this.program.provider.connection
      );
      this.network = isMainnet
        ? CrossbarNetwork.SolanaMainnet
        : CrossbarNetwork.SolanaDevnet;
    }

    // Set the crossbar network based on detected/cached network
    crossbar.setNetwork(this.network);

    // Handle both feed hashes and OracleFeed array input
    let feedHashes: string[];
    if (feedHashesOrFeeds.length === 0) {
      throw new Error('At least one feed hash or OracleFeed must be provided');
    }

    // Check if first element is a string (feed hash) or object (OracleFeed)
    if (typeof feedHashesOrFeeds[0] === 'string') {
      // Input is an array of feed hashes
      feedHashes = feedHashesOrFeeds as string[];
    } else {
      // Input is an array of OracleFeeds - compute hash for each
      feedHashes = (feedHashesOrFeeds as IOracleFeed[]).map(feed => {
        if (!feed.jobs || feed.jobs.length === 0) {
          throw new Error(
            `OracleFeed "${feed.name || 'unnamed'}" must contain at least one job to compute the feed hash`
          );
        }
        const feedHashBuffer = FeedHash.computeOracleFeedId(feed);
        return '0x' + feedHashBuffer.toString('hex');
      });
    }

    // Derive the canonical oracle account from feed hashes
    const [oracleAccount, bump] = OracleQuote.getCanonicalPubkey(
      this.pubkey,
      feedHashes,
      QUOTE_PROGRAM_ID
    );

    // First get the Ed25519 signature verification instruction
    // Pass the original feedHashesOrFeeds to avoid double IPFS fetch
    const ed25519Ix = await this.fetchQuoteIx(
      crossbar,
      feedHashesOrFeeds,
      config
    );

    // Get payer from config or program provider
    const payer = config.payer ?? getNodePayer(this.program).publicKey;
    const opcode = 0;

    // Create the quote program verified_update instruction
    const quoteProgramIx = new web3.TransactionInstruction({
      programId: QUOTE_PROGRAM_ID,
      keys: [
        { pubkey: this.pubkey, isSigner: false, isWritable: false }, // queue_account [0]
        { pubkey: oracleAccount, isSigner: false, isWritable: true }, // oracle_account [1]
        {
          pubkey: SPL_SYSVAR_INSTRUCTIONS_ID,
          isSigner: false,
          isWritable: false,
        }, // ix_sysvar [2]
        {
          pubkey: SPL_SYSVAR_SLOT_HASHES_ID,
          isSigner: false,
          isWritable: false,
        }, // slot_sysvar [3]
        {
          pubkey: web3.SYSVAR_CLOCK_PUBKEY,
          isSigner: false,
          isWritable: false,
        }, // clock_sysvar [4]
        { pubkey: payer, isSigner: true, isWritable: true }, // payer [5]
        {
          pubkey: web3.SystemProgram.programId,
          isSigner: false,
          isWritable: false,
        }, // system_program [6]
      ],
      data: Buffer.from([opcode, config.instructionIdx ?? 0, bump]), // ix_idx - index of Ed25519 instruction in transaction
    });

    return [ed25519Ix, quoteProgramIx];
  }
}
