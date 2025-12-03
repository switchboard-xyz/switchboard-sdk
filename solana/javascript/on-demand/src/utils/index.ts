import { Oracle } from '../accounts/oracle.js';
import type { PullFeed } from '../accounts/pullFeed.js';
import { Queue } from '../accounts/queue.js';
import { AnchorUtils } from '../anchor-utils/anchor-utils.js';
import {
  SPL_ASSOCIATED_TOKEN_ACCOUNT_PROGRAM_ID,
  SPL_TOKEN_PROGRAM_ID,
} from '../constants.js';

import type { Program } from '@coral-xyz/anchor-31';
import { web3 } from '@coral-xyz/anchor-31';
import type { IOracleJob } from '@switchboard-xyz/common';
import { CrossbarClient } from '@switchboard-xyz/common';
import { Buffer } from 'buffer';

type Account = {
  pubkey: web3.PublicKey;
  loadLookupTable: () => Promise<web3.AddressLookupTableAccount>;
};

export function createLoadLookupTables() {
  const promiseMap: Map<
    string,
    Promise<web3.AddressLookupTableAccount>
  > = new Map();

  async function loadLookupTables(
    accounts: Account[]
  ): Promise<web3.AddressLookupTableAccount[]> {
    for (const account of accounts) {
      const pubkey = account.pubkey.toString();
      if (pubkey && account.loadLookupTable) {
        if (!promiseMap.has(pubkey)) {
          promiseMap.set(pubkey, account.loadLookupTable());
        }
      }
    }

    const out: Promise<web3.AddressLookupTableAccount>[] = [];
    for (const account of accounts) {
      const promise = promiseMap.get(account.pubkey.toString());
      if (promise) out.push(promise);
    }
    return Promise.all(out).then(arr => {
      return arr.filter(x => Boolean(x));
    });
  }

  return loadLookupTables;
}

export const loadLookupTables = createLoadLookupTables();

/**
 * Mainnet program and queue addresses
 *
 * These are the official Switchboard On-Demand addresses for mainnet.
 * Most applications should use the default queue rather than deploying
 * their own.
 */

/** Switchboard On-Demand program ID on mainnet */
export const ON_DEMAND_MAINNET_PID = new web3.PublicKey(
  'SBondMDrcV3K4kxZR1HNVT7osZxAHVHgYXL5Ze1oMUv'
);

/** Guardian queue for mainnet (internal use) */
export const ON_DEMAND_MAINNET_GUARDIAN_QUEUE = new web3.PublicKey(
  'B7WgdyAgzK7yGoxfsBaNnY6d41bTybTzEh4ZuQosnvLK'
);

/** Default oracle queue on mainnet - use this for production */
export const ON_DEMAND_MAINNET_QUEUE = new web3.PublicKey(
  'A43DyUGA7s8eXPxqEjJY6EBu1KKbNgfxF8h17VAHn13w'
);

/** Queue PDA (Program Derived Address) for mainnet */
export const ON_DEMAND_MAINNET_QUEUE_PDA =
  web3.PublicKey.findProgramAddressSync(
    [Buffer.from('Queue'), ON_DEMAND_MAINNET_QUEUE.toBuffer()],
    ON_DEMAND_MAINNET_PID
  )[0];

/**
 * Devnet program and queue addresses
 *
 * These are the official Switchboard On-Demand addresses for devnet.
 * Use these for development and testing.
 */

/** Switchboard On-Demand program ID on devnet */
export const ON_DEMAND_DEVNET_PID = new web3.PublicKey(
  'Aio4gaXjXzJNVLtzwtNVmSqGKpANtXhybbkhtAC94ji2'
);

/** Guardian queue for devnet (internal use) */
export const ON_DEMAND_DEVNET_GUARDIAN_QUEUE = new web3.PublicKey(
  'BeZ4tU4HNe2fGQGUzJmNS2UU2TcZdMUUgnCH6RPg4Dpi'
);

/** Default oracle queue on devnet - use this for testing */
export const ON_DEMAND_DEVNET_QUEUE = new web3.PublicKey(
  'EYiAmGSdsQTuCw413V5BzaruWuCCSDgTPtBGvLkXHbe7'
);

/** Queue PDA for devnet (note: uses mainnet PID for compatibility) */
export const ON_DEMAND_DEVNET_QUEUE_PDA = web3.PublicKey.findProgramAddressSync(
  [Buffer.from('Queue'), ON_DEMAND_DEVNET_QUEUE.toBuffer()],
  ON_DEMAND_MAINNET_PID // SVM Devnet networks should be launched with SBond... as PID
)[0];

/**
 * Check if the connection is to the mainnet
 * @param connection - Connection: The connection
 * @returns - Promise<boolean> - Whether the connection is to the mainnet
 */
export async function isMainnetConnection(
  connection: web3.Connection
): Promise<boolean> {
  try {
    const genesisHash = await connection.getGenesisHash();
    return genesisHash === '5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d';
  } catch {
    return false;
  }
}

/**
 * Check if the connection is to the devnet
 * @param connection - Connection: The connection
 * @returns - Promise<boolean> - Whether the connection is to the devnet
 */
export async function isDevnetConnection(
  connection: web3.Connection
): Promise<boolean> {
  try {
    const genesisHash = await connection.getGenesisHash();
    return genesisHash === 'EtWTRABZaYq6iMfeYKouRu166VU2xqa1wcaWoxPkrZBG';
  } catch {
    return false;
  }
}

/**
 * Get the program ID for the Switchboard program based on the connection
 * @param connection - Connection: The connection
 * @returns - Promise<PublicKey> - The program ID
 */
export async function getProgramId(
  connection: web3.Connection
): Promise<web3.PublicKey> {
  const isDevnet = connection.rpcEndpoint.includes('devnet');
  return isDevnet ? ON_DEMAND_DEVNET_PID : ON_DEMAND_MAINNET_PID;
}

/**
 * Get the default devnet queue for the Switchboard program
 * @param solanaRPCUrl - (optional) string: The Solana RPC URL
 * @returns - Promise<Queue> - The default devnet queue
 */
export async function getDefaultDevnetQueue(
  solanaRPCUrl: string = 'https://api.devnet.solana.com'
): Promise<Queue> {
  return getQueue({
    solanaRPCUrl,
    queueAddress: ON_DEMAND_DEVNET_QUEUE.toString(),
  });
}

/**
 * Get the default devnet guardian queue for the Switchboard program
 * @param solanaRPCUrl - (optional) string: The Solana RPC URL
 * @returns - Promise<Queue> - The default devnet guardian queue
 */
export async function getDefaultDevnetGuardianQueue(
  solanaRPCUrl: string = 'https://api.devnet.solana.com'
): Promise<Queue> {
  return getQueue({
    solanaRPCUrl,
    queueAddress: ON_DEMAND_DEVNET_GUARDIAN_QUEUE.toString(),
  });
}

/**
 * Get the default queue address for the Switchboard program on Solana.
 *
 * @param isMainnet - boolean: Whether the connection is to the mainnet
 * @returns - web3.PublicKey: The default queue address
 */
export function getDefaultQueueAddress(isMainnet: boolean) {
  return isMainnet ? ON_DEMAND_MAINNET_QUEUE : ON_DEMAND_DEVNET_QUEUE;
}

/**
 * Gets the default Switchboard queue for the specified network
 *
 * Automatically detects whether you're on mainnet or devnet and returns
 * the appropriate default queue. This is the recommended way to get started
 * with Switchboard On-Demand.
 *
 * @param {string} solanaRPCUrl - Solana RPC endpoint URL (defaults to mainnet)
 * @returns {Promise<Queue>} The default queue instance
 *
 * @example
 * ```typescript
 * // Get default queue for current network
 * const queue = await getDefaultQueue();
 *
 * // Specify custom RPC
 * const queue = await getDefaultQueue('https://api.devnet.solana.com');
 * ```
 */
export async function getDefaultQueue(
  solanaRPCUrl: string = web3.clusterApiUrl('mainnet-beta')
): Promise<Queue> {
  const connection = new web3.Connection(solanaRPCUrl, 'confirmed');
  const isMainnet = await isMainnetConnection(connection);
  const queueAddress = getDefaultQueueAddress(isMainnet);
  return getQueue({ solanaRPCUrl, queueAddress });
}

/**
 * Get the default guardian queue for the Switchboard program
 * @param solanaRPCUrl - (optional) string: The Solana RPC URL
 * @returns - Promise<Queue> - The default guardian queue
 * @NOTE - SWITCHBOARD PID AND GUARDIAN QUEUE PUBKEY ARE WRONG
 */
export async function getDefaultGuardianQueue(
  solanaRPCUrl: string = web3.clusterApiUrl('mainnet-beta')
): Promise<Queue> {
  return getQueue({
    solanaRPCUrl,
    queueAddress: ON_DEMAND_MAINNET_GUARDIAN_QUEUE.toString(),
  });
}

/**
 * Get the queue for the Switchboard program
 * @param solanaRPCUrl - string: The Solana RPC URL
 * @param switchboardProgramId - string: The Switchboard program ID
 * @param queueAddress - string: The queue address
 * @returns - Promise<Queue> - The queue
 */
export async function getQueue(
  params: {
    queueAddress: string | web3.PublicKey;
  } & ({ solanaRPCUrl: string } | { program: Program })
): Promise<Queue> {
  const queue = new web3.PublicKey(params.queueAddress);
  const program =
    'program' in params
      ? params.program
      : await AnchorUtils.loadProgramFromConnection(
          new web3.Connection(params.solanaRPCUrl, 'confirmed')
        );
  return new Queue(program, queue);
}

/**
 * Get the unique LUT keys for the queue, all oracles in the queue, and all feeds
 * provided
 * @param queue - Queue: The queue
 * @param feeds - PullFeed[]: The feeds
 * @returns - Promise<PublicKey[]>: The unique LUT keys
 */
export async function fetchAllLutKeys(
  queue: Queue,
  feeds: PullFeed[]
): Promise<web3.PublicKey[]> {
  type LutOwner = {
    loadLookupTable: () => Promise<web3.AddressLookupTableAccount>;
  };

  const oracles = await queue.fetchOracleKeys();
  const lutOwners: LutOwner[] = [];
  lutOwners.push(queue);
  feeds.forEach(feed => lutOwners.push(feed));
  oracles.forEach(oracle => lutOwners.push(new Oracle(queue.program, oracle)));

  const lutPromises = lutOwners.map(lutOwner => lutOwner.loadLookupTable());
  const luts = await Promise.all(lutPromises);

  const keyset = new Set<web3.PublicKey>();
  luts.forEach(lut => lut.state.addresses.forEach(keyset.add));
  return Array.from(keyset).map(key => new web3.PublicKey(key));
}

/**
 * @deprecated Use CrossbarClient.storeOracleFeed() instead. This function uses the legacy v1 format.
 * @param queue Queue pubkey as base58 string
 * @param jobs Array of jobs to store (Oracle Jobs Object)
 * @param crossbarUrl
 * @returns
 */
export async function storeFeed(
  queue: string,
  jobs: IOracleJob[],
  crossbarUrl = CrossbarClient.default().crossbarUrl
): Promise<{
  cid: string;
  feedHash: string;
  queueHex: string;
}> {
  console.warn(
    '[Switchboard] Warning: storeFeed() is deprecated. Use CrossbarClient.storeOracleFeed() instead.'
  );
  const crossbar = crossbarUrl.endsWith('/')
    ? crossbarUrl.slice(0, -1)
    : crossbarUrl;

  const x = new CrossbarClient(crossbar);
  return await x.store(queue, jobs);
}

export async function getAssociatedTokenAddress(
  mint: web3.PublicKey,
  owner: web3.PublicKey,
  allowOwnerOffCurve = false,
  programId = SPL_TOKEN_PROGRAM_ID,
  associatedTokenProgramId = SPL_ASSOCIATED_TOKEN_ACCOUNT_PROGRAM_ID
): Promise<web3.PublicKey> {
  if (!allowOwnerOffCurve && !web3.PublicKey.isOnCurve(owner.toBuffer())) {
    throw new Error('TokenOwnerOffCurveError');
  }

  const [address] = await web3.PublicKey.findProgramAddress(
    [owner.toBuffer(), programId.toBuffer(), mint.toBuffer()],
    associatedTokenProgramId
  );

  return address;
}

export function getAssociatedTokenAddressSync(
  mint: web3.PublicKey,
  owner: web3.PublicKey,
  allowOwnerOffCurve = false,
  programId = SPL_TOKEN_PROGRAM_ID,
  associatedTokenProgramId = SPL_ASSOCIATED_TOKEN_ACCOUNT_PROGRAM_ID
): web3.PublicKey {
  if (!allowOwnerOffCurve && !web3.PublicKey.isOnCurve(owner.toBuffer())) {
    throw new Error('TokenOwnerOffCurveError');
  }

  const [address] = web3.PublicKey.findProgramAddressSync(
    [owner.toBuffer(), programId.toBuffer(), mint.toBuffer()],
    associatedTokenProgramId
  );

  return address;
}

export function getNodePayer(program: Program): web3.Keypair {
  return (program.provider as any).wallet.payer; // eslint-disable-line @typescript-eslint/no-explicit-any
}

// Re-export SPL Token constants
export { SPL_TOKEN_PROGRAM_ID as TOKEN_PROGRAM_ID } from '../constants.js';
export { SOL_NATIVE_MINT as NATIVE_MINT } from '../constants.js';
