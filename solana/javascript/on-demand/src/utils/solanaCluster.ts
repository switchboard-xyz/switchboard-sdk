import { web3 } from '@coral-xyz/anchor-31';
import { CrossbarNetwork } from '@switchboard-xyz/common';
import { Buffer } from 'buffer';

export type SupportedSolanaCluster = 'mainnet' | 'devnet';

export const SOLANA_MAINNET_BETA_GENESIS_HASH =
  '5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d';
export const SOLANA_DEVNET_GENESIS_HASH =
  'EtWTRABZaYq6iMfeYKouRu166VU2xqa1wcaWoxPkrZBG';

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
  ON_DEMAND_MAINNET_PID
)[0];

export class UnsupportedSolanaClusterError extends Error {
  constructor(context: string, details?: string) {
    const extra = details ? ` ${details}` : '';
    super(
      `${context} supports automatic Switchboard defaults only for official Solana mainnet-beta and devnet.${extra} Pass explicit local/custom program and queue addresses, or explicitly force official mainnet/devnet defaults where supported.`
    );
    this.name = 'UnsupportedSolanaClusterError';
  }
}

export function normalizeSupportedSolanaCluster(
  cluster: string,
  context: string
): SupportedSolanaCluster {
  if (cluster === 'mainnet' || cluster === 'mainnet-beta') {
    return 'mainnet';
  }
  if (cluster === 'devnet') {
    return 'devnet';
  }
  throw new UnsupportedSolanaClusterError(
    context,
    `Unsupported network "${cluster}".`
  );
}

export async function detectSupportedSolanaCluster(
  connection: Pick<web3.Connection, 'getGenesisHash'>
): Promise<SupportedSolanaCluster | null> {
  try {
    const genesisHash = await connection.getGenesisHash();
    if (genesisHash === SOLANA_MAINNET_BETA_GENESIS_HASH) {
      return 'mainnet';
    }
    if (genesisHash === SOLANA_DEVNET_GENESIS_HASH) {
      return 'devnet';
    }
  } catch {
    return null;
  }
  return null;
}

export async function requireSupportedSolanaCluster(
  connection: Pick<web3.Connection, 'getGenesisHash' | 'rpcEndpoint'>,
  context: string
): Promise<SupportedSolanaCluster> {
  const cluster = await detectSupportedSolanaCluster(connection);
  if (cluster) {
    return cluster;
  }

  const rpcSuffix =
    typeof connection.rpcEndpoint === 'string'
      ? ` RPC endpoint: ${connection.rpcEndpoint}.`
      : '';
  throw new UnsupportedSolanaClusterError(context, rpcSuffix);
}

export function getOfficialClusterForProgramId(
  programId: web3.PublicKey
): SupportedSolanaCluster | null {
  if (programId.equals(ON_DEMAND_MAINNET_PID)) {
    return 'mainnet';
  }
  if (programId.equals(ON_DEMAND_DEVNET_PID)) {
    return 'devnet';
  }
  return null;
}

export function getProgramIdForCluster(
  cluster: SupportedSolanaCluster
): web3.PublicKey {
  return cluster === 'mainnet' ? ON_DEMAND_MAINNET_PID : ON_DEMAND_DEVNET_PID;
}

export function getDefaultQueueAddressForCluster(
  cluster: SupportedSolanaCluster
): web3.PublicKey {
  return cluster === 'mainnet' ? ON_DEMAND_MAINNET_QUEUE : ON_DEMAND_DEVNET_QUEUE;
}

export function getDefaultGuardianQueueAddressForCluster(
  cluster: SupportedSolanaCluster
): web3.PublicKey {
  return cluster === 'mainnet'
    ? ON_DEMAND_MAINNET_GUARDIAN_QUEUE
    : ON_DEMAND_DEVNET_GUARDIAN_QUEUE;
}

export function getCrossbarNetworkForCluster(
  cluster: SupportedSolanaCluster
): CrossbarNetwork {
  return cluster === 'mainnet'
    ? CrossbarNetwork.SolanaMainnet
    : CrossbarNetwork.SolanaDevnet;
}

export function getCrossbarGatewayNetwork(
  cluster: SupportedSolanaCluster
): 'mainnet' | 'devnet' {
  return cluster === 'mainnet' ? 'mainnet' : 'devnet';
}
