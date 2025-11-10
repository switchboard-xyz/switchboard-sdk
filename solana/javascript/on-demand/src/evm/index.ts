import type { Queue } from '../accounts/index.js';
import type {
  BridgeEnclaveResponse,
  FeedEvalResponse,
  Gateway,
} from '../oracle-interfaces/index.js';

import {
  createAttestationHexString,
  createV0AttestationHexString,
} from './message.js';

import { CrossbarClient, IOracleJob, OracleJob } from '@switchboard-xyz/common';
import axios from 'axios';
import { Buffer } from 'buffer';

export * as message from './message.js';

// Common options for feed updates
export interface FeedUpdateCommonOptions {
  jobs: OracleJob[]; // Array of job definitions
  numSignatures?: number; // Number of signatures to fetch
  maxVariance?: number; // Maximum variance allowed for the feed
  minResponses?: number; // Minimum number of responses to consider the feed valid
  recentHash?: string; // Hex string of length 64 (32 bytes) which does not start with 0x
  aggregatorId?: string; // Specify the aggregator ID if the feed already exists
  blockNumber?: number; // The block number
  gateway: Gateway; // Gateway
}

// Define a type for the input parameters
export type FeedUpdateParams = FeedUpdateCommonOptions;

// Attestation options
export interface AttestationOptions {
  guardianQueue: Queue; // The guardian queue account
  recentHash: string; // The blockhash to get the attestation for
  blockNumber: number; // The timestamp
  queueId: string; // The queue ID (queue pubkey as hex)
  oracleId: string; // The oracle ID (oracle pubkey as hex)
  gateway: Gateway; // The gateway account
}

// Feed simulation result
export interface FeedSimulateResult {
  result: string;
  feedId: string;
  response: FeedEvalResponse;
}

// Feed update result
export interface FeedUpdateResult {
  feedId: string;
  result: string;
  encoded: string;
  response: FeedEvalResponse;
}

// Attestation result
export interface AttestationResult {
  oracleId: string; // Attestee oracle pubkey as hex
  queueId: string; // Attestee queue pubkey as hex
  guardian: string; // Guardian pubkey as hex
  encoded: string; // The attestation as a hex string
  response: BridgeEnclaveResponse; // The attestation response from guardian
}

// Fetch feed response
export interface FetchFeedResponse {
  results: FeedEvalResponse[];
  encoded: string[];
}

// Fetch randomness response
export interface FetchRandomnessResponse {
  encoded: string;
  response: {
    signature: string;
    recovery_id: number;
    value: string;
  };
}

// Fetch result response
export interface FetchResultResponse extends FetchFeedResponse {
  feedId: string;
}

// Fetch results response
export interface FetchResultsArgs {
  feedIds: string[];
  chainId: number;
  crossbarUrl?: string;
  minResponses?: number;
  maxVariance?: number;
  numSignatures?: number;
  syncOracles?: boolean;
  syncGuardians?: boolean;
  gateway?: string;
}

// Fetch result args
export interface FetchResultArgs {
  feedId: string;
  chainId: number;
  crossbarUrl?: string;
  minResponses?: number;
  maxVariance?: number;
  numSignatures?: number;
  syncOracles?: boolean;
  syncGuardians?: boolean;
  gateway?: string;
}

// Feed evaluation response
export interface FetchRandomnessArgs {
  chainId: number;
  crossbarUrl: string;
  randomnessId: string;
  timestamp?: number;
  minStalenessSeconds?: number;
}

/**
 * Get an oracle job from object definition
 * @param params the job parameters
 * @returns
 */
export function createJob(params: IOracleJob): OracleJob {
  return OracleJob.fromObject(params);
}

function getCrossbarUrl(crossbarUrl?: string): string {
  return crossbarUrl ?? CrossbarClient.default().crossbarUrl;
}

/**
 * Get attestation for a particular oracle on a particular queue
 * @param options - AttestationOptions: Options for the attestation
 * @returns - Promise<string> - The attestation as a hex string
 */
export async function getAttestation(
  options: AttestationOptions
): Promise<AttestationResult> {
  const { recentHash, queueId, oracleId, gateway, blockNumber } = options;
  const gatewayAccount = gateway;
  const chainHash = recentHash.startsWith('0x')
    ? recentHash.slice(2)
    : recentHash;
  const attestation = await gatewayAccount!.fetchBridgingMessage({
    chainHash,
    queuePubkey: queueId,
    oraclePubkey: oracleId,
  });

  if (!options.recentHash) {
    options.recentHash = '0'.repeat(64);
  }

  // slice if the recentHash starts with 0x
  if (options.recentHash.startsWith('0x')) {
    options.recentHash = options.recentHash.slice(2);
  }

  // Decode from Base64 to a Buffer
  const signatureBuffer = new Uint8Array(
    Buffer.from(attestation.signature, 'base64')
  );

  // Assuming each component (r and s) is 32 bytes long
  const r = Buffer.from(signatureBuffer.slice(0, 32)).toString('hex');
  const s = Buffer.from(signatureBuffer.slice(32, 64)).toString('hex');
  const v = attestation.recovery_id;

  // Create the attestation bassed on message contents (it'll either be v0 or ordinary)
  if (attestation.oracle_ed25519_enclave_signer) {
    const hexString = createV0AttestationHexString({
      discriminator: 2,
      oracleId,
      queueId,
      ed25519Key: attestation.oracle_ed25519_enclave_signer,
      secp256k1Key: attestation.oracle_secp256k1_enclave_signer,
      r,
      s,
      v,
      mrEnclave: attestation.mr_enclave,
      blockNumber: blockNumber.toString(),
    });

    return {
      oracleId,
      queueId,
      guardian: attestation.guardian,
      encoded: hexString,
      response: attestation,
    };
  } else if (attestation.timestamp) {
    const hexString = createAttestationHexString({
      discriminator: 2,
      oracleId,
      queueId,
      secp256k1Key: attestation.oracle_secp256k1_enclave_signer,
      timestamp: attestation.timestamp.toString(),
      mrEnclave: attestation.mr_enclave,
      r,
      s,
      v,
      blockNumber: blockNumber.toString(),
      guardianId: attestation.guardian,
    });

    return {
      oracleId: attestation.oracle,
      queueId: attestation.queue,
      guardian: attestation.guardian,
      encoded: hexString,
      response: attestation,
    };
  }
  throw new Error('Invalid attestation response');
}

/**
 * Crossbar API for EVM
 */

/**
 * Fetch result from the Switchboard API
 * @param param0 The parameters to fetch results
 * @returns
 */
export async function fetchResult({
  feedId,
  chainId,
  crossbarUrl,
  minResponses,
  maxVariance,
  numSignatures,
  syncOracles,
  syncGuardians,
}: FetchResultArgs): Promise<FetchResultResponse> {
  return {
    feedId,
    ...(await fetchUpdateData(
      getCrossbarUrl(crossbarUrl),
      chainId.toString(),
      feedId,
      minResponses,
      maxVariance,
      numSignatures,
      syncOracles,
      syncGuardians
    )),
  };
}

/**
 * Fetch results from the Switchboard API
 * @param param0 The parameters to fetch results
 * @returns
 */
export async function fetchResults({
  feedIds,
  chainId,
  crossbarUrl,
  minResponses,
  maxVariance,
  numSignatures,
  syncOracles,
  syncGuardians,
}: FetchResultsArgs): Promise<FetchResultResponse[]> {
  if (!crossbarUrl) crossbarUrl = CrossbarClient.default().crossbarUrl;

  const responses = await Promise.all(
    feedIds.map(feedId => {
      return fetchUpdateData(
        crossbarUrl,
        chainId.toString(),
        feedId,
        minResponses,
        maxVariance,
        numSignatures,
        syncOracles,
        syncGuardians
      );
    })
  );

  return responses.map((response, index) => {
    return {
      feedId: feedIds[index],
      ...response,
    };
  });
}

/**
 * Fetch data to settle randomness
 * @param param0 The parameters to fetch randomness
 * @returns
 */
export async function fetchRandomness({
  chainId,
  crossbarUrl,
  randomnessId,
  timestamp,
  minStalenessSeconds,
}: FetchRandomnessArgs): Promise<{
  encoded: string;
  response: {
    signature: string;
    recovery_id: number;
    value: string;
  };
}> {
  if (!crossbarUrl) {
    crossbarUrl = 'https://crossbar.switchboard.xyz';
  }

  return fetchRandomnessData(
    crossbarUrl,
    chainId.toString(),
    randomnessId,
    timestamp,
    minStalenessSeconds
  );
}

/**
 * Fetch update data from the Switchboard API
 * @param crossbarUrl The Crossbar URL
 * @param chainId The chain ID
 * @param feedId The feed ID
 * @param minResponses Minimum number of responses
 * @param maxVariance Maximum variance
 * @param numSignatures Number of signatures
 * @param syncOracles Sync oracles
 * @param syncGuardians Sync guardians
 * @param gateway Gateway
 * @returns
 */
async function fetchUpdateData(
  crossbarUrl: string,
  chainId: string,
  feedId: string,
  minResponses: number = 1,
  maxVariance: number = 1e9,
  numSignatures: number = 1,
  syncOracles: boolean = true,
  syncGuardians: boolean = true,
  gateway?: string
): Promise<FetchFeedResponse> {
  const cleanedCrossbarUrl = crossbarUrl.endsWith('/')
    ? crossbarUrl.slice(0, -1)
    : crossbarUrl;

  const url = new URL(`${cleanedCrossbarUrl}/updates/evm/${chainId}/${feedId}`);

  // Add query parameters to the URL
  if (minResponses !== undefined) {
    url.searchParams.append('minResponses', minResponses.toString());
  }
  if (maxVariance !== undefined) {
    url.searchParams.append('maxVariance', maxVariance.toString());
  }
  if (numSignatures !== undefined) {
    url.searchParams.append('numSignatures', numSignatures.toString());
  }
  if (syncOracles !== undefined) {
    url.searchParams.append('syncOracles', syncOracles.toString());
  }
  if (syncGuardians !== undefined) {
    url.searchParams.append('syncGuardians', syncGuardians.toString());
  }
  if (gateway !== undefined) {
    url.searchParams.append('gateway', gateway);
  }

  try {
    const response = await axios.get(url.toString());
    if (response.status !== 200) {
      throw new Error(`Error fetching data: ${response.statusText}`);
    }
    return response.data as FetchFeedResponse;
  } catch (error) {
    console.error('Error fetching feed data:', error);
    throw error;
  }
}

/**
 * Fetch randomness data from the Switchboard API
 * @param chainId The chain ID
 * @param randomnessId The randomness ID configured on-chain
 * @param timestamp The timestamp that the randomness was configured at
 * @param minStalenessSeconds The minimum staleness of the data in seconds
 * @returns
 */
async function fetchRandomnessData(
  crossbarUrl: string,
  chainId: string,
  randomnessId: string,
  timestamp?: number,
  minStalenessSeconds?: number
): Promise<FetchRandomnessResponse> {
  const cleanedCrossbarUrl = crossbarUrl.endsWith('/')
    ? crossbarUrl.slice(0, -1)
    : crossbarUrl;
  const url = new URL(
    `${cleanedCrossbarUrl}/randomness/evm/${chainId}/${randomnessId}`
  );

  // Add query parameters to the URL
  if (timestamp !== undefined) {
    url.searchParams.append('timestamp', timestamp.toString());
  }
  if (minStalenessSeconds !== undefined) {
    url.searchParams.append(
      'minStalenessSeconds',
      minStalenessSeconds.toString()
    );
  }

  try {
    const response = await axios.get(url.toString());
    if (response.status !== 200) {
      throw new Error(`Error fetching data: ${response.statusText}`);
    }
    return response.data as FetchRandomnessResponse;
  } catch (error) {
    console.error('Error fetching randomness data:', error);
    throw error;
  }
}
