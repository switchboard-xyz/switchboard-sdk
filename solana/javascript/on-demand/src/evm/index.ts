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

import { keccak_256 } from '@noble/hashes/sha3';
import {
  createFallbackOracleInfo,
  CrossbarClient,
  evaluateRandomnessOracleCandidate,
  fetchHealthyOracleSnapshots,
  getActiveRandomnessEvmDeployment,
  getCrossbarOracleNetworkForEvmChainId,
  IOracleJob,
  isRandomnessOracleCandidateEligible,
  type MergedHealthyOracleIndex,
  mergeHealthyOracleSnapshots,
  type OracleHealthData,
  type OracleInfo,
  OracleJob,
  type RandomnessOracleSelectionMetadata,
  type RandomnessOracleSelectorCandidate,
  selectRandomnessOracle as selectRandomnessOracleCandidate,
} from '@switchboard-xyz/common';
import axios from 'axios';
import { Buffer } from 'buffer';

export * as message from './message.js';

// Common options for feed updates
export interface FeedUpdateCommonOptions {
  jobs: OracleJob[]; // Array of job definitions
  numSignatures?: number; // Number of signatures to fetch
  maxVariance?: number; // Maximum variance scaled by 1e9 (1e9 = 1%)
  minResponses?: number; // Minimum number of responses to consider the feed valid, unscaled
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
  /** Minimum number of responses, unscaled. */
  minResponses?: number;
  /** Maximum variance scaled by 1e9 (1e9 = 1%). */
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
  /** Minimum number of responses, unscaled. */
  minResponses?: number;
  /** Maximum variance scaled by 1e9 (1e9 = 1%). */
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

export interface SelectEvmRandomnessOracleArgs {
  chainId: number;
  crossbarUrl?: string;
}

export interface EvmRandomnessOracleCandidate
  extends RandomnessOracleSelectorCandidate {
  authority: string;
  pubkey: string;
  secp256k1Key: string;
  signingAddress: string;
}

export interface EvmRandomnessOracleSelection {
  candidate: EvmRandomnessOracleCandidate;
  deploymentId: string;
  metadata: RandomnessOracleSelectionMetadata;
  network: 'mainnet' | 'devnet';
  oracle: string;
}

export interface EvmRandomnessOracleInspection
  extends EvmRandomnessOracleSelection {
  candidates: EvmRandomnessOracleCandidate[];
  liveHealth: MergedHealthyOracleIndex;
  oracles: OracleInfo[];
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

function normalizeUnixTimestamp(value?: number): number | undefined {
  const numericValue = value ?? Number.NaN;
  if (!Number.isFinite(numericValue)) {
    return undefined;
  }

  return numericValue > 1_000_000_000_000
    ? Math.floor(numericValue / 1000)
    : numericValue;
}

function resolveGatewayUrl(
  oracle: OracleInfo,
  healthData?: OracleHealthData
): string | undefined {
  return oracle.gatewayUrl ?? healthData?.oracle_config?.gateway_ingress;
}

function trimHexPrefix(value: string): string {
  return value.replace(/^0x/i, '');
}

function normalizeExplicitSigningAddress(
  signingAddress?: string | null
): string | undefined {
  const normalized = signingAddress?.trim().toLowerCase();
  return normalized && normalized.length > 0 ? normalized : undefined;
}

function normalizeUncompressedSecp256k1Key(
  secp256k1Key?: string | null
): string | undefined {
  let key = trimHexPrefix(secp256k1Key?.trim() ?? '');
  if (key.length === 130 && key.slice(0, 2).toLowerCase() === '04') {
    key = key.slice(2);
  }

  if (key.length !== 128 || !/^[0-9a-fA-F]+$/.test(key)) {
    return undefined;
  }

  return key;
}

export function deriveEvmAddressFromSecp256k1Key(
  secp256k1Key?: string | null
): string | undefined {
  const normalizedKey = normalizeUncompressedSecp256k1Key(secp256k1Key);
  if (!normalizedKey) {
    return undefined;
  }

  const hash = keccak_256(Buffer.from(normalizedKey, 'hex'));
  return `0x${Buffer.from(hash.slice(-20)).toString('hex')}`;
}

export function resolveEvmOracleSigningAddress(
  oracle: Pick<OracleInfo, 'signingAddress' | 'secp256k1Key'>
): string | undefined {
  return (
    normalizeExplicitSigningAddress(oracle.signingAddress) ??
    deriveEvmAddressFromSecp256k1Key(oracle.secp256k1Key)
  );
}

export function buildEvmRandomnessOracleCandidate(
  oracle: OracleInfo,
  healthData?: OracleHealthData
): EvmRandomnessOracleCandidate {
  const lastHeartbeatUnix = normalizeUnixTimestamp(
    healthData?.oracle_config?.system_time
  );
  const fallbackCandidate = createFallbackOracleInfo(oracle);
  const signingAddress = resolveEvmOracleSigningAddress(oracle);
  const gatewayUrl = resolveGatewayUrl(oracle, healthData);

  return {
    ...fallbackCandidate,
    oracleId: signingAddress ?? fallbackCandidate.oracleId,
    authority: oracle.authority,
    pubkey: oracle.pubkey,
    secp256k1Key: oracle.secp256k1Key,
    signingAddress: signingAddress ?? '',
    gatewayUrl,
    version:
      healthData?.oracle_config?.version ??
      oracle.version ??
      fallbackCandidate.version,
    liveHealthy: Boolean(healthData),
    heartbeatFresh: healthData ? true : fallbackCandidate.heartbeatFresh,
    // Crossbar expirationTime tracks discovery freshness, not enclave quote
    // validity, so EVM randomness keeps quote freshness independent of it.
    quoteFresh: fallbackCandidate.quoteFresh,
    restricted:
      healthData?.oracle_config?.restricted ??
      oracle.restricted ??
      fallbackCandidate.restricted,
    gatewayEnabled: healthData
      ? Boolean(gatewayUrl)
      : fallbackCandidate.gatewayEnabled,
    pullOracleEnabled: healthData
      ? healthData.oracle_config?.enable_pull_oracle === 1
      : fallbackCandidate.pullOracleEnabled,
    lastHeartbeatUnix,
    validUntilUnix: undefined,
    activeConnections: healthData?.active_connections,
    totalSubscriptions: healthData?.total_subscriptions,
    totalFeeds: healthData?.total_feeds,
  };
}

export function selectStrictEvmRandomnessOracleCandidate(
  candidates: EvmRandomnessOracleCandidate[]
): {
  candidate: EvmRandomnessOracleCandidate;
  metadata: RandomnessOracleSelectionMetadata;
} {
  if (candidates.length === 0) {
    throw new Error('No randomness oracle candidates were provided');
  }

  const evaluations = candidates.map(evaluateRandomnessOracleCandidate);
  const liveEligibleCandidates = candidates.filter(
    candidate =>
      candidate.liveHealthy && isRandomnessOracleCandidateEligible(candidate)
  );

  if (liveEligibleCandidates.length === 0) {
    throw new Error('No eligible randomness oracle candidates were found');
  }

  const liveSelection = selectRandomnessOracleCandidate(liveEligibleCandidates);
  const majorityVersion = liveSelection.metadata.majorityVersion;

  for (const evaluation of evaluations) {
    if (evaluation.oracleId === liveSelection.candidate.oracleId) {
      evaluation.tier = 'live';
    } else if (
      majorityVersion !== null &&
      evaluation.version !== null &&
      evaluation.version !== majorityVersion
    ) {
      evaluation.rejectionReasons = [
        ...evaluation.rejectionReasons,
        'version-mismatch',
      ];
    }
  }

  return {
    candidate: liveSelection.candidate,
    metadata: {
      tier: 'live',
      majorityVersion,
      liveHealthyCandidateCount: liveEligibleCandidates.length,
      fallbackCandidateCount: 0,
      evaluations,
    },
  };
}

/**
 * Select a healthy EVM randomness oracle from the active deployment inventory.
 *
 * This requires live gateway health and does not fall back to inventory-only
 * candidates.
 */
export async function inspectRandomnessOracleSelection({
  chainId,
  crossbarUrl,
}: SelectEvmRandomnessOracleArgs): Promise<EvmRandomnessOracleInspection> {
  const deployment = getActiveRandomnessEvmDeployment(chainId);
  if (!deployment) {
    throw new Error(
      `Unsupported active randomness EVM deployment for chainId ${chainId}`
    );
  }

  const network = getCrossbarOracleNetworkForEvmChainId(chainId);
  const crossbar = new CrossbarClient(getCrossbarUrl(crossbarUrl));
  const oracles = (await crossbar.fetchOracles(network)) as OracleInfo[];

  if (oracles.length === 0) {
    throw new Error(
      `No Crossbar oracle inventory found for ${deployment.id} (${network})`
    );
  }

  const gatewayUrls = await crossbar
    .fetchGateways(network)
    .catch(() =>
      oracles
        .map(oracle => oracle.gatewayUrl)
        .filter((gatewayUrl): gatewayUrl is string => Boolean(gatewayUrl))
    );
  const healthySnapshots = await fetchHealthyOracleSnapshots(gatewayUrls);
  const mergedSnapshots = mergeHealthyOracleSnapshots(healthySnapshots);
  const candidates = oracles
    .map(oracle =>
      buildEvmRandomnessOracleCandidate(
        oracle,
        mergedSnapshots.bySecp256k1Key.get(oracle.secp256k1Key)
      )
    )
    .filter(candidate => candidate.signingAddress.length > 0);

  if (candidates.length === 0) {
    throw new Error(
      `No EVM randomness oracles exposed a signing address for ${deployment.id}`
    );
  }

  const { candidate, metadata } =
    selectStrictEvmRandomnessOracleCandidate(candidates);

  return {
    candidate,
    candidates,
    deploymentId: deployment.id,
    liveHealth: mergedSnapshots,
    metadata,
    network,
    oracle: candidate.signingAddress,
    oracles,
  };
}

/**
 * Select a healthy EVM randomness oracle from the active deployment inventory.
 *
 * This requires live gateway health and does not fall back to inventory-only
 * candidates.
 */
export async function selectRandomnessOracle({
  chainId,
  crossbarUrl,
}: SelectEvmRandomnessOracleArgs): Promise<EvmRandomnessOracleSelection> {
  const inspection = await inspectRandomnessOracleSelection({
    chainId,
    crossbarUrl,
  });

  return {
    candidate: inspection.candidate,
    deploymentId: inspection.deploymentId,
    metadata: inspection.metadata,
    network: inspection.network,
    oracle: inspection.oracle,
  };
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
 * @param minResponses Minimum number of responses, unscaled
 * @param maxVariance Maximum variance scaled by 1e9 (1e9 = 1%)
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
  minResponses = 1,
  maxVariance = 1e9,
  numSignatures = 1,
  syncOracles = true,
  syncGuardians = true,
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
