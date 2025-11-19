/**
 * Crossbar API types
 */

import type { IOracleJob } from '../protos.js';

import type { FeedEvalResponse } from './gateway.js';

/**
 * Crossbar feed fetch response
 */
export type CrossbarFetchResponse = {
  feedHash: string;
  queueHex: string;
  jobs: IOracleJob[];
};

/**
 * Crossbar oracle feed fetch response
 */
export type CrossbarOracleFeedFetchResponse = {
  cid: string;
  data: string;
  size: number;
  version: string;
};

/**
 * Crossbar simulation response (V1)
 */
export type CrossbarSimulateResponse = {
  feedHash: string;
  results: (number | string)[] | null;
  receipts: unknown[] | null;
  error?: string;
};

/**
 * Crossbar simulation response (V2)
 */
export type CrossbarSimulateV2Response = {
  feedHash: string;
  feedName?: string;
  results: unknown[] | null;
  receipts: unknown[] | null;
  error?: string;
  variableOverrides?: Record<string, string>;
  network?: string;
};

/**
 * Crossbar simulation response for protobuf feeds
 */
export type CrossbarSimulateProtoResponse = {
  feedId: string;
  feedName?: string;
  results: unknown[] | null;
  receipts: unknown[] | null;
  error?: string;
  variableOverrides?: Record<string, string>;
  network?: string;
};

/**
 * Request to simulate oracle jobs
 */
export type SimulateJobsRequest = {
  jobs: IOracleJob[];
  includeReceipts?: boolean;
  variableOverrides?: Record<string, string>;
};

/**
 * Response from job simulation
 */
export type SimulateJobsResponse = {
  results: unknown[] | null;
  receipts?: unknown[] | null;
  error?: string;
};

/**
 * Oracle information
 */
export type OracleInfo = {
  pubkey: string;
  secp256k1Key: string;
  authority: string;
  queue: string;
  mrEnclave: string;
  expirationTime: number;
};

/**
 * Request to fetch signatures for a feed
 */
export type FetchSignaturesRequest = {
  feedHash: string;
  numSignatures: number;
  maxVariance: number;
  minResponses: number;
  useTimestamp?: boolean;
  gateway?: string;
  recentHash?: string;
};

/**
 * CrossbarClient's internal FeedRequest type for API communication
 * Note: This is different from Gateway's FeedRequest type
 */
export type FeedRequest = {
  // For FeedRequestV1
  jobsB64Encoded?: string[];
  maxVariance?: number;
  minResponses?: number;
  // For FeedRequestV2
  feedProtoB64?: string;
};

/**
 * Request to fetch consensus signatures
 */
export type FetchSignaturesConsensusRequest = {
  apiVersion: string;
  recentHash?: string;
  signatureScheme: string;
  hashScheme: string;
  feedRequests: FeedRequest[];
  numOracles: number;
  useTimestamp?: boolean;
};

/**
 * Response from consensus signature fetch
 */
export type FetchSignaturesConsensusResponse = {
  medianResponses: { value: string; feedHash: string; numOracles: number }[];
  oracleResponses: {
    oraclePublickey: string;
    ethAddress: string;
    signature: string;
    checksum: string;
    recoveryId: number;
    feedResponses: FeedEvalResponse[];
    errors: (string | null)[];
  }[];
  timestamp?: number;
};

/**
 * Query parameters for V2 update endpoint
 */
export interface V2UpdateQuery {
  network?: string;
  signature_scheme?: string;
  hash_scheme?: string;
  num_oracles?: number;
  use_timestamp?: boolean;
  recent_hash?: string;
  gateway?: string;
  chain?: string;
}

/**
 * Response structure for V2 update endpoint
 */
export interface V2UpdateResponse {
  medianResponses: V2MedianResponse[];
  oracleResponses: V2ConsensusOracleResponse[];
  timestamp: number;
  slot: number;
  recentHash: string;
  queue?: string;
  encoded?: string;
}

/**
 * Median response structure for V2 update
 */
export interface V2MedianResponse {
  value: string;
  feedHash: string;
  numOracles: number;
}

/**
 * Oracle response structure for V2 update with consensus
 */
export interface V2ConsensusOracleResponse {
  oraclePubkey: string;
  ethAddress: string;
  signature: string;
  checksum: string;
  recoveryId?: number;
  oracleId?: string;
  feedResponses: FeedEvalResponse[];
  errors: (string | null)[];
}
