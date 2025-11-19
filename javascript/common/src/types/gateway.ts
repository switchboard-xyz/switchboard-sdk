/**
 * Gateway API types
 */

import type { IOracleFeed, IOracleJob } from '../protos.js';

/**
 * Configuration for a feed request to oracle operators (V1)
 */
export type FeedRequestV1 = {
  /** Maximum allowed variance between oracle responses (e.g., 1.0 = 100%) */
  maxVariance?: number;
  /** Minimum number of oracle responses required */
  minResponses?: number;
  /** Array of oracle job definitions */
  jobs: IOracleJob[];
};

/**
 * Configuration for a feed request to oracle operators (V2)
 * Uses protobuf-encoded feed instead of individual job definitions
 */
export type FeedRequestV2 = {
  /** Oracle feed proto definition */
  feed: IOracleFeed;
};

/**
 * Feed request type - either V1 (individual jobs) or V2 (protobuf feed)
 */
export type FeedRequest = FeedRequestV1 | FeedRequestV2;

/**
 * The response from the gateway after fetching signatures
 * Variables are snake_case for serialization
 */
export type FeedEvalResponse = {
  /** Hex encoded oracle pubkey */
  oracle_pubkey: string;
  /** Hex encoded queue pubkey */
  queue_pubkey: string;
  /** Hex encoded oracle signing pubkey */
  oracle_signing_pubkey: string;
  /** Hex encoded feed id */
  feed_hash: string;
  /** Hex encoded blockhash/slothash the response was signed with */
  recent_hash: string;
  /** Errors encountered while fetching feed value */
  failure_error: string;
  /** Feed values derived */
  success_value: string;
  /** Signed message of the result and blockhash */
  msg: string;
  /**
   * Oracle signature of the result and blockhash
   * Sha256(success_feed_hashes || results || slothash)
   */
  signature: string;
  recovery_id: number;
  /** If the feed fetch failed, get other recent successes */
  recent_successes_if_failed: Array<FeedEvalResponse>;
  /** Timestamp marking when the result was fetched */
  timestamp?: number;
  /** Minimum number of oracle samples required */
  min_oracle_samples?: number;
  /** Receipts from oracle job execution */
  receipts?: unknown[];
};

/**
 * Response from fetching signatures from multiple feeds
 */
export type FeedEvalManyResponse = {
  responses: FeedEvalResponse[];
};

/**
 * Response from fetching signatures from multiple oracles
 */
export type FetchSignaturesMultiResponse = {
  responses: FeedEvalResponse[];
};

/**
 * Response from batch feed evaluation
 */
export type FeedEvalBatchResponse = {
  results: FeedEvalResponse[];
};

/**
 * Response from batch signature fetching
 */
export type FetchSignaturesBatchResponse = {
  oracle_responses: {
    oracle_pubkey: string;
    eth_address: string;
    signature: string;
    checksum: string;
    recovery_id: number;
    ed25519_enclave_signer: string;
    feed_responses: FeedEvalResponse[];
    errors: string[];
    oracle_idx: number;
  }[];
  failed_oracle_responses: {
    oracle_pubkey: string;
    eth_address: string;
    signature: string;
    checksum: string;
    recovery_id: number;
    ed25519_enclave_signer: string;
    feed_responses: FeedEvalResponse[];
    errors: string[];
    oracle_idx: number;
  }[];
  recent_hash: string;
  slot: number;
};

/**
 * Response from consensus signature fetch
 */
export type FetchSignaturesConsensusResponse = {
  median_responses: {
    value: string;
    feed_hash: string;
    num_successful_responses: number;
    min_oracle_samples: number;
  }[];
  oracle_responses: {
    oracle_pubkey: string;
    eth_address: string;
    signature: string;
    checksum: string;
    recovery_id: number;
    ed25519_enclave_signer: string;
    feed_responses: FeedEvalResponse[];
    errors: string[];
    oracle_idx: number;
  }[];
  failed_oracle_responses: {
    oracle_pubkey: string;
    eth_address: string;
    signature: string;
    checksum: string;
    recovery_id: number;
    ed25519_enclave_signer: string;
    feed_responses: FeedEvalResponse[];
    errors: string[];
    oracle_idx: number;
  }[];
  recent_hash: string;
  slot: number;
};

/**
 * The response from the gateway after revealing randomness
 * Variables are snake_case for serialization
 */
export type RandomnessRevealResponse = {
  randomness_pubkey: string;
  randomness: string;
  signature: string;
  recovery_id: number;
  value: string;
};

/**
 * The response from the gateway after attesting an enclave
 * Variables are snake_case for serialization
 */
export type AttestEnclaveResponse = {
  enclave_signer: string;
  signing_address: string;
  quote: string;
};

/**
 * Health check response from gateway/oracle test endpoints
 */
export type PingResponse = {
  status: string;
  version?: string;
  timestamp?: number;
};

/**
 * The Quote info from the gateway_fetch_quote endpoint
 */
export type FetchQuoteResponse = {
  enclave_signer: string;
  mr_enclave: string;
  verification_timestamp: number;
  valid_until: number;
  is_on_queue: boolean;
  queue_idx: number;
};

/**
 * Bridge enclave response
 */
export interface BridgeEnclaveResponse {
  /**
   * The guardian's public key
   */
  guardian: string;

  /**
   * The oracle's public key
   */
  oracle: string;

  /**
   * The queue (pubkey) that the oracle belongs to
   */
  queue: string;

  /**
   * The enclave measurement for the oracle
   */
  mr_enclave: string;

  /**
   * The chain hash read on the guardian
   */
  chain_hash: string;

  /**
   * The secp256k1 enclave signer for the oracle
   */
  oracle_secp256k1_enclave_signer: string;

  /**
   * The checksum of the attestation message
   */
  msg: string;

  /**
   * (UNUSED) The attestation message before being hashed
   */
  msg_prehash: string;

  /**
   * The ed25519 enclave signer for the oracle
   */
  oracle_ed25519_enclave_signer?: string;

  /**
   * The timestamp of the attestation
   */
  timestamp?: number;

  /**
   * The signature from the guardian
   */
  signature: string;
  recovery_id: number;
}

/**
 * Health metrics for a single oracle
 * Variables are snake_case for serialization
 */
export interface OracleHealthData {
  /** Base URL of the oracle (e.g., "http://oracle1.example.com") */
  oracle_url: string;
  /** Number of active WebSocket connections from clients */
  active_connections: number;
  /** Number of unique client IP addresses */
  unique_ips: number;
  /** Total number of price feed subscriptions across all connections */
  total_subscriptions: number;
  /** Number of active price monitoring actors */
  active_monitors: number;
  /** Total number of symbols across all exchanges */
  total_symbols: number;
  /** Total number of feeds (including WEIGHTED) - same count as surge_feeds endpoint */
  total_feeds: number;
  /** WebSocket connection health (optional, for newer oracles) */
  websocket_connection_health?: Record<string, unknown>;
  /** Priority pairs status (optional, for newer oracles) */
  priority_pairs_status?: Record<string, unknown>;
  /** Requested feeds status (only present if custom feeds were queried) */
  requested_feeds_status?: Record<string, unknown>;
  /** User-specific connection stats (only present if user_pubkey was queried) */
  user_stats?: Record<string, unknown>;
}

/**
 * Response from the healthy oracles endpoint
 * Variables are snake_case for serialization
 */
export interface HealthyOraclesResponse {
  /** List of healthy oracles with their health metrics */
  oracles: OracleHealthData[];
  /** Total number of healthy oracles found */
  count: number;
}

/**
 * Response from the Binance time proxy endpoint
 * Proxies https://api.binance.com/api/v3/time
 */
export interface ClockResponse {
  /** Server time in milliseconds since Unix epoch */
  serverTime: number;
}
