/**
 * Chain-specific types for cross-chain oracle support
 */

import type { FeedEvalResponse } from './gateway.js';

/**
 * EVM-specific feed evaluation result
 * Extends base FeedEvalResponse with parsed result value
 */
export type EVMResult = FeedEvalResponse & {
  /**
   * The result of the feed evaluation (parsed from success_value)
   */
  result: string;
};

/**
 * Sui-specific oracle result
 */
export interface SuiResult {
  successValue: string;
  isNegative: boolean;
  timestamp: number;
  signature: string;
  oracleId: string;
}

/**
 * Sui aggregator response
 */
export interface SuiAggregatorResponse {
  results: SuiResult[];
  feedConfigs: {
    feedHash: string;
    maxVariance: number;
    minResponses: number;
    minSampleSize: number;
  };
  failures: string[];
  fee: number;
  queue: string;
  gateway: string;
}

/**
 * Sui simulation result
 */
export interface SuiSimulationResult {
  feed: string;
  feedHash: string;
  results: string[];
  receipts?: unknown[];
  result: string;
  stdev: string;
  variance: string;
}

/**
 * IOTA-specific oracle result (same structure as Sui)
 */
export type IotaResult = SuiResult;

/**
 * IOTA aggregator response (same structure as Sui)
 */
export interface IotaAggregatorResponse {
  results: IotaResult[];
  feedConfigs: {
    feedHash: string;
    maxVariance: number;
    minResponses: number;
    minSampleSize: number;
  };
  failures: string[];
  fee: number;
  queue: string;
  gateway: string;
}

/**
 * IOTA simulation result (same structure as Sui)
 */
export type IotaSimulationResult = SuiSimulationResult;
