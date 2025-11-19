import type { IOracleFeed, IOracleJob } from '../protos.js';
import { OracleFeed } from '../protos.js';

import { NonEmptyArrayUtils } from './index.js';
import { normalizeOracleJob } from './oracle-job.js';

import { Buffer } from 'buffer';

class OracleFeedError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'OracleFeedError';
    Object.setPrototypeOf(this, OracleFeedError.prototype);
  }
}

/**
 * Validates an OracleFeed definition.
 */
function validateOracleFeed(data: unknown): {
  success: boolean;
  data?: unknown;
  error?: string;
} {
  if (!data || typeof data !== 'object') {
    return { success: false, error: 'Expected object' };
  }

  const obj = data as Record<string, unknown>;
  if (!NonEmptyArrayUtils.safeValidate(obj.jobs as unknown[])) {
    return {
      success: false,
      error: '"jobs" property is expected to be a non-empty array',
    };
  }

  // Validate each job
  const jobs = obj.jobs as unknown[];
  for (let idx = 0; idx < jobs.length; idx++) {
    try {
      normalizeOracleJob(
        jobs[idx] as string | IOracleJob | Record<string, unknown>
      );
    } catch (error) {
      const errorStr = error instanceof Error ? error.message : String(error);
      return {
        success: false,
        error: `Job at index ${idx} failed validation: ${errorStr}`,
      };
    }
  }

  return { success: true, data: obj };
}

/**
 * Normalizes and validates an OracleFeed definition
 *
 * @param feed - OracleFeed definition in various formats:
 *   - String: JSON with optional comments
 *   - IOracleFeed: Protocol buffer object
 *   - Record: Plain object with tasks array
 * @returns A validated OracleFeed instance
 * @throws {OracleFeedError} If validation fails or feed is invalid
 *
 * @remarks
 * - Handles JSON strings with both inline (//) and block (/* *\/) comments
 * - Validates task array existence and non-emptiness
 * - Performs basic OracleJob schema validation
 * - Uses regex pattern from https://regex101.com/r/B8WkuX/1 for comment stripping
 */
export function normalizeOracleFeed(
  data: string | IOracleFeed | Record<string, unknown>
): OracleFeed {
  const parseFeedObject = (data: object) => {
    if (!data) throw new OracleFeedError(`No job data provided: ${data}`);
    const validation = validateOracleFeed(data);
    if (!validation.success) throw new OracleFeedError(validation.error!);
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    return OracleFeed.fromObject(validation.data as any);
  };
  const parseFeedString = (jobString: string) => {
    // Strip comments using regex from https://regex101.com/r/B8WkuX/1
    const cleanJson = jobString.replace(
      /\/\*[\s\S]*?\*\/|([^\\:]|^)\/\/.*$/g,
      ''
    );
    return parseFeedObject(JSON.parse(cleanJson));
  };
  return typeof data === 'string'
    ? parseFeedString(data)
    : parseFeedObject(data);
}

/**
 * Encodes an OracleFeed definition into a binary format
 *
 * @param data - OracleFeed definition in various formats:
 *   - String: JSON with optional comments
 *   - IOracleFeed: Protocol buffer object
 *   - Record: Plain object with tasks array
 * @returns Serialized OracleFeeed as Buffer
 * @throws {OracleFeedError} If validation or encoding fails
 *
 * @example
 * ```typescript
 * // From JSON string
 * const encoded1 = serializeOracleFeed(`{
 *   "jobs": [
 *     {
 *       "tasks": [
 *         {"httpTask": {"url": "https://api.coinbase.com/v2/prices/BTC-USD/spot"}}
 *       ]
 *     }
 *   ]
 * }`);
 *
 * // From plain object
 * const encoded2 = serializeOracleFeed({
 *   jobs: [
 *     {
 *       tasks: [
 *         {"httpTask": {"url": "https://api.coinbase.com/v2/prices/BTC-USD/spot"}}
 *       ]
 *     }
 *   ]
 * });
 * ```
 *
 * @remarks
 * - Uses normalizeOracleFeed() for initial validation and normalization
 * - Encodes using Protocol Buffers delimited format
 */
export function serializeOracleFeed(
  data: string | IOracleFeed | Record<string, unknown>
): Buffer {
  const feed = normalizeOracleFeed(data);
  return Buffer.from(OracleFeed.encodeDelimited(feed).finish());
}

/**
 * Deserializes an OracleFeed from on-chain buffer data
 *
 * @param data - Serialized OracleFeed data as Buffer or Uint8Array
 * @returns A decoded OracleFeed instance
 * @throws {Error} If deserialization fails or data is invalid
 *
 * @example
 * ```typescript
 * // From Buffer
 * const buffer = Buffer.from('...'); // serialized feed data
 * const feed1 = deserializeOracleFeed(buffer);
 *
 * // From Uint8Array
 * const uint8Array = new Uint8Array([...]); // serialized feed data
 * const feed2 = deserializeOracleFeed(uint8Array);
 * ```
 *
 * @remarks
 * - Uses Protocol Buffers delimited format decoding
 * - Accepts both Node.js Buffer and Uint8Array formats
 */
export function deserializeOracleFeed(data: Buffer | Uint8Array): OracleFeed {
  return OracleFeed.decodeDelimited(data);
}
