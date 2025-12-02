import type {
  AttestEnclaveResponse,
  BridgeEnclaveResponse,
  ClockResponse,
  FeedRequest,
  FetchQuoteResponse,
  FetchSignaturesConsensusResponse,
  HealthyOraclesResponse,
  PingResponse,
  RandomnessRevealResponse,
} from './types/gateway.js';
import { OracleFeedUtils, OracleJobUtils } from './utils/index.js';
import { CrossbarClient } from './crossbar-client.js';
import type { IOracleJob } from './protos.js';

import type { AxiosInstance } from 'axios';
import axios from 'axios';
import bs58 from 'bs58';

// Re-export all gateway types
export * from './types/gateway.js';

const TIMEOUT = 10_000;

const axiosClient: () => AxiosInstance = (() => {
  let instance: AxiosInstance;
  return () => (instance ??= axios.create());
})();

/**
 * Helper function to encode an array of oracle jobs to base64 strings
 */
function encodeJobs(jobs: IOracleJob[]): string[] {
  return jobs.map(job =>
    OracleJobUtils.serializeOracleJob(job).toString('base64')
  );
}

/**
 * Gateway - Interface to Switchboard Oracle Network
 *
 * The Gateway class provides methods to interact with Switchboard's oracle network,
 * allowing you to fetch signatures, quotes, and randomness from oracle operators.
 *
 * ## Variable Overrides
 *
 * Many methods support `variableOverrides` which allow you to inject custom values
 * into oracle job execution. This is particularly useful for:
 *
 * - **API Keys**: Securely pass API credentials without hardcoding in job definitions
 * - **Environment Config**: Switch between dev/staging/prod endpoints dynamically
 * - **Custom Parameters**: Override any variable defined in your oracle jobs
 *
 * ### How Variable Overrides Work
 *
 * When you provide variableOverrides, the gateway will:
 * 1. Parse the oracle job definitions
 * 2. Replace any matching variable references with your provided values
 * 3. Execute the jobs with the overridden values
 * 4. Return signed oracle responses
 *
 * ### Example Usage
 *
 * \`\`\`typescript
 * // Your oracle job might reference ${API_KEY} and ${NETWORK}
 * const overrides = {
 *   "API_KEY": "your-secret-key",
 *   "NETWORK": "mainnet"
 * };
 *
 * const response = await gateway.fetchSignatures({
 *   recentHash: slothash,
 *   encodedJobs: [jobBase64],
 *   numSignatures: 3,
 *   variableOverrides: overrides
 * });
 * \`\`\`
 *
 * ### Best Practices
 *
 * 1. **Security**: Never hardcode sensitive API keys in jobs - use overrides
 * 2. **Environment Management**: Use overrides to switch between dev/staging/prod
 * 3. **Flexibility**: Design jobs with variables for maximum reusability
 * 4. **Validation**: Ensure all required variables are provided in overrides
 * 5. **Documentation**: Document expected variables in your job definitions
 *
 * @class Gateway
 */
export class Gateway {
  readonly gatewayUrl: string;

  /**
   * Constructs a Gateway instance
   *
   * @param {string | Gateway} gatewayUrlOrInstance - The URL of the switchboard gateway, or another Gateway instance to clone
   *
   * @example
   * ```typescript
   * // Create from URL string
   * const gateway1 = new Gateway("https://gateway.switchboard.xyz");
   *
   * // Clone from another Gateway instance
   * const gateway2 = new Gateway(gateway1);
   * ```
   */
  constructor(gatewayUrlOrInstance: string | Gateway) {
    this.gatewayUrl =
      typeof gatewayUrlOrInstance === 'string'
        ? gatewayUrlOrInstance
        : gatewayUrlOrInstance.gatewayUrl;
  }

  async ping(): Promise<PingResponse> {
    const url = `${this.gatewayUrl}/gateway/api/v1/ping`;
    const method = 'POST';
    const headers = { 'Content-Type': 'application/json' };
    const body = JSON.stringify({ api_version: '1.0.0' });
    return axiosClient()
      .post(url, body, { method, headers, timeout: TIMEOUT })
      .then(r => r.data);
  }

  /**
   *
   * Fetches signatures from the gateway.
   * REST API endpoint: /api/v1/gateway_attest_enclave
   * @param timestamp The timestamp of the attestation
   * @param quote The quote of the attestation
   * @param oracle_pubkey The oracle's public key
   * @param oracle_reward_wallet The oracle's reward wallet
   * @param oracle_ed25519_enclave_signer The oracle's ed25519 enclave signer
   * @param oracle_secp256k1_enclave_signer The oracle's secp256k1 enclave signer
   * @param recentHash The chain metadata to sign with. Blockhash or slothash.
   * @returns A promise that resolves to the attestation response.
   * @throws if the request fails.
   */
  async fetchAttestation(params: {
    timestamp: number;
    quote: string;
    oracle_pubkey: string;
    oracle_reward_wallet: string;
    oracle_ed25519_enclave_signer: string;
    oracle_secp256k1_enclave_signer: string;
    recentHash: string;
  }): Promise<AttestEnclaveResponse> {
    const url = `${this.gatewayUrl}/gateway/api/v1/gateway_attest_enclave`;
    const method = 'POST';
    const headers = { 'Content-Type': 'application/json' };
    const body = JSON.stringify({
      api_version: '1.0.0',
      timestamp: params.timestamp,
      quote: params.quote,
      oracle_pubkey: params.oracle_pubkey,
      oracle_reward_wallet: params.oracle_reward_wallet,
      oracle_ed25519_enclave_signer: params.oracle_ed25519_enclave_signer,
      oracle_secp256k1_enclave_signer: params.oracle_secp256k1_enclave_signer,
      chain_hash: params.recentHash,
    });

    return axiosClient()
      .post(url, { method, headers, data: body, timeout: TIMEOUT })
      .then(r => r.data);
  }

  /**
   * Fetches an attestation quote from the gateway.
   *
   * REST API endpoint: /api/v1/gateway_fetch_quote
   *
   *
   * @param blockhash The blockhash to fetch the quote for.
   * @param get_for_oracle Whether to fetch the quote for the oracle.
   * @param get_for_guardian Whether to fetch the quote for the guardian.
   * @returns A promise that resolves to the quote response.
   * @throws if the request fails.
   */
  async fetchAttestationQuote(params: {
    blockhash: string;
    get_for_oracle: boolean;
    get_for_guardian: boolean;
  }): Promise<FetchQuoteResponse[]> {
    const url = `${this.endpoint()}/gateway/api/v1/gateway_fetch_quote`;
    const method = 'POST';
    const headers = { 'Content-Type': 'application/json' };
    const body = JSON.stringify({
      api_version: '1.0.0',
      blockhash: params.blockhash,
      get_for_oracle: params.get_for_oracle,
      get_for_guardian: params.get_for_guardian,
    });
    return axiosClient()
      .post(url, { method, headers, data: body, timeout: TIMEOUT })
      .then(r => r.data);
  }

  /**
   * Fetches signatures using consensus mechanism
   * REST API endpoint: /api/v1/fetch_signatures_consensus
   *
   * @param feedConfigs Array of feed configurations to fetch signatures for.
   * @param useTimestamp Whether to use the timestamp in the response & to encode update signature.
   * @param numSignatures The number of oracles to fetch signatures from.
   * @param useEd25519 Whether to use Ed25519 signatures instead of secp256k1.
   * @param variableOverrides Optional variable overrides for task execution (e.g., {"API_KEY": "custom-key"})
   * @returns A promise that resolves to the consensus response.
   * @throws if the request fails.
   */
  async fetchSignaturesConsensus(params: {
    feedConfigs: FeedRequest[];
    useTimestamp?: boolean;
    numSignatures?: number;
    useEd25519?: boolean;
    variableOverrides?: Record<string, string>;
  }): Promise<FetchSignaturesConsensusResponse> {
    const { feedConfigs } = params;

    const isV1 = feedConfigs.every(config => 'jobs' in config);
    const isV2 = feedConfigs.every(config => 'feed' in config);

    if (!isV1 && !isV2) {
      throw new Error(
        '[Switchboard] Malformed input: feedConfigs must be either FeedRequestV1 or FeedRequestV2'
      );
    }

    const feedRequests = isV1
      ? feedConfigs.map(config => ({
          jobs_b64_encoded: encodeJobs(config.jobs),
          max_variance: Math.floor(Number(config.maxVariance ?? 1) * 1e9),
          min_responses: config.minResponses ?? 1,
        }))
      : feedConfigs.map(config => ({
          feed_proto_b64: OracleFeedUtils.serializeOracleFeed(
            config.feed
          ).toString('base64'),
        }));

    // if numSignatures is provided, use it, otherwise use the max of the minOracleSamples for each feed (or 1 for v1)
    const numOracles =
      params.numSignatures ??
      (isV2
        ? Math.max(...feedConfigs.map(fc => fc.feed.minOracleSamples ?? 1))
        : 1);

    const url = `${this.gatewayUrl}/gateway/api/v1/fetch_signatures_consensus`;
    const method = 'POST';
    const headers = { 'Content-Type': 'application/json' };
    const useEd25519 = params.useEd25519 ?? false;
    const data = JSON.stringify({
      api_version: '1.0.0',
      recent_hash: '',
      signature_scheme: useEd25519 ? 'Ed25519' : 'Secp256k1',
      hash_scheme: 'Sha256',
      feed_requests: feedRequests,
      num_oracles: numOracles,
      use_timestamp: params.useTimestamp ?? false,
      use_ed25519: useEd25519,
      variable_overrides: params.variableOverrides ?? {},
    });

    try {
      const resp = await axiosClient()(url, { method, headers, data });
      return resp.data;
    } catch (err) {
      console.error('fetchSignaturesConsensus error', err);
      throw err;
    }
  }

  /**
   * Fetches oracle quote data from the gateway
   *
   * This method retrieves signed price quotes from oracle operators through
   * the gateway interface. It's the primary method for fetching oracle data
   * using the modern quote terminology.
   *
   * ## Protocol Details
   * - Uses Ed25519 signature scheme for efficient verification
   * - Supports both protobuf and legacy job specifications
   * - Implements consensus mechanism across multiple oracles
   * - Returns structured response with oracle metadata
   *
   * ## Response Structure
   * The returned response contains:
   * - `oracle_responses`: Array of signed oracle data
   * - `recent_hash`: Recent Solana block hash for replay protection
   * - `slot`: Recent slot number for temporal validation
   *
   * @param {CrossbarClient} crossbar - Crossbar client for data routing and feed resolution
   * @param {string[]} feedHashes - Array of feed hashes to fetch (hex strings, max 16)
   * @param {number} numSignatures - Number of oracle signatures required (default: 1, max based on queue config)
   * @returns {Promise<FetchSignaturesConsensusResponse>} Oracle quote response with signatures
   *
   * @throws {Error} When gateway is unreachable or returns error
   * @throws {Error} When crossbar cannot resolve feed hashes
   * @throws {Error} When insufficient oracles are available
   *
   * @since 2.14.0
   * @see {@link fetchUpdateBundle} - Deprecated equivalent method
   * @see {@link Queue.fetchQuoteIx} - High-level method that uses this internally
   *
   * @example
   * ```typescript
   * import { CrossbarClient } from '@switchboard-xyz/common';
   *
   * // Initialize crossbar client
   * const crossbar = CrossbarClient.default();
   *
   * // Single feed quote
   * const btcQuote = await gateway.fetchQuote(
   *   crossbar,
   *   ['0xef0d8b6fcd0104e3e75096912fc8e1e432893da4f18faedaacca7e5875da620f'], // BTC/USD
   *   1 // Single signature for fast updates
   * );
   *
   * // Multi-feed quote for DeFi protocol
   * const defiAssets = [
   *   '0xef0d8b6fcd0104e3e75096912fc8e1e432893da4f18faedaacca7e5875da620f', // BTC/USD
   *   '0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef', // ETH/USD
   *   '0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890', // SOL/USD
   *   '0x9876543210fedcba9876543210fedcba9876543210fedcba9876543210fedcba'  // USDC/USD
   * ];
   *
   * const portfolioQuote = await gateway.fetchQuote(
   *   crossbar,
   *   defiAssets,
   *   5 // Higher consensus for financial operations
   * );
   *
   * // Access oracle responses
   * console.log('Oracle responses:', portfolioQuote.oracle_responses.length);
   * console.log('Recent slot:', portfolioQuote.slot);
   *
   * // Process individual feed responses
   * portfolioQuote.oracle_responses.forEach((oracle, index) => {
   *   oracle.feed_responses.forEach((feed, feedIndex) => {
   *     console.log(`Oracle ${index}, Feed ${feedIndex}:`, {
   *       feedHash: feed.feed_hash,
   *       value: feed.success_value,
   *       confidence: feed.min_oracle_samples
   *     });
   *   });
   * });
   * ```
   */
  async fetchQuote(
    crossbar: CrossbarClient,
    feedHashes: string[],
    numSignatures: number = 1,
    variableOverrides?: Record<string, string>
  ): Promise<FetchSignaturesConsensusResponse> {
    const feedRequests = await Promise.all(
      feedHashes.map(async hash => {
        const ipfsData = await crossbar.fetchOracleFeed(hash);
        return {
          feed_proto_b64: ipfsData.data,
        };
      })
    );

    const url = `${this.gatewayUrl}/gateway/api/v1/fetch_signatures_consensus`;
    const method = 'POST';
    const headers = { 'Content-Type': 'application/json' };
    const data = JSON.stringify({
      api_version: '1.0.0',
      signature_scheme: 'Ed25519',
      hash_scheme: 'Sha256',
      feed_requests: feedRequests,
      recent_hash: '',
      num_oracles: numSignatures,
      use_ed25519: true,
      variable_overrides: variableOverrides ?? {},
    });

    try {
      const resp = await axiosClient()(url, { method, headers, data });
      return resp.data;
    } catch (err) {
      console.error('fetchQuote error', err);
      throw err;
    }
  }

  /**
   * Sends a request to the gateway bridge enclave.
   *
   * REST API endpoint: /api/v1/gateway_bridge_enclave
   *
   * @param chainHash The chain hash to include in the request.
   * @param oraclePubkey The public key of the oracle.
   * @param queuePubkey The public key of the queue.
   * @returns A promise that resolves to the response.
   * @throws if the request fails.
   */
  async fetchBridgingMessage(params: {
    chainHash: string;
    oraclePubkey: string;
    queuePubkey: string;
  }): Promise<BridgeEnclaveResponse> {
    const url = `${this.gatewayUrl}/gateway/api/v1/gateway_bridge_enclave`;
    const method = 'POST';
    const headers = { 'Content-Type': 'application/json' };
    const body = {
      api_version: '1.0.0',
      chain_hash: params.chainHash,
      oracle_pubkey: params.oraclePubkey,
      queue_pubkey: params.queuePubkey,
    };
    const data = JSON.stringify(body);
    const resp = await axiosClient()(url, { method, headers, data });
    return resp.data;
  }

  /**
   * Fetches the randomness reveal from the gateway.
   * @param params The parameters for the randomness reveal.
   * @returns The randomness reveal response.
   */
  async fetchRandomnessReveal(
    params:
      | {
          randomnessAccount: { toBuffer(): Buffer };
          slothash: string;
          slot: number;
          rpc?: string;
        }
      | {
          randomnessId: string;
          timestamp: number;
          minStalenessSeconds: number;
        }
  ): Promise<RandomnessRevealResponse> {
    const url = `${this.gatewayUrl}/gateway/api/v1/randomness_reveal`;
    const method = 'POST';
    const responseType = 'text';
    const headers = { 'Content-Type': 'application/json' };

    // Handle Solana and Cross-Chain Randomness
    let data: string;
    if ('slot' in params) {
      // Solana Randomness
      data = JSON.stringify({
        slothash: [...bs58.decode(params.slothash)],
        randomness_key: params.randomnessAccount.toBuffer().toString('hex'),
        slot: params.slot,
        rpc: params.rpc,
      });
    } else {
      // Cross-chain randomness
      data = JSON.stringify({
        timestamp: params.timestamp,
        min_staleness_seconds: params.minStalenessSeconds,
        randomness_key: params.randomnessId,
      });
    }
    try {
      const txtResponse = await axiosClient()(url, {
        method,
        headers,
        data,
        responseType,
      });
      return JSON.parse(txtResponse.data);
    } catch (err) {
      console.error('fetchRandomnessReveal error', err);
      throw err;
    }
  }

  /**
   * Fetches a list of healthy oracles with their current health metrics
   *
   * This method queries the gateway for all available oracles that are currently
   * healthy and operational. The response includes comprehensive health metrics
   * for each oracle including active connections, subscriptions, feed counts,
   * and WebSocket connection status.
   *
   * REST API endpoint: GET /gateway/api/v1/healthy_oracles
   *
   * @returns A promise that resolves to the healthy oracles response containing
   *          a list of oracles and their health metrics
   * @throws if the request fails
   *
   * @example
   * ```typescript
   * const gateway = new Gateway("https://gateway.switchboard.xyz");
   * const response = await gateway.fetchHealthyOracles();
   *
   * console.log(`Found ${response.count} healthy oracles`);
   * response.oracles.forEach(oracle => {
   *   console.log(`Oracle: ${oracle.oracle_url}`);
   *   console.log(`Active connections: ${oracle.active_connections}`);
   *   console.log(`Total feeds: ${oracle.total_feeds}`);
   * });
   * ```
   */
  async fetchHealthyOracles(): Promise<HealthyOraclesResponse> {
    const url = `${this.gatewayUrl}/gateway/api/v1/healthy_oracles`;
    const method = 'GET';
    const headers = { 'Content-Type': 'application/json' };

    try {
      const response = await axiosClient().get(url, {
        method,
        headers,
        timeout: TIMEOUT,
      });
      return response.data;
    } catch (err) {
      console.error('fetchHealthyOracles error', err);
      throw err;
    }
  }

  /**
   * Fetches the current server time from the Binance clock proxy
   *
   * This method queries the gateway's Binance time proxy endpoint which returns
   * the current server time in milliseconds. This is useful for clock synchronization
   * and ensuring timestamps are accurate for trading operations.
   *
   * REST API endpoint: GET /api/v3/time
   * Proxies: https://api.binance.com/api/v3/time
   *
   * @returns A promise that resolves to the clock response containing server time
   * @throws if the request fails
   *
   * @example
   * ```typescript
   * const gateway = new Gateway("https://gateway.switchboard.xyz");
   * const response = await gateway.fetchClock();
   *
   * console.log(`Server time: ${response.serverTime}`);
   * console.log(`Server date: ${new Date(response.serverTime)}`);
   *
   * // Calculate clock offset
   * const localTime = Date.now();
   * const offset = response.serverTime - localTime;
   * console.log(`Clock offset: ${offset}ms`);
   * ```
   */
  async fetchClock(): Promise<ClockResponse> {
    const url = `${this.gatewayUrl}/api/v3/time`;
    const method = 'GET';
    const headers = { 'Content-Type': 'application/json' };

    try {
      const response = await axiosClient().get(url, {
        method,
        headers,
        timeout: TIMEOUT,
      });
      return response.data;
    } catch (err) {
      console.error('fetchClock error', err);
      throw err;
    }
  }

  endpoint(): string {
    return this.gatewayUrl;
  }

  toString(): string {
    return JSON.stringify({
      gatewayUrl: this.gatewayUrl,
    });
  }

  [Symbol.toPrimitive](hint: string) {
    return hint === 'string' ? `Gateway: ${this.toString()}` : null;
  }
}
