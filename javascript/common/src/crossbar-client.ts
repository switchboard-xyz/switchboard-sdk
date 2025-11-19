import type {
  EVMResult,
  IotaAggregatorResponse,
  IotaSimulationResult,
  SuiAggregatorResponse,
  SuiSimulationResult,
} from './types/chains.js';
import type {
  CrossbarFetchResponse,
  CrossbarOracleFeedFetchResponse,
  CrossbarSimulateProtoResponse,
  FeedRequest,
  FetchSignaturesConsensusRequest,
  FetchSignaturesConsensusResponse,
  FetchSignaturesRequest,
  OracleInfo,
  SimulateJobsRequest,
  SimulateJobsResponse,
  V2UpdateQuery,
  V2UpdateResponse,
} from './types/crossbar.js';
import type { FeedEvalResponse } from './types/gateway.js';
import { IxFromHex } from './utils/instructions.js';
import { decodeString } from './utils/string.js';
import { Gateway } from './gateway.js';
import type { IOracleFeed, IOracleJob } from './protos.js';
import { OracleFeed } from './protos.js';

import type { TransactionInstruction } from '@solana/web3.js';
import axios from 'axios';
import bs58 from 'bs58';

/**
 * Network options for CrossbarClient operations
 */
export enum CrossbarNetwork {
  SolanaMainnet = 'mainnet',
  SolanaDevnet = 'devnet',
}

export class CrossbarClient {
  private static _instance: CrossbarClient | null = null;
  readonly crossbarUrl: string;
  readonly verbose: boolean;

  // feed hash -> crossbar response
  readonly feedCache = new Map<string, CrossbarFetchResponse>();
  readonly oracleFeedCache = new Map<string, CrossbarOracleFeedFetchResponse>();

  // network -> gateway instance (cached)
  private readonly gatewayCache = new Map<string, Gateway>();

  // default network for operations (can be overridden per method call)
  private network: string = CrossbarNetwork.SolanaMainnet;

  /**
   * Create a FeedRequestV1 object
   * @param {string[]} jobsB64Encoded - Base64 encoded oracle jobs
   * @param {number} maxVariance - Maximum variance allowed for the feed
   * @param {number} minResponses - Minimum number of oracle responses required
   * @returns {FeedRequest} - A FeedRequestV1 object
   */
  static createFeedRequestV1(
    jobsB64Encoded: string[],
    maxVariance: number,
    minResponses: number
  ): FeedRequest {
    return {
      jobsB64Encoded,
      maxVariance,
      minResponses,
    };
  }

  /**
   * Create a FeedRequestV2 object
   * @param {string} feedProtoB64 - Base64 encoded feed protobuf
   * @returns {FeedRequest} - A FeedRequestV2 object
   */
  static createFeedRequestV2(feedProtoB64: string): FeedRequest {
    return {
      feedProtoB64,
    };
  }

  static default(verbose?: boolean) {
    if (!CrossbarClient._instance) {
      CrossbarClient._instance = new CrossbarClient(
        'https://crossbar.switchboard.xyz',
        verbose
      );
    }
    return CrossbarClient._instance;
  }

  constructor(crossbarUrl: string, verbose?: boolean) {
    this.crossbarUrl = new URL(crossbarUrl).origin;
    this.verbose = !!verbose;
  }

  /**
   * Set the default network for CrossbarClient operations
   *
   * This network will be used as the default for all operations that accept
   * a network parameter. Individual method calls can still override this by
   * passing their own network parameter.
   *
   * @param {CrossbarNetwork} network - The network to use (SolanaMainnet or SolanaDevnet)
   *
   * @example
   * ```typescript
   * const crossbar = CrossbarClient.default();
   * crossbar.setNetwork(CrossbarNetwork.SolanaDevnet);
   *
   * // Now all operations will default to devnet
   * const gateway = await crossbar.fetchGateway(); // Uses devnet
   * const simResult = await crossbar.simulateFeed(feedId); // Uses devnet
   * ```
   */
  setNetwork(network: CrossbarNetwork): void {
    this.network = network;
  }

  /**
   * Get the currently configured default network
   *
   * @returns {string} The current network (mainnet or devnet)
   */
  getNetwork(): string {
    return this.network;
  }

  /**
   * GET /fetch/:feedHash
   * Fetch data from the crossbar using the provided feedHash
   * @param {string} feedHash - The hash of the feed to fetch data for
   * @returns {Promise<CrossbarFetchResponse>} - The data fetched from the crossbar
   */
  async fetch(feedHash: string): Promise<CrossbarFetchResponse> {
    const v2CachedValue = this.oracleFeedCache.get(feedHash);
    if (v2CachedValue) {
      throw new Error('V2 cached value found, use fetchOracleFeed instead.');
    }
    try {
      // Check if the feedHash is already in the cache
      const cached = this.feedCache.get(feedHash);
      if (cached) return cached;

      // Fetch the data from the crossbar

      const response = await axios
        .get(`${this.crossbarUrl}/fetch/${feedHash}`)
        .then(resp => resp.data);

      // Cache the response on the crossbar instance
      this.feedCache.set(feedHash, response);

      return response;
    } catch (err) {
      if (!axios.isAxiosError(err)) throw err;

      const response = err.response;
      if (!response) throw err;

      // If response is outside of the 200 range, log the status and throw an error.
      if (this.verbose) console.error(`${response.status}: ${response.data}`);
      throw new Error(`Bad Crossbar fetch status: ${response.status}`);
    }
  }

  /**
   * GET /v2/fetch/:feedHash
   * Fetch OracleFeed data from a crossbar server using the provided feedId
   * @param {string} feedId - The identifier of the OracleFeed to fetch
   * @returns {Promise<CrossbarOracleFeedFetchResponse>} - The data fetched from the crossbar
   */
  async fetchOracleFeed(
    feedId: string
  ): Promise<CrossbarOracleFeedFetchResponse> {
    const legacyProtoCachedValue = this.feedCache.get(feedId);
    if (legacyProtoCachedValue) {
      throw new Error('Legacy proto cached value found, use fetch instead.');
    }

    try {
      // Check if the feedId is already in the cache
      const cached = this.oracleFeedCache.get(feedId);
      if (cached) return cached;

      const url = `${this.crossbarUrl}/v2/fetch/${feedId}`;
      // Fetch the data from the crossbar
      const response = await axios.get(url).then(resp => resp.data);
      // Cache the response on the crossbar instance
      this.oracleFeedCache.set(feedId, response);
      return response;
    } catch (err) {
      if (!axios.isAxiosError(err)) throw err;

      const response = err.response;
      if (!response) throw err;

      // If response is outside of the 200 range, log the status and throw an error.
      if (this.verbose) console.error(`${response.status}: ${response.data}`);
      throw new Error(`Bad Crossbar /v2/fetch status: ${response.status}`);
    }
  }

  /**
   * POST /store
   * Store oracle jobs on the crossbar, associated with a queue address
   * @param {string} queueAddress - The address of the queue
   * @param {IOracleJob[]} jobs - The oracle jobs to store
   * @returns {Promise<{ cid: string; feedHash: string; queueHex: string }>} - The stored data information
   */
  async store(
    queueAddress: string,
    jobs: IOracleJob[]
  ): Promise<{ cid: string; feedHash: string; queueHex: string }> {
    try {
      // Try to decode the queueAddress to a Buffer so that we can send it in the expected format,
      // base58, to the Crossbar node.
      const queue = decodeString(queueAddress);
      if (!queue) throw new Error(`Unable to parse queue: ${queueAddress}`);

      return await axios
        .post(
          `${this.crossbarUrl}/store`,
          { queue: bs58.encode(queue), jobs },
          { headers: { 'Content-Type': 'application/json' } }
        )
        .then(resp => {
          if (resp.status === 200) return resp.data;
          throw new Error(`Bad Crossbar store response: ${resp.status}`);
        });
    } catch (err) {
      if (!axios.isAxiosError(err)) throw err;

      const response = err.response;
      if (!response) throw err;

      if (this.verbose) console.error(`${response.status}: ${response.data}`);
      throw new Error(`Bad Crossbar store response: ${response.status}`);
    }
  }

  /**
   * POST /v2/store
   * Store an OracleFeed on IPFS using crossbar.
   * @param {IOracleFeed} feed - The OracleFeed to store
   * @returns {Promise<{ cid: string; feedHash: string; queueHex: string }>} - The stored data information
   */
  async storeOracleFeed(
    feed: IOracleFeed
  ): Promise<{ cid: string; feedId: string }> {
    try {
      return await axios
        .post(
          `${this.crossbarUrl}/v2/store`,
          { feed },
          { headers: { 'Content-Type': 'application/json' } }
        )
        .then(resp => {
          if (resp.status === 200) return resp.data;
          throw new Error(`Bad Crossbar v2/store response: ${resp.status}`);
        });
    } catch (err) {
      if (!axios.isAxiosError(err)) throw err;

      const response = err.response;
      if (!response) throw err;

      if (this.verbose) console.error(`${response.status}: ${response.data}`);
      throw new Error(`Bad Crossbar v2/store response: ${response.status}`);
    }
  }

  /**
   * GET /updates/solana/:network/:feedpubkeys
   * Fetch updates for Solana network feeds from the crossbar
   * @param {string} network - The Solana network to fetch updates for
   * @param {string[]} feedpubkeys - The public keys of the feeds to fetch updates for
   * @param {number} [numSignatures] - The number of signatures to fetch (optional)
   * @returns {Promise<{ success: boolean; pullIx: TransactionInstruction; responses: { oracle: string; result: number | null; errors: string }[]; lookupTables: string[] }[]>} - The updates for the specified feeds
   */
  async fetchSolanaUpdates(
    network: string,
    feedpubkeys: string[],
    payer: string,
    numSignatures?: number
  ): Promise<
    {
      success: boolean;
      pullIxns: TransactionInstruction[];
      responses: { oracle: string; result: number | null; errors: string }[];
      lookupTables: string[];
    }[]
  > {
    try {
      if (!network) throw new Error('Network is required');
      if (!feedpubkeys || feedpubkeys.length === 0)
        throw new Error('At least one feed is required');

      const feedsParam = feedpubkeys.join(',');
      const response = await axios
        .get(`${this.crossbarUrl}/updates/solana/${network}/${feedsParam}`, {
          params: { numSignatures, payer },
        })
        .then(resp => resp.data);

      // Convert pullIx from hex to TransactionInstruction using IxFromHex
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const updates = response.map((update: any) => ({
        ...update,
        pullIxns: update.pullIxns.map(IxFromHex),
      }));

      return updates;
    } catch (err) {
      if (!axios.isAxiosError(err)) throw err;

      const response = err.response;
      if (!response) throw err;

      if (this.verbose) console.error(`${response.status}: ${response.data}`);
      throw new Error(
        `Bad Crossbar fetchSolanaUpdates response: ${response.status}`
      );
    }
  }

  /**
   * GET /simulate/solana/:network/:feedpubkeys
   * Simulate fetching Solana feed results from the crossbar
   * @param {string} network - The Solana network to simulate
   * @param {string[]} feedpubkeys - The public keys of the feeds to simulate
   * @returns {Promise<{ feed: string; feedHash: string; results: number[] }[]>} - The simulated feed results
   */
  async simulateSolanaFeeds(
    network: string,
    feedpubkeys: string[]
  ): Promise<{ feed: string; feedHash: string; results: number[] }[]> {
    try {
      if (!network) throw new Error('Network is required');
      if (!feedpubkeys || feedpubkeys.length === 0)
        throw new Error('At least one feed is required');

      const feedsParam = feedpubkeys.join(',');
      return await axios
        .get(`${this.crossbarUrl}/simulate/solana/${network}/${feedsParam}`)
        .then(resp => resp.data);
    } catch (err) {
      if (!axios.isAxiosError(err)) throw err;

      const response = err.response;
      if (!response) throw err;

      if (this.verbose) console.error(`${response.status}: ${response.data}`);
      throw new Error(
        `Bad Crossbar simulateSolanaFeeds response: ${response.status}`
      );
    }
  }

  /**
   * GET /updates/evm/:chainId/:aggregatorIds
   * Fetch updates for EVM network feeds from the crossbar
   * @param param0 - The chain ID and aggregator IDs to fetch updates for
   * @returns Promise<{ results: EVMResult[]; encoded: string[] }> - The updates for the specified feeds
   */
  async fetchEVMResults({
    chainId,
    aggregatorIds,
  }: {
    chainId: number;
    aggregatorIds: string[];
  }): Promise<{ results: EVMResult[]; encoded: string[] }> {
    try {
      if (!chainId) throw new Error('Chain ID is required');
      if (!aggregatorIds || aggregatorIds.length === 0)
        throw new Error('At least one feed is required');

      const feedsParam = aggregatorIds.join(',');
      return await axios
        .get(`${this.crossbarUrl}/updates/evm/${chainId}/${feedsParam}`)
        .then(resp => resp.data);
    } catch (err) {
      if (!axios.isAxiosError(err)) throw err;

      const response = err.response;
      if (!response) throw err;

      if (this.verbose) console.error(`${response.status}: ${response.data}`);
      throw new Error(
        `Bad Crossbar fetchEVMUpdates response: ${response.status}`
      );
    }
  }

  /**
   * GET /oracle/quote/:aggregatorIds
   * Fetch the quote for the given aggregator IDs
   * @param aggregatorIds - The aggregator IDs to fetch the quote for
   * @param network - The network to fetch the quote for (mainnet / testnet)
   * @returns {Promise<V2UpdateResponse>} - The quote for the given aggregator IDs with timestamp and encoded data
   */
  async fetchOracleQuote(
    aggregatorIds: string[],
    network: string = 'mainnet'
  ): Promise<V2UpdateResponse & { encoded: string }> {
    return this.fetchV2Update(aggregatorIds, {
      chain: 'evm',
      network,
      use_timestamp: true,
    }) as Promise<V2UpdateResponse & { encoded: string }>;
  }

  /**
   * GET /simulate/evm/:network/:aggregatorIds
   * Simulate fetching Solana feed results from the crossbar
   * @param {string} network - The Solana network to simulate
   * @param {string[]} aggregatorIds - The public keys of the feeds to simulate
   * @returns {Promise<{ feed: string; feedHash: string; results: number[] }[]>} - The simulated feed results
   */
  async simulateEVMFeeds(
    network: number,
    aggregatorIds: string[]
  ): Promise<{ aggregatorId: string; feedHash: string; results: number[] }[]> {
    try {
      if (!network) throw new Error('Network is required');
      if (!aggregatorIds || aggregatorIds.length === 0)
        throw new Error('At least one feed is required');

      const feedsParam = aggregatorIds.join(',');
      return await axios
        .get(`${this.crossbarUrl}/simulate/evm/${network}/${feedsParam}`)
        .then(resp => resp.data);
    } catch (err) {
      if (!axios.isAxiosError(err)) throw err;

      const response = err.response;
      if (!response) throw err;

      if (this.verbose) console.error(`${response.status}: ${response.data}`);
      throw new Error(
        `Bad Crossbar simulateEVMFeeds response: ${response.status}`
      );
    }
  }

  /**
   * GET /randomness/evm/:chainId/:randomnessId
   * @param param0 - The chain ID and randomness ID to resolve
   */
  async resolveEVMRandomness({
    chainId,
    randomnessId,
  }: {
    chainId: number;
    randomnessId: string;
  }): Promise<{
    encoded: string;
    response: {
      signature: string;
      recovery_id: number;
      value: string;
    };
  }> {
    try {
      return await axios
        .get(`${this.crossbarUrl}/randomness/evm/${chainId}/${randomnessId}`)
        .then(resp => resp.data);
    } catch (err) {
      if (!axios.isAxiosError(err)) throw err;

      const response = err.response;
      if (!response) throw err;

      if (this.verbose) console.error(`${response.status}: ${response.data}`);
      throw new Error(
        `Bad Crossbar resolveEVMRandomness response: ${response.status}`
      );
    }
  }

  /**
   * GET /updates/sui/:network/:aggregatorAddresses
   * Fetch updates for Sui network feeds from the crossbar
   * @param {string} network - The Sui network to fetch updates for (mainnet / testnet)
   * @param {string[]} aggregatorAddresses - The addresses of the aggregators to fetch updates for
   * @returns {Promise<{ responses: SuiAggregatorResponse[]; failures: string[] }>} - The updates and any failures for the specified feeds
   */
  async fetchSuiUpdates(
    network: string,
    aggregatorAddresses: string[]
  ): Promise<{ responses: SuiAggregatorResponse[]; failures: string[] }> {
    try {
      if (!network) throw new Error('Network is required');
      if (!aggregatorAddresses || aggregatorAddresses.length === 0)
        throw new Error('At least one aggregator address is required');

      const addressesParam = aggregatorAddresses.join(',');
      return await axios
        .get(`${this.crossbarUrl}/updates/sui/${network}/${addressesParam}`)
        .then(resp => resp.data);
    } catch (err) {
      if (!axios.isAxiosError(err)) throw err;

      const response = err.response;
      if (!response) throw err;

      if (this.verbose) console.error(`${response.status}: ${response.data}`);
      throw new Error(
        `Bad Crossbar fetchSuiUpdates response: ${response.status}`
      );
    }
  }

  /**
   * GET /simulate/sui/:network/:aggregatorAddresses
   * Simulate fetching Sui feed results from the crossbar
   * @param {string} network - The Sui network to simulate (mainnet / testnet)
   * @param {string[]} aggregatorAddresses - The feed IDs to simulate
   * @param {boolean} [includeReceipts] - Whether to include receipts in the response
   * @returns {Promise<SuiSimulationResult[]>} - The simulated feed results
   */
  async simulateSuiFeeds(
    network: string,
    aggregatorAddresses: string[],
    includeReceipts?: boolean
  ): Promise<SuiSimulationResult[]> {
    try {
      if (!network) throw new Error('Network is required');
      if (!aggregatorAddresses || aggregatorAddresses.length === 0)
        throw new Error('At least one feed ID is required');

      const feedsParam = aggregatorAddresses.join(',');
      return await axios
        .get(`${this.crossbarUrl}/simulate/sui/${network}/${feedsParam}`, {
          params: { includeReceipts },
        })
        .then(resp => resp.data);
    } catch (err) {
      if (!axios.isAxiosError(err)) throw err;

      const response = err.response;
      if (!response) throw err;

      if (this.verbose) console.error(`${response.status}: ${response.data}`);
      throw new Error(
        `Bad Crossbar simulateSuiFeeds response: ${response.status}`
      );
    }
  }

  /**
   * GET /updates/iota/:network/:aggregatorAddresses
   * Fetch updates for Iota network feeds from the crossbar
   * @param {string} network - The Iota network to fetch updates for (mainnet / testnet)
   * @param {string[]} aggregatorAddresses - The addresses of the aggregators to fetch updates for
   * @returns {Promise<{ responses: IotaAggregatorResponse[]; failures: string[] }>} - The updates and any failures for the specified feeds
   */
  async fetchIotaUpdates(
    network: string,
    aggregatorAddresses: string[]
  ): Promise<{ responses: IotaAggregatorResponse[]; failures: string[] }> {
    try {
      if (!network) throw new Error('Network is required');
      if (!aggregatorAddresses || aggregatorAddresses.length === 0)
        throw new Error('At least one aggregator address is required');

      const addressesParam = aggregatorAddresses.join(',');
      return await axios
        .get(`${this.crossbarUrl}/updates/iota/${network}/${addressesParam}`)
        .then(resp => resp.data);
    } catch (err) {
      if (!axios.isAxiosError(err)) throw err;

      const response = err.response;
      if (!response) throw err;

      if (this.verbose) console.error(`${response.status}: ${response.data}`);
      throw new Error(
        `Bad Crossbar fetchIotaUpdates response: ${response.status}`
      );
    }
  }

  /**
   * GET /simulate/iota/:network/:aggregatorAddresses
   * Simulate fetching Iota feed results from the crossbar
   * @param {string} network - The Iota network to simulate (mainnet / testnet)
   * @param {string[]} aggregatorAddresses - The feed IDs to simulate
   * @param {boolean} [includeReceipts] - Whether to include receipts in the response
   * @returns {Promise<IotaSimulationResult[]>} - The simulated feed results
   */
  async simulateIotaFeeds(
    network: string,
    aggregatorAddresses: string[],
    includeReceipts?: boolean
  ): Promise<IotaSimulationResult[]> {
    try {
      if (!network) throw new Error('Network is required');
      if (!aggregatorAddresses || aggregatorAddresses.length === 0)
        throw new Error('At least one feed ID is required');

      const feedsParam = aggregatorAddresses.join(',');
      return await axios
        .get(`${this.crossbarUrl}/simulate/iota/${network}/${feedsParam}`, {
          params: { includeReceipts },
        })
        .then(resp => resp.data);
    } catch (err) {
      if (!axios.isAxiosError(err)) throw err;

      const response = err.response;
      if (!response) throw err;

      if (this.verbose) console.error(`${response.status}: ${response.data}`);
      throw new Error(
        `Bad Crossbar simulateIotaFeeds response: ${response.status}`
      );
    }
  }

  /**
   * GET /gateways
   * Fetch all gateways from the crossbar
   * @param {string} [network] - Optional network parameter (devnet/testnet/mainnet). Defaults to mainnet.
   * @returns {Promise<string[]>} - The gateways response containing an array of Gateway urls
   */
  async fetchGateways(network: string = 'mainnet'): Promise<string[]> {
    try {
      const gateways = await axios
        .get(`${this.crossbarUrl}/gateways?network=${network}`)
        .then(resp => resp.data);

      return gateways;
    } catch (err) {
      console.error('Error fetching gateways:', err);
      if (!axios.isAxiosError(err)) throw err;

      const response = err.response;
      if (!response) throw err;

      if (this.verbose) console.error(`${response.status}: ${response.data}`);
      throw new Error(
        `Bad Crossbar fetchGateways response: ${response.status}`
      );
    }
  }

  /**
   * Fetch a Gateway instance for the specified network with caching
   *
   * This method fetches gateway URLs from the crossbar service and creates
   * a Gateway instance. Results are cached per network to avoid repeated
   * network calls.
   *
   * @param {string} [network] - Network to fetch gateway for (devnet/testnet/mainnet).
   *                             If not provided, uses the network set via setNetwork()
   * @returns {Promise<Gateway>} - A Gateway instance for the specified network
   * @throws {Error} If no gateways are available for the specified network
   *
   * @example
   * ```typescript
   * const crossbar = CrossbarClient.default();
   * const gateway = await crossbar.fetchGateway('mainnet');
   * const response = await gateway.fetchQuote(...);
   * ```
   */
  async fetchGateway(network?: string): Promise<Gateway> {
    const targetNetwork = network ?? this.network;
    // Return cached gateway if available for this network
    const cached = this.gatewayCache.get(targetNetwork);
    if (cached) {
      return cached;
    }

    // Fetch gateway URLs from crossbar
    const gatewayUrls = await this.fetchGateways(targetNetwork);
    if (!gatewayUrls || gatewayUrls.length === 0) {
      throw new Error(`No gateways available for network: ${targetNetwork}`);
    }

    // Create Gateway instance from first available URL
    const gateway = new Gateway(gatewayUrls[0]);

    // Cache the gateway for future use
    this.gatewayCache.set(targetNetwork, gateway);

    return gateway;
  }

  /**
   * GET /oracles
   * Fetch all oracles for a given network
   * @param {string} [network] - Optional network parameter (devnet/mainnet). Defaults to mainnet.
   * @returns {Promise<Oracle[]>} - Array of oracle information
   */
  async fetchOracles(network?: string): Promise<OracleInfo[]> {
    try {
      return await axios
        .get(`${this.crossbarUrl}/oracles`, {
          params: { network },
        })
        .then(resp => resp.data);
    } catch (err) {
      if (!axios.isAxiosError(err)) throw err;

      const response = err.response;
      if (!response) throw err;

      if (this.verbose) console.error(`${response.status}: ${response.data}`);
      throw new Error(`Bad Crossbar fetchOracles response: ${response.status}`);
    }
  }

  /**
   * POST /gateways/fetch_signatures
   * @deprecated Use fetchSignaturesConsensus instead
   * Fetch signatures from oracles for a given set of jobs
   * @param {FetchSignaturesRequest} request - The request parameters
   * @param {string} [network] - Optional network parameter (devnet/mainnet). Defaults to mainnet.
   * @returns {Promise<FeedEvalResponse[]>} - Array of oracle signatures and results
   */
  async fetchSignatures(
    request: FetchSignaturesRequest,
    network?: string
  ): Promise<{ responses: FeedEvalResponse[]; failures: string[] }> {
    try {
      return await axios
        .post(`${this.crossbarUrl}/gateways/fetch_signatures`, request, {
          params: { network },
        })
        .then(resp => resp.data);
    } catch (err) {
      if (!axios.isAxiosError(err)) throw err;

      const response = err.response;
      if (!response) throw err;

      if (this.verbose) console.error(`${response.status}: ${response.data}`);
      throw new Error(
        `Bad Crossbar fetchOracleSignatures response: ${response.status}`
      );
    }
  }

  /**
   * POST /gateways/fetch_signatures_consensus
   * Fetch consensus signatures from oracles for a given set of feed requests
   * @param {FetchSignaturesConsensusRequest} request - The request parameters with camelCase fields
   * @param {string} [network] - Optional network parameter (devnet/mainnet). Defaults to mainnet.
   * @returns {Promise<FetchSignaturesConsensusResponse>} - Consensus signatures and median responses in camelCase
   *
   * @example
   * // Using FeedRequestV1
   * const feedRequestV1 = CrossbarClient.createFeedRequestV1(
   *   ['base64EncodedJob1', 'base64EncodedJob2'],
   *   1000000, // maxVariance
   *   3        // minResponses
   * );
   *
   * // Using FeedRequestV2
   * const feedRequestV2 = CrossbarClient.createFeedRequestV2('base64EncodedProtoBuf');
   *
   * const response = await client.fetchSignaturesConsensus({
   *   apiVersion: '1.0',
   *   recentHash: 'someRecentHash',
   *   signatureScheme: 'ed25519',
   *   hashScheme: 'sha256',
   *   feedRequests: [feedRequestV1, feedRequestV2],
   *   numOracles: 3,
   *   useTimestamp: false
   * });
   */
  async fetchSignaturesConsensus(
    request: FetchSignaturesConsensusRequest,
    network?: string
  ): Promise<FetchSignaturesConsensusResponse> {
    try {
      return await axios
        .post(
          `${this.crossbarUrl}/gateways/fetch_signatures_consensus`,
          request,
          {
            params: { network },
            headers: { 'Content-Type': 'application/json' },
          }
        )
        .then(resp => resp.data);
    } catch (err) {
      if (!axios.isAxiosError(err)) throw err;

      const response = err.response;
      if (!response) throw err;

      if (this.verbose) console.error(`${response.status}: ${response.data}`);
      throw new Error(
        `Bad Crossbar fetchSignaturesConsensus response: ${response.status}`
      );
    }
  }

  /**
   * POST /simulate/jobs
   * Simulate oracle jobs execution without storing them
   * @param {SimulateJobsRequest} request - The simulation request containing jobs and optional parameters
   * @returns {Promise<SimulateJobsResponse>} - The simulation results
   *
   * @example
   * const response = await client.simulateJobs({
   *   jobs: [
   *     {
   *       tasks: [
   *         {
   *           httpTask: {
   *             url: "https://api.coinbase.com/v2/prices/BTC-USD/spot"
   *           }
   *         },
   *         {
   *           jsonParseTask: {
   *             path: "$.data.amount"
   *           }
   *         }
   *       ]
   *     }
   *   ],
   *   includeReceipts: true,
   *   variableOverrides: {
   *     MY_VARIABLE: "custom_value"
   *   }
   * });
   */
  async simulateJobs(
    request: SimulateJobsRequest
  ): Promise<SimulateJobsResponse> {
    try {
      return await axios
        .post(`${this.crossbarUrl}/simulate/jobs`, request, {
          headers: { 'Content-Type': 'application/json' },
        })
        .then(resp => resp.data);
    } catch (err) {
      if (!axios.isAxiosError(err)) throw err;

      const response = err.response;
      if (!response) throw err;

      if (this.verbose) console.error(`${response.status}: ${response.data}`);
      throw new Error(`Bad Crossbar simulateJobs response: ${response.status}`);
    }
  }

  /**
   * GET /v2/update/{feedHashes}
   * Fetch V2 oracle consensus updates for feed hashes with enhanced chain support
   * @param {string[]} feedHashes - Array of feed hashes to fetch updates for
   * @param {V2UpdateQuery} [options] - Optional query parameters for the request
   * @returns {Promise<V2UpdateResponse>} - The V2 update response with oracle consensus data
   *
   * @example
   * // Basic usage with single feed hash
   * const response = await client.fetchV2Update(
   *   ['0x7418dc6408f5e0eb4724dabd81922ee7b0814a43abc2b30ea7a08222cd1e23ee']
   * );
   *
   * @example
   * // With chain-specific options for Sui mainnet
   * const response = await client.fetchV2Update(
   *   ['0x7418dc6408f5e0eb4724dabd81922ee7b0814a43abc2b30ea7a08222cd1e23ee'],
   *   {
   *     chain: 'sui',
   *     network: 'mainnet',
   *     use_timestamp: true,
   *     num_oracles: 5
   *   }
   * );
   *
   * @example
   * // Multiple feed hashes with custom gateway
   * const response = await client.fetchV2Update(
   *   [
   *     '0x7418dc6408f5e0eb4724dabd81922ee7b0814a43abc2b30ea7a08222cd1e23ee',
   *     '0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef'
   *   ],
   *   {
   *     network: 'testnet',
   *     gateway: 'https://custom-gateway.example.com',
   *     signature_scheme: 'Secp256k1'
   *   }
   * );
   */
  async fetchV2Update(
    feedHashes: string[],
    options?: V2UpdateQuery
  ): Promise<V2UpdateResponse> {
    try {
      if (!feedHashes || feedHashes.length === 0) {
        throw new Error('At least one feed hash is required');
      }

      // Join feed hashes with commas for the URL path
      const feedHashesParam = feedHashes.join(',');

      // Build query parameters from options
      const params: Record<string, string | number | boolean> = {};
      if (options) {
        Object.entries(options).forEach(([key, value]) => {
          if (value !== undefined) {
            params[key] = value;
          }
        });
      }

      const response = await axios
        .get(`${this.crossbarUrl}/v2/update/${feedHashesParam}`, {
          params,
        })
        .then(resp => resp.data);

      return response;
    } catch (err) {
      if (!axios.isAxiosError(err)) throw err;

      const response = err.response;
      if (!response) throw err;

      if (this.verbose) console.error(`${response.status}: ${response.data}`);
      throw new Error(
        `Bad Crossbar fetchV2Update response: ${response.status}`
      );
    }
  }

  /**
   * POST /v2/simulate/proto
   * Simulate an OracleFeed from a protobuf object or feed hash
   * @param {IOracleFeed | string} feedOrHash - The OracleFeed protobuf object or feed hash to simulate
   * @param {boolean} [includeReceipts] - Whether to include receipts in the response
   * @param {Record<string, string>} [variableOverrides] - Variable overrides for the simulation
   * @param {string} [network] - Network to use for simulation. If not provided, uses the network set via setNetwork()
   * @returns {Promise<CrossbarSimulateProtoResponse>} - The simulation results
   *
   * @example
   * const feed = OracleFeed.create({
   *   name: "BTC/USD",
   *   jobs: [
   *     {
   *       tasks: [
   *         {
   *           httpTask: {
   *             url: "https://api.coinbase.com/v2/prices/BTC-USD/spot"
   *           }
   *         },
   *         {
   *           jsonParseTask: {
   *             path: "$.data.amount"
   *           }
   *         }
   *       ]
   *     }
   *   ]
   * });
   *
   * const response = await client.simulateFeed(
   *   feed,
   *   true,
   *   { MY_VAR: "value" },
   *   "mainnet"
   * );
   */
  async simulateFeed(
    feedOrHash: IOracleFeed | string,
    includeReceipts?: boolean,
    variableOverrides?: Record<string, string>,
    network?: string
  ): Promise<CrossbarSimulateProtoResponse> {
    try {
      // If we received a feed hash (string), fetch the feed first
      let feed: IOracleFeed;
      if (typeof feedOrHash === 'string') {
        const response = await this.fetchOracleFeed(feedOrHash);
        const protoBytes = Buffer.from(response.data, 'base64');
        feed = OracleFeed.decodeDelimited(protoBytes);
      } else {
        feed = feedOrHash;
      }

      // Encode the OracleFeed proto to bytes
      const protoBytes = OracleFeed.encode(feed).finish();
      // Convert to base64
      const oracleFeedB64 = Buffer.from(protoBytes).toString('base64');

      const targetNetwork = network ?? this.network;
      return await axios
        .post(
          `${this.crossbarUrl}/v2/simulate/proto`,
          {
            oracleFeed: oracleFeedB64,
            includeReceipts,
            variableOverrides,
            network: targetNetwork,
          },
          { headers: { 'Content-Type': 'application/json' } }
        )
        .then(resp => resp.data);
    } catch (err) {
      if (!axios.isAxiosError(err)) throw err;

      const response = err.response;
      if (!response) throw err;

      if (this.verbose) console.error(`${response.status}: ${response.data}`);
      throw new Error(`Bad Crossbar simulateFeed response: ${response.status}`);
    }
  }

  /**
   * Simulate fetching feed results from the crossbar using feed hashes
   * @param {string[]} feedHashes - The hashes of the feeds to simulate
   * @param {boolean} [includeReceipts] - Whether to include receipts in the response
   * @returns {Promise<CrossbarSimulateProtoResponse[]>} - The simulated feed results
   */
  async simulateFeeds(
    feedHashes: string[],
    includeReceipts?: boolean
  ): Promise<CrossbarSimulateProtoResponse[]> {
    if (!feedHashes || feedHashes.length === 0)
      throw new Error('At least one feed is required');

    return await Promise.all(
      feedHashes.map(feedHash => this.simulateFeed(feedHash, includeReceipts))
    );
  }

  /**
   * Simulate V2 oracle feeds with variable overrides and network selection
   * @deprecated Use simulateFeeds instead
   * @param {string[]} feedHashes - The hashes of the V2 feeds to simulate
   * @param {boolean} [includeReceipts] - Whether to include receipts in the response
   * @param {Record<string, string>} [variableOverrides] - Variable overrides for the simulation
   * @param {string} [network] - Network to use for simulation (defaults to "mainnet")
   * @returns {Promise<CrossbarSimulateProtoResponse[]>} - The simulated feed results
   */
  async simulateOracleFeeds(
    feedHashes: string[],
    includeReceipts?: boolean,
    variableOverrides?: Record<string, string>,
    network?: string
  ): Promise<CrossbarSimulateProtoResponse[]> {
    if (!feedHashes || feedHashes.length === 0)
      throw new Error('At least one feed is required');

    return await Promise.all(
      feedHashes.map(feedHash =>
        this.simulateFeed(feedHash, includeReceipts, variableOverrides, network)
      )
    );
  }

  /**
   * Fetches the current server time from the gateway's Binance clock proxy
   *
   * This method retrieves the current server time via the gateway's Binance time
   * proxy endpoint. This is useful for clock synchronization and ensuring accurate
   * timestamps for trading operations. The gateway instance is automatically
   * fetched and cached based on the configured network.
   *
   * @param {string} [network] - Network to use for the gateway (devnet/testnet/mainnet).
   *                             If not provided, uses the network set via setNetwork()
   * @returns {Promise<{ serverTime: number }>} Server time in milliseconds since Unix epoch
   * @throws {Error} When gateway is unreachable or returns an error
   *
   * @example
   * ```typescript
   * const crossbar = CrossbarClient.default();
   *
   * // Fetch server time using default network
   * const clock = await crossbar.fetchClock();
   * console.log(`Server time: ${clock.serverTime}`);
   * console.log(`Server date: ${new Date(clock.serverTime)}`);
   *
   * // Calculate clock offset
   * const localTime = Date.now();
   * const offset = clock.serverTime - localTime;
   * console.log(`Clock offset: ${offset}ms`);
   *
   * // Fetch from specific network
   * crossbar.setNetwork(CrossbarNetwork.SolanaDevnet);
   * const devnetClock = await crossbar.fetchClock();
   * ```
   */
  async fetchClock(network?: string): Promise<{ serverTime: number }> {
    const gateway = await this.fetchGateway(network);
    return gateway.fetchClock();
  }
}
