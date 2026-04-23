import { Queue } from '../src/accounts/queue.ts';
import { PullFeed } from '../src/accounts/pullFeed.ts';
import { AnchorUtils } from '../src/anchor-utils/anchor-utils.ts';
import { Surge } from '../src/classes/surge.ts';
import {
  getDefaultGuardianQueue,
  getDefaultQueue,
  getProgramId,
} from '../src/utils/index.ts';
import {
  detectSupportedSolanaCluster,
  getCrossbarGatewayNetwork,
  normalizeSupportedSolanaCluster,
  ON_DEMAND_DEVNET_GUARDIAN_QUEUE,
  ON_DEMAND_DEVNET_PID,
  ON_DEMAND_DEVNET_QUEUE,
  ON_DEMAND_MAINNET_GUARDIAN_QUEUE,
  ON_DEMAND_MAINNET_PID,
  ON_DEMAND_MAINNET_QUEUE,
  SOLANA_DEVNET_GENESIS_HASH,
  SOLANA_MAINNET_BETA_GENESIS_HASH,
  UnsupportedSolanaClusterError,
} from '../src/utils/solanaCluster.ts';

import { Program, web3 } from '@coral-xyz/anchor-31';
import { CrossbarNetwork } from '@switchboard-xyz/common';
import { LegacyCrossbarClient } from '@switchboard-xyz/common-legacy';
import assert from 'node:assert/strict';

const LOCAL_GENESIS_HASH = 'localnet-genesis-hash';

function makeConnection(
  genesisHash: string,
  rpcEndpoint = 'http://127.0.0.1:8899'
): web3.Connection {
  return {
    rpcEndpoint,
    getGenesisHash: async () => genesisHash,
  } as unknown as web3.Connection;
}

function makeProgram(
  programId: web3.PublicKey,
  connection: web3.Connection
): Program {
  return {
    programId,
    provider: {
      connection,
    },
    account: {
      queueAccountData: {
        fetch: async () => ({
          authority: web3.PublicKey.default,
          mrEnclaves: [],
          oracleKeys: [],
          maxQuoteVerificationAge: 0,
          lastHeartbeat: 0,
          nodeTimeout: 0,
          oracleMinStake: 0,
          allowAuthorityOverrideAfter: 0,
          mrEnclavesLen: 0,
          oracleKeysLen: 0,
          reward: 0,
          currIdx: 0,
          gcIdx: 0,
          requireAuthorityHeartbeatPermission: false,
          requireAuthorityVerifyPermission: false,
          requireUsagePermissions: false,
          signerBump: 0,
          mint: web3.PublicKey.default,
          lutSlot: 0,
          allowSubsidies: false,
          ncn: web3.PublicKey.default,
          oracleFeeProportionBps: 0,
        }),
      },
    },
  } as unknown as Program;
}

async function withPatchedProgramAt(
  callback: (
    calls: Array<{ programId: web3.PublicKey; provider: unknown }>
  ) => Promise<void>
): Promise<void> {
  const calls: Array<{ programId: web3.PublicKey; provider: unknown }> = [];
  const descriptor = Object.getOwnPropertyDescriptor(Program, 'at');

  Object.defineProperty(Program, 'at', {
    configurable: true,
    writable: true,
    value: async (programId: web3.PublicKey, provider: unknown) => {
      calls.push({ programId, provider });
      return {
        programId,
        provider,
      } as Program;
    },
  });

  try {
    await callback(calls);
  } finally {
    if (descriptor) {
      Object.defineProperty(Program, 'at', descriptor);
    }
  }
}

async function withPatchedLoadProgramFromConnection(
  callback: () => Promise<void>
): Promise<void> {
  const original = AnchorUtils.loadProgramFromConnection;
  AnchorUtils.loadProgramFromConnection = (async (
    connection: web3.Connection,
    _wallet?: unknown,
    programId?: web3.PublicKey
  ) => {
    return makeProgram(programId ?? ON_DEMAND_MAINNET_PID, connection);
  }) as typeof AnchorUtils.loadProgramFromConnection;

  try {
    await callback();
  } finally {
    AnchorUtils.loadProgramFromConnection = original;
  }
}

async function withPatchedLoadEnv(
  env: Awaited<ReturnType<typeof AnchorUtils.loadEnv>>,
  callback: () => Promise<void>
): Promise<void> {
  const original = AnchorUtils.loadEnv;
  AnchorUtils.loadEnv = (async () => env) as typeof AnchorUtils.loadEnv;

  try {
    await callback();
  } finally {
    AnchorUtils.loadEnv = original;
  }
}

async function withPatchedLegacyCrossbarFetch(
  callback: (calls: string[], expectedJobs: unknown[]) => Promise<void>
): Promise<void> {
  const original = LegacyCrossbarClient.prototype.fetch;
  const calls: string[] = [];
  const expectedJobs = [{ tasks: [] }];

  LegacyCrossbarClient.prototype.fetch = (async function (feedHash: string) {
    calls.push(feedHash);
    return { jobs: expectedJobs } as Awaited<ReturnType<typeof original>>;
  }) as typeof LegacyCrossbarClient.prototype.fetch;

  try {
    await callback(calls, expectedJobs);
  } finally {
    LegacyCrossbarClient.prototype.fetch = original;
  }
}

function makeFeedConfigs(queue = ON_DEMAND_DEVNET_QUEUE, byte = 1) {
  return {
    queue,
    maxVariance: 1,
    minResponses: 1,
    feedHash: Buffer.alloc(32, byte),
    minSampleSize: 1,
  };
}

async function testClusterHelpers(): Promise<void> {
  assert.equal(
    await detectSupportedSolanaCluster(
      makeConnection(SOLANA_MAINNET_BETA_GENESIS_HASH)
    ),
    'mainnet'
  );
  assert.equal(
    await detectSupportedSolanaCluster(makeConnection(SOLANA_DEVNET_GENESIS_HASH)),
    'devnet'
  );
  assert.equal(
    await detectSupportedSolanaCluster(makeConnection(LOCAL_GENESIS_HASH)),
    null
  );
  assert.equal(
    await detectSupportedSolanaCluster({
      getGenesisHash: async () => {
        throw new Error('boom');
      },
    } as unknown as web3.Connection),
    null
  );

  assert.equal(
    normalizeSupportedSolanaCluster('mainnet-beta', 'cluster helper test'),
    'mainnet'
  );
  assert.equal(
    normalizeSupportedSolanaCluster('devnet', 'cluster helper test'),
    'devnet'
  );
  assert.equal(getCrossbarGatewayNetwork('mainnet'), 'mainnet');
  assert.equal(getCrossbarGatewayNetwork('devnet'), 'devnet');

  assert.throws(
    () => normalizeSupportedSolanaCluster('testnet', 'cluster helper test'),
    UnsupportedSolanaClusterError
  );
}

async function testGetProgramId(): Promise<void> {
  const mainnetConnection = makeConnection(
    SOLANA_MAINNET_BETA_GENESIS_HASH,
    'https://api.mainnet-beta.solana.com'
  );
  const devnetConnection = makeConnection(
    SOLANA_DEVNET_GENESIS_HASH,
    'https://api.devnet.solana.com'
  );
  const localConnection = makeConnection(LOCAL_GENESIS_HASH);

  assert.equal(
    (await getProgramId(mainnetConnection)).toBase58(),
    ON_DEMAND_MAINNET_PID.toBase58()
  );
  assert.equal(
    (await getProgramId(devnetConnection)).toBase58(),
    ON_DEMAND_DEVNET_PID.toBase58()
  );
  await assert.rejects(
    () => getProgramId(localConnection),
    UnsupportedSolanaClusterError
  );
  assert.equal(
    (await getProgramId(localConnection, { cluster: 'devnet' })).toBase58(),
    ON_DEMAND_DEVNET_PID.toBase58()
  );
  assert.equal(
    (await getProgramId(localConnection, { cluster: 'mainnet' })).toBase58(),
    ON_DEMAND_MAINNET_PID.toBase58()
  );

  await assert.rejects(
    () => getProgramId(makeConnection(LOCAL_GENESIS_HASH, 'http://local.invalid')),
    error => {
      assert(error instanceof UnsupportedSolanaClusterError);
      assert.match(error.message, /http:\/\/local\.invalid/);
      return true;
    }
  );
}

async function testGetDefaultQueue(): Promise<void> {
  await withPatchedLoadProgramFromConnection(async () => {
    const mainnetQueue = await getDefaultQueue('http://127.0.0.1:8899', {
      cluster: 'mainnet',
    });
    assert.equal(mainnetQueue.pubkey.toBase58(), ON_DEMAND_MAINNET_QUEUE.toBase58());
    assert.equal(
      mainnetQueue.program.programId.toBase58(),
      ON_DEMAND_MAINNET_PID.toBase58()
    );

    const queue = await getDefaultQueue('http://127.0.0.1:8899', {
      cluster: 'devnet',
    });

    assert.equal(queue.pubkey.toBase58(), ON_DEMAND_DEVNET_QUEUE.toBase58());
    assert.equal(
      queue.program.programId.toBase58(),
      ON_DEMAND_DEVNET_PID.toBase58()
    );
  });

  await assert.rejects(
    () => getDefaultQueue('http://127.0.0.1:8899'),
    UnsupportedSolanaClusterError
  );
}

async function testGetDefaultGuardianQueue(): Promise<void> {
  await withPatchedLoadProgramFromConnection(async () => {
    const mainnetGuardianQueue = await getDefaultGuardianQueue(
      'http://127.0.0.1:8899',
      { cluster: 'mainnet' }
    );
    assert.equal(
      mainnetGuardianQueue.pubkey.toBase58(),
      ON_DEMAND_MAINNET_GUARDIAN_QUEUE.toBase58()
    );
    assert.equal(
      mainnetGuardianQueue.program.programId.toBase58(),
      ON_DEMAND_MAINNET_PID.toBase58()
    );

    const devnetGuardianQueue = await getDefaultGuardianQueue(
      'http://127.0.0.1:8899',
      { cluster: 'devnet' }
    );
    assert.equal(
      devnetGuardianQueue.pubkey.toBase58(),
      ON_DEMAND_DEVNET_GUARDIAN_QUEUE.toBase58()
    );
    assert.equal(
      devnetGuardianQueue.program.programId.toBase58(),
      ON_DEMAND_DEVNET_PID.toBase58()
    );
  });

  await assert.rejects(
    () => getDefaultGuardianQueue('http://127.0.0.1:8899'),
    UnsupportedSolanaClusterError
  );
}

async function testAnchorUtilsLoadProgramFromProvider(): Promise<void> {
  await withPatchedProgramAt(async calls => {
    const devnetConnection = makeConnection(SOLANA_DEVNET_GENESIS_HASH);
    const provider = {
      connection: devnetConnection,
    } as unknown as Parameters<typeof AnchorUtils.loadProgramFromProvider>[0];

    await AnchorUtils.loadProgramFromProvider(provider);
    assert.equal(calls.length, 1);
    assert.equal(
      calls[0].programId.toBase58(),
      ON_DEMAND_DEVNET_PID.toBase58()
    );
  });

  const localProvider = {
    connection: makeConnection(LOCAL_GENESIS_HASH),
  } as unknown as Parameters<typeof AnchorUtils.loadProgramFromProvider>[0];
  await assert.rejects(
    () => AnchorUtils.loadProgramFromProvider(localProvider),
    UnsupportedSolanaClusterError
  );

  await withPatchedProgramAt(async calls => {
    await AnchorUtils.loadProgramFromProvider(
      localProvider,
      ON_DEMAND_MAINNET_PID
    );
    assert.equal(calls.length, 1);
    assert.equal(
      calls[0].programId.toBase58(),
      ON_DEMAND_MAINNET_PID.toBase58()
    );
  });
}

async function testQueueLoadDefault(): Promise<void> {
  const mainnetProgram = makeProgram(
    ON_DEMAND_MAINNET_PID,
    makeConnection(SOLANA_MAINNET_BETA_GENESIS_HASH)
  );
  const devnetProgram = makeProgram(
    ON_DEMAND_DEVNET_PID,
    makeConnection(SOLANA_DEVNET_GENESIS_HASH)
  );
  const customProgram = makeProgram(
    new web3.PublicKey(new Uint8Array(32).fill(7)),
    makeConnection(LOCAL_GENESIS_HASH)
  );

  const mainnetQueue = await Queue.loadDefault(mainnetProgram);
  assert.equal(mainnetQueue.pubkey.toBase58(), ON_DEMAND_MAINNET_QUEUE.toBase58());
  assert.equal(mainnetQueue.getNetwork(), CrossbarNetwork.SolanaMainnet);

  const devnetQueue = await Queue.loadDefault(devnetProgram);
  assert.equal(devnetQueue.pubkey.toBase58(), ON_DEMAND_DEVNET_QUEUE.toBase58());
  assert.equal(devnetQueue.getNetwork(), CrossbarNetwork.SolanaDevnet);

  await assert.rejects(
    () => Queue.loadDefault(customProgram),
    UnsupportedSolanaClusterError
  );
}

async function testQueueAutodetectFailureAndExplicitOverride(): Promise<void> {
  const queue = new Queue(
    makeProgram(
      new web3.PublicKey(new Uint8Array(32).fill(8)),
      makeConnection(LOCAL_GENESIS_HASH)
    ),
    new web3.PublicKey(new Uint8Array(32).fill(9))
  );

  const crossbar = {
    crossbarUrl: 'https://crossbar.example',
    setNetwork: () => undefined,
    fetchGateway: async () => ({
      fetchSignaturesConsensus: async () => ({
        oracle_responses: [],
      }),
    }),
  } as unknown as Parameters<Queue['fetchSignaturesConsensus']>[0]['crossbarClient'];

  await assert.rejects(
    () =>
      queue.fetchSignaturesConsensus({
        crossbarClient: crossbar,
        feedConfigs: [],
      }),
    UnsupportedSolanaClusterError
  );

  let selectedNetwork: CrossbarNetwork | null = null;
  const explicitCrossbar = {
    crossbarUrl: 'https://crossbar.example',
    setNetwork: (network: CrossbarNetwork) => {
      selectedNetwork = network;
    },
    fetchGateway: async () => ({
      fetchSignaturesConsensus: async () => ({
        oracle_responses: [],
      }),
    }),
  } as unknown as Parameters<Queue['fetchSignaturesConsensus']>[0]['crossbarClient'];

  queue.setNetwork(CrossbarNetwork.SolanaDevnet);
  await queue.fetchSignaturesConsensus({
    crossbarClient: explicitCrossbar,
    feedConfigs: [],
  });
  assert.equal(selectedNetwork, CrossbarNetwork.SolanaDevnet);
}

async function testQueueProgramIdPrecedence(): Promise<void> {
  let selectedNetwork: CrossbarNetwork | null = null;
  const queue = new Queue(
    makeProgram(
      ON_DEMAND_DEVNET_PID,
      makeConnection(LOCAL_GENESIS_HASH, 'http://unsupported.local')
    ),
    new web3.PublicKey(new Uint8Array(32).fill(11))
  );

  const crossbar = {
    crossbarUrl: 'https://crossbar.example',
    setNetwork: (network: CrossbarNetwork) => {
      selectedNetwork = network;
    },
    fetchGateway: async () => ({
      fetchSignaturesConsensus: async () => ({
        oracle_responses: [],
      }),
    }),
  } as unknown as Parameters<Queue['fetchSignaturesConsensus']>[0]['crossbarClient'];

  await queue.fetchSignaturesConsensus({
    crossbarClient: crossbar,
    feedConfigs: [],
  });
  assert.equal(selectedNetwork, CrossbarNetwork.SolanaDevnet);
}

async function testPullFeedLoadJobsResolution(): Promise<void> {
  await assert.rejects(async () => {
    const [feed] = PullFeed.generate(
      makeProgram(
        new web3.PublicKey(new Uint8Array(32).fill(12)),
        makeConnection(LOCAL_GENESIS_HASH)
      )
    );
    feed.configs = makeFeedConfigs();

    await feed.loadJobs({
      crossbarUrl: 'https://crossbar.example',
      setNetwork: () => undefined,
    } as never);
  }, UnsupportedSolanaClusterError);

  await withPatchedLegacyCrossbarFetch(async (calls, expectedJobs) => {
    let selectedNetwork: CrossbarNetwork | null = null;
    const [feed] = PullFeed.generate(
      makeProgram(
        new web3.PublicKey(new Uint8Array(32).fill(13)),
        makeConnection(LOCAL_GENESIS_HASH)
      )
    );
    feed.configs = makeFeedConfigs(ON_DEMAND_MAINNET_QUEUE, 3);
    feed.setNetwork(CrossbarNetwork.SolanaMainnet);

    const jobs = await feed.loadJobs({
      crossbarUrl: 'https://crossbar.example',
      setNetwork: (network: CrossbarNetwork) => {
        selectedNetwork = network;
      },
    } as never);

    assert.equal(selectedNetwork, CrossbarNetwork.SolanaMainnet);
    assert.deepEqual(jobs, expectedJobs);
    assert.deepEqual(calls, [Buffer.alloc(32, 3).toString('hex')]);
  });

  await withPatchedLegacyCrossbarFetch(async (calls, expectedJobs) => {
    let selectedNetwork: CrossbarNetwork | null = null;
    const [feed] = PullFeed.generate(
      makeProgram(ON_DEMAND_DEVNET_PID, makeConnection(LOCAL_GENESIS_HASH))
    );
    feed.configs = makeFeedConfigs(ON_DEMAND_DEVNET_QUEUE, 5);

    const jobs = await feed.loadJobs({
      crossbarUrl: 'https://crossbar.example',
      setNetwork: (network: CrossbarNetwork) => {
        selectedNetwork = network;
      },
    } as never);

    assert.equal(selectedNetwork, CrossbarNetwork.SolanaDevnet);
    assert.deepEqual(jobs, expectedJobs);
    assert.deepEqual(calls, [Buffer.alloc(32, 5).toString('hex')]);
  });
}

async function testPullFeedRejectsInvalidExplicitNetwork(): Promise<void> {
  const program = makeProgram(
    ON_DEMAND_MAINNET_PID,
    makeConnection(SOLANA_MAINNET_BETA_GENESIS_HASH)
  );
  const [feed] = PullFeed.generate(program);

  await assert.rejects(
    () =>
      PullFeed.fetchUpdateManyLightIx(
        program,
        {
          feeds: [feed],
          chain: 'evm',
          network: 'testnet' as never,
          numSignatures: 1,
        },
        false
      ),
    UnsupportedSolanaClusterError
  );
}

async function testSurgeNetworkResolution(): Promise<void> {
  await assert.rejects(async () => {
    const unsupportedCrossbar = {
      setNetwork: () => undefined,
      fetchGateway: async () => ({
        gatewayUrl: 'https://gateway.unsupported.example',
      }),
    };
    const surge = new Surge({
      apiKey: 'sb_test_key',
      connection: makeConnection(LOCAL_GENESIS_HASH),
      crossbarClient: unsupportedCrossbar as never,
    });
    await (surge as any).ensureGatewayUrl();
  }, UnsupportedSolanaClusterError);

  let selectedNetwork: CrossbarNetwork | null = null;
  const explicitCrossbar = {
    setNetwork: (network: CrossbarNetwork) => {
      selectedNetwork = network;
    },
    fetchGateway: async () => ({
      gatewayUrl: 'https://gateway.devnet.example',
    }),
  };
  const surge = new Surge({
    apiKey: 'sb_test_key',
    network: 'devnet',
    crossbarClient: explicitCrossbar as never,
  });

  await (surge as any).ensureGatewayUrl();
  assert.equal(selectedNetwork, CrossbarNetwork.SolanaDevnet);
}

async function testSurgeResolutionPrecedence(): Promise<void> {
  let selectedNetwork: CrossbarNetwork | null = null;
  const queue = new Queue(
    makeProgram(ON_DEMAND_MAINNET_PID, makeConnection(LOCAL_GENESIS_HASH)),
    ON_DEMAND_MAINNET_QUEUE
  );
  queue.setNetwork(CrossbarNetwork.SolanaMainnet);

  const explicitNetworkCrossbar = {
    setNetwork: (network: CrossbarNetwork) => {
      selectedNetwork = network;
    },
    fetchGateway: async () => ({
      gatewayUrl: 'https://gateway.devnet.example',
    }),
  };
  const explicitNetworkSurge = new Surge({
    apiKey: 'sb_test_key',
    network: 'devnet',
    queue,
    crossbarClient: explicitNetworkCrossbar as never,
  });

  await (explicitNetworkSurge as any).ensureGatewayUrl();
  assert.equal(selectedNetwork, CrossbarNetwork.SolanaDevnet);

  selectedNetwork = null;
  const officialProgramCrossbar = {
    setNetwork: (network: CrossbarNetwork) => {
      selectedNetwork = network;
    },
    fetchGateway: async () => ({
      gatewayUrl: 'https://gateway.devnet-program.example',
    }),
  };
  const officialProgramSurge = new Surge({
    apiKey: 'sb_test_key',
    queue: new Queue(
      makeProgram(ON_DEMAND_DEVNET_PID, makeConnection(LOCAL_GENESIS_HASH)),
      ON_DEMAND_DEVNET_QUEUE
    ),
    crossbarClient: officialProgramCrossbar as never,
  });

  await (officialProgramSurge as any).ensureGatewayUrl();
  assert.equal(selectedNetwork, CrossbarNetwork.SolanaDevnet);

  selectedNetwork = null;
  const envQueue = new Queue(
    makeProgram(ON_DEMAND_MAINNET_PID, makeConnection(LOCAL_GENESIS_HASH)),
    ON_DEMAND_MAINNET_QUEUE
  );
  envQueue.setNetwork(CrossbarNetwork.SolanaMainnet);

  await withPatchedLoadEnv(
    {
      queue: envQueue,
      connection: envQueue.program.provider.connection,
    } as Awaited<ReturnType<typeof AnchorUtils.loadEnv>>,
    async () => {
      const envCrossbar = {
        setNetwork: (network: CrossbarNetwork) => {
          selectedNetwork = network;
        },
        fetchGateway: async () => ({
          gatewayUrl: 'https://gateway.env.example',
        }),
      };

      const envSurge = new Surge({
        apiKey: 'sb_test_key',
        crossbarClient: envCrossbar as never,
      });
      await (envSurge as any).ensureGatewayUrl();
    }
  );

  assert.equal(selectedNetwork, CrossbarNetwork.SolanaMainnet);
}

async function run(): Promise<void> {
  await testClusterHelpers();
  await testGetProgramId();
  await testGetDefaultQueue();
  await testGetDefaultGuardianQueue();
  await testAnchorUtilsLoadProgramFromProvider();
  await testQueueLoadDefault();
  await testQueueAutodetectFailureAndExplicitOverride();
  await testQueueProgramIdPrecedence();
  await testPullFeedLoadJobsResolution();
  await testPullFeedRejectsInvalidExplicitNetwork();
  await testSurgeNetworkResolution();
  await testSurgeResolutionPrecedence();
}

run()
  .then(() => {
    console.log('network resolution smoke test passed');
  })
  .catch(error => {
    console.error(error);
    process.exitCode = 1;
  });
