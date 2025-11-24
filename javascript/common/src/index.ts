export * as BigUtils from './big.js';
export * from './crossbar-client.js';
export * from './evm-utils.js';
export * from './feed-hash.js';
export * from './gateway.js';
export { default as networks } from './networks/index.js';
export {
  getSupportedChain,
  getSupportedEvmChain,
  getSupportedEvmChainId,
  isSupportedChain,
  isSupportedChainId,
  isSupportedEvmChain,
} from './networks/index.js';
export * from './networks/types.js';
export {
  IOracleFeed,
  IOracleJob,
  ITask,
  OracleFeed,
  OracleJob,
  Task,
} from './protos.js';
export * from './secrets.js';
export * from './switchboard-decimal.js';
export * from './types.js';
export * from './types/chains.js';
export {
  V2ConsensusOracleResponse,
  V2MedianResponse,
  V2UpdateQuery,
  V2UpdateResponse,
} from './types/crossbar.js';
export { sleep } from './utils/async.js';
export * from './utils/index.js';
export {
  deserializeOracleJob,
  serializeOracleJob,
} from './utils/oracle-job.js'; // NOTE: Exported here for compatibility with old code
export { Big } from 'big.js';
export { default as BN } from 'bn.js';
export { default as bs58 } from 'bs58';

// import { OracleJob } from "./protos/index.js";
// export import ITask = OracleJob.ITask;
// export import Task = OracleJob.Task;

import protobuf from 'protobufjs/minimal.js';
protobuf.util.toJSONOptions = {
  longs: String,
  enums: String,
  bytes: String,
  json: true,
  // oneofs: true,
};
