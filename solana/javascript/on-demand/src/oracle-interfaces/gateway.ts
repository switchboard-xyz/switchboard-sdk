// Re-export Gateway types and classes from @switchboard-xyz/common
// Note: Importing from main entry point instead of subpath for better compatibility
export type {
  AttestEnclaveResponse,
  BridgeEnclaveResponse,
  FeedEvalBatchResponse,
  FeedEvalManyResponse,
  FeedEvalResponse,
  FeedRequest,
  FeedRequestV1,
  FeedRequestV2,
  FetchQuoteResponse,
  FetchSignaturesBatchResponse,
  FetchSignaturesConsensusResponse,
  FetchSignaturesMultiResponse,
  PingResponse,
  RandomnessRevealResponse,
} from '@switchboard-xyz/common';
export { Gateway } from '@switchboard-xyz/common';
