import type { FetchSignaturesConsensusResponse } from './types/crossbar.js';

const SCALE_FACTOR = 1e18;

export class EVMUtils {
  /**
   * Convert fetch signatures consensus response to EVM-compatible bytes
   *
   * IMPORTANT: This function uses feed-level ECDSA signatures, NOT consensus signatures.
   * - Consensus signatures (oracle.signature) are used for the consensus protocol
   * - Feed signatures (oracle.feed_responses[].signature) are used for EVM verification
   *
   * Format expected by EVM contract parseFeedUpdateData:
   * - Timestamp (8 bytes)
   * - Feed count (1 byte)
   * - For each feed (49 bytes each):
   *   - Feed ID (32 bytes)
   *   - Value (16 bytes, int128)
   *   - Min oracle samples (1 byte)
   * - Signature count (1 byte)
   * - For each signature (65 bytes each):
   *   - Signature data (65 bytes: r + s + v)
   */
  static convertToEVMUpdateData(response: FetchSignaturesConsensusResponse): {
    updateData: string;
    feedCount: number;
    signatureCount: number;
    totalBytes: number;
  } {
    // Calculate feed signature count first (always use feed signatures for EVM)
    const feedSignatureCount = response.oracleResponses.reduce(
      (count, oracle) => count + oracle.feedResponses.length,
      0
    );

    const buffer = new ArrayBuffer(
      8 + // timestamp
        1 + // feed count
        response.medianResponses.length * 49 + // feed data
        1 + // signature count
        feedSignatureCount * 65 // signatures (always feed signatures for EVM)
    );

    const view = new DataView(buffer);
    const uint8View = new Uint8Array(buffer);
    let offset = 0;

    // 1. Write timestamp (8 bytes) - use current timestamp or custom
    const timestamp = response.timestamp;

    if (!timestamp) {
      throw new Error(
        '[FeedFetchConsensus] Timestamp is required for EVM update data'
      );
    }

    view.setBigUint64(offset, BigInt(timestamp), false); // big endian

    offset += 8;

    // 2. Write feed count (1 byte)
    const feedCount = response.medianResponses.length;

    view.setUint8(offset, feedCount);

    offset += 1;

    // 3. Write feed data (49 bytes each)
    for (
      let feedIndex = 0;
      feedIndex < response.medianResponses.length;
      feedIndex++
    ) {
      const median = response.medianResponses[feedIndex];

      // Feed ID (32 bytes) - convert hex feed_hash to bytes
      const feedIdHex = median.feedHash.startsWith('0x')
        ? median.feedHash.slice(2)
        : median.feedHash;
      const feedIdBytes = this.hexToBytes(feedIdHex);

      if (feedIdBytes.length !== 32) {
        throw new Error(
          `Invalid feed hash length: ${feedIdBytes.length}, expected 32`
        );
      }

      uint8View.set(feedIdBytes, offset);
      offset += 32;

      // Value (16 bytes, int128) - convert decimal string to int128 bytes
      const valueStr = median.value;
      const scaledValue = Math.floor(parseFloat(valueStr) / SCALE_FACTOR);

      // Convert to 16-byte big-endian signed integer
      const valueBigInt = BigInt(scaledValue);
      const valueBytes = this.bigIntToBytes(valueBigInt, 16);

      uint8View.set(valueBytes, offset);
      offset += 16;

      // Min oracle samples (1 byte)

      view.setUint8(offset, median.numOracles);

      offset += 1;
    }

    // 4. Determine feed signatures to use for EVM (consensus signatures are for different purpose)
    const feedSignatures = response.oracleResponses.flatMap(oracle =>
      oracle.feedResponses.map(feed => ({
        signature: feed.signature,
        recovery_id: feed.recovery_id,
        oracle_pubkey: feed.oracle_pubkey || oracle.oraclePublickey,
        feed_hash: feed.feed_hash,
      }))
    );

    // Write signature count (1 byte) - use feed signatures count
    const sigCount = feedSignatures.length;

    view.setUint8(offset, sigCount);

    offset += 1;

    for (let sigIndex = 0; sigIndex < feedSignatures.length; sigIndex++) {
      const feedSig = feedSignatures[sigIndex];

      // Convert Base64 signature to bytes (standard format for feed signatures)
      let signatureBytes: Uint8Array;

      try {
        // Try Base64 decoding first (standard format)
        const base64Sig = feedSig.signature;
        const sigBuffer = Buffer.from(base64Sig, 'base64');
        signatureBytes = new Uint8Array(sigBuffer);
      } catch {
        // If Base64 fails, try hex decoding
        const sigHex = feedSig.signature.startsWith('0x')
          ? feedSig.signature.slice(2)
          : feedSig.signature;
        signatureBytes = this.hexToBytes(sigHex);
      }

      // Create 65-byte EVM signature (r + s + v)
      let finalSigBytes: Uint8Array;

      if (signatureBytes.length === 65) {
        // Already in correct format
        finalSigBytes = signatureBytes;
      } else if (signatureBytes.length === 64) {
        // r + s format, need to append recovery_id as v
        finalSigBytes = new Uint8Array(65);
        finalSigBytes.set(signatureBytes, 0); // r (32) + s (32)
        finalSigBytes[64] = feedSig.recovery_id; // v
      } else {
        throw new Error(
          `❌ Invalid feed signature length: ${signatureBytes.length} bytes. Expected 64 or 65.\nSignature: ${feedSig.signature}\nThis might indicate the endpoint doesn't provide EVM-compatible feed signatures.`
        );
      }

      uint8View.set(finalSigBytes, offset);

      offset += 65;
    }

    // Convert to hex string
    const finalHex =
      '0x' +
      Array.from(uint8View)
        .map(b => b.toString(16).padStart(2, '0'))
        .join('');

    return {
      updateData: finalHex,
      feedCount: response.medianResponses.length,
      signatureCount: feedSignatures.length,
      totalBytes: (finalHex.length - 2) / 2,
    };
  }

  /**
   * Convert hex string to bytes
   */
  private static hexToBytes(hex: string): Uint8Array {
    const bytes = new Uint8Array(hex.length / 2);
    for (let i = 0; i < hex.length; i += 2) {
      bytes[i / 2] = parseInt(hex.substr(i, 2), 16);
    }
    return bytes;
  }

  /**
   * Convert BigInt to bytes with specified length
   */
  private static bigIntToBytes(value: bigint, length: number): Uint8Array {
    const bytes = new Uint8Array(length);
    let tempValue = value;

    // Handle negative numbers (two's complement)
    if (tempValue < 0) {
      // Convert to unsigned representation
      tempValue = (BigInt(1) << BigInt(length * 8)) + tempValue;
    }

    for (let i = length - 1; i >= 0; i--) {
      bytes[i] = Number(tempValue & BigInt(0xff));
      tempValue = tempValue >> BigInt(8);
    }

    return bytes;
  }

  /**
   * Utility function to create EVM update data from consensus response
   * This can be used in other parts of your application
   */
  static createEVMUpdateDataFromConsensus(
    response: FetchSignaturesConsensusResponse
  ): {
    updateData: string;
    feedCount: number;
    signatureCount: number;
    totalBytes: number;
  } {
    return this.convertToEVMUpdateData(response);
  }
}
