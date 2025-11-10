import * as web3 from '@solana/web3.js';
import bs58 from 'bs58';
import { Buffer } from 'buffer';
import { inspect } from 'util';

export const QUOTE_PROGRAM_ID = new web3.PublicKey(
  'orac1eFjzWL5R3RbbdMV68K9H6TaCVVcL6LjvQQWAbz'
);

export interface FeedInfo {
  feedHash: Buffer;
  value: bigint;
  minOracleSamples: number;
}

export interface Ed25519SignatureOffsets {
  signatureOffset: number;
  signatureInstructionIndex: number;
  publicKeyOffset: number;
  publicKeyInstructionIndex: number;
  messageDataOffset: number;
  messageDataSize: number;
  messageInstructionIndex: number;
}

export interface OracleSignature {
  offsets: Ed25519SignatureOffsets;
  pubkey: web3.PublicKey;
  signature: Buffer;
}

export interface SwitchboardQuoteJSON {
  signatures: Array<{
    offsets: Ed25519SignatureOffsets;
    pubkey: string;
    signature: string;
  }>;
  signedSlothash: string;
  feeds: Array<{
    feedHash: string;
    value: string;
    minOracleSamples: number;
  }>;
  oracleIdxs: number[];
  slot: number;
  version: number;
  tailDiscriminator: string;
}

export interface SwitchboardQuote {
  signatures: OracleSignature[];
  signedSlothash: Buffer;
  feeds: FeedInfo[];
  oracleIdxs: number[];
  slot: number;
  version: number;
  tailDiscriminator: string;
  toString(): string;
  toJSON(): SwitchboardQuoteJSON;
  [Symbol.toPrimitive](hint: string): string | null;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  [inspect.custom](depth: number, options: any): string;
}

export class OracleQuote {
  static getCanonicalPubkey(
    queueKey: web3.PublicKey,
    feedHashes: Array<string | Buffer>,
    programId?: web3.PublicKey
  ): [web3.PublicKey, number] {
    // Convert feed hashes to seed buffers (feed IDs)
    // This matches the Rust logic: self.feeds().iter().map(|info| info.feed_id().as_slice())
    const feedIdSeeds = feedHashes.map(hash => {
      if (typeof hash === 'string') {
        if (!/^(?:0x)?[0-9a-fA-F]{64}$/.test(hash)) {
          throw new Error(`Invalid feed hash format: ${hash}`);
        }
        return Buffer.from(hash.replace('0x', ''), 'hex');
      } else if (Buffer.isBuffer(hash)) {
        if (hash.length !== 32) {
          throw new Error(
            `Feed hash buffer must be 32 bytes, got ${hash.length}`
          );
        }
        return hash;
      }
      throw new Error('Feed hash must be a hex string or Buffer');
    });

    // Build seeds array with queue key first, then feed IDs
    // This matches Rust logic: seeds.push(queue_key.as_ref()); for (id in feed_ids) seeds.push(id.as_slice());
    const seedRefs: Buffer[] = [
      Buffer.from(queueKey.toBytes()),
      ...feedIdSeeds,
    ];

    // Derive the oracle account using queue key + feed IDs as seeds
    // This matches: Pubkey::find_program_address(&seed_refs, program_id)
    const [quoteAccount, bump] = web3.PublicKey.findProgramAddressSync(
      seedRefs,
      programId ?? QUOTE_PROGRAM_ID
    );

    return [quoteAccount, bump];
  }

  /**
   * Decodes a SwitchboardQuote from a TransactionInstruction
   *
   * This is a convenience method that extracts the data buffer from a
   * TransactionInstruction and calls decode() to deserialize it.
   *
   * @param instruction - The TransactionInstruction containing the serialized data
   * @returns Decoded SwitchboardQuote data
   */
  static decodeIx(instruction: web3.TransactionInstruction): SwitchboardQuote {
    if (!instruction.data || instruction.data.length === 0) {
      throw new Error('Instruction data is empty');
    }

    // Convert instruction data to Buffer if needed
    const buffer = Buffer.isBuffer(instruction.data)
      ? instruction.data
      : Buffer.from(instruction.data);

    return this.decode(buffer);
  }

  /**
   * Decodes a SwitchboardQuote from an Ed25519 instruction buffer
   *
   * The Ed25519 instruction data format created by buildEd25519Instruction:
   * 1. count (1 byte) - number of signatures
   * 2. padding (1 byte)
   * 3. signature offsets (14 bytes each)
   * 4. signatures (64 bytes each)
   * 5. pubkeys (32 bytes each)
   * 6. message (variable length):
   *    - signed_slothash (32 bytes)
   *    - feed_infos (49 bytes each): feed_hash (32) + value (16) + min_oracle_samples (1)
   * 7. oracle indexes (1 byte each)
   * 8. recent_slot (8 bytes, u64 LE)
   * 9. version (1 byte, u8)
   * 10. discriminator (4 bytes, "SBOD")
   *
   * @param buffer - The Ed25519 instruction data buffer
   * @returns Decoded SwitchboardQuote data
   */
  static decode(buffer: Buffer): SwitchboardQuote {
    if (!buffer || buffer.length === 0) {
      throw new Error('Invalid buffer: cannot be empty');
    }

    const SLOT_SIZE = 8;
    const VERSION_SIZE = 1;
    const DISCRIMINATOR_SIZE = 4;

    // 1. Read count (number of signatures)
    const count = buffer[0];
    if (count === 0) {
      throw new Error('Invalid count: must be greater than 0');
    }

    // Calculate offsets
    const SIGNATURE_OFFSETS_START = 2;
    const ED25519_SIGNATURE_OFFSETS_SIZE = 14;

    // Read message offset from first signature's offsets
    const firstOffsetBlock = buffer.slice(
      SIGNATURE_OFFSETS_START,
      SIGNATURE_OFFSETS_START + ED25519_SIGNATURE_OFFSETS_SIZE
    );

    // Message offset is at bytes 8-9 (LE u16)
    const messageOffset = firstOffsetBlock.readUInt16LE(8);

    // Message size is at bytes 10-11 (LE u16)
    const messageSize = firstOffsetBlock.readUInt16LE(10);

    // Parse signature data for the SwitchboardQuote format
    const signatures: OracleSignature[] = [];
    let offsetsPos = SIGNATURE_OFFSETS_START;

    for (let i = 0; i < count; i++) {
      const offsetBlock = buffer.slice(
        offsetsPos,
        offsetsPos + ED25519_SIGNATURE_OFFSETS_SIZE
      );

      const offsets: Ed25519SignatureOffsets = {
        signatureOffset: offsetBlock.readUInt16LE(0),
        signatureInstructionIndex: offsetBlock.readUInt16LE(2),
        publicKeyOffset: offsetBlock.readUInt16LE(4),
        publicKeyInstructionIndex: offsetBlock.readUInt16LE(6),
        messageDataOffset: offsetBlock.readUInt16LE(8),
        messageDataSize: offsetBlock.readUInt16LE(10),
        messageInstructionIndex: offsetBlock.readUInt16LE(12),
      };

      const pubkeyBytes = buffer.slice(
        offsets.publicKeyOffset,
        offsets.publicKeyOffset + 32
      );
      const pubkey = new web3.PublicKey(pubkeyBytes);

      const signature = buffer.slice(
        offsets.signatureOffset,
        offsets.signatureOffset + 64
      );

      signatures.push({ offsets, pubkey, signature });
      offsetsPos += ED25519_SIGNATURE_OFFSETS_SIZE;
    }

    // Extract the full message
    const message = buffer.slice(messageOffset, messageOffset + messageSize);

    // Parse message structure:
    // 1. signed_slothash (32 bytes)
    const signedSlothash = message.slice(0, 32);

    // 2. feed_infos (49 bytes each)
    const feedInfosData = message.slice(32);
    const FEED_INFO_SIZE = 49; // 32 (feed_hash) + 16 (value) + 1 (min_oracle_samples)
    const numFeeds = Math.floor(feedInfosData.length / FEED_INFO_SIZE);

    const feeds: FeedInfo[] = [];
    for (let i = 0; i < numFeeds; i++) {
      const offset = i * FEED_INFO_SIZE;
      const feedInfoBuffer = feedInfosData.slice(
        offset,
        offset + FEED_INFO_SIZE
      );

      const feedHash = feedInfoBuffer.slice(0, 32);
      const valueBytes = feedInfoBuffer.slice(32, 48);

      // Convert 16-byte little-endian to bigint
      let value = BigInt(0);
      for (let j = 0; j < 16; j++) {
        value |= BigInt(valueBytes[j]) << (BigInt(j) * BigInt(8));
      }

      const minOracleSamples = feedInfoBuffer[48];

      feeds.push({
        feedHash,
        value,
        minOracleSamples,
      });
    }

    // 3. Oracle indexes (after message)
    const oracleIndexesOffset = messageOffset + messageSize;
    const oracleIdxs: number[] = [];
    for (let i = 0; i < count; i++) {
      oracleIdxs.push(buffer[oracleIndexesOffset + i]);
    }

    // 4. Recent slot (8 bytes u64 LE)
    const slotOffset = oracleIndexesOffset + count;
    const slotBuffer = buffer.slice(slotOffset, slotOffset + SLOT_SIZE);
    const slot = Number(slotBuffer.readBigUInt64LE(0));

    // 5. Version (1 byte u8)
    const versionOffset = slotOffset + SLOT_SIZE;
    const version = buffer[versionOffset];

    // 6. Discriminator (4 bytes)
    const discriminatorOffset = versionOffset + VERSION_SIZE;
    const tailDiscriminator = buffer
      .slice(discriminatorOffset, discriminatorOffset + DISCRIMINATOR_SIZE)
      .toString('utf8');

    const quote = {
      signatures,
      signedSlothash,
      feeds,
      oracleIdxs,
      slot,
      version,
      tailDiscriminator,
      toString(): string {
        return OracleQuote.toString(this as SwitchboardQuote);
      },
      toJSON() {
        return OracleQuote.toJSON(this as SwitchboardQuote);
      },
      [Symbol.toPrimitive](hint: string): string | null {
        return hint === 'string' ? this.toString() : null;
      },
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      [inspect.custom](depth: number, options: any) {
        return inspect(this.toJSON(), {
          ...options,
          depth: null,
          colors: false,
        });
      },
    };

    return quote;
  }

  /**
   * Converts a SwitchboardQuote to a string representation
   *
   * @param quote - The SwitchboardQuote to convert
   * @param precision - Number of decimal places for feed values (default: 18)
   * @returns String representation
   */
  static toString(quote: SwitchboardQuote, precision = 18): string {
    const lines: string[] = [];

    lines.push('SwitchboardQuote {');
    lines.push(`  Signatures: ${quote.signatures.length}`);

    quote.signatures.forEach((sig, i) => {
      lines.push(`    [${i}] Oracle: ${sig.pubkey.toBase58()}`);
    });

    lines.push(`  Signed Slot Hash: 0x${quote.signedSlothash.toString('hex')}`);
    lines.push(`  Feeds: ${quote.feeds.length}`);

    quote.feeds.forEach((feed, i) => {
      const feedId = `0x${feed.feedHash.toString('hex')}`;
      // Convert i128 fixed-point to decimal
      // Calculate 10^precision without using ** operator (for older TS targets)
      let divisor = BigInt(1);
      for (let j = 0; j < precision; j++) {
        divisor *= BigInt(10);
      }
      const integerPart = feed.value / divisor;
      const fractionalPart = feed.value % divisor;
      const fractionalStr = fractionalPart
        .toString()
        .padStart(precision, '0')
        .replace(/0+$/, '');
      const valueStr = fractionalStr
        ? `${integerPart}.${fractionalStr}`
        : integerPart.toString();

      lines.push(`    [${i}] Feed ID: ${feedId}`);
      lines.push(`        Value: ${valueStr}`);
      lines.push(`        Min Samples: ${feed.minOracleSamples}`);
    });

    lines.push(`  Oracle Indices: [${quote.oracleIdxs.join(', ')}]`);
    lines.push(`  Slot: ${quote.slot}`);
    lines.push(`  Version: ${quote.version}`);
    lines.push(`  Discriminator: ${quote.tailDiscriminator}`);
    lines.push('}');

    return lines.join('\n');
  }

  /**
   * Converts a SwitchboardQuote to a JSON-serializable object
   *
   * @param quote - The SwitchboardQuote to convert
   * @returns JSON-serializable object
   */
  static toJSON(quote: SwitchboardQuote): SwitchboardQuoteJSON {
    return {
      signatures: quote.signatures.map(sig => ({
        offsets: { ...sig.offsets },
        pubkey: sig.pubkey.toBase58(),
        signature: bs58.encode(sig.signature),
      })),
      signedSlothash: bs58.encode(quote.signedSlothash),
      feeds: quote.feeds.map(feed => {
        // Convert i128 fixed-point to decimal (divide by 10^18)
        const precision = 18;
        let divisor = BigInt(1);
        for (let j = 0; j < precision; j++) {
          divisor *= BigInt(10);
        }
        const integerPart = feed.value / divisor;
        const fractionalPart = feed.value % divisor;
        const fractionalStr = fractionalPart
          .toString()
          .padStart(precision, '0')
          .replace(/0+$/, '');
        const valueStr = fractionalStr
          ? `${integerPart}.${fractionalStr}`
          : integerPart.toString();

        return {
          feedHash: feed.feedHash.toString('hex'),
          value: valueStr,
          minOracleSamples: feed.minOracleSamples,
        };
      }),
      oracleIdxs: quote.oracleIdxs,
      slot: quote.slot,
      version: quote.version,
      tailDiscriminator: quote.tailDiscriminator,
    };
  }
}
