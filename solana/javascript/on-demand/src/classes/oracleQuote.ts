import * as web3 from '@solana/web3.js';
import bs58 from 'bs58';
import { Buffer } from 'buffer';
import { inspect } from 'util';

export const QUOTE_PROGRAM_ID = new web3.PublicKey(
  'orac1eFjzWL5R3RbbdMV68K9H6TaCVVcL6LjvQQWAbz'
);
export const FEED_AUTHORITY_UPDATE_OPCODE = 0x01;
export const MAX_AUTHORITY_FEED_IDS = 14;
export const MAX_AUTHORITY_FEED_ID_BYTES = MAX_AUTHORITY_FEED_IDS * 32;

const AUTHORITY_QUOTE_SCHEME_TAG = Buffer.from('AUTH');
const QUOTE_TAIL_DISCRIMINATOR = Buffer.from('SBOD');
const ZERO_SLOT_HASH = Buffer.alloc(32);
const FEED_INFO_SIZE = 49;
const ED25519_SIGNATURE_OFFSETS_SIZE = 14;
const SLOT_SIZE = 8;
const VERSION_SIZE = 1;
const DISCRIMINATOR_SIZE = 4;

export type QuoteSourceScheme = 'oracle' | 'authority';

export interface FeedInfo {
  feedHash: Buffer;
  value: bigint;
  minOracleSamples: number;
}

export interface AuthorityFeedInfoInput {
  feedHash: string | Buffer;
  value: bigint;
  minOracleSamples?: number;
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
  sourceScheme: QuoteSourceScheme;
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
  sourceScheme: QuoteSourceScheme;
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

interface ParsedAuthorityQuotePayload {
  feeds: FeedInfo[];
  slot: number;
  version: number;
  tailDiscriminator: string;
}

function normalizeFeedHash(hash: string | Buffer): Buffer {
  if (typeof hash === 'string') {
    if (!/^(?:0x)?[0-9a-fA-F]{64}$/.test(hash)) {
      throw new Error(`Invalid feed hash format: ${hash}`);
    }
    return Buffer.from(hash.replace(/^0x/, ''), 'hex');
  }
  if (Buffer.isBuffer(hash)) {
    if (hash.length !== 32) {
      throw new Error(`Feed hash buffer must be 32 bytes, got ${hash.length}`);
    }
    return Buffer.from(hash);
  }
  throw new Error('Feed hash must be a hex string or Buffer');
}

function normalizeFeedHashes(feedHashes: Array<string | Buffer>): Buffer[] {
  return feedHashes.map(normalizeFeedHash);
}

function validateAuthorityFeedCount(feedCount: number): void {
  if (feedCount > MAX_AUTHORITY_FEED_IDS) {
    throw new Error(
      `Authority quote payload supports at most ${MAX_AUTHORITY_FEED_IDS} feed IDs (${MAX_AUTHORITY_FEED_ID_BYTES} bytes)`
    );
  }
}

function readI128LE(buffer: Buffer): bigint {
  if (buffer.length !== 16) {
    throw new Error(`Expected 16 bytes for i128, got ${buffer.length}`);
  }

  let value = BigInt(0);
  for (let i = 0; i < 16; i++) {
    value |= BigInt(buffer[i]) << (BigInt(i) * BigInt(8));
  }

  const signBit = BigInt(1) << BigInt(127);
  if ((value & signBit) !== BigInt(0)) {
    value -= BigInt(1) << BigInt(128);
  }

  return value;
}

function writeI128LE(value: bigint): Buffer {
  const min = -(BigInt(1) << BigInt(127));
  const max = (BigInt(1) << BigInt(127)) - BigInt(1);
  if (value < min || value > max) {
    throw new Error('Feed value is out of range for i128');
  }

  let normalized = value;
  if (normalized < BigInt(0)) {
    normalized += BigInt(1) << BigInt(128);
  }

  const out = Buffer.alloc(16);
  for (let i = 0; i < 16; i++) {
    out[i] = Number((normalized >> (BigInt(i) * BigInt(8))) & BigInt(0xff));
  }
  return out;
}

function formatFeedValue(value: bigint, precision = 18): string {
  let divisor = BigInt(1);
  for (let i = 0; i < precision; i++) {
    divisor *= BigInt(10);
  }

  const isNegative = value < BigInt(0);
  const absValue = isNegative ? -value : value;
  const integerPart = absValue / divisor;
  const fractionalPart = absValue % divisor;
  const fractionalStr = fractionalPart
    .toString()
    .padStart(precision, '0')
    .replace(/0+$/, '');
  const sign = isNegative ? '-' : '';

  return fractionalStr
    ? `${sign}${integerPart}.${fractionalStr}`
    : `${sign}${integerPart}`;
}

function createQuoteObject(
  sourceScheme: QuoteSourceScheme,
  signatures: OracleSignature[],
  signedSlothash: Buffer,
  feeds: FeedInfo[],
  oracleIdxs: number[],
  slot: number,
  version: number,
  tailDiscriminator: string
): SwitchboardQuote {
  return {
    sourceScheme,
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
}

function parseAuthorityQuotePayload(
  buffer: Buffer
): ParsedAuthorityQuotePayload {
  const minimumLength =
    AUTHORITY_QUOTE_SCHEME_TAG.length + 1 + SLOT_SIZE + 1 + 4;
  if (buffer.length < minimumLength) {
    throw new Error('Authority quote payload is too short');
  }
  if (!buffer.subarray(0, 4).equals(AUTHORITY_QUOTE_SCHEME_TAG)) {
    throw new Error('Invalid authority quote payload tag');
  }

  const feedCount = buffer[4];
  if (feedCount === 0) {
    throw new Error('Authority quote payload requires at least one feed');
  }
  validateAuthorityFeedCount(feedCount);

  const expectedLength = 4 + 1 + feedCount * FEED_INFO_SIZE + SLOT_SIZE + 1 + 4;
  if (buffer.length !== expectedLength) {
    throw new Error(
      `Invalid authority quote payload length: expected ${expectedLength}, got ${buffer.length}`
    );
  }

  const feeds: FeedInfo[] = [];
  let offset = 5;
  for (let i = 0; i < feedCount; i++) {
    const feedHash = buffer.subarray(offset, offset + 32);
    const value = readI128LE(buffer.subarray(offset + 32, offset + 48));
    const minOracleSamples = buffer[offset + 48];
    feeds.push({
      feedHash,
      value,
      minOracleSamples,
    });
    offset += FEED_INFO_SIZE;
  }

  const slot = Number(buffer.readBigUInt64LE(offset));
  const version = buffer[offset + SLOT_SIZE];
  const tailDiscriminator = buffer
    .subarray(
      offset + SLOT_SIZE + VERSION_SIZE,
      offset + SLOT_SIZE + VERSION_SIZE + DISCRIMINATOR_SIZE
    )
    .toString('utf8');
  if (tailDiscriminator !== QUOTE_TAIL_DISCRIMINATOR.toString('utf8')) {
    throw new Error('Invalid authority quote tail discriminator');
  }

  return {
    feeds,
    slot,
    version,
    tailDiscriminator,
  };
}

export class OracleQuote {
  static getCanonicalPubkey(
    queueKey: web3.PublicKey,
    feedHashes: Array<string | Buffer>,
    programId?: web3.PublicKey
  ): [web3.PublicKey, number] {
    const seedRefs: Buffer[] = [
      Buffer.from(queueKey.toBytes()),
      ...normalizeFeedHashes(feedHashes),
    ];

    return web3.PublicKey.findProgramAddressSync(
      seedRefs,
      programId ?? QUOTE_PROGRAM_ID
    );
  }

  static deriveAuthorityQuotePubkey(
    authority: web3.PublicKey,
    feedHashes: Array<string | Buffer>,
    programId?: web3.PublicKey
  ): [web3.PublicKey, number] {
    const normalizedFeedHashes = normalizeFeedHashes(feedHashes);
    validateAuthorityFeedCount(normalizedFeedHashes.length);

    return web3.PublicKey.findProgramAddressSync(
      [
        AUTHORITY_QUOTE_SCHEME_TAG,
        Buffer.from(authority.toBytes()),
        ...normalizedFeedHashes,
      ],
      programId ?? QUOTE_PROGRAM_ID
    );
  }

  static buildAuthorityQuotePayload(
    feeds: AuthorityFeedInfoInput[],
    slot: bigint | number,
    version = 1
  ): Buffer {
    if (feeds.length === 0) {
      throw new Error('Authority quote payload requires at least one feed');
    }
    validateAuthorityFeedCount(feeds.length);

    const parts: Buffer[] = [
      AUTHORITY_QUOTE_SCHEME_TAG,
      Buffer.from([feeds.length]),
    ];
    for (const feed of feeds) {
      const feedHash = normalizeFeedHash(feed.feedHash);
      const minOracleSamples = feed.minOracleSamples ?? 1;
      if (minOracleSamples < 0 || minOracleSamples > 255) {
        throw new Error('minOracleSamples must fit in u8');
      }

      parts.push(feedHash);
      parts.push(writeI128LE(feed.value));
      parts.push(Buffer.from([minOracleSamples]));
    }

    const slotBuffer = Buffer.alloc(8);
    slotBuffer.writeBigUInt64LE(BigInt(slot), 0);
    parts.push(slotBuffer);
    parts.push(Buffer.from([version]));
    parts.push(QUOTE_TAIL_DISCRIMINATOR);

    return Buffer.concat(parts);
  }

  static buildFeedAuthorityUpdateInstruction(params: {
    authority: web3.PublicKey;
    payer: web3.PublicKey;
    payload: Buffer;
    quoteAccount?: web3.PublicKey;
    programId?: web3.PublicKey;
  }): web3.TransactionInstruction {
    const programId = params.programId ?? QUOTE_PROGRAM_ID;
    const payload = Buffer.isBuffer(params.payload)
      ? Buffer.from(params.payload)
      : Buffer.from(params.payload);
    const parsed = parseAuthorityQuotePayload(payload);
    const [derivedQuoteAccount] = OracleQuote.deriveAuthorityQuotePubkey(
      params.authority,
      parsed.feeds.map(feed => feed.feedHash),
      programId
    );

    const quoteAccount = params.quoteAccount ?? derivedQuoteAccount;
    if (!quoteAccount.equals(derivedQuoteAccount)) {
      throw new Error(
        'Provided quoteAccount does not match authority quote derivation'
      );
    }

    return new web3.TransactionInstruction({
      programId,
      keys: [
        { pubkey: quoteAccount, isSigner: false, isWritable: true },
        { pubkey: params.authority, isSigner: true, isWritable: false },
        {
          pubkey: web3.SYSVAR_CLOCK_PUBKEY,
          isSigner: false,
          isWritable: false,
        },
        { pubkey: params.payer, isSigner: true, isWritable: true },
        {
          pubkey: web3.SystemProgram.programId,
          isSigner: false,
          isWritable: false,
        },
      ],
      data: Buffer.concat([
        Buffer.from([FEED_AUTHORITY_UPDATE_OPCODE]),
        payload,
      ]),
    });
  }

  static decodeIx(instruction: web3.TransactionInstruction): SwitchboardQuote {
    if (!instruction.data || instruction.data.length === 0) {
      throw new Error('Instruction data is empty');
    }

    const buffer = Buffer.isBuffer(instruction.data)
      ? instruction.data
      : Buffer.from(instruction.data);

    return this.decode(buffer);
  }

  static decode(buffer: Buffer): SwitchboardQuote {
    if (!buffer || buffer.length === 0) {
      throw new Error('Invalid buffer: cannot be empty');
    }

    if (buffer.subarray(0, 4).equals(AUTHORITY_QUOTE_SCHEME_TAG)) {
      const parsed = parseAuthorityQuotePayload(buffer);
      return createQuoteObject(
        'authority',
        [],
        Buffer.from(ZERO_SLOT_HASH),
        parsed.feeds,
        [],
        parsed.slot,
        parsed.version,
        parsed.tailDiscriminator
      );
    }

    const count = buffer[0];
    if (count === 0) {
      throw new Error(
        'Invalid oracle quote: signature count must be greater than 0'
      );
    }

    const firstOffsetBlock = buffer.subarray(
      2,
      2 + ED25519_SIGNATURE_OFFSETS_SIZE
    );
    const messageOffset = firstOffsetBlock.readUInt16LE(8);
    const messageSize = firstOffsetBlock.readUInt16LE(10);

    const signatures: OracleSignature[] = [];
    let offsetsPos = 2;
    for (let i = 0; i < count; i++) {
      const offsetBlock = buffer.subarray(
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

      signatures.push({
        offsets,
        pubkey: new web3.PublicKey(
          buffer.subarray(offsets.publicKeyOffset, offsets.publicKeyOffset + 32)
        ),
        signature: buffer.subarray(
          offsets.signatureOffset,
          offsets.signatureOffset + 64
        ),
      });
      offsetsPos += ED25519_SIGNATURE_OFFSETS_SIZE;
    }

    const message = buffer.subarray(messageOffset, messageOffset + messageSize);
    const signedSlothash = message.subarray(0, 32);
    const feedInfosData = message.subarray(32);
    const numFeeds = Math.floor(feedInfosData.length / FEED_INFO_SIZE);

    const feeds: FeedInfo[] = [];
    for (let i = 0; i < numFeeds; i++) {
      const offset = i * FEED_INFO_SIZE;
      const feedInfoBuffer = feedInfosData.subarray(
        offset,
        offset + FEED_INFO_SIZE
      );
      feeds.push({
        feedHash: feedInfoBuffer.subarray(0, 32),
        value: readI128LE(feedInfoBuffer.subarray(32, 48)),
        minOracleSamples: feedInfoBuffer[48],
      });
    }

    const oracleIndexesOffset = messageOffset + messageSize;
    const oracleIdxs: number[] = [];
    for (let i = 0; i < count; i++) {
      oracleIdxs.push(buffer[oracleIndexesOffset + i]);
    }

    const slotOffset = oracleIndexesOffset + count;
    const slot = Number(buffer.readBigUInt64LE(slotOffset));
    const versionOffset = slotOffset + SLOT_SIZE;
    const version = buffer[versionOffset];
    const tailDiscriminator = buffer
      .subarray(
        versionOffset + VERSION_SIZE,
        versionOffset + VERSION_SIZE + DISCRIMINATOR_SIZE
      )
      .toString('utf8');

    return createQuoteObject(
      'oracle',
      signatures,
      signedSlothash,
      feeds,
      oracleIdxs,
      slot,
      version,
      tailDiscriminator
    );
  }

  static toString(quote: SwitchboardQuote, precision = 18): string {
    const lines: string[] = [];

    lines.push('SwitchboardQuote {');
    lines.push(`  Source: ${quote.sourceScheme}`);
    lines.push(`  Signatures: ${quote.signatures.length}`);

    quote.signatures.forEach((sig, i) => {
      lines.push(`    [${i}] Oracle: ${sig.pubkey.toBase58()}`);
    });

    lines.push(`  Signed Slot Hash: 0x${quote.signedSlothash.toString('hex')}`);
    lines.push(`  Feeds: ${quote.feeds.length}`);

    quote.feeds.forEach((feed, i) => {
      lines.push(`    [${i}] Feed ID: 0x${feed.feedHash.toString('hex')}`);
      lines.push(`        Value: ${formatFeedValue(feed.value, precision)}`);
      lines.push(`        Min Samples: ${feed.minOracleSamples}`);
    });

    lines.push(`  Oracle Indices: [${quote.oracleIdxs.join(', ')}]`);
    lines.push(`  Slot: ${quote.slot}`);
    lines.push(`  Version: ${quote.version}`);
    lines.push(`  Discriminator: ${quote.tailDiscriminator}`);
    lines.push('}');

    return lines.join('\n');
  }

  static toJSON(quote: SwitchboardQuote): SwitchboardQuoteJSON {
    return {
      sourceScheme: quote.sourceScheme,
      signatures: quote.signatures.map(sig => ({
        offsets: { ...sig.offsets },
        pubkey: sig.pubkey.toBase58(),
        signature: bs58.encode(sig.signature),
      })),
      signedSlothash: bs58.encode(quote.signedSlothash),
      feeds: quote.feeds.map(feed => ({
        feedHash: feed.feedHash.toString('hex'),
        value: formatFeedValue(feed.value),
        minOracleSamples: feed.minOracleSamples,
      })),
      oracleIdxs: quote.oracleIdxs,
      slot: quote.slot,
      version: quote.version,
      tailDiscriminator: quote.tailDiscriminator,
    };
  }
}
