import { PublicKey, TransactionInstruction } from '@solana/web3.js';
import { Buffer } from 'buffer';

export interface HexInstructionAccountMeta {
  pubkey: string;
  isSigner: boolean;
  isWritable: boolean;
}

export interface HexInstructionObject {
  keys: HexInstructionAccountMeta[];
  programId: string;
  data: string;
}

export type CrossbarInstructionWire = string | HexInstructionObject;

function trimHexPrefix(hex: string): string {
  return hex.startsWith('0x') ? hex.slice(2) : hex;
}

function readU64LE(buffer: Buffer, offset: number, label: string): number {
  if (offset + 8 > buffer.length) {
    throw new Error(`Truncated serialized instruction while reading ${label}`);
  }

  const value = Number(buffer.readBigUInt64LE(offset));
  if (!Number.isSafeInteger(value)) {
    throw new Error(`Serialized instruction ${label} exceeds safe integer size`);
  }
  return value;
}

function assertReadable(
  buffer: Buffer,
  offset: number,
  length: number,
  label: string
): void {
  if (offset + length > buffer.length) {
    throw new Error(`Truncated serialized instruction while reading ${label}`);
  }
}

function legacyIxFromHex(pullIx: HexInstructionObject): TransactionInstruction {
  const keys = pullIx.keys.map(key => ({
    ...key,
    pubkey: new PublicKey(Buffer.from(trimHexPrefix(key.pubkey), 'hex')),
  }));
  const programId = new PublicKey(
    Buffer.from(trimHexPrefix(pullIx.programId), 'hex')
  );
  const data = Buffer.from(trimHexPrefix(pullIx.data), 'hex');

  return new TransactionInstruction({
    keys,
    programId,
    data,
  });
}

function serializedIxFromHex(serializedHex: string): TransactionInstruction {
  const cleanHex = trimHexPrefix(serializedHex);
  if (cleanHex.length % 2 !== 0) {
    throw new Error('Serialized instruction hex must have an even length');
  }

  const buffer = Buffer.from(cleanHex, 'hex');
  let offset = 0;

  assertReadable(buffer, offset, 32, 'programId');
  const programId = new PublicKey(buffer.subarray(offset, offset + 32));
  offset += 32;

  const accountCount = readU64LE(buffer, offset, 'account count');
  offset += 8;

  const keys = Array.from({ length: accountCount }, (_, index) => {
    assertReadable(buffer, offset, 34, `account meta ${index}`);

    const pubkey = new PublicKey(buffer.subarray(offset, offset + 32));
    offset += 32;

    const isSigner = buffer[offset] !== 0;
    offset += 1;

    const isWritable = buffer[offset] !== 0;
    offset += 1;

    return {
      pubkey,
      isSigner,
      isWritable,
    };
  });

  const dataLength = readU64LE(buffer, offset, 'data length');
  offset += 8;

  assertReadable(buffer, offset, dataLength, 'data');
  const data = Buffer.from(buffer.subarray(offset, offset + dataLength));
  offset += dataLength;

  if (offset !== buffer.length) {
    throw new Error('Serialized instruction contains unexpected trailing bytes');
  }

  return new TransactionInstruction({
    keys,
    programId,
    data,
  });
}

// Convert a Crossbar instruction wire payload into a TransactionInstruction.
export function IxFromHex(
  pullIx: CrossbarInstructionWire
): TransactionInstruction {
  return typeof pullIx === 'string'
    ? serializedIxFromHex(pullIx)
    : legacyIxFromHex(pullIx);
}
