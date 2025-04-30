import { web3 } from '@coral-xyz/anchor-30';
import { NonEmptyArrayUtils } from '@switchboard-xyz/common';

// The serialized size of a secp256k1 signature
const SIGNATURE_SERIALIZED_SIZE = 64;
// The serialized size of a hashed pubkey
const HASHED_PUBKEY_SERIALIZED_SIZE = 20;
// The serialized size of the signature offsets
const SIGNATURE_OFFSETS_SERIALIZED_SIZE = 11;

export type Secp256k1Signature = {
  ethAddress: Buffer;
  signature: Buffer;
  message: Buffer;
  recoveryId: number;
};

export class Secp256k1InstructionUtils {
  /**
   *  Disable instantiation of the InstructionUtils class
   */
  private constructor() {}

  static buildSecp256k1Instruction(
    signatures: Secp256k1Signature[],
    instructionIndex: number
  ): web3.TransactionInstruction {
    // Ensure that the `instructionIndex` is both a valid finite number and non-negative
    if (!Number.isFinite(instructionIndex) || instructionIndex < 0) {
      throw new Error('Invalid instruction index');
    } else if (!NonEmptyArrayUtils.safeValidate(signatures)) {
      // Ensure that the `signatures` array is non-empty and that all signatures share the same
      // common message
      throw new Error('Invalid `signatures` array: cannot be empty');
    }

    const diffIdx = signatures.findIndex(
      sig => !sig.message.equals(signatures[0].message)
    );
    if (diffIdx !== -1) {
      const expectedMessage = signatures[0].message.toString('base64');
      const differentMessage = signatures[diffIdx].message.toString('base64');
      throw new Error(`
        All signatures must share the same message. The signed message at #${diffIdx}
        (${differentMessage}) does not match the expected message (${expectedMessage})
      `);
    }

    // We've validated that all signatures share the same message, so we can use the first
    // signature's message as the common message.
    const commonMessage = signatures[0].message;
    const commonMessageSize = commonMessage.length;

    const signatureBlockSize =
      SIGNATURE_SERIALIZED_SIZE + 1 + HASHED_PUBKEY_SERIALIZED_SIZE;
    const numSignatures = signatures.length;
    const offsetsAreaSize =
      1 + numSignatures * SIGNATURE_OFFSETS_SERIALIZED_SIZE;
    const messageOffset = offsetsAreaSize + numSignatures * signatureBlockSize;

    const signatureOffsets: Uint8Array[] = [];
    const signatureBuffer: number[] = [];

    for (const sig of signatures) {
      // Calculate the offset of the current signature block
      const currentOffset = offsetsAreaSize + signatureBuffer.length;
      // Create a new Uint8Array to store the signature offsets
      const offsetsBytes = new Uint8Array(SIGNATURE_OFFSETS_SERIALIZED_SIZE);
      let position = 0;

      // Write signature offset (2 bytes LE)
      const signatureOffset = currentOffset;
      offsetsBytes.set(writeUInt16LE(signatureOffset), position);
      position += 2;

      // 1. Write signature instruction index (1 byte)
      offsetsBytes[position] = instructionIndex;
      position += 1;
      // 2. Write eth address offset (2 bytes LE)
      const ethAddressOffset = currentOffset + SIGNATURE_SERIALIZED_SIZE + 1;
      offsetsBytes.set(writeUInt16LE(ethAddressOffset), position);
      position += 2;
      // 3. Write eth address instruction index (1 byte)
      offsetsBytes[position] = instructionIndex;
      position += 1;
      // 4. Write message offset (2 bytes LE)
      const messageDataOffset = messageOffset;
      offsetsBytes.set(writeUInt16LE(messageDataOffset), position);
      position += 2;
      // 5. Write message size (2 bytes LE)
      offsetsBytes.set(writeUInt16LE(commonMessageSize), position);
      position += 2;
      // 6. Write message instruction index (1 byte)
      offsetsBytes[position] = instructionIndex;

      // Append the signature offsets to the list of signature offsets
      signatureOffsets.push(offsetsBytes);

      // Append the signature block to the signature buffer
      signatureBuffer.push(...Array.from(sig.signature));
      signatureBuffer.push(sig.recoveryId);
      signatureBuffer.push(...Array.from(sig.ethAddress));
    }

    // Build final instruction data
    let position = 0;
    const instrData = new Uint8Array(
      1 + // count byte
        signatureOffsets.length * SIGNATURE_OFFSETS_SERIALIZED_SIZE + // offsets area
        signatureBuffer.length + // signature blocks
        commonMessage.length // common message
    );
    // 1. Write count byte
    instrData[position] = numSignatures;
    position += 1;
    // 2. Write offsets area
    for (const offs of signatureOffsets) {
      instrData.set(offs, position);
      position += SIGNATURE_OFFSETS_SERIALIZED_SIZE;
    }
    // 3. Write signature blocks
    instrData.set(new Uint8Array(signatureBuffer), position);
    position += signatureBuffer.length;
    // 4. Write common message
    instrData.set(commonMessage, position);

    return new web3.TransactionInstruction({
      programId: web3.Secp256k1Program.programId,
      data: Buffer.from(instrData),
      keys: [],
    });
  }
}

function writeUInt16LE(value: number): Uint8Array {
  const buf = Buffer.alloc(2);
  buf.writeUInt16LE(value, 0);
  return new Uint8Array(buf);
}
