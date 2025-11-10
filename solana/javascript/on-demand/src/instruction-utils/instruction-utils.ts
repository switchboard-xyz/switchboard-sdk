import { web3 } from '@coral-xyz/anchor-31';

/**
 * Transaction building utilities for Switchboard On-Demand
 *
 * The InstructionUtils class provides helper methods for building
 * optimized Solana transactions, particularly versioned transactions
 * (v0) with automatic compute budget management.
 *
 * @class InstructionUtils
 */
export class InstructionUtils {
  /**
   *  Disable instantiation of the InstructionUtils class
   */
  private constructor() {}

  /**
   * Builds a versioned transaction with automatic compute budget optimization
   *
   * This method simplifies transaction creation by:
   * - Automatically simulating to determine compute requirements
   * - Adding appropriate compute budget instructions
   * - Using address lookup tables for smaller transactions
   * - Handling transaction size limits gracefully
   *
   * The method performs two key optimizations:
   * 1. **Compute Budget**: Simulates first to determine actual compute usage,
   *    then sets the limit based on actual needs (with optional buffer)
   * 2. **Transaction Size**: Uses v0 transactions with lookup tables to
   *    minimize transaction size
   *
   * @param {Object} params - Transaction building parameters
   * @param {web3.Connection} params.connection - Solana RPC connection
   * @param {web3.TransactionInstruction[]} params.ixs - Instructions to include
   * @param {web3.PublicKey} params.payer - Transaction fee payer (defaults to first signer)
   * @param {number} params.computeUnitLimitMultiple - Multiplier for compute limit (e.g., 1.3 = 30% buffer)
   * @param {number} params.computeUnitPrice - Priority fee in microlamports per compute unit
   * @param {web3.AddressLookupTableAccount[]} params.lookupTables - Address lookup tables to use
   * @param {web3.Signer[]} params.signers - Transaction signers
   * @returns {Promise<web3.VersionedTransaction>} Signed versioned transaction ready to send
   *
   * @throws {Error} If transaction is too large or payer not provided
   *
   * @example
   * ```typescript
   * const tx = await InstructionUtils.asV0TxWithComputeIxs({
   *   connection,
   *   ixs: [updateIx, userIx],
   *   signers: [payer],
   *   computeUnitPrice: 10_000, // 0.01 lamports per compute unit
   *   computeUnitLimitMultiple: 1.3, // 30% safety buffer
   *   lookupTables: [lut],
   * });
   *
   * const signature = await connection.sendTransaction(tx);
   * ```
   */
  static async asV0TxWithComputeIxs(params: {
    connection: web3.Connection;
    ixs: web3.TransactionInstruction[];
    payer?: web3.PublicKey;
    computeUnitLimitMultiple?: number;
    computeUnitPrice?: number;
    lookupTables?: web3.AddressLookupTableAccount[];
    signers?: web3.Signer[];
  }): Promise<web3.VersionedTransaction> {
    let payer = params.payer;
    if (!payer) {
      if (!params.signers?.length) {
        throw new Error('Payer not provided');
      }
      payer = params.signers[0].publicKey;
    }
    const priorityFeeIx = web3.ComputeBudgetProgram.setComputeUnitPrice({
      microLamports: params.computeUnitPrice ?? 0,
    });
    const simulationComputeLimitIx =
      web3.ComputeBudgetProgram.setComputeUnitLimit({
        units: 1_400_000, // 1.4M compute units
      });
    const recentBlockhash = (await params.connection.getLatestBlockhash())
      .blockhash;

    const simulateMessageV0 = new web3.TransactionMessage({
      recentBlockhash,
      instructions: [...params.ixs, priorityFeeIx, simulationComputeLimitIx],
      payerKey: payer,
    }).compileToV0Message(params.lookupTables ?? []);
    const simulateTx = new web3.VersionedTransaction(simulateMessageV0);
    try {
      simulateTx.serialize();
    } catch (e) {
      if (e instanceof RangeError) {
        throw new Error(
          'Transaction failed to serialize: Transaction too large'
        );
      }
      throw e;
    }
    const simulationResult = await params.connection.simulateTransaction(
      simulateTx,
      { commitment: 'processed', sigVerify: false }
    );

    const simulationUnitsConsumed = simulationResult.value.unitsConsumed!;
    const computeLimitIx = web3.ComputeBudgetProgram.setComputeUnitLimit({
      units: Math.floor(
        simulationUnitsConsumed * (params.computeUnitLimitMultiple ?? 1)
      ),
    });
    const messageV0 = new web3.TransactionMessage({
      recentBlockhash,
      instructions: [...params.ixs, priorityFeeIx, computeLimitIx],
      payerKey: payer,
    }).compileToV0Message(params.lookupTables ?? []);
    const tx = new web3.VersionedTransaction(messageV0);
    tx.sign(params.signers ?? []);
    return tx;
  }
}
