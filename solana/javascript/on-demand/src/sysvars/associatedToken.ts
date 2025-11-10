import {
  SPL_ASSOCIATED_TOKEN_ACCOUNT_PROGRAM_ID,
  SPL_TOKEN_PROGRAM_ID,
} from '../constants.js';

import { web3 } from '@coral-xyz/anchor-31';

export class AssociatedTokenProgram {
  private constructor() {}
  /**
   * Find the associated token address for the given wallet and token mint
   */
  findAssociatedTokenAddress(
    walletAddress: web3.PublicKey,
    tokenMintAddress: web3.PublicKey
  ): [web3.PublicKey, number] {
    return web3.PublicKey.findProgramAddressSync(
      [
        walletAddress.toBuffer(),
        SPL_TOKEN_PROGRAM_ID.toBuffer(),
        tokenMintAddress.toBuffer(),
      ],
      SPL_ASSOCIATED_TOKEN_ACCOUNT_PROGRAM_ID
    );
  }
}
