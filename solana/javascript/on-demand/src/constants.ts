import { web3 } from '@coral-xyz/anchor-31';

/**
 *  Address of the SPL Token program.
 */
export const SPL_TOKEN_PROGRAM_ID = new web3.PublicKey(
  'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
);
/**
 *  The public key of the Solana SPL Associated Token Account program.
 */
export const SPL_ASSOCIATED_TOKEN_ACCOUNT_PROGRAM_ID = new web3.PublicKey(
  'ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL'
);
/**
 *  The public key of the Solana SlotHashes sysvar.
 */
export const SPL_SYSVAR_SLOT_HASHES_ID = new web3.PublicKey(
  'SysvarS1otHashes111111111111111111111111111'
);
/**
 *  The public key of the Solana Instructions sysvar.
 */
export const SPL_SYSVAR_INSTRUCTIONS_ID = new web3.PublicKey(
  'Sysvar1nstructions1111111111111111111111111'
);

/**
 *  Address of the SPL Token 2022 program.
 */
export const SPL_TOKEN_2022_PROGRAM_ID = new web3.PublicKey(
  'TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb'
);

/**
 *  Address of the special mint for wrapped native SOL in spl-token.
 */
export const SOL_NATIVE_MINT = new web3.PublicKey(
  'So11111111111111111111111111111111111111112'
);
/**
 *  Address of the special mint for wrapped native SOL in spl-token-2022.
 */
export const SOL_NATIVE_MINT_2022 = new web3.PublicKey(
  '9pan9bMn5HatX4EJdBwg9VgCa7Uz5HL8N1m5D3NdXejP'
);
