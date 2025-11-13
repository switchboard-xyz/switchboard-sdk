/**
 * Switchboard Subscription SDK
 *
 * High-level functions for managing Switchboard On-Demand subscriptions.
 * Allows users to create, upgrade, downgrade, and extend their subscriptions.
 */

import { Queue } from '../accounts/queue.js';
import { asV0Tx } from '../index.js';

import { OracleQuote } from './oracleQuote.js';

import { BN, Program, web3 } from '@coral-xyz/anchor-31';
import {
  createAssociatedTokenAccountInstruction,
  getAssociatedTokenAddressSync,
  TOKEN_PROGRAM_ID,
} from '@solana/spl-token';
import {
  Connection,
  Keypair,
  PublicKey,
  SystemProgram,
  SYSVAR_CLOCK_PUBKEY,
  SYSVAR_INSTRUCTIONS_PUBKEY,
  SYSVAR_SLOT_HASHES_PUBKEY,
} from '@solana/web3.js';
import { CrossbarClient } from '@switchboard-xyz/common';

/**
 * Switchboard Subscriber Program ID
 */
export const SUBSCRIPTION_PROGRAM_ID = new PublicKey(
  'SbsCRB7Y3SGNEwVgvwE4R8ZYuhertgc7ekUXQbD6pNH'
);

/**
 * SWTCH Token Mint Address
 */
export const SWTCH_MINT = new PublicKey(
  'SW1TCHLmRGTfW5xZknqQdpdarB8PD95sJYWpNp9TbFx'
);

/**
 * SWTCH/USDT Feed ID for oracle pricing
 */
export const SWTCH_FEED_ID =
  '148aaedb33ff23593805377f131af7fe01b4770ae2b9e4a2f7f8b3a180314711';

/**
 * Subscription account information
 */
export interface SubscriptionInfo {
  owner: PublicKey;
  tierId: number;
  tierIdInTransition: number;
  subscriptionStartEpoch: BN;
  subscriptionEndEpoch: BN;
  lastTierChangeEpoch: BN;
  isActive: number;
  authorizedUsers: PublicKey[];
  authorizedUserCount: number;
  totalTokensPaid: BN;
  lastPaymentAmount: BN;
  lastPaymentToken: PublicKey;
  contactName: number[];
  contactEmail: number[];
}

/**
 * Parameters for creating a new subscription
 */
export interface CreateSubscriptionParams {
  /** Solana connection */
  connection: Connection;
  /** Payer and subscription owner */
  payer: Keypair;
  /** Tier ID (1=Free, 2=Pro, 3=Surge, 10+=Enterprise) */
  tierId: number;
  /** Number of epochs to pay for (minimum 2) */
  epochAmount: number;
  /** Optional contact name */
  contactName?: string;
  /** Optional contact email */
  contactEmail?: string;
  /** Switchboard queue for oracle updates */
  queue: Queue;
}

/**
 * Parameters for upgrading a subscription
 */
export interface UpgradeSubscriptionParams {
  /** Solana connection */
  connection: Connection;
  /** Subscription owner */
  owner: Keypair;
  /** New tier ID (must be more expensive than current) */
  newTierId: number;
  /** Number of epochs requested (credit applied, minimum 1) */
  epochAmount: number;
  /** Switchboard queue for oracle updates */
  queue: Queue;
}

/**
 * Parameters for downgrading a subscription
 * Note: Downgrade is credit-only - no payment or epoch amount needed
 */
export interface DowngradeSubscriptionParams {
  /** Solana connection */
  connection: Connection;
  /** Subscription owner */
  owner: Keypair;
  /** New tier ID (must be less expensive than current) */
  newTierId: number;
  // No epochAmount - downgrade converts all credit automatically
  // No queue needed - no oracle price verification for credit-only operation
}

/**
 * Parameters for extending a subscription
 */
export interface ExtendSubscriptionParams {
  /** Solana connection */
  connection: Connection;
  /** Subscription owner (or admin if adminExtend=true) */
  owner: Keypair;
  /** Number of epochs to add */
  epochAmount: number;
  /** If true, no payment required (admin only) */
  adminExtend?: boolean;
  /** Switchboard queue for oracle updates (required if adminExtend=false) */
  queue?: Queue;
}

/**
 * Parameters for managing team members
 */
export interface ManageTeamMemberParams {
  /** Solana connection */
  connection: Connection;
  /** Subscription owner */
  owner: Keypair;
  /** User to add/remove */
  user: PublicKey;
}

// ============================================================================
// PDA Derivation Helpers
// ============================================================================

/**
 * Derive the state PDA
 */
function getStatePda(programId: PublicKey): [PublicKey, number] {
  return PublicKey.findProgramAddressSync([Buffer.from('STATE')], programId);
}

/**
 * Derive the tier PDA
 */
function getTierPda(tierId: number, programId: PublicKey): [PublicKey, number] {
  return PublicKey.findProgramAddressSync(
    [Buffer.from('TIER'), Buffer.from(new Uint16Array([tierId]).buffer)],
    programId
  );
}

/**
 * Derive the subscription PDA
 */
function getSubscriptionPda(
  owner: PublicKey,
  programId: PublicKey
): [PublicKey, number] {
  return PublicKey.findProgramAddressSync(
    [Buffer.from('SUBSCRIPTION'), owner.toBuffer()],
    programId
  );
}

/**
 * Derive the token vault PDA
 */
function getTokenVaultPda(
  mint: PublicKey,
  programId: PublicKey
): [PublicKey, number] {
  return PublicKey.findProgramAddressSync(
    [Buffer.from('TOKEN_VAULT'), mint.toBuffer()],
    programId
  );
}

// ============================================================================
// Oracle Helper
// ============================================================================

/**
 * Bundle oracle update with subscription instruction
 * (Pattern from test file's executeWithOracleUpdate function)
 */
async function executeWithOracleUpdate(
  connection: Connection,
  queue: Queue,
  quoteAccount: PublicKey,
  subscriptionIx: web3.TransactionInstruction,
  signer: Keypair
): Promise<string> {
  console.log(
    `Bundling oracle update for ${signer.publicKey.toBase58().slice(0, 8)}...`
  );

  // Create crossbar client (don't need to pass it in)
  const crossbar = new CrossbarClient('https://crossbar.switchboard.xyz');

  // Fetch oracle instructions with retry logic
  let oracleIxs: web3.TransactionInstruction[];
  let retries = 0;
  const maxRetries = 5;

  while (retries < maxRetries) {
    try {
      const result = await queue.fetchManagedUpdateIxs(
        crossbar,
        [SWTCH_FEED_ID],
        {
          payer: signer.publicKey,
          instructionIdx: 0,
        }
      );
      oracleIxs = result;
      console.log(`Oracle instructions fetched: ${oracleIxs.length}`);
      break;
    } catch (e: unknown) {
      retries++;
      const errMsg = e instanceof Error ? e.message : String(e);
      console.log(
        `Oracle fetch failed (attempt ${retries}/${maxRetries}): ${errMsg.slice(0, 80)}`
      );
      if (retries >= maxRetries) {
        throw new Error(
          `Oracle fetch failed after ${maxRetries} attempts. Last error: ${errMsg}`
        );
      }
      console.log('Retrying in 3 seconds...');
      await new Promise(r => setTimeout(r, 3000));
    }
  }

  // Bundle in atomic transaction
  const tx = await asV0Tx({
    connection,
    ixs: [...oracleIxs!, subscriptionIx],
    signers: [signer],
    computeUnitPrice: 20_000,
    computeUnitLimitMultiple: 1.3,
  });

  const sig = await connection.sendTransaction(tx);
  await connection.confirmTransaction(sig, 'confirmed');

  console.log(`Transaction confirmed: ${sig}`);

  // Rate limiting
  await new Promise(r => setTimeout(r, 2000));

  return sig;
}

/**
 * Get the canonical oracle quote account PDA
 */
function getQuoteAccount(queuePubkey: PublicKey): PublicKey {
  // Use OracleQuote.getCanonicalPubkey directly - same as test file
  const [quoteAccount] = OracleQuote.getCanonicalPubkey(queuePubkey, [
    SWTCH_FEED_ID,
  ]);
  return quoteAccount;
}

// ============================================================================
// Program Loading Helper
// ============================================================================

/**
 * Type for program with subscription account
 */
interface SubscriberProgram extends Program {
  account: {
    subscription: {
      fetch: (address: PublicKey) => Promise<SubscriptionInfo>;
    };
  };
}

/**
 * Load the Subscriber program (fetches IDL from on-chain)
 */
async function loadSubscriberProgram(
  connection: Connection,
  programId: PublicKey
): Promise<SubscriberProgram> {
  try {
    const idl = await Program.fetchIdl(programId, {
      connection,
    } as unknown as { connection: Connection });

    if (!idl) {
      throw new Error('Subscriber program IDL not found on-chain');
    }

    return new Program(idl, {
      connection,
    } as unknown as { connection: Connection }) as SubscriberProgram;
  } catch (e) {
    throw new Error(
      `Failed to load Subscriber program IDL: ${e instanceof Error ? e.message : String(e)}`
    );
  }
}

// ============================================================================
// Public API Functions
// ============================================================================

/**
 * Create a new subscription
 *
 * @example
 * ```typescript
 * const sig = await createSubscription({
 *   connection,
 *   payer: userKeypair,
 *   tierId: 2, // Pro tier
 *   epochAmount: 15,
 *   contactName: 'Alice',
 *   contactEmail: 'alice@example.com',
 *   queue,
 * });
 * ```
 */
export async function createSubscription(
  params: CreateSubscriptionParams
): Promise<string> {
  const programId = SUBSCRIPTION_PROGRAM_ID;
  const program = await loadSubscriberProgram(params.connection, programId);

  // Derive PDAs
  const [statePda] = getStatePda(programId);
  const [tierPda] = getTierPda(params.tierId, programId);
  const [subscriptionPda] = getSubscriptionPda(
    params.payer.publicKey,
    programId
  );
  const [tokenVaultPda] = getTokenVaultPda(SWTCH_MINT, programId);
  const payerTokenAccount = getAssociatedTokenAddressSync(
    SWTCH_MINT,
    params.payer.publicKey
  );
  const quoteAccount = getQuoteAccount(params.queue.pubkey);

  // Check if token account exists - create if needed (required by program even for free tier)
  const ataInfo = await params.connection.getAccountInfo(payerTokenAccount);

  if (!ataInfo) {
    console.log('📝 Creating SWTCH token account (required by program)...');
    const createAtaIx = createAssociatedTokenAccountInstruction(
      params.payer.publicKey, // payer
      payerTokenAccount, // ata
      params.payer.publicKey, // owner
      SWTCH_MINT // mint
    );

    // Send ATA creation in separate transaction first
    const ataTx = new web3.Transaction().add(createAtaIx);
    const latestBlockhash = await params.connection.getLatestBlockhash();
    ataTx.recentBlockhash = latestBlockhash.blockhash;
    ataTx.feePayer = params.payer.publicKey;
    ataTx.partialSign(params.payer);

    const ataSig = await params.connection.sendRawTransaction(
      ataTx.serialize()
    );
    await params.connection.confirmTransaction({
      signature: ataSig,
      blockhash: latestBlockhash.blockhash,
      lastValidBlockHeight: latestBlockhash.lastValidBlockHeight,
    });
    console.log(`✅ Token account created: ${ataSig.slice(0, 8)}...`);
  }

  // Build subscription init instruction
  const subscriptionIx = await program.methods
    .subscriptionInit({
      tierId: params.tierId,
      contactName: params.contactName || null,
      contactEmail: params.contactEmail || null,
      epochAmount: new BN(params.epochAmount),
    })
    .accounts({
      state: statePda,
      subscription: subscriptionPda,
      tier: tierPda,
      owner: params.payer.publicKey,
      payer: params.payer.publicKey,
      paymentMint: SWTCH_MINT,
      payerTokenAccount,
      tokenVault: tokenVaultPda,
      tokenProgram: TOKEN_PROGRAM_ID,
      systemProgram: SystemProgram.programId,
      quoteAccount,
      sysvars: {
        clock: SYSVAR_CLOCK_PUBKEY,
        slothashes: SYSVAR_SLOT_HASHES_PUBKEY,
        instructions: SYSVAR_INSTRUCTIONS_PUBKEY,
      },
    })
    .instruction();

  // Execute with oracle update
  return executeWithOracleUpdate(
    params.connection,
    params.queue,
    quoteAccount,
    subscriptionIx,
    params.payer
  );
}

/**
 * Upgrade subscription to a more expensive tier
 *
 * @example
 * ```typescript
 * const sig = await upgradeSubscription({
 *   connection,
 *   owner: userKeypair,
 *   newTierId: 3, // Surge tier
 *   epochAmount: 15,
 *   queue,
 * });
 * ```
 */
export async function upgradeSubscription(
  params: UpgradeSubscriptionParams
): Promise<string> {
  const programId = SUBSCRIPTION_PROGRAM_ID;
  const program = await loadSubscriberProgram(params.connection, programId);

  // Derive PDAs
  const [statePda] = getStatePda(programId);
  const [newTierPda] = getTierPda(params.newTierId, programId);
  const [subscriptionPda] = getSubscriptionPda(
    params.owner.publicKey,
    programId
  );
  const [tokenVaultPda] = getTokenVaultPda(SWTCH_MINT, programId);
  const ownerTokenAccount = getAssociatedTokenAddressSync(
    SWTCH_MINT,
    params.owner.publicKey
  );
  const quoteAccount = getQuoteAccount(params.queue.pubkey);

  // Fetch subscription to get current tier
  const subscription =
    await program.account.subscription.fetch(subscriptionPda);
  const currentTierId = subscription.tierId;
  const [currentTierPda] = getTierPda(currentTierId, programId);

  // Build upgrade instruction
  const upgradeIx = await program.methods
    .subscriptionUpgrade({
      newTierId: params.newTierId,
      epochAmount: new BN(params.epochAmount),
    })
    .accounts({
      state: statePda,
      subscription: subscriptionPda,
      newTier: newTierPda,
      owner: params.owner.publicKey,
      paymentMint: SWTCH_MINT,
      payerTokenAccount: ownerTokenAccount,
      tokenVault: tokenVaultPda,
      tokenProgram: TOKEN_PROGRAM_ID,
      quoteAccount,
      sysvars: {
        clock: SYSVAR_CLOCK_PUBKEY,
        slothashes: SYSVAR_SLOT_HASHES_PUBKEY,
        instructions: SYSVAR_INSTRUCTIONS_PUBKEY,
      },
    })
    .remainingAccounts([
      { pubkey: currentTierPda, isSigner: false, isWritable: false },
    ])
    .instruction();

  // Execute with oracle update
  return executeWithOracleUpdate(
    params.connection,
    params.queue,
    quoteAccount,
    upgradeIx,
    params.owner
  );
}

/**
 * Downgrade subscription to a less expensive tier
 *
 * @example
 * ```typescript
 * const sig = await downgradeSubscription({
 *   connection,
 *   owner: userKeypair,
 *   newTierId: 1, // Free tier
 *   epochAmount: 15,
 *   queue,
 * });
 * ```
 */
export async function downgradeSubscription(
  params: DowngradeSubscriptionParams
): Promise<string> {
  const programId = SUBSCRIPTION_PROGRAM_ID;
  const program = await loadSubscriberProgram(params.connection, programId);

  // Derive PDAs
  const [statePda] = getStatePda(programId);
  const [newTierPda] = getTierPda(params.newTierId, programId);
  const [subscriptionPda] = getSubscriptionPda(
    params.owner.publicKey,
    programId
  );

  // Fetch subscription to get current tier
  const subscription =
    await program.account.subscription.fetch(subscriptionPda);
  const currentTierId = subscription.tierId;
  const [currentTierPda] = getTierPda(currentTierId, programId);

  // Build downgrade instruction (credit-only, no payment)
  const downgradeIx = await program.methods
    .subscriptionDowngrade({
      newTierId: params.newTierId,
      // No epochAmount - credit-only!
    })
    .accounts({
      state: statePda,
      subscription: subscriptionPda,
      newTier: newTierPda,
      owner: params.owner.publicKey,
    })
    .remainingAccounts([
      { pubkey: currentTierPda, isSigner: false, isWritable: false },
    ])
    .instruction();

  // Execute transaction (no oracle needed - credit-only operation)
  const tx = new web3.Transaction().add(downgradeIx);
  tx.recentBlockhash = (await params.connection.getLatestBlockhash()).blockhash;
  tx.feePayer = params.owner.publicKey;
  tx.partialSign(params.owner);

  const sig = await params.connection.sendRawTransaction(tx.serialize());
  const latestBlockhash = await params.connection.getLatestBlockhash();
  await params.connection.confirmTransaction({
    signature: sig,
    blockhash: latestBlockhash.blockhash,
    lastValidBlockHeight: latestBlockhash.lastValidBlockHeight,
  });

  return sig;
}

/**
 * Extend an existing subscription
 *
 * @example
 * ```typescript
 * // User pays to extend
 * const sig = await extendSubscription({
 *   connection,
 *   owner: userKeypair,
 *   epochAmount: 10,
 *   queue,
 * });
 *
 * // Admin extends for free
 * const adminSig = await extendSubscription({
 *   connection,
 *   owner: adminKeypair,
 *   epochAmount: 30,
 *   adminExtend: true,
 * });
 * ```
 */
export async function extendSubscription(
  params: ExtendSubscriptionParams
): Promise<string> {
  const programId = SUBSCRIPTION_PROGRAM_ID;
  const program = await loadSubscriberProgram(params.connection, programId);

  // Derive PDAs
  const [statePda] = getStatePda(programId);
  const [subscriptionPda] = getSubscriptionPda(
    params.owner.publicKey,
    programId
  );

  // Fetch subscription to get current tier
  const subscription =
    await program.account.subscription.fetch(subscriptionPda);
  const currentTierId = subscription.tierId;
  const [tierPda] = getTierPda(currentTierId, programId);

  if (params.adminExtend) {
    // Admin extend - no payment or oracle needed
    const extendIx = await program.methods
      .subscriptionExtend({
        epochAmount: new BN(params.epochAmount),
        adminExtend: true,
      })
      .accounts({
        state: statePda,
        subscription: subscriptionPda,
        tier: tierPda,
        payer: params.owner.publicKey,
        systemProgram: SystemProgram.programId,
      })
      .instruction();

    // Execute without oracle
    const tx = await asV0Tx({
      connection: params.connection,
      ixs: [extendIx],
      signers: [params.owner],
      computeUnitPrice: 20_000,
      computeUnitLimitMultiple: 1.3,
    });

    const sig = await params.connection.sendTransaction(tx);
    await params.connection.confirmTransaction(sig, 'confirmed');
    return sig;
  } else {
    // User extend - requires payment and oracle
    if (!params.queue) {
      throw new Error('queue required for paid extends');
    }

    const [tokenVaultPda] = getTokenVaultPda(SWTCH_MINT, programId);
    const payerTokenAccount = getAssociatedTokenAddressSync(
      SWTCH_MINT,
      params.owner.publicKey
    );
    const quoteAccount = getQuoteAccount(params.queue.pubkey);

    const extendIx = await program.methods
      .subscriptionExtend({
        epochAmount: new BN(params.epochAmount),
        adminExtend: false,
      })
      .accounts({
        state: statePda,
        subscription: subscriptionPda,
        tier: tierPda,
        payer: params.owner.publicKey,
        paymentMint: SWTCH_MINT,
        payerTokenAccount,
        tokenVault: tokenVaultPda,
        tokenProgram: TOKEN_PROGRAM_ID,
        systemProgram: SystemProgram.programId,
        quoteAccount,
        sysvars: {
          clock: SYSVAR_CLOCK_PUBKEY,
          slothashes: SYSVAR_SLOT_HASHES_PUBKEY,
          instructions: SYSVAR_INSTRUCTIONS_PUBKEY,
        },
      })
      .instruction();

    return executeWithOracleUpdate(
      params.connection,
      params.queue,
      quoteAccount,
      extendIx,
      params.owner
    );
  }
}

/**
 * Add a team member to the subscription
 *
 * @example
 * ```typescript
 * const sig = await addTeamMember({
 *   connection,
 *   owner: userKeypair,
 *   user: teammatePublicKey,
 * });
 * ```
 */
export async function addTeamMember(
  params: ManageTeamMemberParams
): Promise<string> {
  const programId = SUBSCRIPTION_PROGRAM_ID;
  const program = await loadSubscriberProgram(params.connection, programId);

  // Derive PDAs
  const [statePda] = getStatePda(programId);
  const [subscriptionPda] = getSubscriptionPda(
    params.owner.publicKey,
    programId
  );

  // Fetch subscription to get current tier
  const subscription =
    await program.account.subscription.fetch(subscriptionPda);
  const currentTierId = subscription.tierId;
  const [tierPda] = getTierPda(currentTierId, programId);

  const addIx = await program.methods
    .subscriptionManageUsers({
      action: { add: {} },
      user: params.user,
    })
    .accounts({
      state: statePda,
      subscription: subscriptionPda,
      tier: tierPda,
      owner: params.owner.publicKey,
    })
    .instruction();

  // Execute without oracle (no payment)
  const tx = await asV0Tx({
    connection: params.connection,
    ixs: [addIx],
    signers: [params.owner],
    computeUnitPrice: 20_000,
    computeUnitLimitMultiple: 1.3,
  });

  const sig = await params.connection.sendTransaction(tx);
  await params.connection.confirmTransaction(sig, 'confirmed');
  return sig;
}

/**
 * Remove a team member from the subscription
 *
 * @example
 * ```typescript
 * const sig = await removeTeamMember({
 *   connection,
 *   owner: userKeypair,
 *   user: teammatePublicKey,
 * });
 * ```
 */
export async function removeTeamMember(
  params: ManageTeamMemberParams
): Promise<string> {
  const programId = SUBSCRIPTION_PROGRAM_ID;
  const program = await loadSubscriberProgram(params.connection, programId);

  // Derive PDAs
  const [statePda] = getStatePda(programId);
  const [subscriptionPda] = getSubscriptionPda(
    params.owner.publicKey,
    programId
  );

  // Fetch subscription to get current tier
  const subscription =
    await program.account.subscription.fetch(subscriptionPda);
  const currentTierId = subscription.tierId;
  const [tierPda] = getTierPda(currentTierId, programId);

  const removeIx = await program.methods
    .subscriptionManageUsers({
      action: { remove: {} },
      user: params.user,
    })
    .accounts({
      state: statePda,
      subscription: subscriptionPda,
      tier: tierPda,
      owner: params.owner.publicKey,
    })
    .instruction();

  // Execute without oracle (no payment)
  const tx = await asV0Tx({
    connection: params.connection,
    ixs: [removeIx],
    signers: [params.owner],
    computeUnitPrice: 20_000,
    computeUnitLimitMultiple: 1.3,
  });

  const sig = await params.connection.sendTransaction(tx);
  await params.connection.confirmTransaction(sig, 'confirmed');
  return sig;
}

/**
 * Fetch subscription information for an owner
 *
 * @example
 * ```typescript
 * const info = await fetchSubscriptionInfo({
 *   connection,
 *   owner: userPublicKey,
 * });
 *
 * console.log(`Tier: ${info.tierId}`);
 * console.log(`Active: ${info.isActive === 1}`);
 * console.log(`Epochs: ${info.subscriptionStartEpoch} - ${info.subscriptionEndEpoch}`);
 * ```
 */
export async function fetchSubscriptionInfo(params: {
  connection: Connection;
  owner: PublicKey;
}): Promise<SubscriptionInfo | null> {
  const programId = SUBSCRIPTION_PROGRAM_ID;
  const program = await loadSubscriberProgram(params.connection, programId);

  const [subscriptionPda] = getSubscriptionPda(params.owner, programId);

  try {
    const subscription =
      await program.account.subscription.fetch(subscriptionPda);
    return subscription;
  } catch {
    // Subscription doesn't exist
    return null;
  }
}

/**
 * Close an existing subscription and reclaim rent
 *
 * Note: Subscription must be expired or inactive to close
 *
 * @example
 * ```typescript
 * const sig = await closeSubscription({
 *   connection,
 *   owner: userKeypair,
 *   destination: userKeypair.publicKey, // Where to send rent refund
 * });
 * ```
 */
export async function closeSubscription(params: {
  connection: Connection;
  owner: web3.Keypair;
  destination?: PublicKey; // Defaults to owner if not provided
}): Promise<string> {
  const programId = SUBSCRIPTION_PROGRAM_ID;
  const program = await loadSubscriberProgram(params.connection, programId);

  // Derive subscription PDA
  const [subscriptionPda] = getSubscriptionPda(
    params.owner.publicKey,
    programId
  );

  const destination = params.destination || params.owner.publicKey;

  // Build close instruction
  const closeIx = await program.methods
    .subscriptionClose({})
    .accounts({
      subscription: subscriptionPda,
      owner: params.owner.publicKey,
      destination,
    })
    .instruction();

  // Execute transaction
  const tx = new web3.Transaction().add(closeIx);
  const latestBlockhash = await params.connection.getLatestBlockhash();
  tx.recentBlockhash = latestBlockhash.blockhash;
  tx.feePayer = params.owner.publicKey;
  tx.partialSign(params.owner);

  const sig = await params.connection.sendRawTransaction(tx.serialize());
  await params.connection.confirmTransaction({
    signature: sig,
    blockhash: latestBlockhash.blockhash,
    lastValidBlockHeight: latestBlockhash.lastValidBlockHeight,
  });

  return sig;
}

// ============================================================================
// INSTRUCTION-ONLY FUNCTIONS (for wallet adapters)
// ============================================================================

/**
 * Build create subscription instruction (does not send transaction)
 * Use this for wallet adapters that need to sign transactions
 *
 * @example
 * ```typescript
 * const ix = await createSubscriptionIx({
 *   connection,
 *   owner: userPublicKey,
 *   tierId: 2,
 *   epochAmount: 15,
 *   queue,
 * });
 * // Sign and send with wallet adapter
 * await sendTransaction(transaction.add(ix), connection);
 * ```
 */
export async function createSubscriptionIx(params: {
  connection: Connection;
  owner: PublicKey;
  tierId: number;
  epochAmount: number;
  contactName?: string;
  contactEmail?: string;
  queue: Queue;
}): Promise<web3.TransactionInstruction> {
  const programId = SUBSCRIPTION_PROGRAM_ID;
  const program = await loadSubscriberProgram(params.connection, programId);

  // Derive PDAs
  const [statePda] = getStatePda(programId);
  const [tierPda] = getTierPda(params.tierId, programId);
  const [subscriptionPda] = getSubscriptionPda(params.owner, programId);
  const [tokenVaultPda] = getTokenVaultPda(SWTCH_MINT, programId);
  const payerTokenAccount = getAssociatedTokenAddressSync(
    SWTCH_MINT,
    params.owner
  );
  const quoteAccount = getQuoteAccount(params.queue.pubkey);

  // Build subscription init instruction
  const subscriptionIx = await program.methods
    .subscriptionInit({
      tierId: params.tierId,
      contactName: params.contactName || null,
      contactEmail: params.contactEmail || null,
      epochAmount: new BN(params.epochAmount),
    })
    .accounts({
      state: statePda,
      subscription: subscriptionPda,
      tier: tierPda,
      owner: params.owner,
      payer: params.owner,
      paymentMint: SWTCH_MINT,
      payerTokenAccount,
      tokenVault: tokenVaultPda,
      tokenProgram: TOKEN_PROGRAM_ID,
      systemProgram: SystemProgram.programId,
      quoteAccount,
      sysvars: {
        clock: SYSVAR_CLOCK_PUBKEY,
        slothashes: SYSVAR_SLOT_HASHES_PUBKEY,
        instructions: SYSVAR_INSTRUCTIONS_PUBKEY,
      },
    })
    .instruction();

  return subscriptionIx;
}

/**
 * Build upgrade subscription instruction (does not send transaction)
 *
 * @example
 * ```typescript
 * const ix = await upgradeSubscriptionIx({
 *   connection,
 *   owner: userPublicKey,
 *   newTierId: 3,
 *   epochAmount: 15,
 *   queue,
 * });
 * ```
 */
export async function upgradeSubscriptionIx(params: {
  connection: Connection;
  owner: PublicKey;
  newTierId: number;
  epochAmount: number;
  queue: Queue;
}): Promise<web3.TransactionInstruction> {
  const programId = SUBSCRIPTION_PROGRAM_ID;
  const program = await loadSubscriberProgram(params.connection, programId);

  // Derive PDAs
  const [statePda] = getStatePda(programId);
  const [newTierPda] = getTierPda(params.newTierId, programId);
  const [subscriptionPda] = getSubscriptionPda(params.owner, programId);
  const [tokenVaultPda] = getTokenVaultPda(SWTCH_MINT, programId);
  const ownerTokenAccount = getAssociatedTokenAddressSync(
    SWTCH_MINT,
    params.owner
  );
  const quoteAccount = getQuoteAccount(params.queue.pubkey);

  // Fetch subscription to get current tier
  const subscription =
    await program.account.subscription.fetch(subscriptionPda);
  const currentTierId = subscription.tierId;
  const [currentTierPda] = getTierPda(currentTierId, programId);

  // Build upgrade instruction
  const upgradeIx = await program.methods
    .subscriptionUpgrade({
      newTierId: params.newTierId,
      epochAmount: new BN(params.epochAmount),
    })
    .accounts({
      state: statePda,
      subscription: subscriptionPda,
      newTier: newTierPda,
      owner: params.owner,
      paymentMint: SWTCH_MINT,
      payerTokenAccount: ownerTokenAccount,
      tokenVault: tokenVaultPda,
      tokenProgram: TOKEN_PROGRAM_ID,
      quoteAccount,
      sysvars: {
        clock: SYSVAR_CLOCK_PUBKEY,
        slothashes: SYSVAR_SLOT_HASHES_PUBKEY,
        instructions: SYSVAR_INSTRUCTIONS_PUBKEY,
      },
    })
    .remainingAccounts([
      { pubkey: currentTierPda, isSigner: false, isWritable: false },
    ])
    .instruction();

  return upgradeIx;
}

/**
 * Build downgrade subscription instruction (does not send transaction)
 * Note: Downgrade is credit-only, no oracle needed
 *
 * @example
 * ```typescript
 * const ix = await downgradeSubscriptionIx({
 *   connection,
 *   owner: userPublicKey,
 *   newTierId: 1,
 * });
 * ```
 */
export async function downgradeSubscriptionIx(params: {
  connection: Connection;
  owner: PublicKey;
  newTierId: number;
}): Promise<web3.TransactionInstruction> {
  const programId = SUBSCRIPTION_PROGRAM_ID;
  const program = await loadSubscriberProgram(params.connection, programId);

  // Derive PDAs
  const [statePda] = getStatePda(programId);
  const [newTierPda] = getTierPda(params.newTierId, programId);
  const [subscriptionPda] = getSubscriptionPda(params.owner, programId);

  // Fetch subscription to get current tier
  const subscription =
    await program.account.subscription.fetch(subscriptionPda);
  const currentTierId = subscription.tierId;
  const [currentTierPda] = getTierPda(currentTierId, programId);

  // Build downgrade instruction (credit-only, no payment)
  const downgradeIx = await program.methods
    .subscriptionDowngrade({
      newTierId: params.newTierId,
    })
    .accounts({
      state: statePda,
      subscription: subscriptionPda,
      newTier: newTierPda,
      owner: params.owner,
    })
    .remainingAccounts([
      { pubkey: currentTierPda, isSigner: false, isWritable: false },
    ])
    .instruction();

  return downgradeIx;
}

/**
 * Build extend subscription instruction (does not send transaction)
 *
 * @example
 * ```typescript
 * const ix = await extendSubscriptionIx({
 *   connection,
 *   owner: userPublicKey,
 *   epochAmount: 10,
 *   queue,
 * });
 * ```
 */
export async function extendSubscriptionIx(params: {
  connection: Connection;
  owner: PublicKey;
  epochAmount: number;
  adminExtend?: boolean;
  queue?: Queue;
}): Promise<web3.TransactionInstruction> {
  const programId = SUBSCRIPTION_PROGRAM_ID;
  const program = await loadSubscriberProgram(params.connection, programId);

  // Derive PDAs
  const [statePda] = getStatePda(programId);
  const [subscriptionPda] = getSubscriptionPda(params.owner, programId);

  // Fetch subscription to get current tier
  const subscription =
    await program.account.subscription.fetch(subscriptionPda);
  const currentTierId = subscription.tierId;
  const [tierPda] = getTierPda(currentTierId, programId);

  if (params.adminExtend) {
    // Admin extend - no payment or oracle needed
    const extendIx = await program.methods
      .subscriptionExtend({
        epochAmount: new BN(params.epochAmount),
        adminExtend: true,
      })
      .accounts({
        state: statePda,
        subscription: subscriptionPda,
        tier: tierPda,
        payer: params.owner,
        systemProgram: SystemProgram.programId,
      })
      .instruction();

    return extendIx;
  } else {
    // User extend - requires payment and oracle
    if (!params.queue) {
      throw new Error('queue required for paid extends');
    }

    const [tokenVaultPda] = getTokenVaultPda(SWTCH_MINT, programId);
    const payerTokenAccount = getAssociatedTokenAddressSync(
      SWTCH_MINT,
      params.owner
    );
    const quoteAccount = getQuoteAccount(params.queue.pubkey);

    const extendIx = await program.methods
      .subscriptionExtend({
        epochAmount: new BN(params.epochAmount),
        adminExtend: false,
      })
      .accounts({
        state: statePda,
        subscription: subscriptionPda,
        tier: tierPda,
        payer: params.owner,
        paymentMint: SWTCH_MINT,
        payerTokenAccount,
        tokenVault: tokenVaultPda,
        tokenProgram: TOKEN_PROGRAM_ID,
        systemProgram: SystemProgram.programId,
        quoteAccount,
        sysvars: {
          clock: SYSVAR_CLOCK_PUBKEY,
          slothashes: SYSVAR_SLOT_HASHES_PUBKEY,
          instructions: SYSVAR_INSTRUCTIONS_PUBKEY,
        },
      })
      .instruction();

    return extendIx;
  }
}

/**
 * Build add team member instruction (does not send transaction)
 *
 * @example
 * ```typescript
 * const ix = await addTeamMemberIx({
 *   connection,
 *   owner: userPublicKey,
 *   user: teamMemberPublicKey,
 * });
 * ```
 */
export async function addTeamMemberIx(params: {
  connection: Connection;
  owner: PublicKey;
  user: PublicKey;
}): Promise<web3.TransactionInstruction> {
  const programId = SUBSCRIPTION_PROGRAM_ID;
  const program = await loadSubscriberProgram(params.connection, programId);

  // Derive PDAs
  const [statePda] = getStatePda(programId);
  const [subscriptionPda] = getSubscriptionPda(params.owner, programId);

  // Fetch subscription to get current tier
  const subscription =
    await program.account.subscription.fetch(subscriptionPda);
  const currentTierId = subscription.tierId;
  const [tierPda] = getTierPda(currentTierId, programId);

  const addIx = await program.methods
    .subscriptionManageUsers({
      action: { add: {} },
      user: params.user,
    })
    .accounts({
      state: statePda,
      subscription: subscriptionPda,
      tier: tierPda,
      owner: params.owner,
    })
    .instruction();

  return addIx;
}

/**
 * Build remove team member instruction (does not send transaction)
 *
 * @example
 * ```typescript
 * const ix = await removeTeamMemberIx({
 *   connection,
 *   owner: userPublicKey,
 *   user: teamMemberPublicKey,
 * });
 * ```
 */
export async function removeTeamMemberIx(params: {
  connection: Connection;
  owner: PublicKey;
  user: PublicKey;
}): Promise<web3.TransactionInstruction> {
  const programId = SUBSCRIPTION_PROGRAM_ID;
  const program = await loadSubscriberProgram(params.connection, programId);

  // Derive PDAs
  const [statePda] = getStatePda(programId);
  const [subscriptionPda] = getSubscriptionPda(params.owner, programId);

  // Fetch subscription to get current tier
  const subscription =
    await program.account.subscription.fetch(subscriptionPda);
  const currentTierId = subscription.tierId;
  const [tierPda] = getTierPda(currentTierId, programId);

  const removeIx = await program.methods
    .subscriptionManageUsers({
      action: { remove: {} },
      user: params.user,
    })
    .accounts({
      state: statePda,
      subscription: subscriptionPda,
      tier: tierPda,
      owner: params.owner,
    })
    .instruction();

  return removeIx;
}

/**
 * Build close subscription instruction (does not send transaction)
 *
 * @example
 * ```typescript
 * const ix = await closeSubscriptionIx({
 *   connection,
 *   owner: userPublicKey,
 *   destination: userPublicKey,
 * });
 * ```
 */
export async function closeSubscriptionIx(params: {
  connection: Connection;
  owner: PublicKey;
  destination?: PublicKey;
}): Promise<web3.TransactionInstruction> {
  const programId = SUBSCRIPTION_PROGRAM_ID;
  const program = await loadSubscriberProgram(params.connection, programId);

  // Derive subscription PDA
  const [subscriptionPda] = getSubscriptionPda(params.owner, programId);
  const destination = params.destination || params.owner;

  // Build close instruction
  const closeIx = await program.methods
    .subscriptionClose({})
    .accounts({
      subscription: subscriptionPda,
      owner: params.owner,
      destination,
    })
    .instruction();

  return closeIx;
}

// ============================================================================
// ADMIN SDK - Program Authority Functions
// ============================================================================

/**
 * Initialize the subscriber program state (one-time setup, admin only)
 *
 * Must be called by program upgrade authority
 *
 * @example
 * ```typescript
 * const sig = await initializeState({
 *   connection,
 *   authority: authorityKeypair,
 *   swtchMint: new PublicKey('SW1TCH...'),
 *   swtchFeedId: '148aaedb33ff23593805377f131af7fe01b4770ae2b9e4a2f7f8b3a180314711',
 * });
 * ```
 */
export async function initializeState(params: {
  connection: Connection;
  authority: web3.Keypair; // Must be program upgrade authority
  payer?: web3.Keypair; // Defaults to authority
  swtchMint: PublicKey;
  swtchFeedId: string; // Hex string (32 bytes)
}): Promise<string> {
  const programId = SUBSCRIPTION_PROGRAM_ID;
  const program = await loadSubscriberProgram(params.connection, programId);
  const payer = params.payer || params.authority;

  // Derive state PDA
  const [statePda] = getStatePda(programId);

  // Derive program data PDA (for upgrade authority check)
  const BPF_LOADER_UPGRADEABLE = new PublicKey(
    'BPFLoaderUpgradeab1e11111111111111111111111'
  );
  const [programDataPda] = PublicKey.findProgramAddressSync(
    [programId.toBuffer()],
    BPF_LOADER_UPGRADEABLE
  );

  // Convert hex string to byte array
  const hexStr = params.swtchFeedId.replace(/^0x/, '');
  const swtchFeedId = Array.from(Buffer.from(hexStr, 'hex'));

  if (swtchFeedId.length !== 32) {
    throw new Error('Feed ID must be 32 bytes (64 hex characters)');
  }

  const stateParams = {
    swtchMint: params.swtchMint,
    swtchFeedId,
  };

  const ix = await program.methods
    .stateInit(stateParams)
    .accounts({
      state: statePda,
      payer: payer.publicKey,
      authority: params.authority.publicKey,
      programData: programDataPda,
    })
    .instruction();

  // Execute transaction
  const tx = new web3.Transaction().add(ix);
  const latestBlockhash = await params.connection.getLatestBlockhash();
  tx.recentBlockhash = latestBlockhash.blockhash;
  tx.feePayer = payer.publicKey;

  const signers = [payer];
  if (params.authority.publicKey.toBase58() !== payer.publicKey.toBase58()) {
    signers.push(params.authority);
  }
  tx.partialSign(...signers);

  const sig = await params.connection.sendRawTransaction(tx.serialize());
  await params.connection.confirmTransaction({
    signature: sig,
    blockhash: latestBlockhash.blockhash,
    lastValidBlockHeight: latestBlockhash.lastValidBlockHeight,
  });

  return sig;
}

/**
 * Update program state configuration (admin only)
 *
 * @example
 * ```typescript
 * const sig = await updateStateConfig({
 *   connection,
 *   authority: authorityKeypair,
 *   swtchMint: new PublicKey('NEW_MINT...'),
 *   swtchFeedId: 'new_feed_id_hex',
 * });
 * ```
 */
export async function updateStateConfig(params: {
  connection: Connection;
  authority: web3.Keypair;
  swtchMint?: PublicKey;
  swtchFeedId?: string; // Hex string
  epochLength?: number;
}): Promise<string> {
  const programId = SUBSCRIPTION_PROGRAM_ID;
  const program = await loadSubscriberProgram(params.connection, programId);

  const [statePda] = getStatePda(programId);

  let swtchFeedIdBytes: number[] | null = null;
  if (params.swtchFeedId) {
    const hexStr = params.swtchFeedId.replace(/^0x/, '');
    const bytes = Array.from(Buffer.from(hexStr, 'hex'));
    if (bytes.length !== 32) {
      throw new Error('Feed ID must be 32 bytes (64 hex characters)');
    }
    swtchFeedIdBytes = bytes;
  }

  const updateParams = {
    swtchMint: params.swtchMint || null,
    swtchFeedId: swtchFeedIdBytes,
    epochLength: params.epochLength ? new BN(params.epochLength) : null,
  };

  const ix = await program.methods
    .stateUpdateConfig(updateParams)
    .accounts({
      state: statePda,
      authority: params.authority.publicKey,
    })
    .instruction();

  const tx = new web3.Transaction().add(ix);
  const latestBlockhash = await params.connection.getLatestBlockhash();
  tx.recentBlockhash = latestBlockhash.blockhash;
  tx.feePayer = params.authority.publicKey;
  tx.partialSign(params.authority);

  const sig = await params.connection.sendRawTransaction(tx.serialize());
  await params.connection.confirmTransaction({
    signature: sig,
    blockhash: latestBlockhash.blockhash,
    lastValidBlockHeight: latestBlockhash.lastValidBlockHeight,
  });

  return sig;
}

/**
 * Create a new pricing tier (admin only)
 *
 * @example
 * ```typescript
 * const sig = await createTier({
 *   connection,
 *   authority: authorityKeypair,
 *   tierId: 2,
 *   name: 'Pro',
 *   displayName: 'Pro Plan',
 *   description: 'Professional tier with enhanced limits',
 *   maxConnections: 15,
 *   maxFeeds: 300,
 *   maxFeedsPerIx: 6,
 *   minDelayMs: 300,
 *   maxAuthorizedUsers: 5,
 *   costPerEpochUsdCents: 10000, // 100 USD per epoch
 *   isPublic: true,
 * });
 * ```
 */
export async function createTier(params: {
  connection: Connection;
  authority: web3.Keypair;
  payer?: web3.Keypair;
  tierId: number;
  name: string;
  displayName: string;
  description: string;
  maxConnections: number;
  maxFeeds: number;
  maxFeedsPerIx: number;
  minDelayMs: number;
  maxAuthorizedUsers: number;
  costPerEpochUsdCents: number;
  isPublic: boolean;
}): Promise<string> {
  const programId = SUBSCRIPTION_PROGRAM_ID;
  const program = await loadSubscriberProgram(params.connection, programId);
  const payer = params.payer || params.authority;

  const [statePda] = getStatePda(programId);
  const [tierPda] = getTierPda(params.tierId, programId);

  const tierParams = {
    tierId: params.tierId,
    name: params.name,
    displayName: params.displayName,
    description: params.description,
    maxConnections: params.maxConnections,
    maxFeeds: params.maxFeeds,
    maxFeedsPerIx: params.maxFeedsPerIx,
    minDelayMs: params.minDelayMs,
    maxAuthorizedUsers: params.maxAuthorizedUsers,
    costPerEpochUsdCents: new BN(params.costPerEpochUsdCents),
    isPublic: params.isPublic,
  };

  const ix = await program.methods
    .tierCreate(tierParams)
    .accounts({
      state: statePda,
      tier: tierPda,
      payer: payer.publicKey,
      authority: params.authority.publicKey,
    })
    .instruction();

  const tx = new web3.Transaction().add(ix);
  const latestBlockhash = await params.connection.getLatestBlockhash();
  tx.recentBlockhash = latestBlockhash.blockhash;
  tx.feePayer = payer.publicKey;

  const signers = [payer];
  if (params.authority.publicKey.toBase58() !== payer.publicKey.toBase58()) {
    signers.push(params.authority);
  }
  tx.partialSign(...signers);

  const sig = await params.connection.sendRawTransaction(tx.serialize());
  await params.connection.confirmTransaction({
    signature: sig,
    blockhash: latestBlockhash.blockhash,
    lastValidBlockHeight: latestBlockhash.lastValidBlockHeight,
  });

  return sig;
}

/**
 * Update an existing tier (admin only)
 *
 * All parameters are optional - only provided values will be updated
 *
 * @example
 * ```typescript
 * const sig = await updateTier({
 *   connection,
 *   authority: authorityKeypair,
 *   tierId: 2,
 *   costPerEpochUsdCents: 15000, // Update price to 150 USD
 *   minDelayMs: 100, // Update rate limit
 * });
 * ```
 */
export async function updateTier(params: {
  connection: Connection;
  authority: web3.Keypair;
  tierId: number;
  name?: string;
  displayName?: string;
  description?: string;
  maxConnections?: number;
  maxFeeds?: number;
  maxFeedsPerIx?: number;
  minDelayMs?: number;
  maxAuthorizedUsers?: number;
  costPerEpochUsdCents?: number;
  isPublic?: boolean;
  enabled?: boolean;
}): Promise<string> {
  const programId = SUBSCRIPTION_PROGRAM_ID;
  const program = await loadSubscriberProgram(params.connection, programId);

  const [statePda] = getStatePda(programId);
  const [tierPda] = getTierPda(params.tierId, programId);

  const updateParams = {
    name: params.name || null,
    displayName: params.displayName || null,
    description: params.description || null,
    maxConnections: params.maxConnections ?? null,
    maxFeeds: params.maxFeeds ?? null,
    maxFeedsPerIx: params.maxFeedsPerIx ?? null,
    minDelayMs: params.minDelayMs ?? null,
    maxAuthorizedUsers: params.maxAuthorizedUsers ?? null,
    costPerEpochUsdCents: params.costPerEpochUsdCents
      ? new BN(params.costPerEpochUsdCents)
      : null,
    isPublic: params.isPublic ?? null,
    enabled: params.enabled ?? null,
  };

  const ix = await program.methods
    .tierUpdate(updateParams)
    .accounts({
      state: statePda,
      tier: tierPda,
      authority: params.authority.publicKey,
    })
    .instruction();

  const tx = new web3.Transaction().add(ix);
  const latestBlockhash = await params.connection.getLatestBlockhash();
  tx.recentBlockhash = latestBlockhash.blockhash;
  tx.feePayer = params.authority.publicKey;
  tx.partialSign(params.authority);

  const sig = await params.connection.sendRawTransaction(tx.serialize());
  await params.connection.confirmTransaction({
    signature: sig,
    blockhash: latestBlockhash.blockhash,
    lastValidBlockHeight: latestBlockhash.lastValidBlockHeight,
  });

  return sig;
}

/**
 * Admin-managed subscription update (admin only)
 *
 * Allows admins to forcefully change tier, activate/deactivate, extend for free, or update contact info
 *
 * @example
 * ```typescript
 * // Force tier change
 * const sig = await adminManageSubscription({
 *   connection,
 *   authority: authorityKeypair,
 *   owner: new PublicKey('user...'),
 *   setTierId: 3,
 * });
 *
 * // Extend subscription for free
 * const sig = await adminManageSubscription({
 *   connection,
 *   authority: authorityKeypair,
 *   owner: new PublicKey('user...'),
 *   extendEpochs: 30,
 * });
 *
 * // Deactivate subscription
 * const sig = await adminManageSubscription({
 *   connection,
 *   authority: authorityKeypair,
 *   owner: new PublicKey('user...'),
 *   setActive: false,
 * });
 * ```
 */
export async function adminManageSubscription(params: {
  connection: Connection;
  authority: web3.Keypair;
  owner: PublicKey; // Subscription owner (not signer)
  setTierId?: number;
  setActive?: boolean;
  extendEpochs?: number;
  updateContactName?: string;
  updateContactEmail?: string;
}): Promise<string> {
  const programId = SUBSCRIPTION_PROGRAM_ID;
  const program = await loadSubscriberProgram(params.connection, programId);

  const [statePda] = getStatePda(programId);
  const [subscriptionPda] = getSubscriptionPda(params.owner, programId);

  const adminParams = {
    setTierId: params.setTierId ?? null,
    setActive: params.setActive ?? null,
    extendEpochs: params.extendEpochs ? new BN(params.extendEpochs) : null,
    updateContactName: params.updateContactName || null,
    updateContactEmail: params.updateContactEmail || null,
  };

  const ix = await program.methods
    .subscriptionAdminManage(adminParams)
    .accounts({
      state: statePda,
      subscription: subscriptionPda,
      authority: params.authority.publicKey,
    })
    .instruction();

  const tx = new web3.Transaction().add(ix);
  const latestBlockhash = await params.connection.getLatestBlockhash();
  tx.recentBlockhash = latestBlockhash.blockhash;
  tx.feePayer = params.authority.publicKey;
  tx.partialSign(params.authority);

  const sig = await params.connection.sendRawTransaction(tx.serialize());
  await params.connection.confirmTransaction({
    signature: sig,
    blockhash: latestBlockhash.blockhash,
    lastValidBlockHeight: latestBlockhash.lastValidBlockHeight,
  });

  return sig;
}

/**
 * Admin-only tier change with automatic pro-rating (admin only)
 *
 * @example
 * ```typescript
 * const sig = await adminSetTier({
 *   connection,
 *   authority: authorityKeypair,
 *   owner: new PublicKey('user...'),
 *   newTierId: 3,
 * });
 * ```
 */
export async function adminSetTier(params: {
  connection: Connection;
  authority: web3.Keypair;
  owner: PublicKey; // Subscription owner
  newTierId: number;
}): Promise<string> {
  const programId = SUBSCRIPTION_PROGRAM_ID;
  const program = await loadSubscriberProgram(params.connection, programId);

  const [statePda] = getStatePda(programId);
  const [subscriptionPda] = getSubscriptionPda(params.owner, programId);

  // Fetch subscription to get current tier
  const subscription =
    await program.account.subscription.fetch(subscriptionPda);
  const currentTierId = subscription.tierId;

  const [newTierPda] = getTierPda(params.newTierId, programId);
  const [oldTierPda] = getTierPda(currentTierId, programId);

  const tierParams = {
    newTierId: params.newTierId,
  };

  const ix = await program.methods
    .subscriptionSetTier(tierParams)
    .accounts({
      state: statePda,
      subscription: subscriptionPda,
      newTier: newTierPda,
      oldTier: oldTierPda,
      authority: params.authority.publicKey,
    })
    .instruction();

  const tx = new web3.Transaction().add(ix);
  const latestBlockhash = await params.connection.getLatestBlockhash();
  tx.recentBlockhash = latestBlockhash.blockhash;
  tx.feePayer = params.authority.publicKey;
  tx.partialSign(params.authority);

  const sig = await params.connection.sendRawTransaction(tx.serialize());
  await params.connection.confirmTransaction({
    signature: sig,
    blockhash: latestBlockhash.blockhash,
    lastValidBlockHeight: latestBlockhash.lastValidBlockHeight,
  });

  return sig;
}
