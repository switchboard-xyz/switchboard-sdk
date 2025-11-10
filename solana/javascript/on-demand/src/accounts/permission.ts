import { getNodePayer } from '../utils/index.js';

import type { Program, web3 } from '@coral-xyz/anchor-31';

export enum SwitchboardPermission {
  PermitOracleHeartbeat = 1 << 0,
  PermitOracleQueueUsage = 1 << 1,
}

/**
 *  Abstraction around the Switchboard-On-Demand Permission meta-account
 */
export class Permission {
  /**
   *  Set the permission for a given granter and grantee.
   *
   *  @param program - The program that owns the permission account.
   *  @param params - The parameters for setting the permission.
   *  @returns A promise that resolves to the transaction instruction.
   */
  static async setIx(
    program: Program,
    params: {
      authority: web3.PublicKey;
      granter: web3.PublicKey;
      grantee: web3.PublicKey;
      enable?: boolean;
      permission: SwitchboardPermission;
    }
  ): Promise<web3.TransactionInstruction> {
    const payer = getNodePayer(program);
    const ix = await program.instruction.permissionSet(
      {
        enable: params.enable ?? false,
        permission: params.permission,
      },
      {
        accounts: {
          granter: params.granter,
          authority: params.authority,
        },
        remainingAccounts: [
          { pubkey: params.grantee, isSigner: false, isWritable: true },
        ],
        signers: [payer],
      }
    );
    return ix;
  }

  /**
   *  Disable object instantiation.
   */
  private constructor() {}
}
