import { web3 } from '@coral-xyz/anchor-31';
import bs58 from 'bs58';
import * as crypto from 'crypto';
import nacl from 'tweetnacl';

/**
 * Configuration for signature-based authentication
 */
export interface SignatureAuthConfig {
  /** Keypair for signing */
  keypair: web3.Keypair;
  /** Solana connection for fetching blockhash */
  connection: web3.Connection;
  /** Refresh interval in milliseconds (default: 5 minutes for signature freshness) */
  refreshInterval?: number;
}

/**
 * Signed authentication data
 */
export interface SignedAuthData {
  signature: string;
  publicKey: string;
  blockhash: string;
  timestamp: number;
}

/**
 * Manages signature-based authentication for Switchboard service
 * Signs blockhash periodically to prove subscription and freshness
 */
export class SignatureAuth {
  private keypair: web3.Keypair;
  private connection: web3.Connection;
  private refreshInterval: number;
  private currentAuthData?: SignedAuthData;
  private refreshTimer?: NodeJS.Timeout;
  private lastRefreshTime: number = 0;

  constructor(config: SignatureAuthConfig) {
    this.keypair = config.keypair;
    this.connection = config.connection;
    this.refreshInterval = config.refreshInterval || 5 * 60 * 1000; // 5 minutes default (signature freshness window)
  }

  /**
   * Check if signature auth is configured and enabled
   */
  public isEnabled(): boolean {
    return !!(this.keypair && this.connection);
  }

  /**
   * Get the current auth headers, refreshing if necessary
   */
  public async getAuthHeaders(): Promise<Record<string, string> | null> {
    if (!this.isEnabled()) {
      return null;
    }

    // Check if we need to refresh
    const now = Date.now();
    if (
      !this.currentAuthData ||
      now - this.lastRefreshTime >= this.refreshInterval
    ) {
      await this.refresh();
    }

    if (!this.currentAuthData) {
      return null;
    }

    // Return auth headers
    return {
      'X-Switchboard-Signature': this.currentAuthData.signature,
      'X-Switchboard-Pubkey': this.currentAuthData.publicKey,
      'X-Switchboard-Blockhash': this.currentAuthData.blockhash,
      'X-Switchboard-Timestamp': this.currentAuthData.timestamp.toString(),
    };
  }

  /**
   * Get the current auth data as an object (for WebSocket messages)
   */
  public async getAuthData(): Promise<SignedAuthData | null> {
    if (!this.isEnabled()) {
      return null;
    }

    // Check if we need to refresh
    const now = Date.now();
    if (
      !this.currentAuthData ||
      now - this.lastRefreshTime >= this.refreshInterval
    ) {
      await this.refresh();
    }

    return this.currentAuthData || null;
  }

  /**
   * Refresh the signature by fetching latest blockhash and signing
   */
  public async refresh(): Promise<void> {
    if (!this.isEnabled()) {
      return;
    }

    try {
      // Fetch latest blockhash
      const { blockhash } = await this.connection.getLatestBlockhash();

      // Create message to sign
      const timestamp = Date.now();
      const message = this.createMessage(blockhash, timestamp);

      // Sign the message with Ed25519 using tweetnacl
      const signatureBytes = nacl.sign.detached(
        message,
        this.keypair.secretKey
      );

      // Store the auth data (use base58 encoding - Solana standard)
      this.currentAuthData = {
        signature: bs58.encode(signatureBytes),
        publicKey: this.keypair.publicKey.toBase58(),
        blockhash,
        timestamp,
      };

      this.lastRefreshTime = Date.now();
    } catch (error) {
      console.error('Failed to refresh signature auth:', error);
      throw error;
    }
  }

  /**
   * Create the message to sign
   * Format: SHA256(blockhash:timestamp)
   */
  private createMessage(blockhash: string, timestamp: number): Uint8Array {
    // Message format: blockhash:timestamp
    const messageStr = `${blockhash}:${timestamp}`;
    const messageBuffer = Buffer.from(messageStr, 'utf-8');

    // Hash the message with SHA256 (matches Rust implementation)
    const hash = crypto.createHash('sha256').update(messageBuffer).digest();
    return new Uint8Array(hash);
  }

  /**
   * Start automatic refresh timer
   */
  public startAutoRefresh(): void {
    if (!this.isEnabled()) {
      return;
    }

    // Clear any existing timer
    this.stopAutoRefresh();

    // Refresh immediately
    this.refresh().catch(err =>
      console.error('Initial signature refresh failed:', err)
    );

    // Set up periodic refresh
    this.refreshTimer = setInterval(() => {
      this.refresh().catch(err =>
        console.error('Signature refresh failed:', err)
      );
    }, this.refreshInterval);
  }

  /**
   * Stop automatic refresh timer
   */
  public stopAutoRefresh(): void {
    if (this.refreshTimer) {
      clearInterval(this.refreshTimer);
      this.refreshTimer = undefined;
    }
  }

  /**
   * Cleanup resources
   */
  public destroy(): void {
    this.stopAutoRefresh();
    this.currentAuthData = undefined;
  }
}
