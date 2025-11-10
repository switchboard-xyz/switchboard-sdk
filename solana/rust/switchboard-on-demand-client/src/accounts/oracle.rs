use crate::*;
use bytemuck;
use solana_sdk::pubkey::Pubkey;

pub const KEY_ROTATE_KEEPALIVE_SLOTS: u64 = 1500;
pub const MAX_STALE_SECONDS: i64 = 300;

#[repr(C)]
#[derive(Debug, Copy, Clone, bytemuck::Pod, bytemuck::Zeroable)]
pub struct OracleAccountData {
    /// Represents the state of the quote verifiers enclave.
    pub enclave: Quote,

    // Accounts Config
    /// The authority of the EnclaveAccount which is permitted to make account changes.
    pub authority: Pubkey,
    /// Queue used for attestation to verify a MRENCLAVE measurement.
    pub queue: Pubkey,

    // Metadata Config
    /// The unix timestamp when the quote was created.
    pub created_at: i64,

    /// The last time the quote heartbeated on-chain.
    pub last_heartbeat: i64,

    pub secp_authority: [u8; 64],

    /// URI location of the verifier's gateway.
    pub gateway_uri: [u8; 64],
    pub permissions: u64,
    /// Whether the quote is located on the AttestationQueues buffer.
    pub is_on_queue: u8,
    _padding1: [u8; 7],
    pub lut_slot: u64,
    pub last_reward_epoch: u64,

    _ebuf4: [u8; 16],
    _ebuf3: [u8; 32],
    _ebuf2: [u8; 64],
    _ebuf1: [u8; 1024],
}

#[repr(C)]
#[derive(Debug, Copy, Clone, bytemuck::Pod, bytemuck::Zeroable)]
pub struct Quote {
    /// The address of the signer generated within an enclave.
    pub enclave_signer: Pubkey,
    /// The quotes MRENCLAVE measurement dictating the contents of the secure enclave.
    pub mr_enclave: [u8; 32],
    /// The VerificationStatus of the quote.
    pub verification_status: u8,
    padding1: [u8; 7],
    /// The unix timestamp when the quote was last verified.
    pub verification_timestamp: i64,
    /// The unix timestamp when the quotes verification status expires.
    pub valid_until: i64,
    /// The off-chain registry where the verifiers quote can be located.
    pub quote_registry: [u8; 32],
    /// Key to lookup the buffer data on IPFS or an alternative decentralized storage solution.
    pub registry_key: [u8; 64],
    /// The secp256k1 public key of the enclave signer. Derived from the enclave_signer.
    pub secp256k1_signer: [u8; 64],
    pub last_ed25519_signer: Pubkey,
    pub last_secp256k1_signer: [u8; 64],
    pub last_rotate_slot: u64,
    pub guardian_approvers: [Pubkey; 64],
    pub guardian_approvers_len: u8,
    padding2: [u8; 7],
    pub staging_ed25519_signer: Pubkey,
    pub staging_secp256k1_signer: [u8; 64],
    /// Reserved.
    _ebuf4: [u8; 32],
    _ebuf3: [u8; 128],
    _ebuf2: [u8; 256],
    _ebuf1: [u8; 512],
}

impl OracleAccountData {
    pub fn stats_key(oracle: &Pubkey) -> Pubkey {
        Pubkey::find_program_address(
            &[ORACLE_STATS_SEED, &oracle.to_bytes()],
            &get_switchboard_on_demand_program_id(),
        )
        .0
    }

    pub fn gateway_uri(&self) -> Option<String> {
        let uri = self.gateway_uri;
        let uri = String::from_utf8_lossy(&uri);
        let uri = uri
            .split_at(uri.find('\0').unwrap_or(uri.len()))
            .0
            .to_string();
        if uri.is_empty() {
            return None;
        }
        Some(uri)
    }
}

impl LutOwner for OracleAccountData {
    fn lut_slot(&self) -> u64 {
        self.lut_slot
    }
}
