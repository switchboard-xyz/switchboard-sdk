/// Core Crossbar protocol implementations and client functionality
pub mod crossbar;
pub use crossbar::*;

/// Gateway client for interfacing with Switchboard's Crossbar API
pub mod gateway;
pub use gateway::*;

/// Pull-based oracle feed management and data fetching utilities
pub mod pull_feed;
pub use self::pull_feed::PullFeed;

/// SECP256k1 cryptographic utilities and signature verification
pub mod secp256k1;
pub use secp256k1::*;

/// Signature-based authentication for Switchboard services
pub mod signature_auth;
pub use signature_auth::*;

/// Subscription management for Switchboard On-Demand services
pub mod subscription;
pub use subscription::*;

/// Real-time WebSocket streaming for price updates (Surge)
pub mod surge;
pub use surge::*;

/// Lookup table ownership and management functionality
pub mod lut_owner;
pub use lut_owner::*;

/// Solana slot hash utilities and recent hash management
pub mod recent_slothashes;
pub use recent_slothashes::*;

/// Client-specific account structures and deserialization utilities
pub mod accounts;
pub use accounts::*;

/// Client-specific instruction builders for interacting with the Switchboard On-Demand program
pub mod instructions;
pub use instructions::*;

/// Transaction building utilities
pub mod transaction_builder;
pub use transaction_builder::*;

/// Client utility functions and helpers
pub mod utils;
pub use utils::*;

/// Re-export prost for protobuf handling
pub use prost;
