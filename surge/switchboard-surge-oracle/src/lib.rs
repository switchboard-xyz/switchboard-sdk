pub mod auth;
pub mod consensus;
pub mod messages;
pub mod stream;

// Re-export key types
pub use auth::{create_session_token, hash_api_key, parse_auth_header, validate_session_token};
pub use messages::{
    ClientMessage, ServerMessage, FeedBundleRequest, FeedBundleInfo, 
    FeedPair, FeedInfo, ConsensusStreamFeedValue, StreamConsensusResponse,
    UnsignedFeedUpdate, FeedValidationError, ConnectionStatusType
};
pub use stream::handler::{handle_websocket_upgrade, SpawnExecutor};
pub use stream::state::{ConnectionState, CONNECTIONS};