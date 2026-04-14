pub mod pull_feed_submit_response_ix;
pub use pull_feed_submit_response_ix::*;
pub mod pull_feed_submit_response_many_ix;
pub use pull_feed_submit_response_many_ix::*;
pub mod pull_feed_submit_response_consensus;
pub use pull_feed_submit_response_consensus::*;
use sha2::{Digest, Sha256};

pub fn get_discriminator(name: &str) -> Vec<u8> {
    let name = format!("global:{}", name);
    Sha256::digest(&name)[..8].to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    const LEGACY_DISCRIMINATOR: [u8; 8] = [0x96, 0x16, 0xd7, 0xa6, 0x8f, 0x5d, 0x30, 0x89];
    const MANY_DISCRIMINATOR: [u8; 8] = [0x2f, 0x9c, 0x2d, 0x19, 0xc8, 0x47, 0x25, 0xd7];
    const CONSENSUS_DISCRIMINATOR: [u8; 8] = [0xef, 0x7c, 0x27, 0xb8, 0x93, 0xde, 0x10, 0xf8];
    const LIGHT_DISCRIMINATOR: [u8; 8] = [0xb2, 0xb3, 0x58, 0x90, 0xaf, 0x82, 0x9d, 0x57];
    const PLACEHOLDER_DISCRIMINATOR: [u8; 8] = [1, 2, 3, 4, 5, 6, 7, 8];

    fn assert_prefix(data: &[u8], expected: [u8; 8]) {
        assert!(data.starts_with(&expected));
        assert!(!data.starts_with(&PLACEHOLDER_DISCRIMINATOR));
    }

    #[test]
    fn discriminators_match_anchor_instruction_names() {
        assert_eq!(
            get_discriminator("pull_feed_submit_response"),
            LEGACY_DISCRIMINATOR
        );
        assert_eq!(
            get_discriminator("pull_feed_submit_response_many"),
            MANY_DISCRIMINATOR
        );
        assert_eq!(
            get_discriminator("pull_feed_submit_response_consensus"),
            CONSENSUS_DISCRIMINATOR
        );
        assert_eq!(
            get_discriminator("pull_feed_submit_response_consensus_light"),
            LIGHT_DISCRIMINATOR
        );
    }

    #[test]
    fn submit_response_serializers_use_real_anchor_discriminators() {
        let legacy = PullFeedSubmitResponseParams {
            slot: 1,
            submissions: vec![Submission {
                value: 42,
                signature: [9; 64],
                recovery_id: 1,
                offset: 0,
            }],
        }
        .data();
        assert_prefix(&legacy, LEGACY_DISCRIMINATOR);

        let many = PullFeedSubmitResponseManyParams {
            slot: 1,
            submissions: vec![MultiSubmission {
                values: vec![42],
                signature: [9; 64],
                recovery_id: 1,
            }],
        }
        .data();
        assert_prefix(&many, MANY_DISCRIMINATOR);

        let consensus = PullFeedSubmitResponseConsensusParams {
            slot: 1,
            values: vec![42],
        }
        .data();
        assert_prefix(&consensus, CONSENSUS_DISCRIMINATOR);
    }
}
