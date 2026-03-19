use anyhow::{anyhow, bail, Result};
use borsh::{BorshDeserialize, BorshSerialize};

use crate::{
    on_demand::oracle_quote::feed_info::{PackedFeedInfo, PackedQuoteHeader},
    AccountMeta, Instruction, Pubkey, QUOTE_PROGRAM_ID, SYSTEM_PROGRAM_ID,
};

pub const AUTHORITY_QUOTE_SCHEME_TAG: [u8; 4] = *b"AUTH";
pub const QUOTE_TAIL_DISCRIMINATOR: [u8; 4] = *b"SBOD";
pub const FEED_AUTHORITY_UPDATE_OPCODE: u8 = 0x01;
pub const MAX_AUTHORITY_FEED_IDS: usize = 13;
pub const MAX_AUTHORITY_FEED_ID_BYTES: usize = MAX_AUTHORITY_FEED_IDS * 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuoteSourceScheme {
    Oracle,
    Authority,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthorityQuotePayload {
    pub feeds: Vec<PackedFeedInfo>,
    pub slot: u64,
    pub version: u8,
    pub tail_discriminator: [u8; 4],
}

impl AuthorityQuotePayload {
    pub fn parse(data: &[u8]) -> Result<Self> {
        const HEADER_LEN: usize = 4 + 1;
        const FOOTER_LEN: usize = 8 + 1 + 4;

        if data.len() < HEADER_LEN + FOOTER_LEN {
            bail!("Authority quote payload too short");
        }
        if !data.starts_with(&AUTHORITY_QUOTE_SCHEME_TAG) {
            bail!("Invalid authority quote payload tag");
        }

        let feed_count = data[4] as usize;
        if feed_count == 0 {
            bail!("Authority quote payload requires at least one feed");
        }
        validate_authority_feed_count(feed_count)?;

        let feeds_len = feed_count
            .checked_mul(PackedFeedInfo::PACKED_SIZE)
            .ok_or_else(|| anyhow!("Authority quote payload feed section overflow"))?;
        let expected_len = HEADER_LEN
            .checked_add(feeds_len)
            .and_then(|len| len.checked_add(FOOTER_LEN))
            .ok_or_else(|| anyhow!("Authority quote payload size overflow"))?;
        if data.len() != expected_len {
            bail!(
                "Invalid authority quote payload length: expected {}, got {}",
                expected_len,
                data.len()
            );
        }

        let feeds_start = HEADER_LEN;
        let feeds_end = feeds_start + feeds_len;
        let mut feeds = Vec::with_capacity(feed_count);
        for chunk in data[feeds_start..feeds_end].chunks_exact(PackedFeedInfo::PACKED_SIZE) {
            feeds.push(PackedFeedInfo::try_from_slice(chunk)?);
        }
        validate_unique_feed_ids_from_feeds(&feeds)?;

        let slot_start = feeds_end;
        let slot = u64::from_le_bytes(data[slot_start..slot_start + 8].try_into().unwrap());
        let version = data[slot_start + 8];
        let tail_discriminator: [u8; 4] = data[slot_start + 9..slot_start + 13].try_into().unwrap();
        if tail_discriminator != QUOTE_TAIL_DISCRIMINATOR {
            bail!("Invalid authority quote payload tail discriminator");
        }

        Ok(Self {
            feeds,
            slot,
            version,
            tail_discriminator,
        })
    }

    pub fn build(feeds: &[PackedFeedInfo], slot: u64, version: u8) -> Result<Vec<u8>> {
        if feeds.is_empty() {
            bail!("Authority quote payload requires at least one feed");
        }
        validate_authority_feed_count(feeds.len())?;
        validate_unique_feed_ids_from_feeds(feeds)?;

        let mut data =
            Vec::with_capacity(4 + 1 + (feeds.len() * PackedFeedInfo::PACKED_SIZE) + 8 + 1 + 4);
        data.extend_from_slice(&AUTHORITY_QUOTE_SCHEME_TAG);
        data.push(feeds.len() as u8);
        for feed in feeds {
            feed.serialize(&mut data)?;
        }
        data.extend_from_slice(&slot.to_le_bytes());
        data.push(version);
        data.extend_from_slice(&QUOTE_TAIL_DISCRIMINATOR);
        Ok(data)
    }

    pub fn feed_ids(&self) -> Vec<[u8; 32]> {
        self.feeds.iter().map(|feed| *feed.feed_id()).collect()
    }

    pub fn quote_header(&self) -> PackedQuoteHeader {
        PackedQuoteHeader {
            signed_slothash: [0u8; 32],
        }
    }
}

fn validate_authority_feed_count(feed_count: usize) -> Result<()> {
    if feed_count > MAX_AUTHORITY_FEED_IDS {
        bail!(
            "Authority quote payload supports at most {} feed IDs ({} bytes)",
            MAX_AUTHORITY_FEED_IDS,
            MAX_AUTHORITY_FEED_ID_BYTES
        );
    }
    Ok(())
}

fn validate_unique_feed_ids(feed_ids: &[[u8; 32]]) -> Result<()> {
    for (i, feed_id) in feed_ids.iter().enumerate() {
        for other_feed_id in feed_ids.iter().skip(i + 1) {
            if feed_id == other_feed_id {
                bail!("Authority quote payload does not allow duplicate feed IDs");
            }
        }
    }
    Ok(())
}

fn validate_unique_feed_ids_from_feeds(feeds: &[PackedFeedInfo]) -> Result<()> {
    for (i, feed) in feeds.iter().enumerate() {
        for other_feed in feeds.iter().skip(i + 1) {
            if feed.feed_id() == other_feed.feed_id() {
                bail!("Authority quote payload does not allow duplicate feed IDs");
            }
        }
    }
    Ok(())
}

pub fn derive_authority_quote_pubkey(
    authority: &Pubkey,
    feed_ids: &[[u8; 32]],
    program_id: &Pubkey,
) -> Result<(Pubkey, u8)> {
    validate_authority_feed_count(feed_ids.len())?;
    validate_unique_feed_ids(feed_ids)?;
    let mut seeds = Vec::with_capacity(feed_ids.len() + 2);
    seeds.push(AUTHORITY_QUOTE_SCHEME_TAG.as_ref());
    seeds.push(authority.as_ref());
    for feed_id in feed_ids {
        seeds.push(feed_id.as_ref());
    }

    Ok(Pubkey::find_program_address(&seeds, program_id))
}

pub fn build_authority_quote_payload(
    feeds: &[PackedFeedInfo],
    slot: u64,
    version: u8,
) -> Result<Vec<u8>> {
    AuthorityQuotePayload::build(feeds, slot, version)
}

pub fn build_feed_authority_update_instruction(
    authority: Pubkey,
    payer: Pubkey,
    payload: &[u8],
    program_id: Option<Pubkey>,
) -> Result<Instruction> {
    let program_id = program_id.unwrap_or(QUOTE_PROGRAM_ID);
    let parsed = AuthorityQuotePayload::parse(payload)?;
    let feed_ids = parsed.feed_ids();
    let (quote_account, _) = derive_authority_quote_pubkey(&authority, &feed_ids, &program_id)?;

    let mut data = Vec::with_capacity(payload.len() + 1);
    data.push(FEED_AUTHORITY_UPDATE_OPCODE);
    data.extend_from_slice(payload);

    Ok(Instruction {
        program_id,
        accounts: vec![
            AccountMeta::new(quote_account, false),
            AccountMeta::new_readonly(authority, true),
            AccountMeta::new_readonly(crate::solana_compat::sysvar::clock::id(), false),
            AccountMeta::new(payer, true),
            AccountMeta::new_readonly(SYSTEM_PROGRAM_ID, false),
        ],
        data,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_feed(feed_seed: u8, value: i128) -> PackedFeedInfo {
        PackedFeedInfo {
            feed_id: [feed_seed; 32],
            feed_value: value,
            min_oracle_samples: 1,
        }
    }

    #[test]
    fn authority_quote_payload_round_trip() {
        let feeds = vec![sample_feed(1, 123), sample_feed(2, 456)];
        let encoded = build_authority_quote_payload(&feeds, 42, 7).unwrap();
        let decoded = AuthorityQuotePayload::parse(&encoded).unwrap();

        assert_eq!(decoded.feeds, feeds);
        assert_eq!(decoded.slot, 42);
        assert_eq!(decoded.version, 7);
        assert_eq!(decoded.tail_discriminator, QUOTE_TAIL_DISCRIMINATOR);
    }

    #[test]
    fn authority_quote_pubkey_is_order_sensitive() {
        let authority = Pubkey::new_unique();
        let program_id = Pubkey::new_unique();

        let (a, _) =
            derive_authority_quote_pubkey(&authority, &[[1u8; 32], [2u8; 32]], &program_id)
                .unwrap();
        let (b, _) =
            derive_authority_quote_pubkey(&authority, &[[2u8; 32], [1u8; 32]], &program_id)
                .unwrap();

        assert_ne!(a, b);
    }

    #[test]
    fn feed_authority_update_instruction_derives_expected_quote_account() {
        let authority = Pubkey::new_unique();
        let payer = Pubkey::new_unique();
        let program_id = Pubkey::new_unique();
        let payload = build_authority_quote_payload(&[sample_feed(9, 999)], 12, 1).unwrap();
        let ix =
            build_feed_authority_update_instruction(authority, payer, &payload, Some(program_id))
                .unwrap();
        let (expected_quote, _) =
            derive_authority_quote_pubkey(&authority, &[[9u8; 32]], &program_id).unwrap();

        assert_eq!(ix.program_id, program_id);
        assert_eq!(ix.accounts[0].pubkey, expected_quote);
        assert_eq!(ix.data[0], FEED_AUTHORITY_UPDATE_OPCODE);
    }

    #[test]
    fn authority_quote_payload_rejects_too_many_feeds() {
        let feeds = (0..=MAX_AUTHORITY_FEED_IDS)
            .map(|i| sample_feed(i as u8, i as i128))
            .collect::<Vec<_>>();

        let err = build_authority_quote_payload(&feeds, 1, 1).unwrap_err();

        assert!(
            err.to_string()
                .contains("Authority quote payload supports at most 13 feed IDs")
        );
    }

    #[test]
    fn authority_quote_payload_rejects_duplicate_feed_ids() {
        let duplicate_feed_id = [7u8; 32];
        let feeds = vec![
            PackedFeedInfo {
                feed_id: duplicate_feed_id,
                feed_value: 1,
                min_oracle_samples: 1,
            },
            PackedFeedInfo {
                feed_id: duplicate_feed_id,
                feed_value: 2,
                min_oracle_samples: 1,
            },
        ];

        let err = build_authority_quote_payload(&feeds, 1, 1).unwrap_err();

        assert!(
            err.to_string()
                .contains("Authority quote payload does not allow duplicate feed IDs")
        );
    }

    #[test]
    fn authority_quote_derivation_rejects_duplicate_feed_ids() {
        let authority = Pubkey::new_unique();
        let program_id = Pubkey::new_unique();
        let duplicate_feed_id = [4u8; 32];

        let err = derive_authority_quote_pubkey(
            &authority,
            &[duplicate_feed_id, duplicate_feed_id],
            &program_id,
        )
        .unwrap_err();

        assert!(
            err.to_string()
                .contains("Authority quote payload does not allow duplicate feed IDs")
        );
    }
}
