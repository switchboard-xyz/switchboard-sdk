use crate::Source;
use once_cell::sync::Lazy;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct ExchangeConfig {
    pub source: Source,
    pub enabled: bool,
    pub weight: u8,
    pub allowed_quotes: Option<Vec<&'static str>>,
}

pub static EXCHANGE_REGISTRY: Lazy<HashMap<Source, ExchangeConfig>> = Lazy::new(|| {
    let configs = vec![
        ExchangeConfig {
            source: Source::Binance,
            enabled: true,
            weight: 3,
            allowed_quotes: Some(vec!["FDUSD", "USDT", "USDC", "USD", "EUR"]),
        },
        ExchangeConfig {
            source: Source::Bybit,
            enabled: true,
            weight: 2,
            allowed_quotes: Some(vec!["USDT", "USDC", "USD"]),
        },
        ExchangeConfig {
            source: Source::Okx,
            enabled: true,
            weight: 2,
            allowed_quotes: Some(vec!["USDT", "USDC", "USD"]),
        },
        ExchangeConfig {
            source: Source::Coinbase,
            enabled: false, // Disabled
            weight: 3,
            allowed_quotes: Some(vec!["USDC", "USDT", "USD"]),
        },
        ExchangeConfig {
            source: Source::Bitget,
            enabled: true,
            weight: 2,
            allowed_quotes: Some(vec!["USDT", "USDC", "USD", "EUR"]),
        },
        ExchangeConfig {
            source: Source::Pyth,
            enabled: true,
            weight: 1,
            allowed_quotes: Some(vec!["USD", "USDT", "USDC"]), // Quote currencies
        },
        ExchangeConfig {
            source: Source::Titan,
            enabled: false, // Disabled - module kept for future use
            weight: 1,
            allowed_quotes: Some(vec!["USDC", "USDT"]), // Titan is a DEX aggregator for Solana
        },
    ];

    configs.into_iter().map(|c| (c.source, c)).collect()
});

/// Get allowed quote currencies for an exchange
pub fn get_allowed_quotes(source: Source) -> Vec<&'static str> {
    EXCHANGE_REGISTRY
        .get(&source)
        .and_then(|config| config.allowed_quotes.clone())
        .unwrap_or_default()
}

/// Get list of active (enabled) exchanges
pub fn get_active_exchanges() -> Vec<Source> {
    EXCHANGE_REGISTRY
        .iter()
        .filter(|(_, config)| config.enabled)
        .map(|(source, _)| *source)
        .collect()
}

/// Check if an exchange is enabled
pub fn is_exchange_enabled(source: Source) -> bool {
    EXCHANGE_REGISTRY
        .get(&source)
        .map(|config| config.enabled)
        .unwrap_or(false)
}

/// Get cached pairs for an exchange if it's enabled
pub fn get_cached_pairs_if_enabled(source: Source) -> Result<Vec<crate::Pair>, anyhow::Error> {
    if !is_exchange_enabled(source) {
        return Ok(Vec::new());
    }

    match source {
        Source::Binance => crate::exchanges::binance::get_cached_pairs(),
        Source::Bybit => crate::exchanges::bybit::get_cached_pairs(),
        Source::Okx => crate::exchanges::okx::get_cached_pairs(),
        Source::Coinbase => crate::exchanges::coinbase::get_cached_pairs(),
        Source::Bitget => crate::exchanges::bitget::get_cached_pairs(),
        Source::Pyth => crate::exchanges::pyth::get_cached_pairs(),
        Source::Titan => Ok(Vec::new()), // Disabled
        Source::Weighted => Ok(Vec::new()),
        Source::Auto => Ok(Vec::new()),
    }
}
