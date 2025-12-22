use anyhow::Context;
use anyhow::Result;
use serde::{Deserialize, Serialize};

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    Default,
    Serialize,
    Deserialize,
    std::cmp::Ord,
    std::cmp::PartialOrd,
)]
pub struct Pair {
    pub base: String,
    pub quote: String,
}

impl std::fmt::Display for Pair {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.base, self.quote)
    }
}

static ALLOWED_SUFFIXES: &[&str] = &[
    // Stablecoins (longest first to match correctly)
    "FDUSD", "USDT", "USDC", // "BUSD", "TUSD", "DAI", "UST", "USDP", "PAX",
    // Fiat currencies
    "USD", // "EUR", "GBP", "AUD", "CAD", "CHF", "JPY", "NZD",
    // Emerging market currencies
    // "TRY", "BRL", "RUB", "INR", "MXN", "ARS", "COP", "PEN", "CLP",
    // // European currencies
    // "PLN", "CZK", "HUF", "RON", "BGN", "HRK", "SEK", "NOK", "DKK",
    // // African & Asian currencies
    // "ZAR", "NGN", "UAH", "KZT", "THB", "VND", "IDR", "MYR", "PHP",
    // // Crypto pairs
    // "BTC", "ETH", "BNB",
];

impl Pair {
    pub fn from_task(symbol: &str) -> Result<Self> {
        let symbol = symbol.to_uppercase();
        let mut parts = symbol.splitn(2, '/');
        let base = parts.next().context("Failed to parse base currency")?;
        let quote = parts.next().context("Failed to parse quote currency")?;
        Ok(Pair {
            base: base.to_string(),
            quote: quote.to_string(),
        })
    }


    pub fn from_okx(symbol: &str) -> Result<Self> {
        let symbol = symbol.to_uppercase();
        let mut parts = symbol.splitn(2, '-');
        let base = parts.next().context("Failed to parse base currency")?;
        let quote = parts.next().context("Failed to parse quote currency")?;
        Ok(Pair {
            base: base.to_string(),
            quote: quote.to_string(),
        })
    }

    pub fn as_okx_str(&self) -> String {
        format!("{}-{}", self.base, self.quote)
    }

    pub fn from_coinbase(symbol: &str) -> Result<Self> {
        Self::from_okx(symbol)
    }

    pub fn as_coinbase_str(&self) -> String {
        self.as_okx_str()
    }

    pub fn from_gate(symbol: &str) -> Result<Self> {
        let symbol = symbol.to_uppercase();
        let mut parts = symbol.splitn(2, '_');
        let base = parts.next().context("Failed to parse base currency")?;
        let quote = parts.next().context("Failed to parse quote currency")?;
        Ok(Pair {
            base: base.to_string(),
            quote: quote.to_string(),
        })
    }

    pub fn as_gate_str(&self) -> String {
        format!("{}_{}", self.base, self.quote)
    }

    pub fn from_bybit(symbol: &str) -> Result<Self> {
        Self::split_suffix(symbol)
    }

    pub fn as_bybit_str(&self) -> String {
        format!("{}{}", self.base, self.quote)
    }

    pub fn from_binance(symbol: &str) -> Result<Self> {
        Self::split_suffix(symbol)
    }

    pub fn as_binance_str(&self) -> String {
        format!("{}{}", self.base, self.quote)
    }

    pub fn from_bitget(symbol: &str) -> Result<Self> {
        Self::split_suffix(symbol)
    }

    pub fn as_bitget_str(&self) -> String {
        format!("{}{}", self.base, self.quote)
    }

    pub fn from_pyth(symbol: &str) -> Result<Self> {
        Self::from_task(symbol)
    }

    pub fn as_pyth_str(&self) -> String {
        format!("{}/{}", self.base, self.quote)
    }

    pub fn from_titan(symbol: &str) -> Result<Self> {
        Self::from_task(symbol)
    }

    pub fn as_titan_str(&self) -> String {
        format!("{}/{}", self.base, self.quote)
    }

    fn split_suffix(symbol: &str) -> Result<Self> {
        let symbol = symbol.to_uppercase();

        // Sort suffixes by length (longest first) to match the most specific suffix
        let mut sorted_suffixes = ALLOWED_SUFFIXES.to_vec();
        sorted_suffixes.sort_by(|a, b| b.len().cmp(&a.len()));

        for &suffix in &sorted_suffixes {
            if symbol.ends_with(suffix) && symbol.len() > suffix.len() {
                let base = &symbol[..symbol.len() - suffix.len()];

                // Validate that base is not empty
                if !base.is_empty() {
                    return Ok(Pair {
                        base: base.to_string(),
                        quote: suffix.to_string(),
                    });
                }
            }
        }
        Err(anyhow::anyhow!("Invalid symbol format: {}", symbol))
    }

    pub fn as_str(&self) -> String {
        format!("{}/{}", self.base, self.quote)
    }

    pub fn as_bytes(&self) -> Vec<u8> {
        format!("{}/{}", self.base, self.quote).into_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_suffix_stablecoin_pairs() {
        // Test FDUSDUSDT
        let pair = Pair::from_binance("FDUSDUSDT").unwrap();
        assert_eq!(pair.base, "FDUSD");
        assert_eq!(pair.quote, "USDT");

        // Test USDTUSDC
        let pair = Pair::from_binance("USDTUSDC").unwrap();
        assert_eq!(pair.base, "USDT");
        assert_eq!(pair.quote, "USDC");

        // Test USDCBUSD
        let pair = Pair::from_binance("USDCBUSD").unwrap();
        assert_eq!(pair.base, "USDC");
        assert_eq!(pair.quote, "BUSD");

        // Test TUSD pairs
        let pair = Pair::from_binance("TUSDUSDT").unwrap();
        assert_eq!(pair.base, "TUSD");
        assert_eq!(pair.quote, "USDT");

        let pair = Pair::from_binance("BTCTUSD").unwrap();
        assert_eq!(pair.base, "BTC");
        assert_eq!(pair.quote, "TUSD");

        // Test regular pairs still work
        let pair = Pair::from_binance("BTCUSDT").unwrap();
        assert_eq!(pair.base, "BTC");
        assert_eq!(pair.quote, "USDT");

        let pair = Pair::from_binance("ETHFDUSD").unwrap();
        assert_eq!(pair.base, "ETH");
        assert_eq!(pair.quote, "FDUSD");
    }
}
