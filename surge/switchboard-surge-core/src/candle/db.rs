//! TimescaleDB persistence layer for TWAP candles.
//!
//! Provides connection pooling and CRUD operations for candle storage.

use anyhow::{Context, Result};
use chrono::{DateTime, TimeZone, Utc};
use once_cell::sync::OnceCell;
use rust_decimal::Decimal;
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::FromRow;

use super::types::{RawCandle, SignedCandle};

/// Global candle database pool for task-runner access.
static CANDLE_POOL: OnceCell<PgPool> = OnceCell::new();

/// Create a connection pool to the candle database.
///
/// Uses a small pool (max 5 connections) since candle writes are batched
/// and infrequent (every 60 seconds for the window closer).
pub async fn create_candle_pool(database_url: &str) -> Result<PgPool> {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .acquire_timeout(std::time::Duration::from_secs(5))
        .connect(database_url)
        .await
        .context("Failed to connect to candle database")?;

    tracing::info!("Connected to candle database");
    Ok(pool)
}

/// Build a database URL from standard PG environment variables.
///
/// Uses: PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD
fn build_database_url() -> Result<String> {
    let host = std::env::var("PGHOST").context("PGHOST not set")?;
    let port = std::env::var("PGPORT").unwrap_or_else(|_| "5432".to_string());
    let database = std::env::var("PGDATABASE").context("PGDATABASE not set")?;
    let user = std::env::var("PGUSER").context("PGUSER not set")?;
    let password = std::env::var("PGPASSWORD").context("PGPASSWORD not set")?;

    Ok(format!(
        "postgres://{}:{}@{}:{}/{}",
        user, password, host, port, database
    ))
}

/// Get or initialize the global candle database pool.
///
/// Reads database config from environment on first call:
/// PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD
///
/// Subsequent calls return the cached pool.
pub async fn get_global_candle_pool() -> Result<&'static PgPool> {
    if let Some(pool) = CANDLE_POOL.get() {
        return Ok(pool);
    }

    let url = build_database_url()?;

    let pool = create_candle_pool(&url).await?;
    CANDLE_POOL
        .set(pool)
        .map_err(|_| anyhow::anyhow!("Candle pool already initialized"))?;

    Ok(CANDLE_POOL.get().unwrap())
}

/// Database row representation of a signed candle.
#[derive(Debug, FromRow)]
struct CandleRow {
    time: DateTime<Utc>,
    exchange: String,
    pair: String,
    window_start_ms: i64,
    window_end_ms: i64,
    time_weighted_sum: Decimal,
    observed_duration_ms: i64,
    sampled_median: Decimal,
    tick_count: i32,
    open_price: Decimal,
    close_price: Decimal,
    high_price: Decimal,
    low_price: Decimal,
    session_id: String,
    coverage_pct: f32,
    oracle_pubkey: String,
    signature: String,
    signed_at_ms: i64,
}

impl From<CandleRow> for SignedCandle {
    fn from(row: CandleRow) -> Self {
        SignedCandle {
            candle: RawCandle {
                exchange: row.exchange,
                pair: row.pair,
                window_start_ms: row.window_start_ms as u64,
                window_end_ms: row.window_end_ms as u64,
                time_weighted_sum: row.time_weighted_sum,
                observed_duration_ms: row.observed_duration_ms as u64,
                sampled_median: row.sampled_median,
                tick_count: row.tick_count as u32,
                open_price: row.open_price,
                close_price: row.close_price,
                high_price: row.high_price,
                low_price: row.low_price,
                session_id: row.session_id,
                coverage_pct: row.coverage_pct,
            },
            oracle_pubkey: row.oracle_pubkey,
            signature: row.signature,
            signed_at_ms: row.signed_at_ms as u64,
        }
    }
}

/// Persist a signed candle to the database.
///
/// Uses upsert (ON CONFLICT DO UPDATE) to handle duplicate candles
/// from retries or window closer overlap.
pub async fn persist_candle(pool: &PgPool, candle: &SignedCandle) -> Result<()> {
    // Convert window_start_ms to DateTime for TimescaleDB hypertable
    let time = Utc
        .timestamp_millis_opt(candle.candle.window_start_ms as i64)
        .single()
        .context("Invalid window_start_ms timestamp")?;

    sqlx::query(
        r#"
        INSERT INTO candles (
            time, exchange, pair, window_start_ms, window_end_ms,
            time_weighted_sum, observed_duration_ms, sampled_median,
            tick_count, open_price, close_price, high_price, low_price,
            session_id, coverage_pct, oracle_pubkey, signature, signed_at_ms
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
        ON CONFLICT (exchange, pair, time, oracle_pubkey) DO UPDATE SET
            time_weighted_sum = EXCLUDED.time_weighted_sum,
            observed_duration_ms = EXCLUDED.observed_duration_ms,
            sampled_median = EXCLUDED.sampled_median,
            tick_count = EXCLUDED.tick_count,
            open_price = EXCLUDED.open_price,
            close_price = EXCLUDED.close_price,
            high_price = EXCLUDED.high_price,
            low_price = EXCLUDED.low_price,
            coverage_pct = EXCLUDED.coverage_pct,
            signature = EXCLUDED.signature,
            signed_at_ms = EXCLUDED.signed_at_ms
        "#,
    )
    .bind(time)
    .bind(&candle.candle.exchange)
    .bind(&candle.candle.pair)
    .bind(candle.candle.window_start_ms as i64)
    .bind(candle.candle.window_end_ms as i64)
    .bind(&candle.candle.time_weighted_sum)
    .bind(candle.candle.observed_duration_ms as i64)
    .bind(&candle.candle.sampled_median)
    .bind(candle.candle.tick_count as i32)
    .bind(&candle.candle.open_price)
    .bind(&candle.candle.close_price)
    .bind(&candle.candle.high_price)
    .bind(&candle.candle.low_price)
    .bind(&candle.candle.session_id)
    .bind(candle.candle.coverage_pct)
    .bind(&candle.oracle_pubkey)
    .bind(&candle.signature)
    .bind(candle.signed_at_ms as i64)
    .execute(pool)
    .await
    .context("Failed to persist candle")?;

    Ok(())
}

/// Persist multiple candles in a single transaction.
///
/// More efficient than persisting one by one when closing multiple windows.
pub async fn persist_candles(pool: &PgPool, candles: &[SignedCandle]) -> Result<()> {
    if candles.is_empty() {
        return Ok(());
    }

    let mut tx = pool.begin().await.context("Failed to begin transaction")?;

    for candle in candles {
        let time = Utc
            .timestamp_millis_opt(candle.candle.window_start_ms as i64)
            .single()
            .context("Invalid window_start_ms timestamp")?;

        sqlx::query(
            r#"
            INSERT INTO candles (
                time, exchange, pair, window_start_ms, window_end_ms,
                time_weighted_sum, observed_duration_ms, sampled_median,
                tick_count, open_price, close_price, high_price, low_price,
                session_id, coverage_pct, oracle_pubkey, signature, signed_at_ms
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
            ON CONFLICT (exchange, pair, time, oracle_pubkey) DO UPDATE SET
                time_weighted_sum = EXCLUDED.time_weighted_sum,
                observed_duration_ms = EXCLUDED.observed_duration_ms,
                sampled_median = EXCLUDED.sampled_median,
                tick_count = EXCLUDED.tick_count,
                open_price = EXCLUDED.open_price,
                close_price = EXCLUDED.close_price,
                high_price = EXCLUDED.high_price,
                low_price = EXCLUDED.low_price,
                coverage_pct = EXCLUDED.coverage_pct,
                signature = EXCLUDED.signature,
                signed_at_ms = EXCLUDED.signed_at_ms
            "#,
        )
        .bind(time)
        .bind(&candle.candle.exchange)
        .bind(&candle.candle.pair)
        .bind(candle.candle.window_start_ms as i64)
        .bind(candle.candle.window_end_ms as i64)
        .bind(&candle.candle.time_weighted_sum)
        .bind(candle.candle.observed_duration_ms as i64)
        .bind(&candle.candle.sampled_median)
        .bind(candle.candle.tick_count as i32)
        .bind(&candle.candle.open_price)
        .bind(&candle.candle.close_price)
        .bind(&candle.candle.high_price)
        .bind(&candle.candle.low_price)
        .bind(&candle.candle.session_id)
        .bind(candle.candle.coverage_pct)
        .bind(&candle.oracle_pubkey)
        .bind(&candle.signature)
        .bind(candle.signed_at_ms as i64)
        .execute(&mut *tx)
        .await
        .context("Failed to persist candle in transaction")?;
    }

    tx.commit().await.context("Failed to commit transaction")?;

    tracing::debug!("Persisted {} candles", candles.len());
    Ok(())
}

/// Fetch candles for an (exchange, pair) since a given timestamp.
///
/// Returns candles ordered by time ascending (oldest first) for
/// deterministic TWAP accumulation.
///
/// **Inclusion Rule**: A candle is included if its window overlaps the lookback period,
/// i.e., `window_end_ms >= since_ms`. This ensures we capture all relevant price data.
pub async fn fetch_candles(
    pool: &PgPool,
    exchange: &str,
    pair: &str,
    since_ms: u64,
    limit: i64,
) -> Result<Vec<SignedCandle>> {
    // Query by window_end_ms to include candles whose windows overlap with the lookback period.
    // A candle with window 15:00-15:05 should be included when querying from 15:01,
    // because its window_end (15:05) >= since_ms (15:01).
    let rows: Vec<CandleRow> = sqlx::query_as(
        r#"
        SELECT
            time, exchange, pair, window_start_ms, window_end_ms,
            time_weighted_sum, observed_duration_ms, sampled_median,
            tick_count, open_price, close_price, high_price, low_price,
            session_id, coverage_pct, oracle_pubkey, signature, signed_at_ms
        FROM candles
        WHERE exchange = $1 AND pair = $2 AND window_end_ms >= $3
        ORDER BY time ASC
        LIMIT $4
        "#,
    )
    .bind(exchange)
    .bind(pair)
    .bind(since_ms as i64)
    .bind(limit)
    .fetch_all(pool)
    .await
    .context("Failed to fetch candles")?;

    Ok(rows.into_iter().map(SignedCandle::from).collect())
}

/// Fetch overlapping FINALIZED candles for an (exchange, pair) for TWAP calculation.
///
/// This function is specifically designed for TWAP queries where:
/// - `since_ms`: Start of the lookback period (may not be on a window boundary)
/// - `now_ms_floor_5m`: End boundary for finalized candles (floored to 5-min boundary)
///
/// **Critical Invariant**: DB fetch MUST NOT include the current open window.
/// The accumulator is the sole source for open-window data.
///
/// **Inclusion Rule**: A candle is included if:
/// - `window_end_ms > since_ms` (overlaps the start of lookback)
/// - `window_start_ms < now_ms_floor_5m` (overlaps the end boundary)
///
/// This ensures we only get finalized (closed) candles, never the current open window.
/// Returns candles ordered by window_start_ms ascending (oldest first).
pub async fn fetch_overlapping_finalized_candles(
    pool: &PgPool,
    exchange: &str,
    pair: &str,
    since_ms: u64,
    now_ms_floor_5m: u64,
) -> Result<Vec<SignedCandle>> {
    let rows: Vec<CandleRow> = sqlx::query_as(
        r#"
        SELECT
            time, exchange, pair, window_start_ms, window_end_ms,
            time_weighted_sum, observed_duration_ms, sampled_median,
            tick_count, open_price, close_price, high_price, low_price,
            session_id, coverage_pct, oracle_pubkey, signature, signed_at_ms
        FROM candles
        WHERE exchange = $1
          AND pair = $2
          AND window_end_ms > $3
          AND window_start_ms < $4
        ORDER BY window_start_ms ASC
        "#,
    )
    .bind(exchange)
    .bind(pair)
    .bind(since_ms as i64)
    .bind(now_ms_floor_5m as i64)
    .fetch_all(pool)
    .await
    .context("Failed to fetch overlapping finalized candles")?;

    Ok(rows.into_iter().map(SignedCandle::from).collect())
}

/// Fetch candles for a pair across ALL exchanges since a given timestamp.
///
/// Useful for TWAP aggregation where we want data from multiple exchanges.
/// Returns candles ordered by time descending (most recent first).
///
/// **Inclusion Rule**: A candle is included if its window overlaps the lookback period,
/// i.e., `window_end_ms >= since_ms`. This ensures we capture all relevant price data.
pub async fn fetch_candles_all_exchanges(
    pool: &PgPool,
    pair: &str,
    since_ms: u64,
    limit: i64,
) -> Result<Vec<SignedCandle>> {
    let rows: Vec<CandleRow> = sqlx::query_as(
        r#"
        SELECT
            time, exchange, pair, window_start_ms, window_end_ms,
            time_weighted_sum, observed_duration_ms, sampled_median,
            tick_count, open_price, close_price, high_price, low_price,
            session_id, coverage_pct, oracle_pubkey, signature, signed_at_ms
        FROM candles
        WHERE pair = $1 AND window_end_ms >= $2
        ORDER BY time DESC
        LIMIT $3
        "#,
    )
    .bind(pair)
    .bind(since_ms as i64)
    .bind(limit)
    .fetch_all(pool)
    .await
    .context("Failed to fetch candles")?;

    Ok(rows.into_iter().map(SignedCandle::from).collect())
}

/// Fetch the latest candle for an (exchange, pair).
pub async fn fetch_latest_candle(
    pool: &PgPool,
    exchange: &str,
    pair: &str,
) -> Result<Option<SignedCandle>> {
    let row: Option<CandleRow> = sqlx::query_as(
        r#"
        SELECT
            time, exchange, pair, window_start_ms, window_end_ms,
            time_weighted_sum, observed_duration_ms, sampled_median,
            tick_count, open_price, close_price, high_price, low_price,
            session_id, coverage_pct, oracle_pubkey, signature, signed_at_ms
        FROM candles
        WHERE exchange = $1 AND pair = $2
        ORDER BY time DESC
        LIMIT 1
        "#,
    )
    .bind(exchange)
    .bind(pair)
    .fetch_optional(pool)
    .await
    .context("Failed to fetch latest candle")?;

    Ok(row.map(SignedCandle::from))
}

/// Count candles for an (exchange, pair) in a time range.
///
/// Useful for checking coverage before computing TWAP.
pub async fn count_candles(
    pool: &PgPool,
    exchange: &str,
    pair: &str,
    since_ms: u64,
    until_ms: u64,
) -> Result<i64> {
    let since = Utc
        .timestamp_millis_opt(since_ms as i64)
        .single()
        .context("Invalid since_ms timestamp")?;
    let until = Utc
        .timestamp_millis_opt(until_ms as i64)
        .single()
        .context("Invalid until_ms timestamp")?;

    let count: (i64,) = sqlx::query_as(
        r#"
        SELECT COUNT(*) as count
        FROM candles
        WHERE exchange = $1 AND pair = $2 AND time >= $3 AND time < $4
        "#,
    )
    .bind(exchange)
    .bind(pair)
    .bind(since)
    .bind(until)
    .fetch_one(pool)
    .await
    .context("Failed to count candles")?;

    Ok(count.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Integration tests would require a running TimescaleDB instance.
    // Unit tests focus on the data conversion logic.

    #[test]
    fn test_candle_row_conversion() {
        let row = CandleRow {
            time: Utc.timestamp_millis_opt(1700000000000).unwrap(),
            exchange: "Binance".to_string(),
            pair: "BTC/FDUSD".to_string(),
            window_start_ms: 1700000000000,
            window_end_ms: 1700000300000,
            time_weighted_sum: Decimal::new(1234567890, 6),
            observed_duration_ms: 290000,
            sampled_median: Decimal::new(50000, 0),
            tick_count: 500,
            open_price: Decimal::new(49900, 0),
            close_price: Decimal::new(50100, 0),
            high_price: Decimal::new(50200, 0),
            low_price: Decimal::new(49800, 0),
            session_id: "test-session".to_string(),
            coverage_pct: 96.67,
            oracle_pubkey: "oracle123".to_string(),
            signature: "sig123".to_string(),
            signed_at_ms: 1700000300001,
        };

        let candle: SignedCandle = row.into();

        assert_eq!(candle.candle.exchange, "Binance");
        assert_eq!(candle.candle.pair, "BTC/FDUSD");
        assert_eq!(candle.candle.window_start_ms, 1700000000000);
        assert_eq!(candle.candle.observed_duration_ms, 290000);
        assert_eq!(candle.oracle_pubkey, "oracle123");
    }
}
