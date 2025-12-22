use std::time::{SystemTime, UNIX_EPOCH, Duration};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use tokio::time::interval;

/// Maximum acceptable clock drift in milliseconds
const MAX_CLOCK_DRIFT_MS: i64 = 50;

/// How often to refresh the clock offset (15 seconds for better accuracy)
const CLOCK_SYNC_INTERVAL: Duration = Duration::from_secs(15);

/// Number of attempts to make when detecting clock drift
const CLOCK_SYNC_ATTEMPTS: usize = 5;

/// Maximum acceptable round-trip time for clock sync (reject outliers)
/// Increased to 500ms to handle occasional network spikes
const MAX_ACCEPTABLE_RTT_MS: i64 = 500;

/// Whether we've already warned about clock sync
static CLOCK_SYNC_WARNED: AtomicBool = AtomicBool::new(false);

/// Last detected clock offset in milliseconds
static LAST_CLOCK_OFFSET_MS: AtomicI64 = AtomicI64::new(0);

/// Whether the background sync task is running
static SYNC_TASK_RUNNING: AtomicBool = AtomicBool::new(false);

/// Checks system clock synchronization and caches the offset
pub async fn check_and_cache_clock_offset() {
    match detect_clock_drift().await {
        Ok(offset_ms) => {
            let previous_offset = LAST_CLOCK_OFFSET_MS.load(Ordering::Relaxed);
            LAST_CLOCK_OFFSET_MS.store(offset_ms, Ordering::Relaxed);

            // Log offset change if significant
            if previous_offset != 0 && (offset_ms - previous_offset).abs() > 10 {
                tracing::info!(
                    "🔄 Clock offset updated: {}ms -> {}ms (delta: {:+}ms)",
                    previous_offset, offset_ms, offset_ms - previous_offset
                );
            }

            if offset_ms.abs() > MAX_CLOCK_DRIFT_MS {
                if !CLOCK_SYNC_WARNED.swap(true, Ordering::Relaxed) {
                    tracing::warn!(
                        "⚠️  CLOCK DRIFT DETECTED: Your system clock is off by {}ms from Binance server time",
                        offset_ms
                    );
                    tracing::warn!(
                        "⚠️  Applying Binance time offset correction to all timestamps"
                    );

                    // Platform-specific advice
                    #[cfg(target_os = "macos")]
                    tracing::info!(
                        "💡 To sync your clock on macOS: sudo sntp -sS time.apple.com"
                    );

                    #[cfg(target_os = "linux")]
                    tracing::info!(
                        "💡 To sync your clock on Linux: sudo ntpdate -s time.google.com"
                    );
                }
            } else if previous_offset == 0 {
                // Only log on initial sync
                tracing::info!(
                    "✓ System clock synchronized with Binance (offset: {}ms)",
                    offset_ms
                );
            }
        }
        Err(e) => {
            tracing::warn!("Failed to check clock sync: {}", e);
        }
    }
}

/// Get current timestamp in milliseconds with cached offset applied
#[inline]
pub fn get_corrected_timestamp_ms() -> u64 {
    let raw_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("System time before Unix epoch")
        .as_millis() as u64;

    let offset = LAST_CLOCK_OFFSET_MS.load(Ordering::Relaxed);
    if offset >= 0 {
        raw_time.saturating_add(offset as u64)
    } else {
        raw_time.saturating_sub((-offset) as u64)
    }
}

/// Detects clock drift by comparing with Binance server time, accounting for network latency
/// Makes multiple attempts and uses statistical filtering to reject outliers
async fn detect_clock_drift() -> Result<i64, Box<dyn std::error::Error>> {
    let client = reqwest::Client::new();
    let url = "https://api.binance.com/api/v3/time";
    let timeout = std::time::Duration::from_secs(5);
    
    // Prewarm DNS
    let _ = client.get(url).timeout(timeout).send().await?;

    let mut measurements = Vec::with_capacity(CLOCK_SYNC_ATTEMPTS);
    
    for attempt in 1..=CLOCK_SYNC_ATTEMPTS {
        match detect_single_clock_drift(&client, url, timeout).await {
            Ok((offset, rtt)) => {
                tracing::debug!(
                    "Clock sync attempt {}/{}: offset={}ms, rtt={}ms",
                    attempt, CLOCK_SYNC_ATTEMPTS, offset, rtt
                );
                
                // Filter out measurements with excessive RTT (likely network spikes)
                if rtt <= MAX_ACCEPTABLE_RTT_MS {
                    measurements.push((offset, rtt));
                } else {
                    tracing::debug!(
                        "Rejecting measurement due to high RTT: {}ms > {}ms",
                        rtt, MAX_ACCEPTABLE_RTT_MS
                    );
                }
            }
            Err(e) => {
                tracing::debug!(
                    "Clock sync attempt {}/{} failed: {}",
                    attempt, CLOCK_SYNC_ATTEMPTS, e
                );
            }
        }
        
        // Small delay between attempts to avoid overwhelming the server
        if attempt < CLOCK_SYNC_ATTEMPTS {
            tokio::time::sleep(tokio::time::Duration::from_millis(150)).await;
        }
    }

    if measurements.is_empty() {
        return Err("All clock sync attempts failed or were rejected due to high RTT".into());
    }

    // Use median of measurements with lowest RTT for best accuracy
    let best_offset = calculate_robust_offset(&measurements);

    tracing::debug!(
        "Clock sync completed: {} valid measurements, selected offset={}ms",
        measurements.len(), best_offset
    );

    Ok(best_offset)
}

/// Calculate robust offset using statistical filtering
fn calculate_robust_offset(measurements: &[(i64, i64)]) -> i64 {
    if measurements.len() == 1 {
        return measurements[0].0;
    }

    // Sort by RTT to prefer faster measurements
    let mut sorted_measurements = measurements.to_vec();
    sorted_measurements.sort_by_key(|(_, rtt)| *rtt);

    // Take the best half of measurements (lowest RTT)
    let take_count = measurements.len().div_ceil(2);
    let best_measurements: Vec<i64> = sorted_measurements
        .iter()
        .take(take_count)
        .map(|(offset, _)| *offset)
        .collect();

    tracing::trace!(
        "Selected {} best measurements from {}: {:?}",
        best_measurements.len(), measurements.len(), best_measurements
    );

    // Return median of the best measurements for robustness
    let mut offsets = best_measurements;
    offsets.sort_unstable();
    let median_idx = offsets.len() / 2;
    
    if offsets.len() % 2 == 0 {
        // Even number: average of two middle values
        (offsets[median_idx - 1] + offsets[median_idx]) / 2
    } else {
        // Odd number: middle value
        offsets[median_idx]
    }
}

/// Performs a single clock drift detection attempt
/// Returns (offset_ms, rtt_ms) tuple
async fn detect_single_clock_drift(
    client: &reqwest::Client,
    url: &str,
    timeout: std::time::Duration,
) -> Result<(i64, i64), Box<dyn std::error::Error>> {
    // Record time before request
    let request_start = SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_millis() as i64;

    let response = client
        .get(url)
        .timeout(timeout)
        .send()
        .await?;

    // Record time after response
    let request_end = SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_millis() as i64;

    let round_trip_time = request_end - request_start;
    let estimated_request_time = request_start + (round_trip_time / 2);

    if response.status().is_success() {
        #[derive(serde::Deserialize)]
        struct BinanceTime {
            #[serde(rename = "serverTime")]
            server_time: u64,
        }

        let time_data: BinanceTime = response.json().await?;
        let server_time = time_data.server_time as i64;

        // Calculate offset accounting for network latency
        let offset = server_time - estimated_request_time;

        tracing::trace!(
            "Single sync: server={}, local_est={}, rtt={}ms, offset={}ms",
            server_time, estimated_request_time, round_trip_time, offset
        );

        return Ok((offset, round_trip_time));
    }

    Err("Could not get Binance server time".into())
}

/// Returns the last detected clock offset in milliseconds
pub fn get_clock_offset_ms() -> i64 {
    LAST_CLOCK_OFFSET_MS.load(Ordering::Relaxed)
}

/// Adjusts a timestamp by the detected clock offset
pub fn adjust_timestamp(timestamp_ms: u64) -> u64 {
    let offset = get_clock_offset_ms();
    if offset >= 0 {
        timestamp_ms.saturating_add(offset as u64)
    } else {
        timestamp_ms.saturating_sub((-offset) as u64)
    }
}

/// Start a background task that periodically updates the clock offset
/// Returns immediately but performs initial sync synchronously to avoid race conditions
pub fn start_clock_sync_task() {
    // Check if clock sync is disabled
    if std::env::var("DISABLE_CLOCK_SYNC").unwrap_or_default().eq_ignore_ascii_case("true") {
        tracing::info!("Clock sync disabled via DISABLE_CLOCK_SYNC environment variable");
        return;
    }

    // Only start one instance
    if SYNC_TASK_RUNNING.swap(true, Ordering::Relaxed) {
        return;
    }

    crate::runtime_separation::spawn_on_ingestion_named(
        "clock-sync-periodic",
        async {
            // Do initial sync FIRST and wait for it to complete
            // This prevents race conditions where timestamps are used before offset is set
            tracing::debug!("Performing initial clock sync...");
            match tokio::time::timeout(
                Duration::from_secs(10),
                check_and_cache_clock_offset()
            ).await {
                Ok(_) => {
                    tracing::debug!("Initial clock sync completed");
                }
                Err(_) => {
                    tracing::warn!("Initial clock sync timed out after 10s, continuing with offset=0");
                }
            }

            // Now start periodic refresh
            let mut interval = interval(CLOCK_SYNC_INTERVAL);

            loop {
                interval.tick().await;

                // Reset the warning flag so we can log changes on each refresh
                CLOCK_SYNC_WARNED.store(false, Ordering::Relaxed);

                // Perform sync directly (no need for spawn_blocking or new runtime)
                match tokio::time::timeout(
                    Duration::from_secs(10),
                    check_and_cache_clock_offset()
                ).await {
                    Ok(_) => {
                        let offset = LAST_CLOCK_OFFSET_MS.load(Ordering::Relaxed);
                        tracing::debug!("Clock sync refresh completed, offset: {}ms", offset);
                    }
                    Err(_) => {
                        tracing::warn!("Clock sync refresh timed out, keeping previous offset");
                    }
                }
            }
        }
    );

    tracing::info!("Started background Binance time synchronization task (updates every 15s)");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_clock_sync_check() {
        check_and_cache_clock_offset().await;
        // Should not panic and should set appropriate flags
    }

    #[test]
    fn test_corrected_timestamp() {
        // Set a known offset
        LAST_CLOCK_OFFSET_MS.store(1000, Ordering::Relaxed);

        let corrected = get_corrected_timestamp_ms();
        let raw = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        // Corrected should be roughly 1000ms ahead
        assert!((corrected as i64 - raw as i64) >= 900 && (corrected as i64 - raw as i64) <= 1100);
    }

    #[test]
    fn test_robust_offset_calculation() {
        // Test median calculation with various scenarios
        
        // Single measurement
        let measurements = vec![(100, 50)];
        assert_eq!(calculate_robust_offset(&measurements), 100);
        
        // Two measurements - should average
        let measurements = vec![(100, 50), (200, 60)];
        assert_eq!(calculate_robust_offset(&measurements), 150);
        
        // Three measurements - should take median of best (lowest RTT)
        let measurements = vec![(100, 50), (200, 60), (150, 40)];
        // Sorted by RTT: (150, 40), (100, 50), (200, 60)
        // Take best 2: [150, 100] -> median = 125
        assert_eq!(calculate_robust_offset(&measurements), 125);
        
        // Five measurements with outlier
        let measurements = vec![
            (10, 30),   // Good measurement
            (15, 35),   // Good measurement
            (12, 40),   // Good measurement
            (200, 150), // High RTT outlier
            (8, 25),    // Best measurement
        ];
        // Sorted by RTT: (8, 25), (10, 30), (15, 35), (12, 40), (200, 150)
        // Take best 3: [8, 10, 15] -> median = 10
        assert_eq!(calculate_robust_offset(&measurements), 10);
    }
}