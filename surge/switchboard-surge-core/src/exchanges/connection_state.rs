use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

/// Shared connection state that can be accessed without blocking
#[derive(Debug, Clone)]
pub struct ConnectionState {
    connected: Arc<AtomicBool>,
    last_update: Arc<AtomicU64>,
}

impl ConnectionState {
    pub fn new() -> Self {
        Self {
            connected: Arc::new(AtomicBool::new(false)),
            last_update: Arc::new(AtomicU64::new(0)),
        }
    }
    
    /// Update connection state (call from async context)
    pub fn set_connected(&self, connected: bool) {
        self.connected.store(connected, Ordering::Relaxed);
        self.last_update.store(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            Ordering::Relaxed
        );
    }
    
    /// Check if connected (non-blocking)
    pub fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Relaxed)
    }
    
    /// Get last update timestamp
    pub fn last_update_ms(&self) -> u64 {
        self.last_update.load(Ordering::Relaxed)
    }
    
    /// Check if state is fresh (updated within timeout_ms)
    pub fn is_fresh(&self, timeout_ms: u64) -> bool {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        
        let last = self.last_update_ms();
        now.saturating_sub(last) < timeout_ms
    }
}

impl Default for ConnectionState {
    fn default() -> Self {
        Self::new()
    }
}