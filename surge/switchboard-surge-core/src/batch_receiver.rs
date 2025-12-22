use anyhow::Result;
use tokio::sync::mpsc;
use tokio::time::{Duration, Instant};
use tracing::{debug, warn};

/// High-performance batch receiver with smart memory management
pub struct BatchReceiver<T> {
    rx: mpsc::UnboundedReceiver<T>,
    buffer: Vec<T>,
    max_batch_size: usize,
    min_batch_time: Duration,
    max_batch_time: Duration,
}

impl<T> BatchReceiver<T> {
    /// Create a new batch receiver
    pub fn new(
        rx: mpsc::UnboundedReceiver<T>,
        max_batch_size: usize,
        min_batch_time_ms: u64,
        max_batch_time_ms: u64,
    ) -> Self {
        Self {
            rx,
            buffer: Vec::with_capacity(max_batch_size.min(1000)), // Reasonable pre-allocation
            max_batch_size,
            min_batch_time: Duration::from_millis(min_batch_time_ms),
            max_batch_time: Duration::from_millis(max_batch_time_ms),
        }
    }

    /// Receive a batch of items with smart timing
    /// - Waits at least min_batch_time to collect more items
    /// - Returns when max_batch_size is reached OR max_batch_time expires
    pub async fn recv_batch(&mut self) -> Result<Vec<T>> {
        self.buffer.clear();
        
        // Wait for first item
        let first_item = match self.rx.recv().await {
            Some(item) => item,
            None => return Err(anyhow::anyhow!("Channel closed")),
        };
        
        self.buffer.push(first_item);
        let batch_start = Instant::now();
        
        // Collect more items with smart timing
        loop {
            let elapsed = batch_start.elapsed();
            
            // Stop if we've reached max batch size
            if self.buffer.len() >= self.max_batch_size {
                debug!("Batch full ({} items), returning", self.buffer.len());
                break;
            }
            
            // Stop if we've exceeded max batch time
            if elapsed >= self.max_batch_time {
                debug!("Max batch time reached ({:?}) with {} items", elapsed, self.buffer.len());
                break;
            }
            
            // Calculate remaining time
            let remaining_time = self.max_batch_time - elapsed;
            let min_remaining = if elapsed < self.min_batch_time {
                self.min_batch_time - elapsed
            } else {
                Duration::from_millis(1) // Minimal wait
            };
            
            // Try to receive with timeout
            match tokio::time::timeout(min_remaining.min(remaining_time), self.rx.recv()).await {
                Ok(Some(item)) => {
                    self.buffer.push(item);
                    // Continue collecting if we have time and space
                    continue;
                }
                Ok(None) => {
                    // Channel closed
                    return Err(anyhow::anyhow!("Channel closed"));
                }
                Err(_) => {
                    // Timeout - check if we should return
                    if elapsed >= self.min_batch_time || self.buffer.len() >= 10 {
                        debug!("Timeout reached with {} items after {:?}", self.buffer.len(), elapsed);
                        break;
                    }
                    // Continue waiting if we haven't reached min time
                }
            }
        }
        
        // Move items out of buffer
        Ok(std::mem::take(&mut self.buffer))
    }
    
    /// Receive all currently available items without waiting
    pub fn try_recv_all(&mut self) -> Option<Vec<T>> {
        self.buffer.clear();
        
        // Get first item if available
        match self.rx.try_recv() {
            Ok(item) => self.buffer.push(item),
            Err(_) => return None,
        }
        
        // Drain all available items
        while let Ok(item) = self.rx.try_recv() {
            self.buffer.push(item);
            
            // Safety check to prevent unbounded growth
            if self.buffer.len() >= self.max_batch_size * 2 {
                warn!("Emergency batch size limit reached: {}", self.buffer.len());
                break;
            }
        }
        
        Some(std::mem::take(&mut self.buffer))
    }
    
    /// Get the number of queued items (approximate)
    pub fn queued_count(&self) -> usize {
        // This is an approximation as there's no direct way to get queue size
        // We can estimate by trying to receive without blocking
        0 // Placeholder - actual implementation would need custom channel
    }
}

/// Memory-bounded batch receiver that drops old items if memory gets too high
pub struct BoundedBatchReceiver<T> {
    rx: mpsc::UnboundedReceiver<T>,
    buffer: Vec<T>,
    max_batch_size: usize,
    max_memory_items: usize,
    dropped_count: u64,
}

impl<T> BoundedBatchReceiver<T> {
    pub fn new(
        rx: mpsc::UnboundedReceiver<T>,
        max_batch_size: usize,
        max_memory_items: usize,
    ) -> Self {
        Self {
            rx,
            buffer: Vec::with_capacity(max_batch_size.min(1000)),
            max_batch_size,
            max_memory_items,
            dropped_count: 0,
        }
    }
    
    /// Receive a batch with memory bounds
    pub async fn recv_bounded(&mut self) -> Result<Vec<T>> {
        self.buffer.clear();
        
        // Wait for first item
        let first_item = match self.rx.recv().await {
            Some(item) => item,
            None => return Err(anyhow::anyhow!("Channel closed")),
        };
        
        self.buffer.push(first_item);
        
        // Collect up to max_batch_size or until queue is manageable
        let mut queue_depth = 0;
        while self.buffer.len() < self.max_batch_size {
            match self.rx.try_recv() {
                Ok(item) => {
                    self.buffer.push(item);
                    queue_depth += 1;
                }
                Err(_) => break, // No more items available
            }
        }
        
        // If queue is too deep, drop excess items
        if queue_depth > self.max_memory_items {
            let mut dropped = 0;
            while let Ok(_) = self.rx.try_recv() {
                dropped += 1;
                if dropped > queue_depth / 2 {
                    break; // Don't drop everything
                }
            }
            
            if dropped > 0 {
                self.dropped_count += dropped as u64;
                warn!("Dropped {} items due to memory pressure (total dropped: {})", 
                      dropped, self.dropped_count);
            }
        }
        
        Ok(std::mem::take(&mut self.buffer))
    }
    
    /// Get the number of dropped items
    pub fn dropped_count(&self) -> u64 {
        self.dropped_count
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::sleep;

    #[tokio::test]
    async fn test_batch_receiver_timing() {
        let (tx, rx) = mpsc::unbounded_channel();
        let mut batch_rx = BatchReceiver::new(rx, 100, 10, 50);
        
        // Send one item
        tx.send(1).unwrap();
        
        let start = Instant::now();
        let batch = batch_rx.recv_batch().await.unwrap();
        let elapsed = start.elapsed();
        
        assert_eq!(batch.len(), 1);
        assert!(elapsed >= Duration::from_millis(10)); // Should wait at least min time
    }
    
    #[tokio::test]
    async fn test_batch_receiver_size_limit() {
        let (tx, rx) = mpsc::unbounded_channel();
        let mut batch_rx = BatchReceiver::new(rx, 5, 1, 100);
        
        // Send many items
        for i in 0..10 {
            tx.send(i).unwrap();
        }
        
        let batch = batch_rx.recv_batch().await.unwrap();
        assert_eq!(batch.len(), 5); // Should be limited by max_batch_size
    }
    
    #[tokio::test]
    async fn test_bounded_receiver_drops() {
        let (tx, rx) = mpsc::unbounded_channel();
        let mut bounded_rx = BoundedBatchReceiver::new(rx, 10, 50);
        
        // Send way too many items
        for i in 0..200 {
            tx.send(i).unwrap();
        }
        
        let _batch = bounded_rx.recv_bounded().await.unwrap();
        assert!(bounded_rx.dropped_count() > 0);
    }
}