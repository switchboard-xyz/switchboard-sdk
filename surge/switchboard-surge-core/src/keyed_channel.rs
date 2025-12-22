use crate::{Pair, Ticker};
use dashmap::DashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::trace;

/// A channel that keeps only the latest value for each key (symbol)
/// Values are removed when consumed
pub struct KeyedSender<T: Clone + Send + Sync + 'static> {
    /// Pending updates keyed by symbol
    pending: Arc<DashMap<Pair, T>>,
    /// Notify channel for new updates
    notify_tx: mpsc::UnboundedSender<()>,
}

impl<T: Clone + Send + Sync + 'static> KeyedSender<T> {
    /// Insert or update a value for a key
    pub fn send(&self, key: Pair, value: T) -> Result<(), mpsc::error::SendError<()>> {
        self.pending.insert(key, value);
        self.notify_tx.send(())
    }
    
    /// Get number of pending items
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }
    
    /// Clear all pending items
    pub fn clear(&self) {
        self.pending.clear();
    }
}

impl<T: Clone + Send + Sync + 'static> Clone for KeyedSender<T> {
    fn clone(&self) -> Self {
        Self {
            pending: Arc::clone(&self.pending),
            notify_tx: self.notify_tx.clone(),
        }
    }
}

pub struct KeyedReceiver<T: Clone + Send + Sync + 'static> {
    /// Shared pending updates
    pending: Arc<DashMap<Pair, T>>,
    /// Notification receiver
    notify_rx: mpsc::UnboundedReceiver<()>,
}

impl<T: Clone + Send + Sync + 'static> KeyedReceiver<T> {
    /// Receive all currently pending items at once
    /// Clears the pending map
    pub async fn recv(&mut self) -> Option<Vec<(Pair, T)>> {
        loop {
            // First check if there are any pending items
            if !self.pending.is_empty() {
                let mut batch = Vec::with_capacity(self.pending.len());
                self.pending.retain(|key, value| {
                    batch.push((key.clone(), value.clone()));
                    false // Remove from map
                });
                
                if !batch.is_empty() {
                    trace!("Consumed {} items in batch", batch.len());
                    return Some(batch);
                }
            }
            
            // Wait for notification of new data
            self.notify_rx.recv().await?;
        }
    }
    
    /// Try to receive pending items without blocking
    pub fn try_recv(&mut self) -> Option<Vec<(Pair, T)>> {
        if !self.pending.is_empty() {
            let mut batch = Vec::with_capacity(self.pending.len());
            self.pending.retain(|key, value| {
                batch.push((key.clone(), value.clone()));
                false // Remove from map
            });
            
            if !batch.is_empty() {
                Some(batch)
            } else {
                None
            }
        } else {
            None
        }
    }
    
    /// Get number of pending items
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }
}

/// Create a new keyed channel
pub fn keyed_channel<T: Clone + Send + Sync + 'static>() -> (KeyedSender<T>, KeyedReceiver<T>) {
    let pending = Arc::new(DashMap::new());
    let (notify_tx, notify_rx) = mpsc::unbounded_channel();
    
    let sender = KeyedSender {
        pending: Arc::clone(&pending),
        notify_tx,
    };
    
    let receiver = KeyedReceiver {
        pending,
        notify_rx,
    };
    
    (sender, receiver)
}

/// Specialized keyed channel for tickers
pub type TickerKeyedSender = KeyedSender<Ticker>;
pub type TickerKeyedReceiver = KeyedReceiver<Ticker>;

/// Create a ticker keyed channel
pub fn ticker_keyed_channel() -> (TickerKeyedSender, TickerKeyedReceiver) {
    keyed_channel()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Source;
    use rust_decimal::Decimal;
    use tokio::time::{sleep, Duration};

    #[tokio::test]
    async fn test_keyed_channel_deduplication() {
        let (sender, mut receiver) = keyed_channel::<String>();
        let pair = Pair { base: "BTC".to_string(), quote: "USDT".to_string() };
        
        // Send multiple updates for same key
        sender.send(pair.clone(), "first".to_string()).unwrap();
        sender.send(pair.clone(), "second".to_string()).unwrap();
        sender.send(pair.clone(), "third".to_string()).unwrap();
        
        // Should only receive the latest value
        let batch = receiver.recv().await.unwrap();
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].1, "third");
    }
    
    #[tokio::test]
    async fn test_keyed_channel_multiple_keys() {
        let (sender, mut receiver) = keyed_channel::<u32>();
        
        let btc = Pair { base: "BTC".to_string(), quote: "USDT".to_string() };
        let eth = Pair { base: "ETH".to_string(), quote: "USDT".to_string() };
        
        sender.send(btc.clone(), 50000).unwrap();
        sender.send(eth.clone(), 3000).unwrap();
        sender.send(btc.clone(), 50001).unwrap(); // Update BTC
        
        let batch = receiver.recv().await.unwrap();
        assert_eq!(batch.len(), 2);
        
        // Find BTC and ETH in batch
        let btc_value = batch.iter().find(|(k, _)| k == &btc).unwrap().1;
        let eth_value = batch.iter().find(|(k, _)| k == &eth).unwrap().1;
        
        assert_eq!(btc_value, 50001); // Latest BTC value
        assert_eq!(eth_value, 3000);  // ETH value
    }
}