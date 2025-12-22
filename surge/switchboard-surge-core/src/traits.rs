use crate::types::Ticker;
use crate::pair::Pair;
use anyhow::Result;
use std::pin::Pin;
use std::task::{Context, Poll};
use futures::Stream;

/// Stream of tickers from an exchange
pub trait TickerStream: Send + Sync {
    /// Start listening to a pair
    fn listen(&mut self, pair: Pair) -> Result<()>;
    
    /// Stop listening to a pair
    fn unlisten(&mut self, pair: &Pair) -> Result<()>;
    
    /// Get list of currently subscribed pairs
    fn subscriptions(&self) -> Vec<Pair>;
    
    /// Check if connected
    fn is_connected(&self) -> bool;
}

/// Extension trait for polling ticker streams
pub trait TickerStreamExt: TickerStream {
    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<Ticker>>>;
}

/// Blanket implementation for Stream types
impl<T> TickerStreamExt for T
where
    T: TickerStream + Stream<Item = Result<Ticker>> + Unpin,
{
    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<Ticker>>> {
        Pin::new(self).poll_next(cx)
    }
}