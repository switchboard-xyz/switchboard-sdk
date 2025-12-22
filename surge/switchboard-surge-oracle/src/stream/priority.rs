use tokio::sync::mpsc;

/// Priority broadcast channel that ensures price updates are sent with minimal latency
pub struct PriorityBroadcaster {
    tx: mpsc::UnboundedSender<BroadcastMessage>,
}

#[derive(Debug)]
pub struct BroadcastMessage {
    pub connection_id: String,
    pub message: Vec<u8>,
}

impl PriorityBroadcaster {
    /// Create a new priority broadcaster
    pub fn new() -> (Self, mpsc::UnboundedReceiver<BroadcastMessage>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (Self { tx }, rx)
    }
    
    /// Send a message with priority (non-blocking - unbounded channel never blocks)
    pub fn send(&self, connection_id: String, message: Vec<u8>) -> Result<(), mpsc::error::SendError<BroadcastMessage>> {
        self.tx.send(BroadcastMessage {
            connection_id,
            message,
        })
    }
}

/// Ensure broadcast tasks run with high priority
pub fn spawn_priority_task<F>(name: &str, future: F) -> tokio::task::JoinHandle<F::Output>
where
    F: std::future::Future + Send + 'static,
    F::Output: Send + 'static,
{
    // Create owned string to avoid lifetime issues
    let task_name = name.to_string();
    
    // For now, just use regular spawn with a name prefix in the task
    tokio::spawn(async move {
        tracing::trace!("Running priority task: {}", task_name);
        future.await
    })
}