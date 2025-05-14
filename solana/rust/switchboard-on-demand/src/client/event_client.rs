use async_trait::async_trait;
use anchor_lang::Event;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_client::nonblocking::pubsub_client;
use solana_client::rpc_config::{RpcTransactionLogsConfig, RpcTransactionLogsFilter};
use solana_program::pubkey::Pubkey;
use crate::OnDemandError;
use base64::engine::general_purpose;
use base64::Engine;
use futures::StreamExt;
use tokio::sync::mpsc;
use tokio::time::Duration;
use tokio_util::sync::CancellationToken;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;

// Create a sender trait so we can handle both bounded and unbounded channels
// Then implement the trait for both mpsc::Sender and mpsc::UnboundedSender
// Originally I used an enum for each channel type but dynamic dispatch introduces performance concerns

#[async_trait]
pub trait EventSenderTrait<E: Event + Send + Sync + 'static>: Send + Sync {
    async fn send(&self, event: E) -> Result<(), OnDemandError>;
}

#[async_trait]
impl<E> EventSenderTrait<E> for mpsc::Sender<E>
where
    E: Event + Send + Sync + 'static,
{
    async fn send(&self, event: E) -> Result<(), OnDemandError> {
        self.send(event).await.map_err(|_e| OnDemandError::NetworkError)
    }
}

#[async_trait]
impl<E> EventSenderTrait<E> for mpsc::UnboundedSender<E>
where
    E: Event + Send + Sync + 'static,
{
    async fn send(&self, event: E) -> Result<(), OnDemandError> {
        self.send(event).map_err(|_e| OnDemandError::NetworkError)
    }
}

#[async_trait]
pub trait EventHandler: Send + Sync {
    async fn handle_event(&self, data: &[u8]) -> Result<(), OnDemandError>;
}

pub struct EventHandlerImpl<E, S>
where
    E: Event + Send + Sync + 'static,
    S: EventSenderTrait<E>,
{
    sender: Arc<S>,
    _marker: PhantomData<E>,
}

#[async_trait]
impl<E, S> EventHandler for EventHandlerImpl<E, S>
where
    E: Event + Send + Sync + 'static,
    S: EventSenderTrait<E>,
{
    async fn handle_event(&self, data: &[u8]) -> Result<(), OnDemandError> {
        match E::try_from_slice(data) {
            Ok(event) => self.sender.send(event).await,
            Err(_) => Err(OnDemandError::AnchorParseError),
        }
    }
}

impl<E, S> EventHandlerImpl<E, S>
where
    E: Event + Send + Sync + 'static,
    S: EventSenderTrait<E>,
{
    pub fn new(sender: S) -> Self {
        EventHandlerImpl {
            sender: Arc::new(sender),
            _marker: PhantomData,
        }
    }
}

/// A builder for creating a new PubSubEventClient
/// ```
/// use tokio::sync::mpsc;
/// use switchboard_solana::{ FunctionVerifyEvent, *SWITCHBOARD_ON_DEMAND_PROGRAM_ID };
///
///
/// let (fn_verify_sender, mut fn_verify_recv) = tokio::sync::mpsc::unbounded_channel::<FunctionVerifyEvent>();
///
/// let event_watcher = pubsub::PubSubEventClientBuilder::new(*SWITCHBOARD_ON_DEMAND_PROGRAM_ID, "https://api.mainnet-beta.solana.com".to_string())
///     .add_event_handler(fn_verify_sender);
///
/// let handler = tokio::spawn(async move {
///     loop {
///         tokio::select! {
///             Some(event) = fn_verify_recv.recv() => {
///                 info!("[Rpc][Event] FunctionVerifyEvent - Function: {:?}", event.function);
///             }
///         }
///     }
/// });
///
/// tokio::select! {
///     _ = handler => {
///         info!("[Rpc] handler exited");
///         return Ok(());
///     }
///     _ = event_watcher.start() => {
///         info!("[Rpc] event_watcher exited");
///         return Ok(());
///     }
/// }
/// ````
pub struct PubSubEventClientBuilder {
    program_id: Pubkey,
    websocket_url: String,

    /// Other pubkeys to watch for mentions in order to process logs
    other_pubkeys: Vec<Pubkey>,

    /// The maximum number of times to retry connecting to the websocket.
    /// None means the websocket will continiously retry.
    max_retries: Option<i32>,
}

pub struct PubSubEventClientWithHandlers {
    program_id: Pubkey,
    websocket_url: String,
    other_pubkeys: Vec<Pubkey>,

    // The maximum number of times to retry connecting to the websocket
    max_retries: Option<i32>,

    cancellation_token: CancellationToken,

    event_handlers: HashMap<[u8; 8], Box<dyn EventHandler>>,
}

impl PubSubEventClientBuilder {
    // Creates a new builder with the provided WebSocket URL
    pub fn new(program_id: Pubkey, websocket_url: String) -> Self {
        Self {
            program_id,
            websocket_url: websocket_url
                .replace("https://", "wss://")
                .replace("http://", "ws://"),
            other_pubkeys: Vec::new(),

            max_retries: None,
        }
    }

    pub fn mentions(mut self, pubkey: Pubkey) -> Self {
        self.other_pubkeys.push(pubkey);
        self
    }

    pub fn set_max_retries(mut self, max_retries: i32) -> Self {
        self.max_retries = Some(max_retries);
        self
    }

    pub fn add_event_handler<E: Event + Send + Sync + 'static, S: EventSenderTrait<E> + 'static>(
        self,
        sender: S,
    ) -> PubSubEventClientWithHandlers {
        PubSubEventClientWithHandlers {
            program_id: self.program_id,
            websocket_url: self.websocket_url,
            other_pubkeys: self.other_pubkeys,
            max_retries: self.max_retries,
            cancellation_token: CancellationToken::new(),
            event_handlers: HashMap::from([(
                E::DISCRIMINATOR,
                Box::new(EventHandlerImpl::<E, S>::new(sender)) as Box<dyn EventHandler + 'static>,
            )]),
        }
    }
}

impl PubSubEventClientWithHandlers {
    pub fn mentions(mut self, pubkey: Pubkey) -> Self {
        self.other_pubkeys.push(pubkey);
        self
    }

    pub fn set_max_retries(mut self, max_retries: i32) -> Self {
        self.max_retries = Some(max_retries);
        self
    }

    pub fn add_event_handler<E: Event + Send + Sync + 'static, S: EventSenderTrait<E> + 'static>(
        mut self,
        sender: S,
    ) -> PubSubEventClientWithHandlers {
        self.event_handlers.insert(
            E::DISCRIMINATOR,
            Box::new(EventHandlerImpl::<E, S>::new(sender)) as Box<dyn EventHandler + 'static>,
        );
        self
    }

    pub fn abort(self) {
        self.cancellation_token.cancel();
    }

    pub async fn start(self) {
        let cancellation_token = self.cancellation_token.clone();
        tokio::select! {
            _ = cancellation_token.cancelled() => {
                // Perform cleanup
                log::info!("pubsub token cancelled");
            },
            _ = self.start_pubsub() => {
                // Perform cleanup
                log::info!("start_pubsub returned unexpectedly");
            }
        }
    }

    // Starts the client
    async fn start_pubsub(&self) {
        let mut retry_count = 0;
        // let max_retries = 3;
        let mut delay = Duration::from_millis(500); // start with a 500ms delay

        loop {
            // Create the pubsub client every iteration in case the internal channel closed
            let pubsub_client = pubsub_client::PubsubClient::new(&self.websocket_url)
                .await
                .expect("Failed to create pubsub client");

            // Attempt to connect
            let connection_result = pubsub_client
                .logs_subscribe(
                    RpcTransactionLogsFilter::Mentions(
                        vec![
                            vec![self.program_id.to_string()],
                            self.other_pubkeys
                                .iter()
                                .map(|pubkey| pubkey.to_string())
                                .collect(),
                        ]
                        .concat(),
                    ),
                    RpcTransactionLogsConfig {
                        commitment: Some(CommitmentConfig::processed()),
                    },
                )
                .await;

            match connection_result {
                Ok((mut stream, _handler)) => {
                    retry_count = 0; // Reset retry count on successful connection
                    delay = Duration::from_millis(500); // Reset delay on successful connection

                    while let Some(event) = stream.next().await {
                        // TODO: A better implementation might immediately spawn a new task to handle the event so we dont block here. We could push to a FIFO queue and have some workers ready to handle the events

                        // Handle the rpc log event
                        for line in event.value.logs {
                            if let Some(encoded_data) = line.strip_prefix("Program data: ") {
                                if let Ok(decoded_data) =
                                    general_purpose::STANDARD.decode(encoded_data)
                                {
                                    if decoded_data.len() <= 8 {
                                        continue;
                                    }

                                    // Found a valid base64 string. Let's try to match a discriminator
                                    let (disc, event_data) = decoded_data.split_at(8);
                                    if let Some(sender) = self.event_handlers.get(disc) {
                                        let _ = sender.handle_event(event_data).await;
                                    }
                                }
                            }
                        }
                    }

                    log::error!("[EVENT][WEBSOCKET] connection closed, attempting to reconnect...");
                }
                Err(e) => {
                    log::error!("[EVENT][WEBSOCKET] Failed to connect: {:?}", e);

                    match self.max_retries {
                        Some(max_retries) => {
                            if retry_count >= max_retries {
                                log::error!("[EVENT][WEBSOCKET] Maximum retry attempts reached, aborting...");
                                break;
                            }

                            tokio::time::sleep(delay).await; // wait before retrying
                            retry_count += 1;
                            delay = std::cmp::min(delay * 2, Duration::from_secs(5));
                            // Double the delay for next retry, up to 5 seconds
                        }
                        None => {
                            tokio::time::sleep(delay).await; // wait before retrying
                            retry_count += 1;
                            delay = std::cmp::min(delay * 2, Duration::from_secs(5));
                            // Double the delay for next retry, up to 5 seconds
                            continue;
                        }
                    }
                }
            }

            if let Some(max_retries) = self.max_retries {
                if retry_count >= max_retries {
                    log::error!("[EVENT][WEBSOCKET] Maximum retry attempts reached, aborting...");
                    break;
                }
            }
        }
    }
}
