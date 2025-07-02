use async_nats::{self, Client, ConnectError, SubscribeError};
use futures::StreamExt;
use thiserror::Error;
use tokio;

#[derive(Debug, Error)]
enum AppError {
    #[error("Nats connect error: {0}")]
    Nats(#[from] ConnectError),
    #[error("Nats subscribe error: {0}")]
    Subscribe(#[from] SubscribeError),
}

#[tokio::main]
async fn main() -> Result<(), AppError> {
    let client: Client = async_nats::connect("0.0.0.0:4222").await?;
    let mut subscriber = client.subscribe("foo").await?;
    while let Some(message) = subscriber.next().await {
        println!("Received message: {:?}", message);
    }
    Ok(())
}
