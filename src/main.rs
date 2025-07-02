use async_nats::error::{
    ConnectErrorKind,
    PublishErrorKind,
    SubscribeError, // This is a distinct error type
};
use async_nats::{
    connect,
    Client,
    Message,    // Used in tests for msg.payload.to_vec()
    Subscriber, // Used directly for subscribe()
};
use futures::StreamExt;
use std::{error::Error, time::Duration};
use tokio::time::sleep;
use tracing::{debug, error, info, Level};
use tracing_subscriber::{EnvFilter, FmtSubscriber};

// Define a custom error type for better error handling
#[derive(Debug, thiserror::Error)]
enum AppError {
    #[error("NATS connect error: {0}")]
    NatsConnect(#[from] async_nats::Error<ConnectErrorKind>),
    #[error("NATS subscribe error: {0}")]
    NatsSubscribe(#[from] SubscribeError), // Specific error type for subscribe
    #[error("NATS publish error: {0}")]
    NatsPublish(#[from] async_nats::Error<PublishErrorKind>),
    #[error("Task join error: {0}")]
    Join(#[from] tokio::task::JoinError),
    #[error("String conversion error: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),
    #[error("Channel send error: {0}")]
    SendError(String),
    #[error("Generic error: {0}")]
    GenericError(String), // For ad-hoc errors in tests or specific scenarios
}

#[tokio::main]
async fn main() -> Result<(), AppError> {
    // Initialize tracing for better logging
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO) // Default log level
        .with_env_filter(EnvFilter::from_default_env()) // Allow RUST_LOG to override
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    info!("Attempting to connect to NATS at localhost:4222");
    let client = connect("localhost:4222").await?;
    info!("Successfully connected to NATS!");

    // Create a channel to signal the subscriber when the publisher is done
    let (tx_shutdown, rx_shutdown) = tokio::sync::mpsc::channel::<()>(1);

    // Start subscriber task
    let sub_client = client.clone();
    let sub_task = tokio::spawn(subscriber_task(sub_client, rx_shutdown));

    // Start publisher task
    let pub_client = client.clone();
    let pub_task = tokio::spawn(publisher_task(pub_client, tx_shutdown));

    // Wait for both tasks to finish.
    // The publisher task will complete after 5 messages.
    // The subscriber task will then receive the shutdown signal and complete.
    let (sub_result, pub_result) = tokio::try_join!(sub_task, pub_task)?;

    // Propagate any errors from the spawned tasks
    sub_result?;
    pub_result?;

    info!("All tasks completed successfully.");
    Ok(())
}

/// Task that subscribes to a subject and processes messages.
/// It stops when it receives a shutdown signal.
async fn subscriber_task(
    client: Client,
    mut rx_shutdown: tokio::sync::mpsc::Receiver<()>,
) -> Result<(), AppError> {
    let mut sub = client.subscribe("demo.subject").await?;
    info!("📡 Subscriber listening on 'demo.subject'...");

    loop {
        tokio::select! {
            // Receive NATS messages
            Some(msg) = sub.next() => {
                let payload = String::from_utf8_lossy(&msg.payload);
                info!("📬 Received: {}", payload);
            }
            // Receive shutdown signal
            _ = rx_shutdown.recv() => {
                info!("👋 Subscriber received shutdown signal. Exiting.");
                break;
            }
            // Optional: Timeout to ensure it doesn't wait forever in controlled examples/tests
            _ = sleep(Duration::from_secs(10)) => {
                info!("Subscriber timed out waiting for messages or shutdown. Exiting.");
                break;
            }
        }
    }
    Ok(())
}

/// Task that publishes messages to a subject.
async fn publisher_task(
    client: Client,
    tx_shutdown: tokio::sync::mpsc::Sender<()>,
) -> Result<(), AppError> {
    info!("🚀 Publisher starting...");
    for i in 0..5 {
        let msg = format!("Hello NATS! Message #{i}");
        client.publish("demo.subject", msg.clone().into()).await?; // Graceful error handling
        info!("✅ Published: {}", msg);
        sleep(Duration::from_millis(500)).await;
    }
    info!("Publisher finished sending 5 messages.");

    // Send shutdown signal to the subscriber
    tx_shutdown
        .send(())
        .await
        .map_err(|e| AppError::SendError(e.to_string()))?;

    Ok(())
}

// --- Tests ---
#[cfg(test)]
mod tests {
    use super::*; // Import items from the outer scope
    use std::sync::atomic::{AtomicUsize, Ordering}; // Not strictly needed for these tests, but good to keep in mind for atomics
    use std::sync::Arc;
    use tokio::time::{sleep, timeout};

    // Helper to connect to a test NATS server
    async fn setup_nats_client() -> Result<Client, AppError> {
        // Return AppError for consistency
        // Use a test-specific address (e.g., "127.0.0.1:4222") or rely on default if running locally
        let client = connect("127.0.0.1:4222").await?;
        Ok(client)
    }

    // Test for basic publish-subscribe functionality
    #[tokio::test]
    async fn test_publish_and_subscribe_basic() -> Result<(), AppError> {
        let client = setup_nats_client().await?;
        let test_subject = "test.basic";
        let expected_message = "Hello from test!";

        // Subscriber setup: receives one message and returns it
        let sub_client = client.clone();
        let sub_handle = tokio::spawn(async move {
            let mut sub = sub_client.subscribe(test_subject).await?; // Use `?` here too
            if let Some(msg) = sub.next().await {
                Ok(String::from_utf8(msg.payload.to_vec())?)
            } else {
                Err(AppError::GenericError("No message received".to_string()))
            }
        });

        // Give subscriber a moment to be ready
        sleep(Duration::from_millis(100)).await;

        // Publisher sends message
        client
            .publish(test_subject, expected_message.into())
            .await?;

        // Wait for subscriber to receive and return the message, with a timeout
        let received_message = timeout(Duration::from_secs(1), sub_handle).await??; // Handles TimeoutError, JoinError, and AppError

        assert_eq!(received_message, expected_message);
        info!("Test `test_publish_and_subscribe_basic` passed!");
        Ok(())
    }

    // Test for multiple messages and order
    #[tokio::test]
    async fn test_publish_and_subscribe_multiple_messages() -> Result<(), AppError> {
        let client = setup_nats_client().await?;
        let test_subject = "test.multiple";
        let num_messages = 3;
        let mut expected_messages: Vec<String> = (0..num_messages)
            .map(|i| format!("Test message #{}", i))
            .collect();
        expected_messages.sort_unstable(); // Sort for consistent comparison

        let received_messages_arc = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let sub_client = client.clone();
        let received_messages_clone = Arc::clone(&received_messages_arc);

        // Subscriber task to collect messages
        let sub_handle = tokio::spawn(async move {
            let mut sub = sub_client.subscribe(test_subject).await?; // Use `?` here
            let mut count = 0;
            // Loop and collect until all expected messages are received
            while let Some(msg) = sub.next().await {
                let payload = String::from_utf8(msg.payload.to_vec())?;
                received_messages_clone.lock().await.push(payload);
                count += 1;
                if count >= num_messages {
                    break;
                }
            }
            Ok::<(), AppError>(())
        });

        // Give subscriber time to be ready
        sleep(Duration::from_millis(100)).await;

        // Publisher sends messages
        for i in 0..num_messages {
            let msg = format!("Test message #{}", i);
            client.publish(test_subject, msg.into()).await?;
            sleep(Duration::from_millis(50)).await; // Small delay to ensure separate messages
        }

        // Wait for the subscriber to finish collecting, with a timeout
        timeout(Duration::from_secs(2), sub_handle).await??;

        let mut actual_messages = received_messages_arc.lock().await;
        actual_messages.sort_unstable(); // Sort for comparison

        assert_eq!(*actual_messages, expected_messages);
        info!("Test `test_publish_and_subscribe_multiple_messages` passed!");
        Ok(())
    }

    // Test for connection error handling
    #[tokio::test]
    async fn test_connection_error_handling() {
        // Try to connect to a non-existent NATS server port
        let result = connect("127.0.0.1:9999").await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        info!("Expected connection error received: {:?}", err);
        // We can assert on the specific kind if we want, e.g.,
        // assert!(matches!(err.kind(), async_nats::error::ConnectErrorKind::NoResponders));
        // But simply checking `is_err()` and logging is fine for basic tests.
    }

    // Test for controlled shutdown of the main subscriber task
    #[tokio::test]
    async fn test_subscriber_shutdown_via_channel() -> Result<(), AppError> {
        let client = setup_nats_client().await?;
        let (tx_shutdown, rx_shutdown) = tokio::sync::mpsc::channel::<()>(1);

        let sub_client_clone = client.clone();
        let sub_handle = tokio::spawn(subscriber_task(sub_client_clone, rx_shutdown));

        // Give subscriber time to set up
        sleep(Duration::from_millis(100)).await;

        // Send a few messages that the subscriber should process
        client.publish("demo.subject", "Message 1".into()).await?;
        sleep(Duration::from_millis(50)).await;
        client.publish("demo.subject", "Message 2".into()).await?;
        sleep(Duration::from_millis(50)).await;

        // Now send the shutdown signal
        tx_shutdown
            .send(())
            .await
            .map_err(|e| AppError::SendError(e.to_string()))?;

        // Wait for the subscriber task to complete after receiving the signal
        let result = timeout(Duration::from_secs(1), sub_handle).await;

        assert!(
            result.is_ok(),
            "Subscriber task should have completed successfully"
        );
        assert!(
            result.unwrap().is_ok(),
            "Subscriber task should have returned Ok(())"
        );
        info!("Test `test_subscriber_shutdown_via_channel` passed!");
        Ok(())
    }
}
