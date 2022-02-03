use rdkafka::message::{BorrowedMessage, OwnedMessage};
use rdkafka::Message;
use std::thread;
use std::time::Duration;

pub fn expensive_computation<'a>(msg: OwnedMessage) -> String {
    println!("Starting expensive computation on message {}", msg.offset());
    thread::sleep(Duration::from_millis(rand::random::<u64>() % 5000));
    println!(
        "Expensive computation completed on message {}",
        msg.offset()
    );

    match msg.payload_view::<str>() {
        Some(Ok(payload)) => format!("Payload len for {} is {}", payload, payload.len()),
        Some(Err(_)) => "Message payload is not a string".to_owned(),
        None => "No payload".to_owned(),
    }
}
pub async fn record_borrowed_message_receipt(msg: &BorrowedMessage<'_>) {
    // Simulate some work that must be done in the same order as messages are
    // received; i.e., before truly parallel processing can begin.
    println!("Message received: {}", msg.offset());
}

pub async fn record_owned_message_receipt(_msg: &OwnedMessage) {
    println!("Message received: {}", _msg.offset());
}
