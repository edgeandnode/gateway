use std::collections::HashMap;

use tokio::sync::watch;

use crate::{KafkaConsumer, auth::ApiKey};

pub async fn api_keys(
    consumer: &KafkaConsumer,
    topic: &str,
) -> anyhow::Result<watch::Receiver<HashMap<String, ApiKey>>> {
    let consumer = consumer.stream_consumer(topic)?;
    // TODO:
    // - fetch latest offset: `recent_offset`
    // - consume up to `recent_offset`
    // - if latest bootstrap message timestamp is over `x` days ago
    //   - request bootstrap
    //   - wait for first bootstrap message + 30 seconds
    // - if latest bootstrap message is less than `y` seconds ago
    //   - wait for 30 seconds
    // - return
    todo!()
}
