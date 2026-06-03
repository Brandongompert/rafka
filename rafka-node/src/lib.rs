#![deny(clippy::all)]

use std::sync::Arc;

use napi::bindgen_prelude::*;
use napi_derive::napi;
use tokio::sync::Mutex;

/// A single message returned from `RafkaClient.fetch()`.
#[napi(object)]
pub struct Message {
    /// Absolute offset within the partition.
    ///
    /// Represented as a JavaScript `number` (f64). Safe up to 2^53 − 1
    /// (~9 quadrillion messages), which is sufficient for all practical use.
    pub offset: f64,
    /// Raw message payload as a Node.js `Buffer`.
    pub payload: Buffer,
}

/// The result of a successful `RafkaClient.produce()` call.
#[napi(object)]
pub struct ProduceResult {
    /// The partition the message was written to.
    pub partition_id: u32,
    /// The offset assigned to the message within that partition.
    pub offset: f64,
}

/// A connected Rafka client.
///
/// All methods are async and safe to call from multiple JavaScript async
/// contexts — internal state is protected by an async `Mutex`.
///
/// @example
/// ```js
/// const client = await RafkaClient.connect('127.0.0.1:9092')
/// await client.createTopic('events', 3)
///
/// const { partitionId, offset } = await client.produce(
///   'events',
///   Buffer.from('user-1'),   // routing key (optional, pass null for keyless)
///   Buffer.from('click'),
/// )
///
/// const messages = await client.fetch('events', partitionId, offset, 100)
/// messages.forEach(m => console.log(m.offset, m.payload.toString()))
/// ```
#[napi]
pub struct RafkaClient {
    inner: Arc<Mutex<rafka::client::Client>>,
}

#[napi]
impl RafkaClient {
    /// Connect to a Rafka broker.
    ///
    /// @param addr - Host and port, e.g. `"127.0.0.1:9092"`.
    #[napi(factory)]
    pub async fn connect(addr: String) -> napi::Result<Self> {
        let client = rafka::client::Client::connect(addr.as_str())
            .await
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;

        Ok(Self {
            inner: Arc::new(Mutex::new(client)),
        })
    }

    /// Create a new topic on the broker.
    ///
    /// Throws if the topic already exists or `partitionCount` is 0.
    ///
    /// @param name - Topic name.
    /// @param partitionCount - Number of partitions (must be ≥ 1).
    #[napi]
    pub async fn create_topic(&self, name: String, partition_count: u32) -> napi::Result<()> {
        self.inner
            .lock()
            .await
            .create_topic(&name, partition_count)
            .await
            .map_err(|e| napi::Error::from_reason(e.to_string()))
    }

    /// Publish a message to a topic.
    ///
    /// @param topic - Target topic name.
    /// @param key   - Optional routing key. Messages with the same key always
    ///               land on the same partition (ordering guarantee). Pass
    ///               `null` or `undefined` for keyless (load-balanced) routing.
    /// @param payload - Message body as a `Buffer`.
    /// @returns The partition and offset where the message was stored.
    #[napi]
    pub async fn produce(
        &self,
        topic: String,
        key: Option<Buffer>,
        payload: Buffer,
    ) -> napi::Result<ProduceResult> {
        let key_slice: Option<&[u8]> = key.as_deref();

        let (partition_id, offset) = self
            .inner
            .lock()
            .await
            .produce(&topic, key_slice, &payload)
            .await
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;

        Ok(ProduceResult {
            partition_id,
            offset: offset as f64,
        })
    }

    /// Fetch messages from a specific partition.
    ///
    /// @param topic       - Topic name.
    /// @param partitionId - Partition to read from.
    /// @param offset      - Starting offset (inclusive). Use `0` to read from
    ///                      the beginning.
    /// @param maxMessages - Maximum number of messages to return.
    /// @returns An array of messages in offset order. An empty array means the
    ///          consumer is caught up (no new messages at this offset).
    #[napi]
    pub async fn fetch(
        &self,
        topic: String,
        partition_id: u32,
        offset: f64,
        max_messages: u32,
    ) -> napi::Result<Vec<Message>> {
        let messages = self
            .inner
            .lock()
            .await
            .fetch(&topic, partition_id, offset as u64, max_messages)
            .await
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;

        Ok(messages
            .into_iter()
            .map(|m| Message {
                offset: m.offset as f64,
                payload: Buffer::from(m.payload),
            })
            .collect())
    }
}
