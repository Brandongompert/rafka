use std::io;

use tokio::{
    io::BufReader,
    net::{
        TcpStream, ToSocketAddrs,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
};

use crate::protocol::{FetchedWireMessage, Request, Response};

/// A connected Rafka client.
///
/// Manages a single TCP connection to a broker. All methods are async and
/// pipelined — each sends one request and waits for one response.
///
/// # Example
///
/// ```no_run
/// # async fn example() -> std::io::Result<()> {
/// let mut client = rafka::client::Client::connect("127.0.0.1:9092").await?;
///
/// client.create_topic("events", 3).await?;
///
/// let (partition_id, offset) = client
///     .produce("events", Some(b"user-1"), b"click")
///     .await?;
///
/// let messages = client
///     .fetch("events", partition_id, offset, 10)
///     .await?;
/// # Ok(())
/// # }
/// ```
pub struct Client {
    reader: BufReader<OwnedReadHalf>,
    writer: OwnedWriteHalf,
}

impl Client {
    /// Open a TCP connection to a Rafka broker.
    pub async fn connect<A: ToSocketAddrs>(addr: A) -> io::Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        let (r, w) = stream.into_split();
        Ok(Self {
            reader: BufReader::new(r),
            writer: w,
        })
    }

    /// Create a new topic with the given number of partitions.
    ///
    /// Returns an error if the topic already exists or `partition_count` is 0.
    pub async fn create_topic(&mut self, name: &str, partition_count: u32) -> io::Result<()> {
        let resp = self
            .roundtrip(Request::CreateTopic {
                name: name.to_string(),
                partition_count,
            })
            .await?;

        match resp {
            Response::Ok => Ok(()),
            Response::Error { message } => Err(broker_error(message)),
            other => Err(unexpected_response(other)),
        }
    }

    /// Produce a message to a topic.
    ///
    /// `key` controls partition routing:
    /// - `Some(key)` → consistent hash, same key always → same partition
    /// - `None`      → broker selects partition
    ///
    /// Returns `(partition_id, offset)` identifying where the message landed.
    pub async fn produce(
        &mut self,
        topic: &str,
        key: Option<&[u8]>,
        payload: &[u8],
    ) -> io::Result<(u32, u64)> {
        let resp = self
            .roundtrip(Request::Produce {
                topic: topic.to_string(),
                key: key.map(|k| k.to_vec()),
                payload: payload.to_vec(),
            })
            .await?;

        match resp {
            Response::Produced {
                partition_id,
                offset,
            } => Ok((partition_id, offset)),
            Response::Error { message } => Err(broker_error(message)),
            other => Err(unexpected_response(other)),
        }
    }

    /// Fetch up to `max_messages` from a specific partition starting at `offset`.
    ///
    /// Returns messages in offset order. An empty vec means no messages are
    /// available at or after `offset` (the consumer is caught up).
    pub async fn fetch(
        &mut self,
        topic: &str,
        partition_id: u32,
        offset: u64,
        max_messages: u32,
    ) -> io::Result<Vec<Message>> {
        let resp = self
            .roundtrip(Request::Fetch {
                topic: topic.to_string(),
                partition_id,
                offset,
                max_messages,
            })
            .await?;

        match resp {
            Response::Fetched { messages } => Ok(messages.into_iter().map(Message::from).collect()),
            Response::Error { message } => Err(broker_error(message)),
            other => Err(unexpected_response(other)),
        }
    }

    // ── internal ─────────────────────────────────────────────────────────────

    async fn roundtrip(&mut self, req: Request) -> io::Result<Response> {
        req.write_to(&mut self.writer).await?;
        Response::read_from(&mut self.reader)
            .await?
            .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "broker closed connection"))
    }
}

/// A message returned by `Client::fetch`.
#[derive(Debug, Clone, PartialEq)]
pub struct Message {
    /// The message's absolute offset within its partition.
    pub offset: u64,
    /// The raw message payload.
    pub payload: Vec<u8>,
}

impl From<FetchedWireMessage> for Message {
    fn from(m: FetchedWireMessage) -> Self {
        Self {
            offset: m.offset,
            payload: m.payload,
        }
    }
}

fn broker_error(message: String) -> io::Error {
    io::Error::new(io::ErrorKind::Other, message)
}

fn unexpected_response(resp: Response) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("unexpected broker response: {:?}", resp),
    )
}
