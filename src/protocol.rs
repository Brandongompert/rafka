//! Binary wire protocol for Rafka.
//!
//! Every message on the wire is a **frame**:
//!
//! ```text
//! [frame_length: u32][body: bytes...]
//! ```
//!
//! `frame_length` is the number of bytes in `body` (does not include itself).
//! The first byte of the body is always a **type tag** that identifies the
//! request or response kind. All integers are big-endian.
//!
//! ## Request types
//!
//! | Tag  | Name         | Fields                                              |
//! |------|--------------|-----------------------------------------------------|
//! | 0x01 | CreateTopic  | name: str16, partition_count: u32                   |
//! | 0x02 | Produce      | topic: str16, has_key: u8, [key: bytes32], msg: bytes32 |
//! | 0x03 | Fetch        | topic: str16, partition_id: u32, offset: u64, max: u32 |
//!
//! ## Response types
//!
//! | Tag  | Name     | Fields                                              |
//! |------|----------|-----------------------------------------------------|
//! | 0x00 | Ok       | (empty)                                             |
//! | 0x01 | Produced | partition_id: u32, offset: u64                      |
//! | 0x02 | Fetched  | count: u32, [(offset: u64, payload: bytes32)...]    |
//! | 0xFF | Error    | message: str16                                      |
//!
//! Encoding helpers:
//! - `str16`   = [length: u16][utf-8 bytes]
//! - `bytes32` = [length: u32][bytes]

use std::io;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

// ── Request ──────────────────────────────────────────────────────────────────

const TAG_CREATE_TOPIC: u8 = 0x01;
const TAG_PRODUCE: u8 = 0x02;
const TAG_FETCH: u8 = 0x03;

/// A decoded client request.
#[derive(Debug)]
pub enum Request {
    CreateTopic {
        name: String,
        partition_count: u32,
    },
    Produce {
        topic: String,
        key: Option<Vec<u8>>,
        payload: Vec<u8>,
    },
    Fetch {
        topic: String,
        partition_id: u32,
        offset: u64,
        max_messages: u32,
    },
}

impl Request {
    /// Read and decode one request frame from `reader`.
    ///
    /// Returns `None` on clean EOF (client disconnected).
    pub async fn read_from<R>(reader: &mut R) -> io::Result<Option<Self>>
    where
        R: AsyncReadExt + Unpin,
    {
        // Read the 4-byte frame length.
        let frame_len = match reader.read_u32().await {
            Ok(n) => n as usize,
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e),
        };

        // Read the full body into a buffer.
        let mut body = vec![0u8; frame_len];
        reader.read_exact(&mut body).await?;

        let mut cur = body.as_slice();
        let tag = read_u8(&mut cur)?;

        let request = match tag {
            TAG_CREATE_TOPIC => {
                let name = read_str16(&mut cur)?;
                let partition_count = read_u32(&mut cur)?;
                Request::CreateTopic {
                    name,
                    partition_count,
                }
            }
            TAG_PRODUCE => {
                let topic = read_str16(&mut cur)?;
                let has_key = read_u8(&mut cur)? != 0;
                let key = if has_key {
                    Some(read_bytes32(&mut cur)?)
                } else {
                    None
                };
                let payload = read_bytes32(&mut cur)?;
                Request::Produce {
                    topic,
                    key,
                    payload,
                }
            }
            TAG_FETCH => {
                let topic = read_str16(&mut cur)?;
                let partition_id = read_u32(&mut cur)?;
                let offset = read_u64(&mut cur)?;
                let max_messages = read_u32(&mut cur)?;
                Request::Fetch {
                    topic,
                    partition_id,
                    offset,
                    max_messages,
                }
            }
            other => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unknown request tag: 0x{:02x}", other),
                ));
            }
        };

        Ok(Some(request))
    }

    /// Encode this request into a frame and write it to `writer`.
    pub async fn write_to<W>(&self, writer: &mut W) -> io::Result<()>
    where
        W: AsyncWriteExt + Unpin,
    {
        let mut body = Vec::new();
        match self {
            Request::CreateTopic {
                name,
                partition_count,
            } => {
                body.push(TAG_CREATE_TOPIC);
                write_str16(&mut body, name);
                write_u32(&mut body, *partition_count);
            }
            Request::Produce {
                topic,
                key,
                payload,
            } => {
                body.push(TAG_PRODUCE);
                write_str16(&mut body, topic);
                body.push(key.is_some() as u8);
                if let Some(k) = key {
                    write_bytes32(&mut body, k);
                }
                write_bytes32(&mut body, payload);
            }
            Request::Fetch {
                topic,
                partition_id,
                offset,
                max_messages,
            } => {
                body.push(TAG_FETCH);
                write_str16(&mut body, topic);
                write_u32(&mut body, *partition_id);
                write_u64(&mut body, *offset);
                write_u32(&mut body, *max_messages);
            }
        }
        writer.write_u32(body.len() as u32).await?;
        writer.write_all(&body).await?;
        Ok(())
    }
}

// ── Response ─────────────────────────────────────────────────────────────────

const TAG_OK: u8 = 0x00;
const TAG_PRODUCED: u8 = 0x01;
const TAG_FETCHED: u8 = 0x02;
const TAG_ERROR: u8 = 0xFF;

/// A decoded broker response.
#[derive(Debug, PartialEq)]
pub enum Response {
    /// Generic success (e.g. CreateTopic).
    Ok,
    /// Successful produce: where the message landed.
    Produced { partition_id: u32, offset: u64 },
    /// Successful fetch: zero or more messages.
    Fetched { messages: Vec<FetchedWireMessage> },
    /// Any broker-side error.
    Error { message: String },
}

/// A single message as it appears in a Fetch response.
#[derive(Debug, PartialEq)]
pub struct FetchedWireMessage {
    pub offset: u64,
    pub payload: Vec<u8>,
}

impl Response {
    /// Encode this response into a frame and write it to `writer`.
    pub async fn write_to<W>(&self, writer: &mut W) -> io::Result<()>
    where
        W: AsyncWriteExt + Unpin,
    {
        let mut body = Vec::new();
        match self {
            Response::Ok => {
                body.push(TAG_OK);
            }
            Response::Produced {
                partition_id,
                offset,
            } => {
                body.push(TAG_PRODUCED);
                write_u32(&mut body, *partition_id);
                write_u64(&mut body, *offset);
            }
            Response::Fetched { messages } => {
                body.push(TAG_FETCHED);
                write_u32(&mut body, messages.len() as u32);
                for msg in messages {
                    write_u64(&mut body, msg.offset);
                    write_bytes32(&mut body, &msg.payload);
                }
            }
            Response::Error { message } => {
                body.push(TAG_ERROR);
                write_str16(&mut body, message);
            }
        }
        writer.write_u32(body.len() as u32).await?;
        writer.write_all(&body).await?;
        Ok(())
    }

    /// Read and decode one response frame from `reader`.
    ///
    /// Returns `None` on clean EOF.
    pub async fn read_from<R>(reader: &mut R) -> io::Result<Option<Self>>
    where
        R: AsyncReadExt + Unpin,
    {
        let frame_len = match reader.read_u32().await {
            Ok(n) => n as usize,
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e),
        };

        let mut body = vec![0u8; frame_len];
        reader.read_exact(&mut body).await?;
        let mut cur = body.as_slice();

        let tag = read_u8(&mut cur)?;
        let response = match tag {
            TAG_OK => Response::Ok,
            TAG_PRODUCED => {
                let partition_id = read_u32(&mut cur)?;
                let offset = read_u64(&mut cur)?;
                Response::Produced {
                    partition_id,
                    offset,
                }
            }
            TAG_FETCHED => {
                let count = read_u32(&mut cur)? as usize;
                let mut messages = Vec::with_capacity(count);
                for _ in 0..count {
                    let offset = read_u64(&mut cur)?;
                    let payload = read_bytes32(&mut cur)?;
                    messages.push(FetchedWireMessage { offset, payload });
                }
                Response::Fetched { messages }
            }
            TAG_ERROR => {
                let message = read_str16(&mut cur)?;
                Response::Error { message }
            }
            other => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unknown response tag: 0x{:02x}", other),
                ));
            }
        };

        Ok(Some(response))
    }
}

// ── Encoding helpers ──────────────────────────────────────────────────────────

fn write_u32(buf: &mut Vec<u8>, v: u32) {
    buf.extend_from_slice(&v.to_be_bytes());
}

fn write_u64(buf: &mut Vec<u8>, v: u64) {
    buf.extend_from_slice(&v.to_be_bytes());
}

fn write_str16(buf: &mut Vec<u8>, s: &str) {
    let bytes = s.as_bytes();
    buf.extend_from_slice(&(bytes.len() as u16).to_be_bytes());
    buf.extend_from_slice(bytes);
}

fn write_bytes32(buf: &mut Vec<u8>, bytes: &[u8]) {
    buf.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    buf.extend_from_slice(bytes);
}

// ── Decoding helpers ──────────────────────────────────────────────────────────

fn read_u8(cur: &mut &[u8]) -> io::Result<u8> {
    if cur.is_empty() {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "expected u8"));
    }
    let v = cur[0];
    *cur = &cur[1..];
    Ok(v)
}

fn read_u32(cur: &mut &[u8]) -> io::Result<u32> {
    if cur.len() < 4 {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "expected u32"));
    }
    let v = u32::from_be_bytes(cur[..4].try_into().unwrap());
    *cur = &cur[4..];
    Ok(v)
}

fn read_u64(cur: &mut &[u8]) -> io::Result<u64> {
    if cur.len() < 8 {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "expected u64"));
    }
    let v = u64::from_be_bytes(cur[..8].try_into().unwrap());
    *cur = &cur[8..];
    Ok(v)
}

fn read_str16(cur: &mut &[u8]) -> io::Result<String> {
    let len = read_u16(cur)? as usize;
    if cur.len() < len {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "expected str body",
        ));
    }
    let s = std::str::from_utf8(&cur[..len])
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
        .to_string();
    *cur = &cur[len..];
    Ok(s)
}

fn read_u16(cur: &mut &[u8]) -> io::Result<u16> {
    if cur.len() < 2 {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "expected u16"));
    }
    let v = u16::from_be_bytes(cur[..2].try_into().unwrap());
    *cur = &cur[2..];
    Ok(v)
}

fn read_bytes32(cur: &mut &[u8]) -> io::Result<Vec<u8>> {
    let len = read_u32(cur)? as usize;
    if cur.len() < len {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "expected bytes body",
        ));
    }
    let bytes = cur[..len].to_vec();
    *cur = &cur[len..];
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::BufReader;

    async fn roundtrip_request(req: Request) -> Request {
        let mut buf = Vec::new();
        req.write_to(&mut buf).await.unwrap();
        let mut reader = BufReader::new(buf.as_slice());
        Request::read_from(&mut reader).await.unwrap().unwrap()
    }

    async fn roundtrip_response(resp: Response) -> Response {
        let mut buf = Vec::new();
        resp.write_to(&mut buf).await.unwrap();
        let mut reader = BufReader::new(buf.as_slice());
        Response::read_from(&mut reader).await.unwrap().unwrap()
    }

    #[tokio::test]
    async fn create_topic_roundtrip() {
        let req = Request::CreateTopic {
            name: "events".into(),
            partition_count: 3,
        };
        let decoded = roundtrip_request(req).await;
        assert!(
            matches!(decoded, Request::CreateTopic { name, partition_count: 3 } if name == "events")
        );
    }

    #[tokio::test]
    async fn produce_with_key_roundtrip() {
        let req = Request::Produce {
            topic: "orders".into(),
            key: Some(b"user-42".to_vec()),
            payload: b"order-data".to_vec(),
        };
        let decoded = roundtrip_request(req).await;
        match decoded {
            Request::Produce {
                topic,
                key,
                payload,
            } => {
                assert_eq!(topic, "orders");
                assert_eq!(key.unwrap(), b"user-42");
                assert_eq!(payload, b"order-data");
            }
            _ => panic!("wrong variant"),
        }
    }

    #[tokio::test]
    async fn produce_keyless_roundtrip() {
        let req = Request::Produce {
            topic: "logs".into(),
            key: None,
            payload: b"log-line".to_vec(),
        };
        let decoded = roundtrip_request(req).await;
        assert!(matches!(decoded, Request::Produce { key: None, .. }));
    }

    #[tokio::test]
    async fn fetch_roundtrip() {
        let req = Request::Fetch {
            topic: "events".into(),
            partition_id: 2,
            offset: 1024,
            max_messages: 50,
        };
        let decoded = roundtrip_request(req).await;
        match decoded {
            Request::Fetch {
                topic,
                partition_id,
                offset,
                max_messages,
            } => {
                assert_eq!(topic, "events");
                assert_eq!(partition_id, 2);
                assert_eq!(offset, 1024);
                assert_eq!(max_messages, 50);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[tokio::test]
    async fn response_ok_roundtrip() {
        assert_eq!(roundtrip_response(Response::Ok).await, Response::Ok);
    }

    #[tokio::test]
    async fn response_produced_roundtrip() {
        let resp = Response::Produced {
            partition_id: 1,
            offset: 42,
        };
        assert_eq!(
            roundtrip_response(resp).await,
            Response::Produced {
                partition_id: 1,
                offset: 42
            }
        );
    }

    #[tokio::test]
    async fn response_fetched_roundtrip() {
        let resp = Response::Fetched {
            messages: vec![
                FetchedWireMessage {
                    offset: 0,
                    payload: b"hello".to_vec(),
                },
                FetchedWireMessage {
                    offset: 1,
                    payload: b"world".to_vec(),
                },
            ],
        };
        let decoded = roundtrip_response(resp).await;
        match decoded {
            Response::Fetched { messages } => {
                assert_eq!(messages.len(), 2);
                assert_eq!(messages[1].payload, b"world");
            }
            _ => panic!("wrong variant"),
        }
    }

    #[tokio::test]
    async fn response_error_roundtrip() {
        let resp = Response::Error {
            message: "topic not found".into(),
        };
        assert_eq!(
            roundtrip_response(resp).await,
            Response::Error {
                message: "topic not found".into()
            }
        );
    }
}
