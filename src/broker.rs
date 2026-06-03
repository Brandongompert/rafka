use std::{
    collections::HashMap,
    io,
    path::{Path, PathBuf},
};

use crate::{
    protocol::{FetchedWireMessage, Request, Response},
    topic::Topic,
};

/// The broker owns all topics and processes decoded requests.
///
/// It is the single source of truth for what topics exist and where their
/// data lives on disk. The server layer wraps this in an `Arc<Mutex<Broker>>`
/// so it can be shared safely across concurrent TCP connections.
pub struct Broker {
    /// All open topics, keyed by name.
    topics: HashMap<String, Topic>,
    /// Root directory where all topic data is stored.
    data_dir: PathBuf,
}

impl Broker {
    /// Create a new broker, loading any topics that already exist under `data_dir`.
    ///
    /// Existing topics are discovered by listing subdirectories. Each subdirectory
    /// is treated as a topic; partitions are subdirectories within that.
    pub fn open(data_dir: &Path) -> io::Result<Self> {
        std::fs::create_dir_all(data_dir)?;

        let mut topics = HashMap::new();

        for entry in std::fs::read_dir(data_dir)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }

            let topic_name = entry.file_name().to_string_lossy().to_string();

            // Count the partition subdirectories to know how many to reopen.
            let partition_count = std::fs::read_dir(entry.path())?
                .filter(|e| e.as_ref().map(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false)).unwrap_or(false))
                .count() as u32;

            if partition_count > 0 {
                let topic = Topic::open(data_dir, &topic_name, partition_count)?;
                topics.insert(topic_name, topic);
            }
        }

        Ok(Self { topics, data_dir: data_dir.to_path_buf() })
    }

    /// Dispatch a decoded request and return the appropriate response.
    pub fn handle(&mut self, request: Request) -> Response {
        match request {
            Request::CreateTopic { name, partition_count } => {
                self.create_topic(name, partition_count)
            }
            Request::Produce { topic, key, payload } => self.produce(topic, key, payload),
            Request::Fetch { topic, partition_id, offset, max_messages } => {
                self.fetch(topic, partition_id, offset, max_messages)
            }
        }
    }

    // ── request handlers ─────────────────────────────────────────────────────

    fn create_topic(&mut self, name: String, partition_count: u32) -> Response {
        if self.topics.contains_key(&name) {
            return Response::Error { message: format!("topic '{}' already exists", name) };
        }
        if partition_count == 0 {
            return Response::Error { message: "partition_count must be >= 1".into() };
        }

        match Topic::open(&self.data_dir, &name, partition_count) {
            Ok(topic) => {
                self.topics.insert(name, topic);
                Response::Ok
            }
            Err(e) => Response::Error { message: e.to_string() },
        }
    }

    fn produce(&mut self, topic_name: String, key: Option<Vec<u8>>, payload: Vec<u8>) -> Response {
        let Some(topic) = self.topics.get_mut(&topic_name) else {
            return Response::Error { message: format!("topic '{}' not found", topic_name) };
        };

        match topic.produce(key.as_deref(), &payload) {
            Ok((partition_id, offset)) => Response::Produced { partition_id, offset },
            Err(e) => Response::Error { message: e.to_string() },
        }
    }

    fn fetch(
        &mut self,
        topic_name: String,
        partition_id: u32,
        offset: u64,
        max_messages: u32,
    ) -> Response {
        let Some(topic) = self.topics.get_mut(&topic_name) else {
            return Response::Error { message: format!("topic '{}' not found", topic_name) };
        };

        match topic.fetch(partition_id, offset, max_messages as usize) {
            Ok(msgs) => Response::Fetched {
                messages: msgs
                    .into_iter()
                    .map(|m| FetchedWireMessage { offset: m.offset, payload: m.payload })
                    .collect(),
            },
            Err(e) => Response::Error { message: e.to_string() },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn tmp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir()
            .join(format!("rafka_broker_test_{}_{}", std::process::id(), name));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn create_and_produce_and_fetch() {
        let dir = tmp_dir("full_flow");
        let mut broker = Broker::open(&dir).unwrap();

        // Create a topic.
        let resp = broker.handle(Request::CreateTopic {
            name: "orders".into(),
            partition_count: 2,
        });
        assert_eq!(resp, Response::Ok);

        // Produce a message.
        let resp = broker.handle(Request::Produce {
            topic: "orders".into(),
            key: Some(b"user-1".to_vec()),
            payload: b"order-data".to_vec(),
        });
        let (pid, offset) = match resp {
            Response::Produced { partition_id, offset } => (partition_id, offset),
            other => panic!("expected Produced, got {:?}", other),
        };

        // Fetch it back.
        let resp = broker.handle(Request::Fetch {
            topic: "orders".into(),
            partition_id: pid,
            offset,
            max_messages: 1,
        });
        match resp {
            Response::Fetched { messages } => {
                assert_eq!(messages.len(), 1);
                assert_eq!(messages[0].payload, b"order-data");
            }
            other => panic!("expected Fetched, got {:?}", other),
        }
    }

    #[test]
    fn duplicate_create_topic_errors() {
        let dir = tmp_dir("duplicate_topic");
        let mut broker = Broker::open(&dir).unwrap();

        broker.handle(Request::CreateTopic { name: "t".into(), partition_count: 1 });
        let resp = broker.handle(Request::CreateTopic { name: "t".into(), partition_count: 1 });
        assert!(matches!(resp, Response::Error { .. }));
    }

    #[test]
    fn produce_to_unknown_topic_errors() {
        let dir = tmp_dir("unknown_topic");
        let mut broker = Broker::open(&dir).unwrap();

        let resp = broker.handle(Request::Produce {
            topic: "ghost".into(),
            key: None,
            payload: b"data".to_vec(),
        });
        assert!(matches!(resp, Response::Error { .. }));
    }

    #[test]
    fn broker_recovers_topics_on_restart() {
        let dir = tmp_dir("broker_restart");
        {
            let mut broker = Broker::open(&dir).unwrap();
            broker.handle(Request::CreateTopic { name: "events".into(), partition_count: 1 });
            broker.handle(Request::Produce {
                topic: "events".into(),
                key: None,
                payload: b"persisted".to_vec(),
            });
        }

        let mut broker = Broker::open(&dir).unwrap();
        let resp = broker.handle(Request::Fetch {
            topic: "events".into(),
            partition_id: 0,
            offset: 0,
            max_messages: 10,
        });
        match resp {
            Response::Fetched { messages } => {
                assert_eq!(messages.len(), 1);
                assert_eq!(messages[0].payload, b"persisted");
            }
            other => panic!("expected Fetched, got {:?}", other),
        }
    }
}
