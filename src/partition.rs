use std::{
    io,
    path::{Path, PathBuf},
};

use crate::log::Log;

/// A single partition within a topic.
///
/// A partition is the unit of ordering and parallelism in Rafka. Each partition
/// owns exactly one `Log` on disk. Consumers read from a partition sequentially
/// by offset; multiple partitions in a topic can be read in parallel.
///
/// ```text
/// Topic "orders"
///   ├── Partition 0  →  log at data/orders/0/
///   ├── Partition 1  →  log at data/orders/1/
///   └── Partition 2  →  log at data/orders/2/
/// ```
///
/// The **high-water mark (HWM)** is the offset up to which messages are
/// considered "committed" and safe for consumers to read. For now (single
/// broker, no replication) the HWM advances with every append. In a
/// replicated setup the HWM would only advance once all in-sync replicas
/// have acknowledged the write.
pub struct Partition {
    /// The topic this partition belongs to.
    pub topic: String,
    /// Zero-based partition index within the topic.
    pub id: u32,
    /// The underlying append-only log.
    log: Log,
    /// Highest offset that consumers are allowed to read (exclusive upper bound).
    /// Consumers may read offsets in the range [oldest_offset, high_water_mark).
    high_water_mark: u64,
}

impl Partition {
    /// Open or recover a partition.
    ///
    /// `base_dir` is the root data directory. The partition's log will be
    /// stored at `{base_dir}/{topic}/{id}/`.
    pub fn open(base_dir: &Path, topic: &str, id: u32) -> io::Result<Self> {
        let dir = partition_dir(base_dir, topic, id);
        let log = Log::open(&dir)?;
        let high_water_mark = log.next_offset();

        Ok(Self {
            topic: topic.to_string(),
            id,
            high_water_mark,
            log,
        })
    }

    /// Append a message to this partition.
    ///
    /// Advances the high-water mark after a successful write, making the
    /// message immediately visible to consumers.
    ///
    /// Returns the offset assigned to the message.
    pub fn append(&mut self, payload: &[u8]) -> io::Result<u64> {
        let offset = self.log.append(payload)?;
        self.high_water_mark = offset + 1;
        Ok(offset)
    }

    /// Fetch up to `max_messages` messages starting from `offset`.
    ///
    /// Only returns messages with offset < high_water_mark (committed messages).
    /// Returns an empty vec if `offset` is at or beyond the high-water mark.
    pub fn fetch(&mut self, offset: u64, max_messages: usize) -> io::Result<Vec<FetchedMessage>> {
        let mut results = Vec::new();
        let mut current = offset;

        while results.len() < max_messages && current < self.high_water_mark {
            match self.log.read(current)? {
                Some(payload) => {
                    results.push(FetchedMessage {
                        offset: current,
                        payload,
                    });
                    current += 1;
                }
                None => break,
            }
        }

        Ok(results)
    }

    /// The offset of the oldest available message.
    pub fn oldest_offset(&self) -> u64 {
        self.log.oldest_offset()
    }

    /// The high-water mark: the next offset to be written.
    /// Consumers may read up to (but not including) this offset.
    pub fn high_water_mark(&self) -> u64 {
        self.high_water_mark
    }
}

/// A message returned by `Partition::fetch`.
#[derive(Debug, PartialEq)]
pub struct FetchedMessage {
    pub offset: u64,
    pub payload: Vec<u8>,
}

/// Construct the directory path for a partition's log.
fn partition_dir(base_dir: &Path, topic: &str, id: u32) -> PathBuf {
    base_dir.join(topic).join(id.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn tmp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "rafka_partition_test_{}_{}",
            std::process::id(),
            name
        ));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn append_and_fetch() {
        let base = tmp_dir("append_and_fetch");
        let mut p = Partition::open(&base, "events", 0).unwrap();

        p.append(b"event-a").unwrap();
        p.append(b"event-b").unwrap();
        p.append(b"event-c").unwrap();

        let msgs = p.fetch(0, 10).unwrap();
        assert_eq!(msgs.len(), 3);
        assert_eq!(
            msgs[0],
            FetchedMessage {
                offset: 0,
                payload: b"event-a".to_vec()
            }
        );
        assert_eq!(
            msgs[1],
            FetchedMessage {
                offset: 1,
                payload: b"event-b".to_vec()
            }
        );
        assert_eq!(
            msgs[2],
            FetchedMessage {
                offset: 2,
                payload: b"event-c".to_vec()
            }
        );
    }

    #[test]
    fn fetch_respects_max_messages() {
        let base = tmp_dir("max_messages");
        let mut p = Partition::open(&base, "events", 0).unwrap();

        for i in 0..10u8 {
            p.append(&[i]).unwrap();
        }

        let msgs = p.fetch(0, 3).unwrap();
        assert_eq!(msgs.len(), 3);
        assert_eq!(msgs[2].offset, 2);
    }

    #[test]
    fn fetch_from_mid_stream() {
        let base = tmp_dir("mid_stream");
        let mut p = Partition::open(&base, "events", 0).unwrap();

        p.append(b"skip-me").unwrap();
        p.append(b"skip-me-too").unwrap();
        p.append(b"want-this").unwrap();

        let msgs = p.fetch(2, 1).unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].payload, b"want-this");
    }

    #[test]
    fn fetch_beyond_hwm_returns_empty() {
        let base = tmp_dir("beyond_hwm");
        let mut p = Partition::open(&base, "events", 0).unwrap();
        p.append(b"only").unwrap();

        let msgs = p.fetch(99, 10).unwrap();
        assert!(msgs.is_empty());
    }

    #[test]
    fn survives_restart() {
        let base = tmp_dir("partition_restart");
        {
            let mut p = Partition::open(&base, "orders", 1).unwrap();
            p.append(b"order-1").unwrap();
            p.append(b"order-2").unwrap();
        }

        let mut p = Partition::open(&base, "orders", 1).unwrap();
        assert_eq!(p.high_water_mark(), 2);

        let msgs = p.fetch(0, 10).unwrap();
        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[1].payload, b"order-2");
    }

    #[test]
    fn partition_dir_is_isolated() {
        let base = tmp_dir("isolation");
        let mut p0 = Partition::open(&base, "topic", 0).unwrap();
        let mut p1 = Partition::open(&base, "topic", 1).unwrap();

        p0.append(b"for-p0").unwrap();
        p1.append(b"for-p1").unwrap();

        // Each partition has its own offset space starting at 0.
        assert_eq!(p0.fetch(0, 1).unwrap()[0].payload, b"for-p0");
        assert_eq!(p1.fetch(0, 1).unwrap()[0].payload, b"for-p1");
    }
}
