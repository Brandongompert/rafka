use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    io,
    path::{Path, PathBuf},
};

use crate::partition::{FetchedMessage, Partition};

/// A named stream of messages, split across one or more partitions.
///
/// The topic is the primary interface for producers and consumers:
///
/// - **Producers** call `produce(key, payload)`. The key is hashed to pick a
///   partition, so messages with the same key always land on the same partition
///   — guaranteeing per-key ordering.
///
/// - **Consumers** call `fetch(partition_id, offset, max)` to pull messages
///   from a specific partition starting at a known offset.
///
/// ```text
/// topic.produce(Some("user-42"), b"clicked")
///   → hash("user-42") % 3 = 1
///   → partition[1].append(b"clicked")
/// ```
pub struct Topic {
    /// The topic name (also the directory name under base_dir).
    pub name: String,
    /// All partitions for this topic, indexed by partition id.
    partitions: Vec<Partition>,
    /// Root data directory (used when dynamically adding partitions).
    _base_dir: PathBuf,
}

impl Topic {
    /// Open or recover a topic with `partition_count` partitions.
    ///
    /// If the topic already exists on disk with a different number of
    /// partitions, the existing partitions are opened as-is.
    ///
    /// Partition logs are stored at `{base_dir}/{name}/{id}/`.
    pub fn open(base_dir: &Path, name: &str, partition_count: u32) -> io::Result<Self> {
        let partitions = (0..partition_count)
            .map(|id| Partition::open(base_dir, name, id))
            .collect::<io::Result<Vec<_>>>()?;

        Ok(Self {
            name: name.to_string(),
            partitions,
            _base_dir: base_dir.to_path_buf(),
        })
    }

    /// Produce a message to this topic.
    ///
    /// `key` is used to select the target partition:
    /// - `Some(key)` → hash(key) % partition_count (stable routing per key)
    /// - `None`      → round-robin is approximated by hashing the current HWM
    ///                 of the first partition (good enough for now)
    ///
    /// Returns `(partition_id, offset)`.
    pub fn produce(&mut self, key: Option<&[u8]>, payload: &[u8]) -> io::Result<(u32, u64)> {
        let partition_id = self.select_partition(key);
        let offset = self.partitions[partition_id as usize].append(payload)?;
        Ok((partition_id, offset))
    }

    /// Fetch up to `max_messages` from `partition_id` starting at `offset`.
    pub fn fetch(
        &mut self,
        partition_id: u32,
        offset: u64,
        max_messages: usize,
    ) -> io::Result<Vec<FetchedMessage>> {
        let partition = self.partition_mut(partition_id)?;
        partition.fetch(offset, max_messages)
    }

    /// High-water mark for a given partition.
    pub fn high_water_mark(&self, partition_id: u32) -> io::Result<u64> {
        let partition = self.partition(partition_id)?;
        Ok(partition.high_water_mark())
    }

    /// Number of partitions in this topic.
    pub fn partition_count(&self) -> u32 {
        self.partitions.len() as u32
    }

    // ── internals ────────────────────────────────────────────────────────────

    /// Hash `key` to a partition index in [0, partition_count).
    fn select_partition(&self, key: Option<&[u8]>) -> u32 {
        let n = self.partitions.len() as u64;
        match key {
            Some(k) => {
                let mut h = DefaultHasher::new();
                k.hash(&mut h);
                (h.finish() % n) as u32
            }
            // Keyless messages rotate across partitions based on total message
            // count — a simple approximation of round-robin.
            None => {
                let total: u64 = self.partitions.iter().map(|p| p.high_water_mark()).sum();
                (total % n) as u32
            }
        }
    }

    fn partition(&self, id: u32) -> io::Result<&Partition> {
        self.partitions.get(id as usize).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "partition {} does not exist (topic has {})",
                    id,
                    self.partitions.len()
                ),
            )
        })
    }

    fn partition_mut(&mut self, id: u32) -> io::Result<&mut Partition> {
        let len = self.partitions.len();
        self.partitions.get_mut(id as usize).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("partition {} does not exist (topic has {})", id, len),
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn tmp_dir(name: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("rafka_topic_test_{}_{}", std::process::id(), name));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn produce_and_fetch() {
        let base = tmp_dir("produce_and_fetch");
        let mut topic = Topic::open(&base, "clicks", 3).unwrap();

        let (pid, offset) = topic.produce(Some(b"user-1"), b"click-a").unwrap();
        let msgs = topic.fetch(pid, offset, 1).unwrap();

        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].payload, b"click-a");
    }

    #[test]
    fn same_key_same_partition() {
        let base = tmp_dir("same_key");
        let mut topic = Topic::open(&base, "events", 4).unwrap();

        let mut partitions_seen = std::collections::HashSet::new();
        for _ in 0..10 {
            let (pid, _) = topic.produce(Some(b"stable-key"), b"data").unwrap();
            partitions_seen.insert(pid);
        }

        // All 10 produces landed on exactly one partition.
        assert_eq!(partitions_seen.len(), 1);
    }

    #[test]
    fn keyless_produces_distribute() {
        let base = tmp_dir("keyless");
        let mut topic = Topic::open(&base, "logs", 3).unwrap();

        // Produce enough messages that we expect to hit multiple partitions.
        for _ in 0..12 {
            topic.produce(None, b"log-line").unwrap();
        }

        // Every partition should have received at least one message.
        for pid in 0..3 {
            assert!(
                topic.high_water_mark(pid).unwrap() > 0,
                "partition {pid} received no messages"
            );
        }
    }

    #[test]
    fn fetch_invalid_partition_errors() {
        let base = tmp_dir("invalid_partition");
        let mut topic = Topic::open(&base, "t", 2).unwrap();
        let result = topic.fetch(99, 0, 1);
        assert!(result.is_err());
    }

    #[test]
    fn survives_restart() {
        let base = tmp_dir("topic_restart");
        {
            let mut topic = Topic::open(&base, "orders", 2).unwrap();
            topic.produce(Some(b"k1"), b"order-A").unwrap();
            topic.produce(Some(b"k1"), b"order-B").unwrap();
        }

        let mut topic = Topic::open(&base, "orders", 2).unwrap();
        // k1 always maps to the same partition — fetch from it.
        let mut h = DefaultHasher::new();
        b"k1".hash(&mut h);
        let pid = (h.finish() % 2) as u32;

        let msgs = topic.fetch(pid, 0, 10).unwrap();
        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0].payload, b"order-A");
    }
}
