pub mod index;
pub mod segment;

use std::{
    fs,
    io::{self},
    path::{Path, PathBuf},
};

use segment::Segment;

/// Default maximum size of a single segment file (1 GiB).
const DEFAULT_MAX_SEGMENT_BYTES: u64 = 1024 * 1024 * 1024;

/// An append-only, segmented log for one partition.
///
/// The log owns a directory on disk. Inside that directory:
///
/// ```text
/// 00000000000000000000.log
/// 00000000000000000000.index
/// 00000000000001048576.log    ← new segment after rollover
/// 00000000000001048576.index
/// ...
/// ```
///
/// Reads are routed to the correct segment via binary search on base offsets.
/// Writes always go to the last (active) segment.
pub struct Log {
    /// Directory that holds all segment and index files.
    dir: PathBuf,
    /// All segments, ordered by base_offset ascending.
    /// The last element is the active (writable) segment.
    segments: Vec<Segment>,
    /// Maximum bytes per segment before a new one is created.
    max_segment_bytes: u64,
}

impl Log {
    /// Open or recover a log stored in `dir`.
    ///
    /// Scans the directory for existing `.log` files and reopens them in order.
    /// If the directory is empty, creates the first segment starting at offset 0.
    pub fn open(dir: &Path) -> io::Result<Self> {
        Self::open_with_max_bytes(dir, DEFAULT_MAX_SEGMENT_BYTES)
    }

    pub fn open_with_max_bytes(dir: &Path, max_segment_bytes: u64) -> io::Result<Self> {
        fs::create_dir_all(dir)?;

        // Collect all base offsets from existing .log files.
        let mut base_offsets: Vec<u64> = fs::read_dir(dir)?
            .filter_map(|entry| {
                let entry = entry.ok()?;
                let name = entry.file_name();
                let name = name.to_str()?;
                if name.ends_with(".log") {
                    name.trim_end_matches(".log").parse::<u64>().ok()
                } else {
                    None
                }
            })
            .collect();

        base_offsets.sort_unstable();

        // If no existing segments, start fresh at offset 0.
        if base_offsets.is_empty() {
            base_offsets.push(0);
        }

        let segments = base_offsets
            .into_iter()
            .map(|base| Segment::open(dir, base, max_segment_bytes))
            .collect::<io::Result<Vec<_>>>()?;

        Ok(Self {
            dir: dir.to_path_buf(),
            segments,
            max_segment_bytes,
        })
    }

    /// Append a message to the log.
    ///
    /// If the active segment is full, a new segment is created first.
    /// Returns the absolute offset assigned to this message.
    pub fn append(&mut self, payload: &[u8]) -> io::Result<u64> {
        if self.active_segment().is_full() {
            self.roll()?;
        }
        self.active_segment_mut().append(payload)
    }

    /// Read the message at `offset`.
    ///
    /// Finds the correct segment via binary search, then delegates to it.
    /// Returns `None` if the offset doesn't exist in any segment.
    pub fn read(&mut self, offset: u64) -> io::Result<Option<Vec<u8>>> {
        let idx = self.find_segment(offset);
        self.segments[idx].read_at(offset)
    }

    /// The offset of the first message ever written (always the base of segment 0).
    pub fn oldest_offset(&self) -> u64 {
        self.segments[0].base_offset
    }

    /// The offset that the next `append` call will assign.
    pub fn next_offset(&self) -> u64 {
        self.active_segment().next_offset
    }

    /// Total number of segments (active + sealed).
    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }

    // ── internals ────────────────────────────────────────────────────────────

    fn active_segment(&self) -> &Segment {
        self.segments
            .last()
            .expect("log always has at least one segment")
    }

    fn active_segment_mut(&mut self) -> &mut Segment {
        self.segments
            .last_mut()
            .expect("log always has at least one segment")
    }

    /// Create a new segment whose base_offset is the current next_offset.
    fn roll(&mut self) -> io::Result<()> {
        let new_base = self.active_segment().next_offset;
        let seg = Segment::open(&self.dir, new_base, self.max_segment_bytes)?;
        self.segments.push(seg);
        Ok(())
    }

    /// Binary search for the segment that owns `offset`.
    ///
    /// Returns the index into `self.segments`. A segment owns offset `o` when:
    ///   segment.base_offset <= o < next_segment.base_offset
    ///
    /// The active segment catches everything >= its base_offset.
    fn find_segment(&self, offset: u64) -> usize {
        match self
            .segments
            .binary_search_by_key(&offset, |s| s.base_offset)
        {
            Ok(i) => i,                    // exact match on a base_offset
            Err(i) => i.saturating_sub(1), // offset falls inside segment i-1
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn tmp_dir(name: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("rafka_log_test_{}_{}", std::process::id(), name));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn append_and_read() {
        let dir = tmp_dir("append_and_read");
        let mut log = Log::open(&dir).unwrap();

        assert_eq!(log.append(b"first").unwrap(), 0);
        assert_eq!(log.append(b"second").unwrap(), 1);

        assert_eq!(log.read(0).unwrap().unwrap(), b"first");
        assert_eq!(log.read(1).unwrap().unwrap(), b"second");
    }

    #[test]
    fn missing_offset_returns_none() {
        let dir = tmp_dir("missing_offset");
        let mut log = Log::open(&dir).unwrap();
        log.append(b"only").unwrap();

        assert!(log.read(99).unwrap().is_none());
    }

    #[test]
    fn segment_rollover() {
        let dir = tmp_dir("rollover");
        // Each frame = 4 (len) + 8 (offset) + payload. "msgN" payload = 4 bytes → 16 bytes/frame.
        // max_bytes = 32 means the segment is full after exactly 2 messages.
        // The fullness check fires before the 3rd append, triggering rollover.
        let mut log = Log::open_with_max_bytes(&dir, 32).unwrap();

        log.append(b"msg0").unwrap(); // frame = 16 bytes, size = 16
        log.append(b"msg1").unwrap(); // frame = 16 bytes, size = 32 → full
        log.append(b"msg2").unwrap(); // is_full() = true → rollover, then write

        assert_eq!(log.segment_count(), 2);
        assert_eq!(log.read(2).unwrap().unwrap(), b"msg2");
    }

    #[test]
    fn read_across_segments() {
        let dir = tmp_dir("cross_segment");
        // Single-byte payloads: frame = 4 + 8 + 1 = 13 bytes. max_bytes = 26 → full after 2.
        let mut log = Log::open_with_max_bytes(&dir, 26).unwrap();

        log.append(b"a").unwrap(); // offset 0, segment 0, size = 13
        log.append(b"b").unwrap(); // offset 1, segment 0, size = 26 → full
        log.append(b"c").unwrap(); // offset 2, segment 1

        assert_eq!(log.read(0).unwrap().unwrap(), b"a");
        assert_eq!(log.read(1).unwrap().unwrap(), b"b");
        assert_eq!(log.read(2).unwrap().unwrap(), b"c");
    }

    #[test]
    fn survives_restart() {
        let dir = tmp_dir("log_restart");
        {
            let mut log = Log::open(&dir).unwrap();
            log.append(b"persisted").unwrap();
        }
        let mut log = Log::open(&dir).unwrap();
        assert_eq!(log.next_offset(), 1);
        assert_eq!(log.read(0).unwrap().unwrap(), b"persisted");
    }
}
