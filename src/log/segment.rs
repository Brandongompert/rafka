use std::{
    fs::{File, OpenOptions},
    io::{self, BufWriter, Read, Seek, SeekFrom, Write},
    path::Path,
};

use super::index::Index;

/// A single segment file in the log.
///
/// A segment is an append-only file on disk. It stores messages sequentially
/// in the format:
///
///   [length: u32][offset: u64][payload: bytes...]
///
/// Each segment has a paired `.index` file for O(1) offset lookups.
/// When a segment reaches `max_bytes`, a new segment is created.
pub struct Segment {
    /// The append-only file writer.
    writer: BufWriter<File>,
    /// A readable handle to the same file (for fetch operations).
    reader: File,
    /// The offset → byte-position index for this segment.
    index: Index,
    /// The offset of the first message in this segment.
    pub base_offset: u64,
    /// The offset that the *next* written message will receive.
    pub next_offset: u64,
    /// Current size of the file in bytes.
    pub size: u64,
    /// Maximum allowed size before this segment is considered full.
    pub max_bytes: u64,
}

impl Segment {
    /// Open or create a segment rooted at `dir` with the given `base_offset`.
    ///
    /// The file is named `{base_offset:020}.log` so lexicographic ordering
    /// equals offset ordering — useful when we need to find which segment
    /// holds a given offset.
    pub fn open(dir: &Path, base_offset: u64, max_bytes: u64) -> io::Result<Self> {
        let path = dir.join(format!("{:020}.log", base_offset));

        let writer_file = OpenOptions::new().create(true).append(true).open(&path)?;

        let size = writer_file.metadata()?.len();

        let reader = OpenOptions::new().read(true).open(&path)?;

        // Scan the file to find the true next_offset.
        // This handles restarts where the file already has messages.
        let next_offset = Self::recover_next_offset(&reader, base_offset)?;

        let index = Index::open(dir, base_offset)?;

        Ok(Self {
            writer: BufWriter::new(writer_file),
            reader,
            index,
            base_offset,
            next_offset,
            size,
            max_bytes,
        })
    }

    /// Append a message payload to this segment.
    ///
    /// Returns the offset assigned to this message.
    pub fn append(&mut self, payload: &[u8]) -> io::Result<u64> {
        let offset = self.next_offset;
        let position = self.size; // byte position of this message in the .log file

        // Write: [length: u32][offset: u64][payload]
        let length = (8 + payload.len()) as u32; // 8 bytes for the offset field
        self.writer.write_all(&length.to_be_bytes())?;
        self.writer.write_all(&offset.to_be_bytes())?;
        self.writer.write_all(payload)?;
        self.writer.flush()?;

        // Record offset → byte position in the index.
        self.index.append(offset, position)?;

        self.size += 4 + length as u64;
        self.next_offset += 1;

        Ok(offset)
    }

    /// Read a message at the given absolute `offset`.
    ///
    /// Uses the index for an O(1) seek directly to the message's byte position.
    pub fn read_at(&mut self, offset: u64) -> io::Result<Option<Vec<u8>>> {
        // Ask the index where this offset lives in the .log file.
        let Some(position) = self.index.lookup(offset)? else {
            return Ok(None);
        };

        self.reader.seek(SeekFrom::Start(position))?;

        // Read the length prefix.
        let mut len_buf = [0u8; 4];
        match self.reader.read_exact(&mut len_buf) {
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e),
        }
        let length = u32::from_be_bytes(len_buf) as usize;

        // Skip the stored offset field (8 bytes) — we already know it.
        self.reader.seek(SeekFrom::Current(8))?;

        // Read the payload.
        let payload_len = length - 8;
        let mut payload = vec![0u8; payload_len];
        self.reader.read_exact(&mut payload)?;

        Ok(Some(payload))
    }

    /// Returns true when the segment has reached its size limit.
    pub fn is_full(&self) -> bool {
        self.size >= self.max_bytes
    }

    /// Scan the file to determine the next available offset after a restart.
    fn recover_next_offset(reader: &File, base_offset: u64) -> io::Result<u64> {
        let mut reader = reader.try_clone()?;
        reader.seek(SeekFrom::Start(0))?;
        let mut last_offset = base_offset;
        let mut found_any = false;

        loop {
            let mut len_buf = [0u8; 4];
            match reader.read_exact(&mut len_buf) {
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }
            let length = u32::from_be_bytes(len_buf) as usize;

            let mut off_buf = [0u8; 8];
            reader.read_exact(&mut off_buf)?;
            last_offset = u64::from_be_bytes(off_buf);
            found_any = true;

            // Skip the payload bytes.
            let payload_len = (length - 8) as u64;
            reader.seek(SeekFrom::Current(payload_len as i64))?;
        }

        Ok(if found_any {
            last_offset + 1
        } else {
            base_offset
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn tmp_dir(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("rafka_test_{}_{}", std::process::id(), name));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn append_and_read() {
        let dir = tmp_dir("append_and_read");
        let mut seg = Segment::open(&dir, 0, 1024 * 1024).unwrap();

        let offset = seg.append(b"hello world").unwrap();
        assert_eq!(offset, 0);

        let msg = seg.read_at(0).unwrap().unwrap();
        assert_eq!(msg, b"hello world");
    }

    #[test]
    fn offsets_increment() {
        let dir = tmp_dir("offsets_increment");
        let mut seg = Segment::open(&dir, 0, 1024 * 1024).unwrap();

        assert_eq!(seg.append(b"first").unwrap(), 0);
        assert_eq!(seg.append(b"second").unwrap(), 1);
        assert_eq!(seg.append(b"third").unwrap(), 2);
    }

    #[test]
    fn read_nonexistent_offset_returns_none() {
        let dir = tmp_dir("read_nonexistent");
        let mut seg = Segment::open(&dir, 0, 1024 * 1024).unwrap();
        seg.append(b"only message").unwrap();

        assert!(seg.read_at(99).unwrap().is_none());
    }

    #[test]
    fn survives_restart() {
        let dir = tmp_dir("survives_restart");

        {
            let mut seg = Segment::open(&dir, 0, 1024 * 1024).unwrap();
            seg.append(b"before restart").unwrap();
            seg.append(b"also before").unwrap();
        }

        // Re-open simulates a broker restart.
        let mut seg = Segment::open(&dir, 0, 1024 * 1024).unwrap();
        assert_eq!(seg.next_offset, 2); // picks up where it left off

        let msg = seg.read_at(0).unwrap().unwrap();
        assert_eq!(msg, b"before restart");
    }
}
