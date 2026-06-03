use std::{
    fs::{File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::Path,
};

/// The number of bytes per index entry.
const ENTRY_SIZE: u64 = 12; // 4 (relative offset) + 8 (position)

/// An append-only index file that maps message offsets to byte positions
/// in the corresponding `.log` segment file.
///
/// Each entry is 12 bytes:
///
///   [relative_offset: u32][position: u64]
///
/// `relative_offset` is stored relative to `base_offset` so we can use u32
/// (max ~4 billion messages per segment) instead of u64, keeping entries small.
///
/// Because entries are fixed-size, lookup is O(log n) via binary search
/// rather than a linear scan. Reads on the segment become a single seek.
pub struct Index {
    file: File,
    /// The base offset of the owning segment (used to convert relative ↔ absolute).
    base_offset: u64,
    /// Number of entries currently in the index.
    pub entries: u64,
}

impl Index {
    /// Open or create an index file alongside the segment.
    ///
    /// The file is named `{base_offset:020}.index`.
    pub fn open(dir: &Path, base_offset: u64) -> io::Result<Self> {
        let path = dir.join(format!("{:020}.index", base_offset));

        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&path)?;

        let entries = file.metadata()?.len() / ENTRY_SIZE;

        Ok(Self {
            file,
            base_offset,
            entries,
        })
    }

    /// Append an entry mapping `absolute_offset` → `position`.
    ///
    /// `position` is the byte offset in the `.log` file where this message starts.
    pub fn append(&mut self, absolute_offset: u64, position: u64) -> io::Result<()> {
        let relative = (absolute_offset - self.base_offset) as u32;
        self.file.write_all(&relative.to_be_bytes())?;
        self.file.write_all(&position.to_be_bytes())?;
        self.entries += 1;
        Ok(())
    }

    /// Look up the byte position of `absolute_offset` using binary search.
    ///
    /// Returns `None` if the offset is not in this index.
    pub fn lookup(&mut self, absolute_offset: u64) -> io::Result<Option<u64>> {
        if self.entries == 0 {
            return Ok(None);
        }

        let target = (absolute_offset - self.base_offset) as u32;

        let mut lo = 0u64;
        let mut hi = self.entries - 1;

        while lo <= hi {
            let mid = lo + (hi - lo) / 2;
            let (rel, pos) = self.read_entry(mid)?;

            match rel.cmp(&target) {
                std::cmp::Ordering::Equal => return Ok(Some(pos)),
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => {
                    if mid == 0 {
                        break;
                    }
                    hi = mid - 1;
                }
            }
        }

        Ok(None)
    }

    /// Read the entry at index position `n` (zero-based).
    fn read_entry(&mut self, n: u64) -> io::Result<(u32, u64)> {
        self.file.seek(SeekFrom::Start(n * ENTRY_SIZE))?;

        let mut rel_buf = [0u8; 4];
        self.file.read_exact(&mut rel_buf)?;
        let relative = u32::from_be_bytes(rel_buf);

        let mut pos_buf = [0u8; 8];
        self.file.read_exact(&mut pos_buf)?;
        let position = u64::from_be_bytes(pos_buf);

        Ok((relative, position))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn tmp_dir(name: &str) -> std::path::PathBuf {
        let dir =
            std::env::temp_dir().join(format!("rafka_index_test_{}_{}", std::process::id(), name));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn append_and_lookup() {
        let dir = tmp_dir("append_and_lookup");
        let mut idx = Index::open(&dir, 0).unwrap();

        idx.append(0, 0).unwrap();
        idx.append(1, 23).unwrap();
        idx.append(2, 46).unwrap();

        assert_eq!(idx.lookup(0).unwrap(), Some(0));
        assert_eq!(idx.lookup(1).unwrap(), Some(23));
        assert_eq!(idx.lookup(2).unwrap(), Some(46));
    }

    #[test]
    fn lookup_missing_returns_none() {
        let dir = tmp_dir("lookup_missing");
        let mut idx = Index::open(&dir, 0).unwrap();

        idx.append(0, 0).unwrap();

        assert_eq!(idx.lookup(99).unwrap(), None);
    }

    #[test]
    fn non_zero_base_offset() {
        let dir = tmp_dir("non_zero_base");
        let mut idx = Index::open(&dir, 1000).unwrap();

        idx.append(1000, 0).unwrap();
        idx.append(1001, 50).unwrap();
        idx.append(1002, 100).unwrap();

        assert_eq!(idx.lookup(1001).unwrap(), Some(50));
    }

    #[test]
    fn survives_restart() {
        let dir = tmp_dir("index_restart");
        {
            let mut idx = Index::open(&dir, 0).unwrap();
            idx.append(0, 0).unwrap();
            idx.append(1, 23).unwrap();
        }
        let mut idx = Index::open(&dir, 0).unwrap();
        assert_eq!(idx.entries, 2);
        assert_eq!(idx.lookup(1).unwrap(), Some(23));
    }
}
