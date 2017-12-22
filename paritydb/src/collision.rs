use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{self, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use error::Result;
use transaction::Operation;

/// A data file representing all the data for a given prefix. All the data for
/// this prefix exists in this file because there was a high threshold of
/// collisions.
///
/// The data file is a log file backed by an in-memory BTree. All mutable
/// operations are appended to the log file, and the in-memory BTree maps the
/// keys to their position in the log file.
///
/// Idea: grow the log file in chunks (instead of appending to a file) and mmap
/// it. When compacting the log we rewrite it with the keys sorted (since we
/// have the in-memory index), so iteration should be fast on compacted log
/// files.
///
/// Alternative: use exactly the same strategy as used for the data file but
/// ignoring the first `n` bits of the prefix and adding extra bits as needed
///
pub struct Collision {
	index: BTreeMap<Vec<u8>, IndexEntry>,
	prefix: u32,
	path: PathBuf,
	file: File,
}

#[derive(Debug)]
pub struct IndexEntry {
    position: u64,
	// TODO: we can optimize our implementation for constant value sizes
    size: usize,
}

impl Collision {
	fn collision_file_path<P: AsRef<Path>>(path: P, prefix: u32) -> PathBuf {
		let collision_file_name = format!("collision-{}.db", prefix);
		path.as_ref().join(collision_file_name)
	}

	fn build_index<P: AsRef<Path>>(path: P) -> Result<BTreeMap<Vec<u8>, IndexEntry>> {
		let log = LogIterator::new(path)?;

		let mut index = BTreeMap::new();
		for entry in log {
			let entry = entry?;
			let position = entry.position;
			let size = LogEntry::len(&entry.key, &entry.value);
			index.insert(entry.key, IndexEntry { position, size });
		}

		println!("Index: {:?}", index);

		Ok(index)
	}

	pub fn create<P: AsRef<Path>>(path: P, prefix: u32) -> Result<Collision> {
		// Create directories if necessary.
		fs::create_dir_all(&path)?;

		let path = Self::collision_file_path(path, prefix);
		let file = fs::OpenOptions::new()
			.append(true)
			.create_new(true)
			.open(&path)?;

		let index = BTreeMap::new();

		Ok(Collision { index, prefix, path, file })
	}

	pub fn open<P: AsRef<Path>>(path: P, prefix: u32) -> Result<Option<Collision>> {
		let path = Self::collision_file_path(path, prefix);
		let open_options = fs::OpenOptions::new()
			.append(true)
			.open(&path);

		let file = match open_options {
			Ok(file) => file,
			Err(ref err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
			Err(err) => return Err(err.into()),
		};

		let index = Collision::build_index(&path)?;

		Ok(Some(Collision { index, prefix, path, file }))
	}

	pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
		let position = LogEntry::write(&mut self.file, key, value)?;
		let size = LogEntry::len(&key, &value);

		self.index.insert(key.to_vec(), IndexEntry { position, size });

		Ok(())
	}

	pub fn delete(&mut self, key: &[u8]) -> Result<()> {
		unimplemented!()
	}

	pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
		if let Some(entry) = self.index.get(key) {
			// TODO: cache file descriptors if necessary
			let file = fs::OpenOptions::new()
				.read(true)
				.open(&self.path)?;

			let mut file = BufReader::new(file);
			file.seek(SeekFrom::Start(entry.position))?;

			let entry = LogEntry::read(&mut file)?;

			assert!(entry.key == key);

			Ok(Some(entry.value))
		} else {
			Ok(None)
		}
	}

	pub fn apply(&mut self, op: Operation) -> Result<()> {
		match op {
			Operation::Delete(key) => self.delete(key),
			Operation::Insert(key, value) => self.put(key, value),
		}
	}
}

struct LogEntry {
	position: u64,
	key: Vec<u8>,
	value: Vec<u8>,
}

impl LogEntry {
	const ENTRY_STATIC_SIZE: usize = 8; // key_size(4) + value_size(4)

	fn write<W: Write + Seek>(writer: &mut W, key: &[u8], value: &[u8]) -> Result<u64> {
		let position = writer.seek(SeekFrom::Current(0))?;
		writer.write_u32::<LittleEndian>(key.len() as u32)?;
		writer.write_all(key)?;
		writer.write_u32::<LittleEndian>(value.len() as u32)?;
		writer.write_all(value)?;
		Ok(position)
	}

	fn read<R: Read + Seek>(reader: &mut R) -> io::Result<LogEntry> {
		let position = reader.seek(SeekFrom::Current(0))?;
		let key_size = reader.read_u32::<LittleEndian>()?;
		let mut key = vec![0u8; key_size as usize];
		reader.read_exact(&mut key)?;
		let value_size = reader.read_u32::<LittleEndian>()?;
		let mut value = vec![0u8; value_size as usize];
		reader.read_exact(&mut value)?;
		Ok(LogEntry { position, key, value })
	}

	fn len(key: &[u8], value: &[u8]) -> usize {
		LogEntry::ENTRY_STATIC_SIZE + key.len() + value.len()
	}
}

struct LogIterator {
	file: BufReader<File>,
}

impl LogIterator {
	fn new<P: AsRef<Path>>(path: P) -> Result<LogIterator> {
		let file = fs::OpenOptions::new()
			.read(true)
			.open(&path)?;

		Ok(LogIterator { file: BufReader::new(file) })
	}
}

impl Iterator for LogIterator {
	type Item = Result<LogEntry>;

	fn next(&mut self) -> Option<Result<LogEntry>> {
		match LogEntry::read(&mut self.file) {
			Err(ref err) if err.kind() == io::ErrorKind::UnexpectedEof => None,
			Err(err) => Some(Err(err.into())),
			Ok(res) => Some(Ok(res)),
		}
	}
}

#[cfg(test)]
mod tests {
	extern crate tempdir;

	use super::Collision;

	#[test]
	fn test_roundtrip() {
		let temp = tempdir::TempDir::new("test_roundtrip").unwrap();

		{
			let mut collision = Collision::create(temp.path(), 0).unwrap();
			collision.put(b"hello", b"world").unwrap();
			assert_eq!(collision.get(b"hello").unwrap().unwrap(), b"world");
		}

		let mut collision = Collision::open(temp.path(), 0).unwrap().unwrap();
		assert_eq!(collision.get(b"hello").unwrap().unwrap(), b"world");
	}
}
