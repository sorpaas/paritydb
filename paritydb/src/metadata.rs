use std::collections::HashSet;

use bit_vec::BitVec;

use prefix_tree::PrefixTree;

/// A structure holding database metadata information.
///
/// Currently we store a prefix tree for fast lookups and iterations
/// and number of bytes occupied by records for determining if
/// key prefix should be increased.
#[derive(Debug, Clone)]
pub struct Metadata {
	/// Database version
	pub db_version: u16,
	/// Number of bytes occupied by records
	/// NOTE: it does not include field headers!
	pub occupied_bytes: u64,
	/// Prefix tree
	pub prefixes: PrefixTree,
	/// Prefixes with too many collisions that are stored separately
	pub collided_prefixes: BitVec<u8>,
}

impl Metadata {
	pub const DB_VERSION: u16 = 0;

	/// Notify that record was inserted.
	pub fn insert_record(&mut self, prefix: u32, len: usize) {
		self.occupied_bytes += len as u64;
		self.prefixes.insert(prefix);
	}

	/// Notify that record was removed.
	///
	/// We can't simply remove prefix from db, cause there might be
	/// more records with the same prefix in the database.
	pub fn remove_record(&mut self, len: usize) {
		self.occupied_bytes -= len as u64;
	}

	/// Notify that record was overwritten.
	pub fn update_record_len(&mut self, old_len: usize, new_len: usize) {
		self.occupied_bytes -= old_len as u64;
		self.occupied_bytes += new_len as u64;
	}

	pub fn add_prefix_collision(&mut self, prefix: u32) {
		self.collided_prefixes.set(prefix as usize, true);
	}

	pub fn collided_prefixes(&self) -> Vec<u32> {
		self.collided_prefixes.iter().enumerate().filter_map(|(i, b)| {
			if b { Some(i as u32) } else { None }
		}).collect()
	}

	/// Returns bytes representation of `Metadata`.
	pub fn as_bytes(&self) -> bytes::Metadata {
		bytes::Metadata::new(self)
	}
}

/// Metadata bytes manipulations.
pub mod bytes {
	use std::collections::HashSet;
	use std::io::{Cursor, Read, Write};

	use bit_vec::BitVec;
	use byteorder::{LittleEndian, ByteOrder, ReadBytesExt, WriteBytesExt};

	use prefix_tree::PrefixTree;

	/// Bytes representation of `Metadata`.
	pub struct Metadata<'a> {
		metadata: &'a super::Metadata,
	}

	impl<'a> Metadata<'a> {
		const VERSION_SIZE: usize = 2;
		const OCCUPIED_SIZE: usize = 8;

		/// Create new.
		pub fn new(metadata: &'a super::Metadata) -> Self {
			Metadata { metadata }
		}

		/// Copy bytes to given slice.
		/// Panics if the length are not matching.
		pub fn copy_to_slice(&self, data: &mut [u8]) {
			let mut cursor = Cursor::new(data);
			cursor.write_u16::<LittleEndian>(self.metadata.db_version);
			cursor.write_u64::<LittleEndian>(self.metadata.occupied_bytes);
			cursor.write_all(self.metadata.collided_prefixes.storage());

			let leaves = self.metadata.prefixes.leaves();
			cursor.write_all(leaves);
		}

		/// Return bytes length of the `Metadata`.
		pub fn len(&self) -> usize {
			len(self.metadata.prefixes.prefix_bits())
		}
	}

	/// Returns expected `Metadata` bytes len given prefix bits.
	pub fn len(prefix_bits: u8) -> usize {
		Metadata::VERSION_SIZE +
			Metadata::OCCUPIED_SIZE +
			((2 << (prefix_bits - 1)) / 8) + // for the collided_prefixes bitvec
			PrefixTree::leaf_data_len(prefix_bits)
	}

	/// Read `Metadata` from given slice.
	pub fn read(data: &[u8], prefix_bits: u8) -> super::Metadata {
		let mut cursor = Cursor::new(data);
		let db_version = cursor.read_u16::<LittleEndian>().unwrap();
		let occupied_bytes = cursor.read_u64::<LittleEndian>().unwrap();

		let collided_prefixes_len = (2 << (prefix_bits - 1)) / 8;
		let mut collided_prefixes_buf = vec![0; collided_prefixes_len];
		cursor.read_exact(&mut collided_prefixes_buf);

		// FIXME: clean this up
		let mut collided_prefixes = BitVec::default();
		collided_prefixes.grow(2 << (prefix_bits - 1), false);
		for (idx, byte) in collided_prefixes_buf.iter().enumerate() {
			let mut current = 1;
			for i in 0..8 {
				if byte & current == current {
					collided_prefixes.set((idx * 8 + i), true);
				}
				current <<= 1;
			}
		}

		let prefixes_len = PrefixTree::leaf_data_len(prefix_bits);
		let mut prefixes_buf = vec![0; prefixes_len];
		cursor.read_exact(&mut prefixes_buf);
		let prefixes = PrefixTree::from_leaves(&prefixes_buf, prefix_bits);

		assert_eq!(db_version, super::Metadata::DB_VERSION);

		super::Metadata {
			db_version,
			occupied_bytes,
			prefixes,
			collided_prefixes,
		}
	}
}
