use crate::BoundedReader;

use bytes::Bytes;
use derive_more::From;
use std::io::{Cursor, Read, Seek};

#[derive(From, derive_more::Debug)]
pub enum SingleBlockFile<T> {
	Data(Bytes),
	Reader(BoundedReader<T>),
}
impl<T> SingleBlockFile<T> {
	pub fn len(&self) -> u64 {
		match self {
			Self::Data(data) => data.len() as u64,
			Self::Reader(reader) => reader.bound_len(),
		}
	}

	pub fn is_empty(&self) -> bool {
		self.len() == 0
	}
}

#[cfg(feature = "vfs")]
impl<T: Read + Seek + Send + 'static> SingleBlockFile<T> {
	pub fn reader(&self) -> Box<dyn vfs::SeekAndRead + Send> {
		match self {
			Self::Data(data) => Box::new(Cursor::new(data.clone())),
			Self::Reader(reader) => Box::new(reader.clone_and_rewind()),
		}
	}
}
