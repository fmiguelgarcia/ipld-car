use crate::bounded_reader::{
	error::BoundedReaderErr,
	sync::BoundedReader,
	traits::{Bounded, BoundedIndex, CloneAndRewind},
};

use std::{
	cmp::min,
	io::{self, Read, Seek, SeekFrom},
};

/// A sequential composition of multiple [`BoundedReader`]s that presents them as a single
/// contiguous stream.
///
/// Implements [`Read`] and [`Seek`] as if all inner readers were concatenated. Reads advance
/// through segments in order; seeks operate on the virtual flat offset space.
#[derive(derive_more::Debug, Clone)]
pub struct ChainedBoundedReader<T> {
	readers: Vec<BoundedReader<T>>,
	/// Current position in the composed virtual stream.
	curr: u64,
	/// Cached sum of all inner readers' lengths.
	total_len: u64,
}

impl<T> ChainedBoundedReader<T> {
	/// Creates a new composed reader from a list of bounded readers.
	pub fn new(readers: Vec<BoundedReader<T>>) -> Self {
		let total_len = readers.iter().map(BoundedReader::bound_len).sum();
		Self { readers, curr: 0, total_len }
	}

	/// Creates an empty reader.
	pub const fn empty() -> Self {
		Self { readers: vec![], curr: 0, total_len: 0 }
	}
}

impl<T> From<BoundedReader<T>> for ChainedBoundedReader<T> {
	fn from(r: BoundedReader<T>) -> Self {
		let total_len = r.bound_len();
		Self { readers: vec![r], curr: 0, total_len }
	}
}

impl<T> Bounded for ChainedBoundedReader<T> {
	fn bounds(&self) -> std::ops::Range<u64> {
		let (starts, ends): (Vec<u64>, Vec<u64>) = self
			.readers
			.iter()
			.map(|r| {
				let b = r.bounds();
				(b.start, b.end)
			})
			.unzip();

		let min_start = starts.into_iter().min().unwrap_or_default();
		let max_end = ends.into_iter().max().unwrap_or_default();
		min_start..max_end
	}

	fn bound_len(&self) -> u64 {
		self.total_len
	}

	/// Creates a new bounded reader that is a sub-range of this one.
	fn sub<R: BoundedIndex<Self>>(&self, range: R) -> Result<Self, BoundedReaderErr> {
		range.get(self)
	}

	fn clamped_sub<R: BoundedIndex<Self>>(&self, range: R) -> Self {
		range.clamped_get(self)
	}
}

impl<T> CloneAndRewind for ChainedBoundedReader<T> {
	fn clone_and_rewind(&self) -> Self {
		Self { readers: self.readers.clone(), curr: 0, total_len: self.total_len }
	}
}

impl<T: Read + Seek> Read for ChainedBoundedReader<T> {
	fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
		if buf.is_empty() || self.curr >= self.total_len {
			return Ok(0);
		}

		let mut offset = self.curr;
		for reader in &mut self.readers {
			let len = reader.bound_len();
			if offset < len {
				reader.seek(SeekFrom::Start(offset))?;
				let max_read = min(buf.len(), (len - offset) as usize);
				let n = reader.read(&mut buf[..max_read])?;
				self.curr += n as u64;
				return Ok(n);
			}
			offset -= len;
		}

		Ok(0)
	}
}

impl<T: Seek> Seek for ChainedBoundedReader<T> {
	fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
		let new_pos = match pos {
			SeekFrom::Start(n) => n,
			SeekFrom::End(n) =>
				if n >= 0 {
					self.total_len.saturating_add(n as u64)
				} else {
					self.total_len.saturating_sub(n.unsigned_abs())
				},
			SeekFrom::Current(n) =>
				if n >= 0 {
					self.curr.saturating_add(n as u64)
				} else {
					self.curr.saturating_sub(n.unsigned_abs())
				},
		};
		self.curr = new_pos.min(self.total_len);
		Ok(self.curr)
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::io::Cursor;
	use test_case::test_case;

	#[test_case(
		vec![b"Hello ", b"world"]
		=> b"Hello world".to_vec(); "two segments full read"
	)]
	#[test_case(
		vec![b"foo", b"bar", b"baz"]
		=> b"foobarbaz".to_vec(); "three segments full read"
	)]
	#[test_case(
		vec![b"", b"hello"]
		=> b"hello".to_vec(); "empty first segment"
	)]
	fn composed_read(segments: Vec<&[u8]>) -> Vec<u8> {
		let readers = segments
			.into_iter()
			.map(|data| BoundedReader::from_reader(Cursor::new(data)).unwrap())
			.collect::<Vec<_>>();
		let mut composed = ChainedBoundedReader::new(readers);
		let mut out = vec![];
		composed.read_to_end(&mut out).unwrap();
		out
	}

	#[test]
	fn composed_seek_from_start() {
		let readers = vec![
			BoundedReader::from_reader(Cursor::new(b"Hello ".to_vec())).unwrap(),
			BoundedReader::from_reader(Cursor::new(b"world".to_vec())).unwrap(),
		];
		let mut composed = ChainedBoundedReader::new(readers);
		composed.seek(SeekFrom::Start(6)).unwrap();
		let mut out = vec![];
		composed.read_to_end(&mut out).unwrap();
		assert_eq!(out, b"world");
	}

	#[test]
	fn composed_seek_from_end() {
		let readers = vec![
			BoundedReader::from_reader(Cursor::new(b"Hello ".to_vec())).unwrap(),
			BoundedReader::from_reader(Cursor::new(b"world".to_vec())).unwrap(),
		];
		let mut composed = ChainedBoundedReader::new(readers);
		composed.seek(SeekFrom::End(-5)).unwrap();
		let mut out = vec![];
		composed.read_to_end(&mut out).unwrap();
		assert_eq!(out, b"world");
	}

	#[test]
	fn composed_seek_from_current() {
		let readers = vec![
			BoundedReader::from_reader(Cursor::new(b"Hello ".to_vec())).unwrap(),
			BoundedReader::from_reader(Cursor::new(b"world".to_vec())).unwrap(),
		];
		let mut composed = ChainedBoundedReader::new(readers);
		composed.seek(SeekFrom::Start(3)).unwrap();
		composed.seek(SeekFrom::Current(3)).unwrap();
		let mut out = vec![];
		composed.read_to_end(&mut out).unwrap();
		assert_eq!(out, b"world");
	}
}
