use crate::ensure;

use derivative::Derivative;
use std::{
	cmp::min,
	io::{self, Read, Seek, SeekFrom},
	ops::{Range, RangeFrom, RangeFull, RangeInclusive, RangeToInclusive},
	sync::{Arc, Mutex, MutexGuard},
};
use thiserror::Error;

/// Errors that can occur when working with bounded readers.
#[derive(Error, Debug)]
pub enum BoundedReaderErr {
	#[error("Bounded reader len is to large")]
	FileTooLarge,
	#[error("Sub bound exceeds limits of original bounds")]
	SubBoundExceedLimits,
	#[error("Range start MUST be less or equal to end")]
	InvalidRange,
	#[error("Shared reader's mutex is poisoned")]
	ReaderPoisoned,
	#[error("Mandatory seek failed: {0:?}")]
	Seek(io::Error),
}

/// Proxy reader providing bounded read access to a range of the inner reader.
///
/// It allows you to have several bounded reader over one shared.
/// Clone is cheap and it tries to avoid the use of the inner shared reader, for instance
/// `BoundedReader::clone_and_rewind` does NOT move the inner reader because it only set member
/// `curr` to zero.
#[derive(derive_more::Debug, Derivative)]
#[derivative(Clone)]
pub struct BoundedReader<T> {
	#[debug(skip)]
	#[derivative(Clone(bound = ""))]
	reader: Arc<Mutex<T>>,
	start: u64,
	end: u64,
	/// Current position.
	curr: u64,
}

impl<T> BoundedReader<T> {
	/// Creates a new bounded reader wrapping the shared reader, restricted to the given range.
	///
	/// # NOTE
	/// It does NOT check that `range` is valid in `reader`, that will fail during read/seek
	/// operations.
	pub fn new(reader: Arc<Mutex<T>>, range: Range<u64>) -> Result<Self, BoundedReaderErr> {
		ensure!(range.start <= range.end, BoundedReaderErr::InvalidRange);
		Ok(Self { reader, start: range.start, end: range.end, curr: 0 })
	}

	/// # Safety
	///
	/// This function should NOT be called with an invalid range (`range.start > range.end`)
	pub unsafe fn new_unchecked(reader: Arc<Mutex<T>>, range: Range<u64>) -> Self {
		Self { reader, start: range.start, end: range.end, curr: 0 }
	}

	/// Returns the absolute range this bounded reader is restricted to.
	pub fn bounds(&self) -> Range<u64> {
		self.start..self.end
	}

	/// Returns the length of the bounded range.
	pub fn bound_len(&self) -> u64 {
		self.end - self.start
	}

	/// Creates a new bounded reader that is a sub-range of this one.
	pub fn sub<R: BoundedIndex<T>>(&self, range: R) -> Result<Self, BoundedReaderErr> {
		range.get(self)
	}

	pub fn clamped_sub<R: BoundedIndex<T>>(&self, range: R) -> Self {
		range.clamped_get(self)
	}

	/// Clones this bounded reader and resets the read position to the start of the range.
	pub fn clone_and_rewind(&self) -> Self {
		Self { reader: Arc::clone(&self.reader), start: self.start, end: self.end, curr: 0 }
	}
}

impl<T> BoundedReader<T> {
	/// Convert relative position to absolute position bounded by file range.
	fn relative_to_abs(&self, relative: u64) -> u64 {
		min(relative.saturating_add(self.start), self.end)
	}

	/// Convert absolute position to relative position within file range.
	fn abs_to_relative(&self, abs: u64) -> u64 {
		let bounded_abs = abs.clamp(self.start, self.end);
		bounded_abs - self.start
	}

	/// Calculate absolute position from offset relative to end of range.
	fn abs_offset_from_end(&self, offset: i64) -> u64 {
		let unbounded_offset = if offset > 0 {
			self.end.saturating_add(offset as u64)
		} else {
			self.end.saturating_sub(offset.unsigned_abs())
		};
		unbounded_offset.clamp(self.start, self.end)
	}

	/// Calculate relative position from offset relative to current position.
	fn offset_from_curr(&self, offset: i64) -> u64 {
		let offset = if offset > 0 {
			self.curr.saturating_add(offset as u64)
		} else {
			self.curr.saturating_sub(offset.unsigned_abs())
		};
		self.abs_to_relative(self.relative_to_abs(offset))
	}

	/// Calculate remaining bytes until end of range.
	fn remaining(&self) -> usize {
		let remaining = self.end.saturating_sub(self.relative_to_abs(self.curr));
		usize::try_from(remaining).unwrap_or(usize::MAX)
	}

	fn lock_reader(&self) -> io::Result<MutexGuard<'_, T>> {
		self.reader.lock().map_err(|poison| io::Error::other(poison.to_string()))
	}
}

impl<T: Seek> BoundedReader<T> {
	/// Creates a bounded reader covering the entire seekable reader.
	pub fn from_reader(mut reader: T) -> Result<Self, io::Error> {
		let end = reader.seek(SeekFrom::End(0))?;
		Ok(Self { reader: Arc::new(Mutex::new(reader)), start: 0, end, curr: 0 })
	}

	/// Creates a bounded reader covering the entire shared seekable reader.
	pub fn from_shared_reader(reader: &Arc<Mutex<T>>) -> Result<Self, BoundedReaderErr> {
		let end = reader
			.lock()
			.map_err(|_| BoundedReaderErr::ReaderPoisoned)?
			.seek(SeekFrom::End(0))
			.map_err(BoundedReaderErr::Seek)?;
		Ok(Self { reader: Arc::clone(reader), start: 0, end, curr: 0 })
	}
}

impl<T: Read + Seek> Read for BoundedReader<T> {
	/// Read bytes within the bounded range from the storage file.
	fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
		let max_read = min(buf.len(), self.remaining());
		if max_read == 0 {
			return Ok(0); // EOF
		}

		let abs_curr = self.relative_to_abs(self.curr);
		let mut locked_reader = self.lock_reader()?;
		locked_reader.seek(SeekFrom::Start(abs_curr))?;
		let n = locked_reader.read(&mut buf[..max_read])?;
		drop(locked_reader);

		self.curr = self.curr.saturating_add(n as u64);
		Ok(n)
	}
}

impl<T: Seek> Seek for BoundedReader<T> {
	/// Seek within the bounded range relative to start, current position, or end.
	fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
		self.curr = match pos {
			SeekFrom::Start(offset) => self.abs_to_relative(self.relative_to_abs(offset)),
			SeekFrom::End(offset) => self.abs_to_relative(self.abs_offset_from_end(offset)),
			SeekFrom::Current(offset) => self.offset_from_curr(offset),
		};

		Ok(self.curr)
	}
}

/// Trait for types that can be used as sub-ranges of a bounded reader.
pub trait BoundedIndex<T> {
	fn get(self, bounded: &BoundedReader<T>) -> Result<BoundedReader<T>, BoundedReaderErr>;
	fn clamped_get(self, bounded: &BoundedReader<T>) -> BoundedReader<T>;
}

impl<T> BoundedIndex<T> for Range<u64> {
	fn get(self, bounded: &BoundedReader<T>) -> Result<BoundedReader<T>, BoundedReaderErr> {
		let start = bounded.start.checked_add(self.start).ok_or(BoundedReaderErr::FileTooLarge)?;
		ensure!(start <= bounded.end, BoundedReaderErr::SubBoundExceedLimits);
		let end = bounded.start.checked_add(self.end).ok_or(BoundedReaderErr::FileTooLarge)?;
		ensure!(end <= bounded.end, BoundedReaderErr::SubBoundExceedLimits);

		let reader = Arc::clone(&bounded.reader);
		BoundedReader::new(reader, start..end)
	}

	fn clamped_get(self, bounded: &BoundedReader<T>) -> BoundedReader<T> {
		let start = bounded.start.saturating_add(self.start);
		let clamped_start = min(start, bounded.end);
		let end = bounded.start.saturating_add(self.end);
		let clamped_end = min(end, bounded.end);
		let clamped_start = min(clamped_start, clamped_end);

		let reader = Arc::clone(&bounded.reader);
		// SAFETY: Boundaries are clamped some lines before.
		unsafe { BoundedReader::new_unchecked(reader, clamped_start..clamped_end) }
	}
}

impl<T> BoundedIndex<T> for RangeFrom<u64> {
	fn get(self, bounded: &BoundedReader<T>) -> Result<BoundedReader<T>, BoundedReaderErr> {
		let start = bounded.start.checked_add(self.start).ok_or(BoundedReaderErr::FileTooLarge)?;
		ensure!(start <= bounded.end, BoundedReaderErr::SubBoundExceedLimits);
		let reader = Arc::clone(&bounded.reader);
		BoundedReader::new(reader, start..bounded.end)
	}

	fn clamped_get(self, bounded: &BoundedReader<T>) -> BoundedReader<T> {
		let start = bounded.start.saturating_add(self.start);
		let clamped_start = min(start, bounded.end);

		let reader = Arc::clone(&bounded.reader);
		unsafe { BoundedReader::new_unchecked(reader, clamped_start..bounded.end) }
	}
}

impl<T> BoundedIndex<T> for RangeInclusive<u64> {
	fn get(self, bounded: &BoundedReader<T>) -> Result<BoundedReader<T>, BoundedReaderErr> {
		let (start, inc_end) = self.into_inner();
		let range = start..(inc_end.checked_add(1).ok_or(BoundedReaderErr::FileTooLarge)?);
		range.get(bounded)
	}

	fn clamped_get(self, bounded: &BoundedReader<T>) -> BoundedReader<T> {
		let (start, inc_end) = self.into_inner();
		let range = start..(inc_end.saturating_add(1));
		range.clamped_get(bounded)
	}
}

impl<T> BoundedIndex<T> for RangeToInclusive<u64> {
	fn get(self, bounded: &BoundedReader<T>) -> Result<BoundedReader<T>, BoundedReaderErr> {
		let end = self.end.checked_add(1).ok_or(BoundedReaderErr::FileTooLarge)?;
		let range = bounded.start..end;
		range.get(bounded)
	}

	fn clamped_get(self, bounded: &BoundedReader<T>) -> BoundedReader<T> {
		let end = self.end.saturating_add(1);
		let clamped_end = min(end, bounded.end);
		let range = bounded.start..clamped_end;
		range.clamped_get(bounded)
	}
}

impl<T> BoundedIndex<T> for RangeFull {
	fn get(self, bounded: &BoundedReader<T>) -> Result<BoundedReader<T>, BoundedReaderErr> {
		Ok(BoundedReader { reader: Arc::clone(&bounded.reader), start: bounded.start, end: bounded.end, curr: 0 })
	}

	fn clamped_get(self, bounded: &BoundedReader<T>) -> BoundedReader<T> {
		BoundedReader { reader: Arc::clone(&bounded.reader), start: bounded.start, end: bounded.end, curr: 0 }
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::io::{Cursor, Read};
	use test_case::test_case;

	#[allow(unused_parens)]
	#[test_case( b"Hello world", 0..4 => b"Hell".to_vec(); "From 0 to 4")]
	#[test_case( b"Hello world", 2..=4 => b"llo".to_vec(); "From 2 to =4")]
	#[test_case( b"Hello world", ..=4 => b"Hello".to_vec(); "to =4")]
	#[test_case( b"Hello world", (0..) => b"Hello world".to_vec(); "From 0 to .." )]
	#[test_case( b"Hello world", (6..) => b"world".to_vec(); "From 5 to .." )]
	#[test_case( b"Hello world", (..) => b"Hello world".to_vec(); "full range" )]
	fn bounded_index<D, R>(data: D, range: R) -> Vec<u8>
	where
		D: AsRef<[u8]>,
		R: BoundedIndex<Cursor<D>>,
	{
		let bounded = BoundedReader::from_reader(Cursor::new(data)).unwrap();
		let mut sub = bounded.sub(range).unwrap();

		let mut sub_content = vec![];
		let _ = sub.read_to_end(&mut sub_content).unwrap();
		sub_content
	}
}
