use crate::{
	bounded_reader::{
		error::BoundedReaderErr,
		traits::{Bounded, BoundedIndex, CloneAndRewind},
	},
	ensure,
};

use bytes::Bytes;
use std::{
	cmp::min,
	io::{self, Cursor, Read, Seek, SeekFrom},
	ops::{Range, RangeFrom, RangeFull, RangeInclusive, RangeToInclusive},
};

static RANGE_VALID_QED: &str = "Range is limited to previous valid values .qed";

#[derive(Debug, Clone)]
pub struct MemBoundedReader {
	content: Bytes,
	reader: Cursor<Bytes>,
}

impl MemBoundedReader {
	pub fn new(content: Bytes) -> Self {
		let reader = Cursor::new(content.clone());
		Self { content, reader }
	}
}

impl Read for MemBoundedReader {
	fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
		self.reader.read(buf)
	}
}

impl Seek for MemBoundedReader {
	fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
		self.reader.seek(pos)
	}
}

impl From<Bytes> for MemBoundedReader {
	fn from(content: Bytes) -> Self {
		Self::new(content)
	}
}

impl Bounded for MemBoundedReader {
	fn bounds(&self) -> Range<u64> {
		0u64..self.content.len() as u64
	}

	/// Returns the length of the bounded range.
	fn bound_len(&self) -> u64 {
		self.content.len() as u64
	}

	/// Creates a new bounded reader that is a sub-range of this one.
	fn sub<R: BoundedIndex<Self>>(&self, range: R) -> Result<Self, BoundedReaderErr> {
		range.get(self)
	}

	fn clamped_sub<R: BoundedIndex<Self>>(&self, range: R) -> Self {
		range.clamped_get(self)
	}
}

impl CloneAndRewind for MemBoundedReader {
	fn clone_and_rewind(&self) -> Self {
		Self::new(self.content.clone())
	}
}

impl BoundedIndex<MemBoundedReader> for Range<u64> {
	fn get(self, bounded: &MemBoundedReader) -> Result<MemBoundedReader, BoundedReaderErr> {
		ensure!(self.start <= self.end, BoundedReaderErr::InvalidRange);
		ensure!(self.start <= bounded.content.len() as u64, BoundedReaderErr::SubBoundExceedLimits);
		ensure!(self.end <= bounded.content.len() as u64, BoundedReaderErr::SubBoundExceedLimits);

		let slice_range = try_into_usize_range(self.start, self.end)?;
		Ok(MemBoundedReader::new(bounded.content.slice(slice_range)))
	}

	fn clamped_get(self, bounded: &MemBoundedReader) -> MemBoundedReader {
		let clamped_start = min(self.start, bounded.content.len() as u64);
		let clamped_end = min(self.end, bounded.content.len() as u64);
		let clamped_start = min(clamped_start, clamped_end);

		let slice_range = try_into_usize_range(clamped_start, clamped_end).expect(RANGE_VALID_QED);
		MemBoundedReader::new(bounded.content.slice(slice_range))
	}
}

impl BoundedIndex<MemBoundedReader> for RangeFrom<u64> {
	fn get(self, bounded: &MemBoundedReader) -> Result<MemBoundedReader, BoundedReaderErr> {
		ensure!(self.start <= bounded.content.len() as u64, BoundedReaderErr::SubBoundExceedLimits);

		let slice_range = try_into_usize(self.start)?..bounded.content.len();
		Ok(MemBoundedReader::new(bounded.content.slice(slice_range)))
	}

	fn clamped_get(self, bounded: &MemBoundedReader) -> MemBoundedReader {
		let clamped_start = min(self.start, bounded.content.len() as u64);
		let clamped_start = try_into_usize(clamped_start).expect(RANGE_VALID_QED);

		MemBoundedReader::new(bounded.content.slice(clamped_start..bounded.content.len()))
	}
}

impl BoundedIndex<MemBoundedReader> for RangeInclusive<u64> {
	fn get(self, bounded: &MemBoundedReader) -> Result<MemBoundedReader, BoundedReaderErr> {
		let (start, inc_end) = self.into_inner();
		let range = start..(inc_end.checked_add(1).ok_or(BoundedReaderErr::FileTooLarge)?);
		range.get(bounded)
	}

	fn clamped_get(self, bounded: &MemBoundedReader) -> MemBoundedReader {
		let (start, inc_end) = self.into_inner();
		let range = start..(inc_end.saturating_add(1));
		range.clamped_get(bounded)
	}
}

impl BoundedIndex<MemBoundedReader> for RangeToInclusive<u64> {
	fn get(self, bounded: &MemBoundedReader) -> Result<MemBoundedReader, BoundedReaderErr> {
		let end = self.end.checked_add(1).ok_or(BoundedReaderErr::FileTooLarge)?;
		let range = 0..end;
		range.get(bounded)
	}

	fn clamped_get(self, bounded: &MemBoundedReader) -> MemBoundedReader {
		let end = self.end.saturating_add(1);
		let clamped_end = min(end, bounded.content.len() as u64);
		let range = 0..clamped_end;
		range.clamped_get(bounded)
	}
}

impl BoundedIndex<MemBoundedReader> for RangeFull {
	fn get(self, bounded: &MemBoundedReader) -> Result<MemBoundedReader, BoundedReaderErr> {
		Ok(self.clamped_get(bounded))
	}

	fn clamped_get(self, bounded: &MemBoundedReader) -> MemBoundedReader {
		let content = bounded.content.clone();
		MemBoundedReader::new(content)
	}
}

fn try_into_usize(n: u64) -> Result<usize, BoundedReaderErr> {
	usize::try_from(n).map_err(|_| BoundedReaderErr::RangeNotSupportedByReader(n, 0))
}

fn try_into_usize_range(start: u64, end: u64) -> Result<Range<usize>, BoundedReaderErr> {
	let try_err = |_| BoundedReaderErr::RangeNotSupportedByReader(start, end);

	let s = usize::try_from(start).map_err(try_err)?;
	let e = usize::try_from(end).map_err(try_err)?;
	Ok(s..e)
}
