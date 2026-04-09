use crate::{
	bounded_reader::{
		error::BoundedReaderErr as BErr,
		traits::{Bounded, BoundedIndex, CloneAndRewind},
	},
	ensure,
};

use bytes::Bytes;
use std::{
	cmp::min,
	io::{self, Cursor, Read, Seek, SeekFrom},
	ops::{Range, RangeFrom, RangeFull, RangeInclusive, RangeTo, RangeToInclusive},
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
	fn sub<R: BoundedIndex<Self>>(&self, range: R) -> Result<Self, BErr> {
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
	fn get(self, bounded: &MemBoundedReader) -> Result<MemBoundedReader, BErr> {
		ensure!(self.start <= self.end, BErr::invalid_range(self));
		ensure!(self.start <= bounded.content.len() as u64, BErr::sub_start_exceed(bounded, self.start));
		ensure!(self.end <= bounded.content.len() as u64, BErr::sub_end_exceed(bounded, self.end));

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
	fn get(self, bounded: &MemBoundedReader) -> Result<MemBoundedReader, BErr> {
		ensure!(self.start <= bounded.content.len() as u64, BErr::sub_start_exceed(bounded, self.start));

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
	fn get(self, bounded: &MemBoundedReader) -> Result<MemBoundedReader, BErr> {
		let (start, end) = self.into_inner();
		let inc_end = end.checked_add(1).ok_or_else(|| BErr::file_too_large(bounded, start, end))?;
		(start..inc_end).get(bounded)
	}

	fn clamped_get(self, bounded: &MemBoundedReader) -> MemBoundedReader {
		let (start, inc_end) = self.into_inner();
		(start..(inc_end.saturating_add(1))).clamped_get(bounded)
	}
}

impl BoundedIndex<MemBoundedReader> for RangeTo<u64> {
	fn get(self, bounded: &MemBoundedReader) -> Result<MemBoundedReader, BErr> {
		(0..self.end).get(bounded)
	}

	fn clamped_get(self, bounded: &MemBoundedReader) -> MemBoundedReader {
		(0..self.end).clamped_get(bounded)
	}
}

impl BoundedIndex<MemBoundedReader> for RangeToInclusive<u64> {
	fn get(self, bounded: &MemBoundedReader) -> Result<MemBoundedReader, BErr> {
		let end = self.end.checked_add(1).ok_or_else(|| BErr::file_too_large(bounded, 0, self.end))?;
		(0..end).get(bounded)
	}

	fn clamped_get(self, bounded: &MemBoundedReader) -> MemBoundedReader {
		let end = self.end.saturating_add(1);
		(0..end).clamped_get(bounded)
	}
}

impl BoundedIndex<MemBoundedReader> for RangeFull {
	fn get(self, bounded: &MemBoundedReader) -> Result<MemBoundedReader, BErr> {
		Ok(self.clamped_get(bounded))
	}

	fn clamped_get(self, bounded: &MemBoundedReader) -> MemBoundedReader {
		MemBoundedReader::new(bounded.content.clone())
	}
}

fn try_into_usize(n: u64) -> Result<usize, BErr> {
	usize::try_from(n).map_err(|_| BErr::range_not_supp(n, 0))
}

fn try_into_usize_range(start: u64, end: u64) -> Result<Range<usize>, BErr> {
	let try_err = |_| BErr::range_not_supp(start, end);

	let s = usize::try_from(start).map_err(try_err)?;
	let e = usize::try_from(end).map_err(try_err)?;
	Ok(s..e)
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::bounded_reader::traits::{Bounded, BoundedIndex};
	use bytes::Bytes;
	use std::io::Read;
	use test_case::test_case;

	const HELLO: &[u8] = b"Hello world";
	const LOREM_IPSUM: &[u8] = b"Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.";

	#[allow(unused_parens)]
	#[test_case( HELLO, 0..4 => b"Hell".to_vec(); "From 0 to 4")]
	#[test_case( HELLO, 2..=4 => b"llo".to_vec(); "From 2 to =4")]
	#[test_case( HELLO, ..=4 => b"Hello".to_vec(); "to =4")]
	#[test_case( HELLO, (0..) => b"Hello world".to_vec(); "From 0 to .." )]
	#[test_case( HELLO, (6..) => b"world".to_vec(); "From 5 to .." )]
	#[test_case( HELLO, (..) => b"Hello world".to_vec(); "full range" )]
	fn bounded_index_as_shared<D, R>(data: D, range: R) -> Vec<u8>
	where
		D: AsRef<[u8]>,
		R: BoundedIndex<MemBoundedReader>,
	{
		let bounded = MemBoundedReader::new(Bytes::copy_from_slice(data.as_ref()));
		let mut sub = bounded.sub(range).unwrap();

		let mut sub_content = vec![];
		let _ = sub.read_to_end(&mut sub_content).unwrap();
		sub_content
	}

	#[test_case( LOREM_IPSUM, &[(28..55),(12..22)] => b"adipiscing".to_vec(); "Nested (27..55)(12..22)" )]
	#[test_case( LOREM_IPSUM, &[(28..55),(0..11)] => b"consectetur".to_vec(); "Nested (27..55)(0..11)" )]
	#[test_case( LOREM_IPSUM, &[28..=55,23..=26] => b"elit".to_vec(); "Nested 27..=55 23..=26" )]
	#[test_case( LOREM_IPSUM, &[..=55, ..=4] => b"Lorem".to_vec(); "Nested ..=55 ..=4" )]
	#[test_case( LOREM_IPSUM, &[(28..), (88..)] => b"aliqua.".to_vec(); "Nested (28..) (88..)" )]
	#[test_case( LOREM_IPSUM, &[(..), (..)] => LOREM_IPSUM.to_vec(); "Nested (..) (..)" )]
	fn nested_bounded_index_as_shared<D, R>(data: D, nested_ranges: &[R]) -> Vec<u8>
	where
		D: AsRef<[u8]>,
		R: BoundedIndex<MemBoundedReader> + Clone,
	{
		let mut bounded = MemBoundedReader::new(Bytes::copy_from_slice(data.as_ref()));
		for range in nested_ranges {
			bounded = bounded.sub(range.clone()).unwrap();
		}

		let mut content = vec![];
		let _ = bounded.read_to_end(&mut content).unwrap();
		content
	}
}
