use crate::{
	bounded_reader::{
		error::BoundedReaderErr,
		sync::BoundedReader,
		traits::{Bounded, BoundedIndex, CloneAndRewind},
	},
	ensure,
};

use derivative::Derivative;
use std::{
	cmp::min,
	io::{self, Read, Seek, SeekFrom},
	ops::{Range, RangeFrom, RangeFull, RangeInclusive, RangeTo, RangeToInclusive},
};

/// A sequential composition of multiple [`BoundedReader`]s that presents them as a single
/// contiguous stream.
///
/// Implements [`Read`] and [`Seek`] as if all inner readers were concatenated. Reads advance
/// through segments in order; seeks operate on the virtual flat offset space.
#[derive(derive_more::Debug, Derivative)]
#[derivative(Clone(bound = ""))]
pub struct ChainedBoundedReader<T> {
	#[debug(skip)]
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

impl<T> ChainedBoundedReader<T> {
	/// Slices this reader to virtual offset range `[start, end)`, returning a new
	/// `ChainedBoundedReader` covering only that portion.
	fn slice(readers: &[BoundedReader<T>], start: u64, end: u64) -> Result<Self, BoundedReaderErr> {
		let mut result = Vec::new();
		let mut cursor = 0u64;
		for reader in readers {
			let rlen = reader.bound_len();
			let reader_vend = cursor + rlen;
			if cursor >= end {
				break;
			}
			if reader_vend > start {
				let sub_start = start.saturating_sub(cursor);
				let sub_end = end.saturating_sub(cursor).min(rlen);
				result.push(reader.sub(sub_start..sub_end)?);
			}
			cursor += rlen;
		}
		Ok(Self::new(result))
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

impl<T> BoundedIndex<ChainedBoundedReader<T>> for Range<u64> {
	fn get(self, bounded: &ChainedBoundedReader<T>) -> Result<ChainedBoundedReader<T>, BoundedReaderErr> {
		ensure!(self.start <= self.end, BoundedReaderErr::invalid_range(self.clone()));
		ensure!(self.start <= bounded.total_len, BoundedReaderErr::sub_start_exceed(bounded, self.start));
		ensure!(self.end <= bounded.total_len, BoundedReaderErr::sub_end_exceed(bounded, self.end));
		ChainedBoundedReader::slice(&bounded.readers, self.start, self.end)
	}

	fn clamped_get(self, bounded: &ChainedBoundedReader<T>) -> ChainedBoundedReader<T> {
		let start = self.start.min(bounded.total_len);
		let end = self.end.min(bounded.total_len);
		let start = start.min(end);
		ChainedBoundedReader::slice(&bounded.readers, start, end).unwrap_or_else(|_| ChainedBoundedReader::empty())
	}
}

impl<T> BoundedIndex<ChainedBoundedReader<T>> for RangeFrom<u64> {
	fn get(self, bounded: &ChainedBoundedReader<T>) -> Result<ChainedBoundedReader<T>, BoundedReaderErr> {
		ensure!(self.start <= bounded.total_len, BoundedReaderErr::sub_start_exceed(bounded, self.start));
		ChainedBoundedReader::slice(&bounded.readers, self.start, bounded.total_len)
	}

	fn clamped_get(self, bounded: &ChainedBoundedReader<T>) -> ChainedBoundedReader<T> {
		let start = self.start.min(bounded.total_len);
		ChainedBoundedReader::slice(&bounded.readers, start, bounded.total_len)
			.unwrap_or_else(|_| ChainedBoundedReader::empty())
	}
}

impl<T> BoundedIndex<ChainedBoundedReader<T>> for RangeTo<u64> {
	fn get(self, bounded: &ChainedBoundedReader<T>) -> Result<ChainedBoundedReader<T>, BoundedReaderErr> {
		ensure!(self.end <= bounded.total_len, BoundedReaderErr::sub_end_exceed(bounded, self.end));
		ChainedBoundedReader::slice(&bounded.readers, 0, self.end)
	}

	fn clamped_get(self, bounded: &ChainedBoundedReader<T>) -> ChainedBoundedReader<T> {
		let end = self.end.min(bounded.total_len);
		ChainedBoundedReader::slice(&bounded.readers, 0, end).unwrap_or_else(|_| ChainedBoundedReader::empty())
	}
}

impl<T> BoundedIndex<ChainedBoundedReader<T>> for RangeInclusive<u64> {
	fn get(self, bounded: &ChainedBoundedReader<T>) -> Result<ChainedBoundedReader<T>, BoundedReaderErr> {
		let (start, end) = self.into_inner();
		let inc_end = end.checked_add(1).ok_or_else(|| BoundedReaderErr::file_too_large(bounded, start, end))?;
		(start..inc_end).get(bounded)
	}

	fn clamped_get(self, bounded: &ChainedBoundedReader<T>) -> ChainedBoundedReader<T> {
		let (start, end) = self.into_inner();
		(start..(end.saturating_add(1))).clamped_get(bounded)
	}
}

impl<T> BoundedIndex<ChainedBoundedReader<T>> for RangeToInclusive<u64> {
	fn get(self, bounded: &ChainedBoundedReader<T>) -> Result<ChainedBoundedReader<T>, BoundedReaderErr> {
		let inc_end = self.end.checked_add(1).ok_or_else(|| BoundedReaderErr::file_too_large(bounded, 0, self.end))?;
		(0..inc_end).get(bounded)
	}

	fn clamped_get(self, bounded: &ChainedBoundedReader<T>) -> ChainedBoundedReader<T> {
		(0..(self.end.saturating_add(1))).clamped_get(bounded)
	}
}

impl<T> BoundedIndex<ChainedBoundedReader<T>> for RangeFull {
	fn get(self, bounded: &ChainedBoundedReader<T>) -> Result<ChainedBoundedReader<T>, BoundedReaderErr> {
		Ok(self.clamped_get(bounded))
	}

	fn clamped_get(self, bounded: &ChainedBoundedReader<T>) -> ChainedBoundedReader<T> {
		bounded.clone_and_rewind()
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::io::Cursor;
	use test_case::test_case;

	const LOREM_IPSUM: &[&[u8]] = &[
		b"Lorem ipsum dolor sit amet, ",      // [0,28)
		b"consectetur adipiscing elit, ",     // [28, 57)
		b"sed do eiusmod tempor incididunt ", // [57, 90)
		b"ut labore et dolore magna aliqua.", // [90, 123)
	];

	fn lorem_ipsum_joined() -> Vec<u8> {
		LOREM_IPSUM.concat()
	}

	#[test_case( LOREM_IPSUM, (..) => lorem_ipsum_joined(); "Full")]
	#[test_case( LOREM_IPSUM, 28..57 => b"consectetur adipiscing elit, ".to_vec(); "2nd slice")]
	#[test_case( LOREM_IPSUM, 57..90 => b"sed do eiusmod tempor incididunt ".to_vec(); "3rd slice")]
	#[test_case( LOREM_IPSUM, 40..63 => b"adipiscing elit, sed do".to_vec(); "partial 2nd & 3rd")]
	#[test_case( LOREM_IPSUM, 40..=62 => b"adipiscing elit, sed do".to_vec(); "partial 2nd & 3rd ToInclusive")]
	#[test_case( LOREM_IPSUM, ..11 => b"Lorem ipsum".to_vec(); "To")]
	#[test_case( LOREM_IPSUM, ..=10 => b"Lorem ipsum".to_vec(); "ToInclusive")]
	#[test_case( LOREM_IPSUM, (103..) => b"dolore magna aliqua.".to_vec(); "From")]
	#[allow(unused_parens)]
	fn bounded_index_as_chained<D, R>(segments: &[D], range: R) -> Vec<u8>
	where
		D: AsRef<[u8]>,
		R: BoundedIndex<ChainedBoundedReader<Cursor<Vec<u8>>>>,
	{
		let readers = segments
			.iter()
			.map(|data| BoundedReader::from_reader(Cursor::new(data.as_ref().to_vec())).unwrap())
			.collect::<Vec<_>>();
		let bounded = ChainedBoundedReader::new(readers);
		let mut sub = bounded.sub(range).unwrap();

		let mut sub_content = vec![];
		let _ = sub.read_to_end(&mut sub_content).unwrap();
		sub_content
	}

	#[test_case( LOREM_IPSUM, &[(28..55), (12..22)] => b"adipiscing".to_vec(); "Nested (28..55)(12..22)")]
	#[test_case( LOREM_IPSUM, &[(28..55), (0..11)] => b"consectetur".to_vec(); "Nested (28..55)(0..11)")]
	#[test_case( LOREM_IPSUM, &[28..=55, 23..=26] => b"elit".to_vec(); "Nested 28..=55 23..=26")]
	#[test_case( LOREM_IPSUM, &[..=55, ..=4] => b"Lorem".to_vec(); "Nested ..=55 ..=4")]
	#[test_case( LOREM_IPSUM, &[(28..), (88..)] => b"aliqua.".to_vec(); "Nested (28..) (88..)")]
	#[test_case( LOREM_IPSUM, &[(..), (..)] => lorem_ipsum_joined(); "Nested (..) (..)")]
	fn nested_bounded_index_as_chained<D, R>(segments: &[D], nested_ranges: &[R]) -> Vec<u8>
	where
		D: AsRef<[u8]>,
		R: BoundedIndex<ChainedBoundedReader<Cursor<Vec<u8>>>> + Clone,
	{
		let readers = segments
			.iter()
			.map(|data| BoundedReader::from_reader(Cursor::new(data.as_ref().to_vec())).unwrap())
			.collect::<Vec<_>>();
		let mut bounded = ChainedBoundedReader::new(readers);
		for range in nested_ranges {
			bounded = bounded.sub(range.clone()).unwrap();
		}

		let mut content = vec![];
		let _ = bounded.read_to_end(&mut content).unwrap();
		content
	}
}
