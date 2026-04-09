use crate::bounded_reader::{
	error::BoundedReaderErr,
	mem_bounded_reader::MemBoundedReader,
	sync::shared_bounded_reader::SharedBoundedReader,
	traits::{Bounded, BoundedIndex, CloneAndRewind},
};

use bytes::Bytes;
use derivative::Derivative;
use std::{
	io::{self, Read, Seek, SeekFrom},
	ops::{Range, RangeFrom, RangeFull, RangeInclusive, RangeTo, RangeToInclusive},
	sync::{Arc, Mutex},
};

#[derive(derive_more::Debug, Derivative)]
#[derivative(Clone(bound = ""))]
pub enum BoundedReader<T> {
	Shared(SharedBoundedReader<T>),
	Mem(MemBoundedReader),
}

impl<T> BoundedReader<T> {
	/// Creates a new bounded reader wrapping the shared reader, restricted to the given range.
	///
	/// # NOTE
	/// It does NOT check that `range` is valid in `reader`, that will fail during read/seek
	/// operations.
	pub fn new(reader: Arc<Mutex<T>>, range: Range<u64>) -> Result<Self, BoundedReaderErr> {
		let shared = SharedBoundedReader::new(reader, range)?;
		Ok(Self::Shared(shared))
	}

	pub fn empty() -> Self {
		Self::Mem(MemBoundedReader::new(Bytes::new()))
	}
}

impl<T: Seek> BoundedReader<T> {
	pub fn from_reader(reader: T) -> Result<Self, io::Error> {
		SharedBoundedReader::from_reader(reader).map(Self::Shared)
	}
}

impl<T> From<Bytes> for BoundedReader<T> {
	fn from(content: Bytes) -> Self {
		Self::Mem(MemBoundedReader::new(content))
	}
}

impl<T> From<()> for BoundedReader<T> {
	fn from(_: ()) -> Self {
		Self::empty()
	}
}

impl<T> Default for BoundedReader<T> {
	fn default() -> Self {
		Self::empty()
	}
}

impl<T> Bounded for BoundedReader<T> {
	/// Returns the absolute range this bounded reader is restricted to.
	fn bounds(&self) -> Range<u64> {
		match self {
			Self::Shared(s) => s.bounds(),
			Self::Mem(m) => m.bounds(),
		}
	}

	/// Returns the length of the bounded range.
	fn bound_len(&self) -> u64 {
		match self {
			Self::Shared(s) => s.bound_len(),
			Self::Mem(m) => m.bound_len(),
		}
	}

	/// Creates a new bounded reader that is a sub-range of this one.
	fn sub<R: BoundedIndex<Self>>(&self, range: R) -> Result<Self, BoundedReaderErr> {
		range.get(self)
	}

	fn clamped_sub<R: BoundedIndex<Self>>(&self, range: R) -> Self {
		range.clamped_get(self)
	}
}

impl<T: Read + Seek> Read for BoundedReader<T> {
	fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
		match self {
			Self::Shared(s) => s.read(buf),
			Self::Mem(m) => m.read(buf),
		}
	}
}

impl<T> CloneAndRewind for BoundedReader<T> {
	fn clone_and_rewind(&self) -> Self {
		match self {
			Self::Shared(s) => Self::Shared(s.clone_and_rewind()),
			Self::Mem(m) => Self::Mem(m.clone_and_rewind()),
		}
	}
}

impl<T: Seek> Seek for BoundedReader<T> {
	fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
		match self {
			Self::Shared(s) => s.seek(pos),
			Self::Mem(m) => m.seek(pos),
		}
	}
}

impl<T> BoundedIndex<BoundedReader<T>> for Range<u64> {
	fn get(self, bounded: &BoundedReader<T>) -> Result<BoundedReader<T>, BoundedReaderErr> {
		match bounded {
			BoundedReader::Shared(s) => self.get(s).map(BoundedReader::Shared),
			BoundedReader::Mem(m) => self.get(m).map(BoundedReader::Mem),
		}
	}

	fn clamped_get(self, bounded: &BoundedReader<T>) -> BoundedReader<T> {
		match bounded {
			BoundedReader::Shared(s) => BoundedReader::Shared(self.clamped_get(s)),
			BoundedReader::Mem(m) => BoundedReader::Mem(self.clamped_get(m)),
		}
	}
}

impl<T> BoundedIndex<BoundedReader<T>> for RangeFrom<u64> {
	fn get(self, bounded: &BoundedReader<T>) -> Result<BoundedReader<T>, BoundedReaderErr> {
		match bounded {
			BoundedReader::Shared(s) => self.get(s).map(BoundedReader::Shared),
			BoundedReader::Mem(m) => self.get(m).map(BoundedReader::Mem),
		}
	}

	fn clamped_get(self, bounded: &BoundedReader<T>) -> BoundedReader<T> {
		match bounded {
			BoundedReader::Shared(s) => BoundedReader::Shared(self.clamped_get(s)),
			BoundedReader::Mem(m) => BoundedReader::Mem(self.clamped_get(m)),
		}
	}
}

impl<T> BoundedIndex<BoundedReader<T>> for RangeTo<u64> {
	fn get(self, bounded: &BoundedReader<T>) -> Result<BoundedReader<T>, BoundedReaderErr> {
		match bounded {
			BoundedReader::Shared(s) => self.get(s).map(BoundedReader::Shared),
			BoundedReader::Mem(m) => self.get(m).map(BoundedReader::Mem),
		}
	}

	fn clamped_get(self, bounded: &BoundedReader<T>) -> BoundedReader<T> {
		match bounded {
			BoundedReader::Shared(s) => BoundedReader::Shared(self.clamped_get(s)),
			BoundedReader::Mem(m) => BoundedReader::Mem(self.clamped_get(m)),
		}
	}
}

impl<T> BoundedIndex<BoundedReader<T>> for RangeInclusive<u64> {
	fn get(self, bounded: &BoundedReader<T>) -> Result<BoundedReader<T>, BoundedReaderErr> {
		match bounded {
			BoundedReader::Shared(s) => self.get(s).map(BoundedReader::Shared),
			BoundedReader::Mem(m) => self.get(m).map(BoundedReader::Mem),
		}
	}

	fn clamped_get(self, bounded: &BoundedReader<T>) -> BoundedReader<T> {
		match bounded {
			BoundedReader::Shared(s) => BoundedReader::Shared(self.clamped_get(s)),
			BoundedReader::Mem(m) => BoundedReader::Mem(self.clamped_get(m)),
		}
	}
}

impl<T> BoundedIndex<BoundedReader<T>> for RangeToInclusive<u64> {
	fn get(self, bounded: &BoundedReader<T>) -> Result<BoundedReader<T>, BoundedReaderErr> {
		match bounded {
			BoundedReader::Shared(s) => self.get(s).map(BoundedReader::Shared),
			BoundedReader::Mem(m) => self.get(m).map(BoundedReader::Mem),
		}
	}

	fn clamped_get(self, bounded: &BoundedReader<T>) -> BoundedReader<T> {
		match bounded {
			BoundedReader::Shared(s) => BoundedReader::Shared(self.clamped_get(s)),
			BoundedReader::Mem(m) => BoundedReader::Mem(self.clamped_get(m)),
		}
	}
}

impl<T> BoundedIndex<BoundedReader<T>> for RangeFull {
	fn get(self, bounded: &BoundedReader<T>) -> Result<BoundedReader<T>, BoundedReaderErr> {
		match bounded {
			BoundedReader::Shared(s) => self.get(s).map(BoundedReader::Shared),
			BoundedReader::Mem(m) => self.get(m).map(BoundedReader::Mem),
		}
	}

	fn clamped_get(self, bounded: &BoundedReader<T>) -> BoundedReader<T> {
		match bounded {
			BoundedReader::Shared(s) => BoundedReader::Shared(self.clamped_get(s)),
			BoundedReader::Mem(m) => BoundedReader::Mem(self.clamped_get(m)),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::bounded_reader::traits::{Bounded, BoundedIndex};
	use std::io::{Cursor, Read};
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
		R: BoundedIndex<BoundedReader<Cursor<D>>>,
	{
		let bounded = BoundedReader::from_reader(Cursor::new(data)).unwrap();
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
		R: BoundedIndex<BoundedReader<Cursor<D>>> + Clone,
	{
		let mut bounded = BoundedReader::from_reader(Cursor::new(data)).unwrap();
		for range in nested_ranges {
			bounded = bounded.sub(range.clone()).unwrap();
		}

		let mut content = vec![];
		let _ = bounded.read_to_end(&mut content).unwrap();
		content
	}
}
