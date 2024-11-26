use bytes::{Bytes, BytesMut};
use derive_more::Constructor;
use libipld::multihash::Error as MultihashError;
use std::{
	io::{Error as IoError, Read},
	num::NonZeroUsize,
};
use thiserror_no_std::Error;

/// Iterate over a reader in order to produce fixed-size chunks.
/// There is no limit on the number of chunks produced, so it is called "flat", following the UnixFS FlatDAG naming
/// convention.
#[derive(Constructor, Debug)]
pub struct FlatIterator<R: Read> {
	reader: R,
	chunk_size: NonZeroUsize,
}

impl<R: Read> FlatIterator<R> {
	pub fn into_inner(self) -> R {
		self.reader
	}

	fn next(&mut self) -> Result<Bytes, FlatIterErr> {
		let mut chunk = BytesMut::zeroed(self.chunk_size.get());
		let read_bytes = self.reader.read(chunk.as_mut())?;
		if read_bytes == 0 {
			return Err(FlatIterErr::Eof);
		}

		chunk.truncate(read_bytes);
		Ok(chunk.freeze())
	}
}

impl<R: Read> Iterator for FlatIterator<R> {
	type Item = Result<Bytes, FlatIterErr>;

	fn next(&mut self) -> Option<Self::Item> {
		match self.next() {
			Ok(r) => Some(Ok(r)),
			Err(err) => match err {
				FlatIterErr::Eof => None,
				_ => Some(Err(err)),
			},
		}
	}
}

#[derive(Debug, Error)]
pub enum FlatIterErr {
	Eof,
	Io(#[from] IoError),
	Multihash(#[from] MultihashError),
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::{io::repeat, num::NonZeroUsize};
	use test_case::test_case;

	#[test_case(10, 5 => vec![5, 5])]
	#[test_case(10, 3 => vec![3, 3, 3, 1])]
	#[test_case(1, 1 => vec![1])]
	#[test_case(0, 1 =>panics)]
	#[test_case(10, 0 =>panics)]
	fn chech_chunk_sizes(len: u64, chunk_size: usize) -> Vec<usize> {
		assert!(len > 0);
		let chunk_size = NonZeroUsize::new(chunk_size).unwrap();
		let mut reader = repeat(0).take(len);

		FlatIterator::new(&mut reader, chunk_size)
			.map(|rs_chunk| rs_chunk.map(|chunk| chunk.len()).unwrap_or_default())
			.collect()
	}
}
