use crate::CidCodec::Raw;

use bytes::Buf;
use derive_more::Constructor;
use libipld::{
	multihash::{Code, Error as MultihashError, Hasher, MultihashDigest as _, Sha2_256},
	Cid,
};
use std::io::{self, Error as IoError, Read};

/// Create a Raw CID from data
pub fn raw_cid_from_data(data: &[u8]) -> Cid {
	Cid::new_v1(Raw.into(), Code::Sha2_256.digest(data))
}

///  Iterator which gives the Raw CID of the next value and the next value.
///
/// # Examples
///
/// Basic usage:
///  ```
///  use ipfs_unixfs::{FlatIterator, WithCid, raw_cid_from_data};
///  use std::num::NonZeroUsize;
///  use bytes::Bytes;
///
///  let chunk_size = NonZeroUsize::new(2).unwrap();
///  let (chunk, cid) = WithCid::new(FlatIterator::new(&b"hello"[..], chunk_size)).next().unwrap().unwrap();
///
///  let expected_cid = raw_cid_from_data(b"he");
///  assert_eq!(cid, expected_cid);
///  assert_eq!(chunk, Bytes::from_static(b"he"));
///  ```
#[derive(Constructor, Debug)]
pub struct WithCid<I> {
	inner: I,
}

impl<I> WithCid<I> {
	pub fn into_inner(self) -> I {
		self.inner
	}
}

pub fn raw_cid_from_reader<E, R>(reader: &mut R) -> Result<Cid, E>
where
	E: From<IoError> + From<MultihashError>,
	R: Read + ?Sized,
{
	let mut hasher = Sha2_256::default();
	let _read_bytes = io::copy(reader, &mut hasher)?;
	let digest = Code::Sha2_256.wrap(hasher.finalize())?;
	let cid = Cid::new_v1(Raw.into(), digest);
	Ok(cid)
}

/// Build the CID from `buf`.
///
/// # Performance
/// Note that `buf` is cloned, so efficient clone implementation, like `bytes::Byte` would be
/// suggested.
pub fn raw_cid_from_buf<E, B>(buf: B) -> Result<Cid, E>
where
	E: From<IoError> + From<MultihashError>,
	B: Buf + Clone,
{
	let mut reader = buf.reader();
	raw_cid_from_reader::<E, _>(&mut reader)
}

pub fn append_cid<E, B>(buf: B) -> Result<(B, Cid), E>
where
	E: From<IoError> + From<MultihashError>,
	B: Buf + Clone,
{
	raw_cid_from_buf::<E, B>(buf.clone()).map(|cid| (buf, cid))
}

impl<I, E, B> Iterator for WithCid<I>
where
	I: Iterator<Item = Result<B, E>>,
	B: Buf + Clone,
	E: From<IoError> + From<MultihashError>,
{
	type Item = Result<(B, Cid), E>;

	fn next(&mut self) -> Option<Self::Item> {
		let inner_item_rs = self.inner.next()?;
		match inner_item_rs {
			Err(inner_err) => Some(Err(inner_err)),
			Ok(reader) => Some(append_cid::<E, _>(reader)),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::FlatIterator;
	use std::{cmp::min, num::NonZeroUsize};
	use test_case::test_case;

	#[test_case(b"hello", 10 => raw_cid_from_data(b"hello"))]
	#[test_case(b"hello", 2 => raw_cid_from_data(b"he"))]
	fn check_first_cid(data: &[u8], chunk_size: usize) -> Cid {
		let max_first_chunk_len = min(data.len(), chunk_size);
		let chunk_size = NonZeroUsize::new(chunk_size).unwrap();

		let (data, cid) = WithCid::new(FlatIterator::new(data, chunk_size)).next().unwrap().unwrap();
		assert_eq!(data, data[..max_first_chunk_len]);

		cid
	}
}
