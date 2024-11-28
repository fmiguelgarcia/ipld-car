use crate::CidCodec::Raw;

use bytes::Buf;
use derive_more::Constructor;
use libipld::{
	multihash::{Code, Error as MultihashError, Hasher, MultihashDigest as _, Sha2_256},
	Cid,
};
use std::io::{self, Error as IoError};
use tracing::trace;

///  Iterator which gives the Raw CID of the next value and the next value.
///
/// # Examples
///
/// Basic usage:
///  ```
///  use ipfs_unixfs::{FlatIterator, WithCid, CidCodec::Raw};
///  use libipld::{Cid, multihash::{Code, MultihashDigest as _}};
///  use std::num::NonZeroUsize;
///  use bytes::Bytes;
///
///  let chunk_size = NonZeroUsize::new(2).unwrap();
///  let (cid, chunk) = WithCid::new(FlatIterator::new(&b"hello"[..], chunk_size)).next().unwrap().unwrap();
///
///  let expected_cid = Cid::new_v1(Raw.into(), Code::Sha2_256.digest(b"he"));
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

/// Build the CID from `buf`.
///
/// # Performance
/// Note that `buf` is cloned, so efficient clone implementation, like `bytes::Byte` would be
/// suggested.
fn build_cid<E, B>(buf: B) -> Result<(Cid, B), E>
where
	E: From<IoError> + From<MultihashError>,
	B: Buf + Clone,
{
	let mut hasher = Sha2_256::default();
	let mut reader = buf.clone().reader();
	let read_bytes = io::copy(&mut reader, &mut hasher)?;
	trace!(read_bytes);
	let digest = Code::Sha2_256.wrap(hasher.finalize())?;
	let cid = Cid::new_v1(Raw.into(), digest);
	Ok((cid, buf))
}

impl<I, E, B> Iterator for WithCid<I>
where
	I: Iterator<Item = Result<B, E>>,
	B: Buf + Clone,
	E: From<IoError> + From<MultihashError>,
{
	type Item = Result<(Cid, B), E>;

	fn next(&mut self) -> Option<Self::Item> {
		let inner_item_rs = self.inner.next()?;
		match inner_item_rs {
			Err(inner_err) => Some(Err(inner_err)),
			Ok(reader) => Some(build_cid::<E, _>(reader)),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::FlatIterator;
	use std::{cmp::min, num::NonZeroUsize};
	use test_case::test_case;

	#[test_case(b"hello", 10 => Cid::new_v1(Raw.into(), Code::Sha2_256.digest(b"hello")))]
	#[test_case(b"hello", 2 => Cid::new_v1(Raw.into() , Code::Sha2_256.digest(b"he")))]
	fn check_first_cid(data: &[u8], chunk_size: usize) -> Cid {
		let max_first_chunk_len = min(data.len(), chunk_size);
		let chunk_size = NonZeroUsize::new(chunk_size).unwrap();

		let (cid, data) = WithCid::new(FlatIterator::new(data, chunk_size)).next().unwrap().unwrap();
		assert_eq!(data, data[..max_first_chunk_len]);

		cid
	}
}
