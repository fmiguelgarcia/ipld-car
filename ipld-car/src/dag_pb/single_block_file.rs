use crate::{
	config::{CidCodec, LeafPolicy},
	error::{DagPbErr, Result},
	proto,
	reader_with_len::ReaderWithLen,
	BoundedReader, CIDBuilder, Config, ContextLen,
};

use bytes::{Buf as _, Bytes};
use derive_more::From;
use libipld::{multihash::MultihashDigest, pb::PbNode, Cid};
use prost::Message;
use std::{
	io::{copy, Cursor, Read, Seek},
	sync::atomic::{AtomicU64, Ordering::Relaxed},
};

#[derive(derive_more::Debug)]
pub struct SingleBlockFile<T> {
	dag_len_cache: AtomicU64,
	content: SBFContent<T>,
}

impl<T> SingleBlockFile<T> {
	#[inline]
	pub fn content(&self) -> &SBFContent<T> {
		&self.content
	}
}

impl<T> Clone for SingleBlockFile<T> {
	fn clone(&self) -> Self {
		Self { content: self.content.clone(), dag_len_cache: AtomicU64::new(self.dag_len_cache.load(Relaxed)) }
	}
}

impl<T> From<SBFContent<T>> for SingleBlockFile<T> {
	fn from(content: SBFContent<T>) -> Self {
		Self { dag_len_cache: AtomicU64::new(0), content }
	}
}

impl<T> ContextLen for SingleBlockFile<T> {
	fn data_len(&self) -> u64 {
		match &self.content {
			SBFContent::Data(data) => data.len() as u64,
			SBFContent::Reader(reader) => reader.bound_len(),
		}
	}

	fn dag_pb_len(&self) -> u64 {
		if self.dag_len_cache.load(Relaxed) == 0 {
			let pb_node_len = PbNode::from(self).into_bytes().len() as u64;
			self.dag_len_cache.store(pb_node_len + self.data_len(), Relaxed);
		}

		self.dag_len_cache.load(Relaxed)
	}

	fn invalidate(&mut self) {
		self.dag_len_cache.store(0, Relaxed);
	}

	fn was_invalidated(&self) -> bool {
		self.dag_len_cache.load(Relaxed) == 0
	}
}

#[cfg(feature = "vfs")]
impl<T: Read + Seek + Send + 'static> SingleBlockFile<T> {
	pub fn reader(&self) -> Box<dyn vfs::SeekAndRead + Send> {
		match &self.content {
			SBFContent::Data(data) => Box::new(Cursor::new(data.clone())),
			SBFContent::Reader(reader) => Box::new(reader.clone_and_rewind()),
		}
	}
}

// Content
// ===========================================================================

#[derive(From, derive_more::Debug)]
pub enum SBFContent<T> {
	Data(Bytes),
	Reader(BoundedReader<T>),
}

impl<T> Clone for SBFContent<T> {
	fn clone(&self) -> Self {
		match self {
			Self::Data(data) => Self::Data(data.clone()),
			Self::Reader(reader) => Self::Reader(reader.clone()),
		}
	}
}

// Ipld & CID related
// ===========================================================================

impl<T> From<&SingleBlockFile<T>> for PbNode {
	fn from(sbf: &SingleBlockFile<T>) -> Self {
		let data = match &sbf.content {
			SBFContent::Data(data) => proto::Data::new_file_with_data(data),
			SBFContent::Reader(reader) => proto::Data::new_file(vec![reader.bound_len()]),
		};
		proto::new_pb_node(vec![], Bytes::from(data.encode_to_vec()))
	}
}

impl<T: Seek + Read + 'static> SingleBlockFile<T> {
	pub fn as_reader_with_len(&self) -> Result<ReaderWithLen> {
		let pb_node = PbNode::from(self);
		let enc_pb_node = Bytes::from(pb_node.into_bytes());
		let enc_pb_node_len = enc_pb_node.len() as u64;

		let this = match self.content() {
			SBFContent::Data(..) => ReaderWithLen::new(enc_pb_node.reader(), enc_pb_node_len),
			SBFContent::Reader(reader) => {
				let chained_reader = enc_pb_node.reader().chain(reader.clone_and_rewind());
				let len = enc_pb_node_len.checked_add(reader.bound_len()).ok_or(DagPbErr::FileTooLarge)?;
				ReaderWithLen::new(chained_reader, len)
			},
		};
		Ok(this)
	}
}

impl<T: Read + Seek> CIDBuilder for SingleBlockFile<T> {
	fn cid(&self, config: &Config) -> Result<Cid> {
		let mut hasher = config.hasher()?;
		let cid_codec = match config.leaf_policy {
			LeafPolicy::Raw => {
				match &self.content {
					SBFContent::Data(data) => {
						copy(&mut data.clone().reader(), &mut hasher)?;
					},
					SBFContent::Reader(reader) => {
						let mut reader = reader.clone_and_rewind();
						copy(&mut reader, &mut hasher)?;
					},
				};
				CidCodec::Raw
			},
			LeafPolicy::UnixFs => {
				let ReaderWithLen { mut reader, len: _ } = ReaderWithLen::from(PbNode::from(self));
				copy(&mut reader, &mut hasher)?;
				CidCodec::DagPb
			},
		};

		let digest = config.hash_code.wrap(hasher.finalize())?;
		let cid = Cid::new_v1(cid_codec as u64, digest);
		Ok(cid)
	}
}
