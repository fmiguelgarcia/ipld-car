use crate::{proto, BoundedReader, ContextLen};

use bytes::Bytes;
use derive_more::From;
use libipld::pb::PbNode;
use prost::Message;
use std::{
	io::{Cursor, Read, Seek},
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

impl<T> From<&SingleBlockFile<T>> for PbNode {
	fn from(sbf: &SingleBlockFile<T>) -> Self {
		let data = match &sbf.content {
			SBFContent::Data(data) => proto::Data::new_file_with_data(data),
			SBFContent::Reader(reader) => proto::Data::new_file(vec![reader.bound_len()]),
		};
		proto::new_pb_node(vec![], Bytes::from(data.encode_to_vec()))
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
