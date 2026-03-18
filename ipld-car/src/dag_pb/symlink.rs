use crate::{proto, ContextLen};

use bytes::Bytes;
use libipld::pb::PbNode;
use prost::Message;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

#[derive(Debug)]
pub struct Symlink {
	dag_len_cache: AtomicU64,
	posix_path: String,
}

impl Symlink {
	pub fn new(posix_path: String) -> Self {
		Self { dag_len_cache: AtomicU64::new(0), posix_path }
	}
}

impl Clone for Symlink {
	fn clone(&self) -> Self {
		Self { posix_path: self.posix_path.clone(), dag_len_cache: AtomicU64::new(self.dag_len_cache.load(Relaxed)) }
	}
}

impl From<&Symlink> for PbNode {
	fn from(s: &Symlink) -> Self {
		let data = Bytes::from(proto::Data::new_symlink(s.posix_path.as_str()).encode_to_vec());
		proto::new_pb_node(vec![], data)
	}
}

impl ContextLen for Symlink {
	#[inline]
	fn data_len(&self) -> u64 {
		0u64
	}

	fn dag_pb_len(&self) -> u64 {
		if self.dag_len_cache.load(Relaxed) == 0 {
			let pb_node_len = PbNode::from(self).into_bytes().len() as u64;
			self.dag_len_cache.store(pb_node_len, Relaxed);
		}

		self.dag_len_cache.load(Relaxed)
	}
}
