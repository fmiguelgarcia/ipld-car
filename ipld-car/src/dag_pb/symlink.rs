use crate::{cid_builder::DagPbCidDefaultBuilder, proto, ContextLen};

use bytes::Bytes;
use derive_new::new;
use libipld::pb::PbNode;
use prost::Message;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

#[derive(Debug, new)]
pub struct Symlink {
	#[new(value = "AtomicU64::new(0)")]
	dag_len_cache: AtomicU64,
	posix_path: String,
}

impl Clone for Symlink {
	fn clone(&self) -> Self {
		Self { posix_path: self.posix_path.clone(), dag_len_cache: AtomicU64::new(self.dag_len_cache.load(Relaxed)) }
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

	fn invalidate(&mut self) {
		self.dag_len_cache.store(0, Relaxed);
	}

	fn was_invalidated(&self) -> bool {
		self.dag_len_cache.load(Relaxed) == 0
	}
}

// Ipld & CID related
// ===========================================================================

impl DagPbCidDefaultBuilder for Symlink {}

impl From<&Symlink> for PbNode {
	fn from(s: &Symlink) -> Self {
		let data = Bytes::from(proto::Data::new_symlink(s.posix_path.as_str()).encode_to_vec());
		proto::new_pb_node(vec![], data)
	}
}
