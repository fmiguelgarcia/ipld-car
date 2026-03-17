use crate::{dag_pb::Link, proto, ContextLen};

use bytes::Bytes;
use libipld::pb::PbNode;
use prost::Message;
use std::{
	collections::BTreeMap,
	sync::atomic::{AtomicU64, Ordering::Relaxed},
};

pub type Entries = BTreeMap<String, Link>;

#[derive(Debug, Default)]
pub struct Directory {
	dag_len_cache: AtomicU64,
	entries: Entries,
}

impl Directory {
	pub fn entries(&self) -> &Entries {
		&self.entries
	}

	pub fn mut_entries(&mut self) -> &mut Entries {
		self.dag_len_cache.swap(0, Relaxed);
		&mut self.entries
	}
}

impl Clone for Directory {
	fn clone(&self) -> Self {
		Self { entries: self.entries.clone(), dag_len_cache: AtomicU64::new(self.dag_len_cache.load(Relaxed)) }
	}
}

impl From<&Directory> for PbNode {
	fn from(dir: &Directory) -> Self {
		let pb_links = dir
			.entries
			.iter()
			.map(|(name, l)| proto::new_pb_link(l.cid, name.clone(), l.cumulative_dag_size))
			.collect();
		let pb_data = Bytes::from(proto::Data::new_directory().encode_to_vec());
		proto::new_pb_node(pb_links, pb_data)
	}
}

impl From<Entries> for Directory {
	fn from(entries: Entries) -> Self {
		Self { dag_len_cache: AtomicU64::new(0), entries }
	}
}

impl ContextLen for Directory {
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
