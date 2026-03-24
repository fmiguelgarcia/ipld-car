use crate::{cid_builder::DagPbCidDefaultBuilder, dag_pb::Link, proto, BoundedReader, ContextLen};

use bytes::Bytes;
use derive_new::new;
use libipld::pb::{PbLink, PbNode};
use prost::Message;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

#[derive(derive_more::Debug, new)]
pub struct MultiBlockFile<T> {
	#[new(value = "AtomicU64::new(0)")]
	dag_len_cache: AtomicU64,
	links: Vec<Link>,
	reader: BoundedReader<T>,
}

impl<T> MultiBlockFile<T> {
	#[inline]
	pub fn links(&self) -> &[Link] {
		&self.links
	}

	#[inline]
	pub fn reader(&self) -> &BoundedReader<T> {
		&self.reader
	}

	pub fn blocksizes(&self) -> impl Iterator<Item = u64> + use<'_, T> {
		self.links.iter().map(|l| l.blocksize.unwrap_or_default())
	}
}

impl<T> Clone for MultiBlockFile<T> {
	fn clone(&self) -> Self {
		Self {
			links: self.links.clone(),
			reader: self.reader.clone(),
			dag_len_cache: AtomicU64::new(self.dag_len_cache.load(Relaxed)),
		}
	}
}

impl<T> ContextLen for MultiBlockFile<T> {
	fn data_len(&self) -> u64 {
		self.blocksizes().sum()
	}

	fn dag_pb_len(&self) -> u64 {
		if self.dag_len_cache.load(Relaxed) == 0 {
			let cumulative_dag_size = self.links.iter().map(|f| f.cumulative_dag_size).sum::<u64>();
			let pb_node_len = PbNode::from(self).into_bytes().len() as u64;

			self.dag_len_cache.store(cumulative_dag_size + pb_node_len, Relaxed);
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

impl<T> DagPbCidDefaultBuilder for MultiBlockFile<T> {}

impl<T> From<&MultiBlockFile<T>> for PbNode {
	fn from(mbf: &MultiBlockFile<T>) -> Self {
		let links: Vec<PbLink> = mbf.links.iter().map(PbLink::from).collect();
		let blocksizes = mbf.blocksizes().collect::<Vec<u64>>();
		let data = Bytes::from(proto::Data::new_file(blocksizes).encode_to_vec());
		proto::new_pb_node(links, data)
	}
}
