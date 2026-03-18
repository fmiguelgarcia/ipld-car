use crate::{dag_pb::Link, proto, BoundedReader, ContextLen};

use bytes::Bytes;
use libipld::pb::{PbLink, PbNode};
use prost::Message;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

#[derive(derive_more::Debug)]
pub struct MultiBlockFile<T> {
	dag_len_cache: AtomicU64,
	links: Vec<Link>,
	reader: BoundedReader<T>,
}

impl<T> MultiBlockFile<T> {
	pub fn new(links: Vec<Link>, reader: BoundedReader<T>) -> Self {
		Self { links, reader, dag_len_cache: AtomicU64::new(0) }
	}

	#[inline]
	pub fn links(&self) -> &[Link] {
		&self.links
	}

	#[inline]
	pub fn reader(&self) -> &BoundedReader<T> {
		&self.reader
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

impl<T> From<&MultiBlockFile<T>> for PbNode {
	fn from(mbf: &MultiBlockFile<T>) -> Self {
		let (blocksizes, links): (Vec<u64>, Vec<PbLink>) =
			mbf.links.iter().map(|link| (link.blocksize.unwrap_or_default(), PbLink::from(link))).unzip();
		let data = Bytes::from(proto::Data::new_file(blocksizes).encode_to_vec());
		proto::new_pb_node(links, data)
	}
}

impl<T> ContextLen for MultiBlockFile<T> {
	fn data_len(&self) -> u64 {
		self.reader.bound_len()
	}

	fn dag_pb_len(&self) -> u64 {
		if self.dag_len_cache.load(Relaxed) == 0 {
			let cumulative_dag_size = self.links.iter().map(|f| f.cumulative_dag_size).sum::<u64>();
			let pb_node_len = PbNode::from(self).into_bytes().len() as u64;

			self.dag_len_cache.store(cumulative_dag_size + pb_node_len, Relaxed);
		}

		self.dag_len_cache.load(Relaxed)
	}
}
