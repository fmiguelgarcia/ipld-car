use crate::{
	bounded_reader::traits::Bounded as _,
	car::{traits::AsBlockFinder as _, BlockType, ContentAddressableArchive},
	error::NODE_IDX_QED,
	traits::{AsBoundedContainer, AsCIDGraph},
};

use libipld::Cid;
use petgraph::{
	visit::{DfsPostOrder, EdgeRef, Walker},
	Direction,
};
use std::{
	collections::HashSet,
	io::{Read, Seek},
	ops::Range,
};

impl<T: Read + Seek> AsCIDGraph for ContentAddressableArchive<T> {
	fn root_cids(&self) -> impl Iterator<Item = &Cid> {
		self.root_ids().into_iter().map(|id| &self.dag.node_weight(id).expect(NODE_IDX_QED).cid)
	}

	fn cids(&self) -> impl Iterator<Item = &Cid> {
		self.dag.node_weights().map(|block| &block.cid)
	}

	fn direct_parents_of_cid(&self, cid: &Cid) -> Vec<&Cid> {
		self.direct_connected_cids(cid, Direction::Incoming)
	}

	fn direct_descendants_of_cid(&self, cid: &Cid) -> Vec<&Cid> {
		self.direct_connected_cids(cid, Direction::Outgoing)
	}

	fn descendants_of_cid(&self, cid: &Cid) -> Vec<&Cid> {
		match self.index_by_cid.get(cid) {
			Some(root_id) => {
				let visitor = DfsPostOrder::new(&self.dag, *root_id);
				visitor
					.iter(&self.dag)
					.filter(|id| id != root_id)
					.filter_map(|id| self.dag.node_weight(id).map(|block| &block.cid))
					.collect::<Vec<_>>()
			},
			None => Vec::new(),
		}
	}
}

impl<T: Read + Seek> AsBoundedContainer for ContentAddressableArchive<T> {
	fn bounds_of(&self, cid: &Cid) -> Option<Range<u64>> {
		self.block_by_cid(cid).map(|block| match &block.r#type {
			BlockType::Raw => block.data.bounds(),
			BlockType::DagPb(dag) => dag.data.bounds(),
		})
	}
}

impl<T: Read + Seek> ContentAddressableArchive<T> {
	fn direct_connected_cids(&self, cid: &Cid, direction: Direction) -> Vec<&Cid> {
		let Some(id) = self.index_by_cid.get(cid) else { return vec![] };
		let connected_ids = self
			.dag
			.edges_directed(*id, direction)
			.map(|edge| edge.source())
			.filter(|other_id| other_id != id)
			.collect::<HashSet<_>>();

		connected_ids
			.into_iter()
			.filter_map(|parent_id| self.dag.node_weight(parent_id).map(|block| &block.cid))
			.collect()
	}
}
