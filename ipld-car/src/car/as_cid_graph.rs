use crate::{car::ContentAddressableArchive, error::NODE_IDX_QED, traits::AsCIDGraph};

use libipld::Cid;
use petgraph::visit::{DfsPostOrder, Walker};
use std::io::{Read, Seek};

impl<T: Read + Seek> AsCIDGraph for ContentAddressableArchive<T> {
	fn root_cids(&self) -> impl Iterator<Item = &Cid> {
		self.root_ids().into_iter().map(|id| &self.dag.node_weight(id).expect(NODE_IDX_QED).cid)
	}

	fn cids(&self) -> impl Iterator<Item = &Cid> {
		self.dag.node_weights().map(|block| &block.cid)
	}

	fn descendants_of_cid(&self, cid: &Cid) -> Vec<&Cid> {
		match self.index_by_cid.get(cid) {
			Some(id) => {
				let visitor = DfsPostOrder::new(&self.dag, *id);
				visitor
					.iter(&self.dag)
					.filter_map(|id| self.dag.node_weight(id).map(|block| &block.cid))
					.collect::<Vec<_>>()
			},
			None => Vec::new(),
		}
	}
}
