use crate::{
	car::{traits::AsBlockFinder, Block, BlockId, SmallBlockIds},
	ensure,
	error::{Error, NotFoundErr, NotSupportedErr, Result},
	fail, ContentAddressableArchive,
};

use libipld::Cid;
use petgraph::{visit::EdgeRef, Direction};
use std::path::{Component, Path};

impl<T> AsBlockFinder<T> for ContentAddressableArchive<T> {
	fn block_by_cid(&self, cid: &Cid) -> Option<&Block<T>> {
		let block_id = self.index_by_cid.get(cid)?;
		self.dag.node_weight(*block_id)
	}

	fn block_by_path<P: AsRef<Path>>(&self, path: P) -> Option<&Block<T>> {
		let block_id = self.path_to_block_id(path).ok()?;
		self.dag.node_weight(block_id)
	}
}

impl<T> ContentAddressableArchive<T> {
	/// Returns the  **unique**`BlockId` associated to `path`.
	///
	/// If there is more that one `BlockId`, it will fail with an `Error::MoreThanOneMatchOnPath(..)`
	pub(crate) fn path_to_block_id<P: AsRef<Path>>(&self, path: P) -> Result<BlockId> {
		let path = path.as_ref();
		let ids = self.path_to_block_ids(path)?;
		ensure!(ids.len() < 2, Error::more_than_one(ids.len(), path));
		ids.first().copied().ok_or_else(|| NotFoundErr::path(path).into())
	}

	/// Returns the `BlockId`s associated to `path`.
	///
	/// Please note that it can be more than one because a CAR can contains multiple roots.
	pub(crate) fn path_to_block_ids<P: AsRef<Path>>(&self, path: P) -> Result<SmallBlockIds> {
		let path = path.as_ref();
		let not_found_path = || NotFoundErr::path(path);
		let mut levels: Vec<SmallBlockIds> = vec![self.root_ids().into()];

		for path_component in path.components() {
			match path_component {
				Component::Normal(os_name) => {
					let name = os_name.to_str().ok_or_else(not_found_path)?;

					let mut new_level = SmallBlockIds::new();
					for block_id in levels.last().ok_or_else(not_found_path)?.iter() {
						let mut targets = self
							.dag
							.edges_directed(*block_id, Direction::Outgoing)
							.filter_map(|edge| (edge.weight().name() == Some(name)).then_some(edge.target()))
							.collect::<SmallBlockIds>();
						new_level.append(&mut targets);
					}

					levels.push(new_level)
				},
				Component::RootDir | Component::CurDir => {},
				Component::ParentDir => {
					levels.pop().ok_or_else(not_found_path)?;
				},
				Component::Prefix(..) => fail!(NotSupportedErr::Prefix),
			}
		}

		levels.pop().ok_or_else(|| not_found_path().into())
	}
}
