use crate::car::Block;

use libipld::Cid;
use std::path::Path;

pub trait AsBlockFinder<T> {
	/// Returns the block associated to the given CID, if it exists
	fn block_by_cid(&self, cid: &Cid) -> Option<&Block<T>>;

	/// Returns the block associated to the given path, if it exists
	fn block_by_path<P: AsRef<Path>>(&self, path: P) -> Option<&Block<T>>;
}
