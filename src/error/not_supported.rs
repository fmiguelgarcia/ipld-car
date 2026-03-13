use crate::config::{ChunkPolicy, DAGLayout, LeafPolicy};
#[cfg(feature = "vfs")]
use crate::error::vfs_err;

use libipld::multihash;
use thiserror::Error;
#[cfg(feature = "vfs")]
use vfs::error::{VfsError, VfsErrorKind};

#[cfg_attr(feature = "std", derive(Debug))]
#[derive(Error)]
pub enum NotSupportedErr {
	#[error("Path prefix is not supported")]
	Prefix,
	#[error("CAR version ({0}) is not supported")]
	Version(u64),
	#[error("Chunk policy `{0:?}` is not supported")]
	ChunkPolicy(ChunkPolicy),
	#[error("DAG layout `{0:?}` is not supported")]
	DAGLayout(DAGLayout),
	#[error("Leaf policy `{0:?}` is not supported")]
	LeafPolicy(LeafPolicy),
	#[error("Hasher `{0:?}` is not supported")]
	Hasher(multihash::Code),
}

#[cfg(feature = "vfs")]
impl From<NotSupportedErr> for VfsError {
	fn from(c: NotSupportedErr) -> Self {
		vfs_err(VfsErrorKind::NotSupported, c)
	}
}
