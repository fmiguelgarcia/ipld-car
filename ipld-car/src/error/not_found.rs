use crate::car::BlockId;
#[cfg(feature = "vfs")]
use crate::error::vfs_err;

use std::path::{Path, PathBuf};
use thiserror::Error;
#[cfg(feature = "vfs")]
use vfs::error::{VfsError, VfsErrorKind};

#[cfg_attr(feature = "std", derive(Debug))]
#[derive(Error)]
pub enum NotFoundErr {
	#[error("Path `{0:?}` not found")]
	Path(PathBuf),
	#[error("BlockId {0:?} not found")]
	BlockId(BlockId),
	#[error("Path `{0:?} does not contain a file name")]
	FileName(PathBuf),
	#[error("CID is missing on an Directory Entry")]
	CidOnDirEntry,
	#[error("CID is missing on block {0:?}")]
	CidOnBlock(BlockId),
}

impl NotFoundErr {
	pub fn path<P: AsRef<Path>>(p: P) -> Self {
		Self::Path(p.as_ref().to_owned())
	}

	pub fn file_name<P: AsRef<Path>>(p: P) -> Self {
		Self::FileName(p.as_ref().to_owned())
	}
}

#[cfg(feature = "vfs")]
impl From<NotFoundErr> for VfsError {
	fn from(nf: NotFoundErr) -> Self {
		match nf {
			e @ NotFoundErr::Path(..) => vfs_err(VfsErrorKind::FileNotFound, e),
			e @ NotFoundErr::FileName(..) => vfs_err(VfsErrorKind::FileNotFound, e),
			e => VfsErrorKind::Other(e.to_string()).into(),
		}
	}
}
