#[cfg(feature = "vfs")]
use crate::error::vfs_err;
use crate::ArenaId;

use libipld::Cid;
use thiserror::Error;
#[cfg(feature = "vfs")]
use vfs::error::{VfsError, VfsErrorKind};

#[cfg_attr(feature = "std", derive(Debug))]
#[derive(Error)]
pub enum NotFoundErr {
	#[error("Path not found")]
	Path,
	#[error("Entry ID ({0}) not found in this CAR")]
	ArenaId(ArenaId),
	#[error("RootCid ({0:?}) is not found in this CAR")]
	RootCid(Cid),
	#[error("PbNode Directory {0:?} is missing a path name at {1}")]
	PathNameOnPbNodeDir(Cid, usize),
	#[error("CID is missing on an Directory Entry")]
	CidOnDirEntry,
}

#[cfg(feature = "vfs")]
impl From<NotFoundErr> for VfsError {
	fn from(nf: NotFoundErr) -> Self {
		match nf {
			NotFoundErr::Path => VfsErrorKind::FileNotFound.into(),
			e @ NotFoundErr::RootCid(..) => vfs_err(VfsErrorKind::InvalidPath, e.to_string()),
			e @ NotFoundErr::PathNameOnPbNodeDir(..) => vfs_err(VfsErrorKind::InvalidPath, e.to_string()),
			e @ NotFoundErr::CidOnDirEntry => VfsErrorKind::Other(e.to_string()).into(),
			e @ NotFoundErr::ArenaId(..) => VfsErrorKind::Other(e.to_string()).into(),
		}
	}
}
