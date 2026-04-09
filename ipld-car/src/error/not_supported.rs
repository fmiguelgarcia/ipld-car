use crate::config::DAGLayout;
#[cfg(feature = "vfs")]
use crate::error::vfs_err;

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
	#[error("DAG layout `{0:?}` is not supported")]
	DAGLayout(DAGLayout),
}

#[cfg(feature = "vfs")]
impl From<NotSupportedErr> for VfsError {
	fn from(c: NotSupportedErr) -> Self {
		vfs_err(VfsErrorKind::NotSupported, c)
	}
}
