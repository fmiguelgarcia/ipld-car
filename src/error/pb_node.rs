#[cfg(feature = "vfs")]
use crate::error::vfs_err;

use std::string::FromUtf8Error;
use thiserror::Error;
#[cfg(feature = "vfs")]
use vfs::error::{VfsError, VfsErrorKind};

#[cfg_attr(feature = "std", derive(Debug))]
#[derive(Error)]
pub enum PbNodeErr {
	#[error("PbNode cannot be decoded because it exceeds the maximum 2MiB size")]
	ExceedLimit,
	#[error("Deprecated Raw DataType (enum value 0), use raw raw codec blocks (0x55)")]
	RawType,
	#[error("PbNode data type ({0}) is not supported")]
	DataTypeNotSupported(i32),
	#[error("PbNode Data is invalid: {0:?}")]
	DecodeData(#[from] prost::DecodeError),
	#[error("PbNode data is empty")]
	EmtpyData,
	#[error("Symlink path is invalid: {0:?}")]
	InvalidSymlinkPath(FromUtf8Error),
	#[error("Symlink SHOULD not contains links")]
	SymlinkWithLinks,
}

#[cfg(feature = "vfs")]
impl From<PbNodeErr> for VfsError {
	fn from(pb: PbNodeErr) -> Self {
		vfs_err(VfsErrorKind::NotSupported, pb)
	}
}
