#[cfg(feature = "vfs")]
use crate::error::vfs_err;

use std::io;
use thiserror::Error;
#[cfg(feature = "vfs")]
use vfs::error::{VfsError, VfsErrorKind};

#[cfg_attr(feature = "std", derive(Debug))]
#[derive(Error)]
pub enum InvalidErr {
	#[error("Invalid path `{0}`")]
	Path(String),
	#[error("Path component is not an utf-8: `{0}`")]
	NotUtf8Component(String),
	#[error("Header length is invalid")]
	HeaderLen,
	#[error("Block length is invalid")]
	BlockLen,
	#[error("Invalid CBOR header: {0}")]
	CborDec(#[from] ciborium::de::Error<io::Error>),
	#[error("Invalid CBOR value: {0}")]
	CborEnc(String),
	#[error("Invalid CID: {0}")]
	Cid(#[from] libipld::cid::Error),
	#[error("Invalid Link")]
	Link,
	#[error("Invalid File ref")]
	FileRef,
	#[error("Directory `{0}` already exists")]
	AlreadyExists(String),
}

#[cfg(feature = "vfs")]
impl From<InvalidErr> for VfsError {
	fn from(ci: InvalidErr) -> Self {
		match ci {
			e @ InvalidErr::Path(..) => vfs_err(VfsErrorKind::InvalidPath, e),
			e @ InvalidErr::NotUtf8Component(..) => vfs_err(VfsErrorKind::InvalidPath, e),
			e @ InvalidErr::HeaderLen => vfs_err(VfsErrorKind::NotSupported, e),
			e @ InvalidErr::BlockLen => vfs_err(VfsErrorKind::NotSupported, e),
			e @ InvalidErr::Link => vfs_err(VfsErrorKind::NotSupported, e),
			e @ InvalidErr::FileRef => vfs_err(VfsErrorKind::NotSupported, e),
			InvalidErr::AlreadyExists(..) => VfsErrorKind::DirectoryExists.into(),
			InvalidErr::CborDec(cbor) => VfsErrorKind::Other(cbor.to_string()).into(),
			InvalidErr::CborEnc(cbor) => VfsErrorKind::Other(cbor).into(),
			InvalidErr::Cid(cid) => VfsErrorKind::Other(cid.to_string()).into(),
		}
	}
}
