#[cfg(feature = "vfs")]
use crate::error::vfs_err;

use libipld::Cid;
use std::{
	io,
	path::{Path, PathBuf},
};
use thiserror::Error;
#[cfg(feature = "vfs")]
use vfs::error::{VfsError, VfsErrorKind};

#[cfg_attr(feature = "std", derive(Debug))]
#[derive(Error)]
pub enum InvalidErr {
	#[error("Path `{0:?}` is NOT a directory")]
	NotADirectory(PathBuf),
	#[error("Path `{0:?}` IS a directory")]
	IsADirectory(PathBuf),
	#[error("Invalid file name in path `{0:?}`")]
	FileName(PathBuf),
	#[error("Path is not an utf-8: `{0:?}`")]
	NotUtf8Path(PathBuf),
	#[error("Directory `{0:?}` already exists")]
	AlreadyExists(PathBuf),
	#[error("Path `{0:?}` points to unavailable block (cid={1}) in this CAR")]
	IsAMissingBlock(PathBuf, String),

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
	#[error("Config builder failed: {0}")]
	ConfigBuilder(String),
}

impl InvalidErr {
	pub fn not_a_dir<P: AsRef<Path>>(path: P) -> Self {
		Self::NotADirectory(path.as_ref().to_owned())
	}

	pub fn is_a_dir<P: AsRef<Path>>(path: P) -> Self {
		Self::IsADirectory(path.as_ref().to_owned())
	}

	pub fn file_name<P: AsRef<Path>>(path: P) -> Self {
		Self::FileName(path.as_ref().to_owned())
	}

	pub fn not_utf8_path<P: AsRef<Path>>(path: P) -> Self {
		Self::NotUtf8Path(path.as_ref().to_owned())
	}

	pub fn exists<P: AsRef<Path>>(path: P) -> Self {
		Self::AlreadyExists(path.as_ref().to_owned())
	}

	pub fn is_a_miss_block<P: AsRef<Path>>(path: P, cid: &Cid) -> Self {
		Self::IsAMissingBlock(path.as_ref().to_owned(), cid.to_string())
	}
}

#[cfg(feature = "vfs")]
impl From<InvalidErr> for VfsError {
	fn from(ci: InvalidErr) -> Self {
		match ci {
			InvalidErr::AlreadyExists(..) => VfsErrorKind::DirectoryExists.into(),
			InvalidErr::CborDec(cbor) => VfsErrorKind::Other(cbor.to_string()).into(),
			InvalidErr::CborEnc(cbor) => VfsErrorKind::Other(cbor).into(),
			InvalidErr::Cid(cid) => VfsErrorKind::Other(cid.to_string()).into(),
			e => vfs_err(VfsErrorKind::NotSupported, e),
		}
	}
}
