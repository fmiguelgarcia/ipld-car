use crate::{error::CidErr, BoundedReaderErr};

use std::io;
use thiserror::Error;

pub type DagPbResult<T> = Result<T, DagPbErr>;

#[derive(Error, Debug)]
pub enum DagPbErr {
	#[error("PbNode cannot be decoded because it exceeds the buffer limit")]
	ExceedBufLimitOnDecode,
	#[error("Shared content reader is poisoned")]
	ReaderPoisoned,
	#[error("File too large")]
	FileTooLarge,

	#[error(transparent)]
	Io(#[from] io::Error),
	#[error(transparent)]
	UnixFs(#[from] UnixFsErr),
	#[error(transparent)]
	BoundedReader(#[from] BoundedReaderErr),
	#[error(transparent)]
	Cid(#[from] CidErr),
}

#[derive(Error, Debug)]
pub enum UnixFsErr {
	#[error("UnixFs data is missing")]
	MissingData,
	#[error("Missing link name (mandatory) in Directory PbNode")]
	MissingLinkNameInDirectory,
	#[error("Invalid UnixFs data")]
	InvalidData,
	#[error("UnixFs data type is not supported: {0}")]
	DataTypeNotSupported(i32),
	#[error("UnixFs file with blocksizes {0} elements and {1} links")]
	BlocksizesLenDiffLinksLen(usize, usize),
	#[error("UnixFs file contains links that overflows the maximum file size")]
	LinkSizeOverflow,
	#[error("UnixFs symlink MUST NOT have children in `PbNode.links`")]
	SymlinkWithChildren,
	#[error("UnixFs symlink DOES NOT contains information about symlink")]
	MissingSymlinkInfo,
	#[error("UnixFs symlink's path is NOT an UFT-8")]
	SymlinkPathUtf8,
	#[error("UnixFs file contains data and reader")]
	FileWithDataAndReader,
}
