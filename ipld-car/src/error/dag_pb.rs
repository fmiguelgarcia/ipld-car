use crate::bounded_reader::error::BoundedReaderErr;

use std::io;
use thiserror::Error;

pub type DagPbResult<T> = Result<T, DagPbErr>;

#[derive(Error, Debug)]
pub enum DagPbErr {
	#[error("PbNode cannot be decoded because it exceeds the buffer limit")]
	ExceedBufLimitOnDecode,
	#[error("File too large")]
	FileTooLarge,

	#[error(transparent)]
	Io(#[from] io::Error),
	#[error(transparent)]
	UnixFs(#[from] UnixFsErr),
	#[error(transparent)]
	BoundedReader(#[from] BoundedReaderErr),
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
	#[error("UnixFs symlink MUST NOT have children in `PbNode.links`")]
	SymlinkWithChildren,
	#[error("UnixFs symlink DOES NOT contains information")]
	MissingSymlinkInfo,
	#[error("UnixFs symlink's path is NOT an UFT-8")]
	SymlinkPathUtf8,
	#[error("UnixFs file contains data and reader")]
	FileWithDataAndReader,
}
