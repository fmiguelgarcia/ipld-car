use std::io;
use thiserror::Error;

/// Errors that can occur when working with bounded readers.
#[derive(Error, Debug)]
pub enum BoundedReaderErr {
	#[error("Bounded reader len is to large")]
	FileTooLarge,
	#[error("Sub bound exceeds limits of original bounds")]
	SubBoundExceedLimits,
	#[error("Range start MUST be less or equal to end")]
	InvalidRange,
	#[error("Shared reader's mutex is poisoned")]
	ReaderPoisoned,
	#[error("Mandatory seek failed: {0:?}")]
	Seek(io::Error),
	#[error("Range ({0}..{1}) is not supported by reader")]
	RangeNotSupportedByReader(u64, u64),
}
