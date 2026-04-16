use crate::{bounded_reader::error::BoundedReaderErr, ContentAddressableArchive};

use ciborium::ser::Error as CborSerErr;
use std::{
	convert::Infallible,
	io::{self, IntoInnerError, Read, Seek},
	path::PathBuf,
	sync::{MutexGuard, PoisonError},
};
#[cfg(feature = "vfs")]
use vfs::error::{VfsError, VfsErrorKind};

mod not_found;
pub use not_found::NotFoundErr;
mod not_supported;
pub use not_supported::NotSupportedErr;
mod invalid;
pub use invalid::InvalidErr;
mod dag_pb;
pub use dag_pb::{DagPbErr, DagPbResult, UnixFsErr};
mod loop_detected;
pub use loop_detected::LoopDetectedErr;

pub type Result<T> = std::result::Result<T, Error>;

#[cfg_attr(feature = "std", derive(Debug))]
#[derive(thiserror::Error)]
pub enum Error {
	#[error("More than one block ({0}) on path `{1:?}`")]
	MoreThanOneMatchOnPath(usize, PathBuf),
	#[error("File too large")]
	FileTooLarge,
	#[error("CAR lock is poisoned")]
	Poison,
	#[error("CID codec `{0}` is not supported")]
	CodecNotSupported(u64),

	#[error(transparent)]
	LoopDetected(#[from] LoopDetectedErr),
	#[error(transparent)]
	Hasher(#[from] libipld::multihash::Error),
	#[error(transparent)]
	NotFound(#[from] NotFoundErr),
	#[error(transparent)]
	NotSupported(#[from] NotSupportedErr),
	#[error(transparent)]
	Invalid(#[from] InvalidErr),
	#[error(transparent)]
	Io(#[from] io::Error),
	#[error(transparent)]
	BoundedReader(#[from] BoundedReaderErr),
	#[error(transparent)]
	DagPb(#[from] DagPbErr),
}

impl Error {
	pub fn more_than_one<P: Into<PathBuf>>(matches: usize, path: P) -> Self {
		Self::MoreThanOneMatchOnPath(matches, path.into())
	}
}

#[cfg(feature = "vfs")]
impl From<Error> for VfsError {
	fn from(ce: Error) -> Self {
		match ce {
			Error::NotSupported(not_supported) => not_supported.into(),
			Error::Io(io_err) => VfsErrorKind::IoError(io_err).into(),
			Error::NotFound(not_found) => not_found.into(),
			Error::Invalid(invalid) => invalid.into(),
			e => VfsErrorKind::Other(e.to_string()).into(),
		}
	}
}

impl From<Infallible> for Error {
	fn from(_: Infallible) -> Self {
		unreachable!("Infallible error");
	}
}

impl<T> From<IntoInnerError<T>> for Error {
	fn from(inner_err: IntoInnerError<T>) -> Self {
		Self::Io(inner_err.into())
	}
}

impl From<CborSerErr<io::Error>> for Error {
	fn from(e: CborSerErr<io::Error>) -> Self {
		match e {
			CborSerErr::Io(io_err) => Error::Io(io_err),
			CborSerErr::Value(msg) => Error::Invalid(InvalidErr::CborEnc(msg)),
		}
	}
}

impl<T: Read + Seek> From<PoisonError<MutexGuard<'_, ContentAddressableArchive<T>>>> for Error {
	#[inline]
	fn from(_: PoisonError<MutexGuard<'_, ContentAddressableArchive<T>>>) -> Self {
		Self::Poison
	}
}

/// Creates a `VfsError` using `kind` and `context`.
#[cfg(feature = "vfs")]
pub(crate) fn vfs_err<S>(kind: VfsErrorKind, context: S) -> VfsError
where
	S: std::fmt::Display + Send + Sync + 'static,
{
	VfsError::from(kind).with_context(move || context.to_string())
}

pub(crate) const NODE_IDX_QED: &str = "NodeIndex exists .qed";
