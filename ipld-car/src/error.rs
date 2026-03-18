use crate::{BoundedReaderErr, ContentAddressableArchive};

use ciborium::{de::Error as CborDeErr, ser::Error as CborSerErr};
use std::{
	convert::Infallible,
	ffi::OsStr,
	io::{self, IntoInnerError, Read, Seek},
	path::Path,
	sync::{MutexGuard, PoisonError},
};
#[cfg(feature = "vfs")]
use vfs::error::{VfsError, VfsErrorKind};

mod not_found;
pub use not_found::NotFoundErr;
mod poisoned;
pub use poisoned::PoisonedErr;
mod not_supported;
pub use not_supported::NotSupportedErr;
mod invalid;
pub use invalid::InvalidErr;
mod cid;
pub use cid::CidErr;
mod pb_node;
pub use pb_node::PbNodeErr;
mod dag_pb;
pub use dag_pb::{DagPbErr, DagPbResult, UnixFsErr};

pub type Result<T> = std::result::Result<T, Error>;

#[cfg_attr(feature = "std", derive(Debug))]
#[derive(thiserror::Error)]
pub enum Error {
	#[error("More than one block ({0}) on path `{1}`")]
	MoreThanOneMatchOnPath(usize, String),
	#[error("File too large")]
	FileTooLarge,
	#[error(transparent)]
	PbNode(#[from] PbNodeErr),
	#[error(transparent)]
	Hasher(#[from] libipld::multihash::Error),
	#[error(transparent)]
	NotFound(#[from] NotFoundErr),
	#[error(transparent)]
	Poison(#[from] PoisonedErr),
	#[error(transparent)]
	NotSupported(#[from] NotSupportedErr),
	#[error(transparent)]
	Invalid(#[from] InvalidErr),
	#[error(transparent)]
	Io(#[from] io::Error),
	#[error(transparent)]
	BoundedReader(#[from] BoundedReaderErr),
	#[error(transparent)]
	Cid(#[from] CidErr),
	#[error(transparent)]
	DagPb(#[from] DagPbErr),
}

impl Error {
	pub fn more_than_one(matches: usize, path: &Path) -> Self {
		let path = path.as_os_str().to_string_lossy().to_string();
		Self::MoreThanOneMatchOnPath(matches, path)
	}
	pub fn invalid_path(path: &Path) -> Self {
		let path = path.as_os_str().to_string_lossy().to_string();
		Self::Invalid(InvalidErr::Path(path))
	}

	pub fn non_utf8_component(component: &OsStr) -> Self {
		let path = component.to_string_lossy().to_string();
		Self::Invalid(InvalidErr::NotUtf8Component(path))
	}
}

#[cfg(feature = "vfs")]
impl From<Error> for VfsError {
	fn from(ce: Error) -> Self {
		match ce {
			e @ Error::MoreThanOneMatchOnPath(..) => VfsErrorKind::Other(e.to_string()).into(),
			e @ Error::FileTooLarge => VfsErrorKind::Other(e.to_string()).into(),
			e @ Error::Hasher(..) => VfsErrorKind::Other(e.to_string()).into(),
			e @ Error::Cid(..) => VfsErrorKind::Other(e.to_string()).into(),
			e @ Error::DagPb(..) => VfsErrorKind::Other(e.to_string()).into(),

			Error::NotSupported(not_supported) => not_supported.into(),
			Error::Poison(poison) => poison.into(),
			Error::Io(io_err) => VfsErrorKind::IoError(io_err).into(),
			Error::NotFound(not_found) => not_found.into(),
			Error::Invalid(invalid) => invalid.into(),
			Error::PbNode(pb_node) => pb_node.into(),
			Error::BoundedReader(bounded_reader) => VfsErrorKind::Other(bounded_reader.to_string()).into(),
		}
	}
}

impl From<Infallible> for Error {
	fn from(_: Infallible) -> Self {
		panic!("Infallible error");
	}
}

impl<T> From<IntoInnerError<T>> for Error {
	fn from(inner_err: IntoInnerError<T>) -> Self {
		Self::Io(inner_err.into())
	}
}

impl From<CborDeErr<io::Error>> for Error {
	#[inline]
	fn from(e: CborDeErr<io::Error>) -> Self {
		Self::Invalid(InvalidErr::CborDec(e))
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
		Self::Poison(PoisonedErr::CAR)
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
