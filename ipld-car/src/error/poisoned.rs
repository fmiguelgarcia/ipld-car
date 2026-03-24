use thiserror::Error;
#[cfg(feature = "vfs")]
use vfs::error::{VfsError, VfsErrorKind};

#[cfg_attr(feature = "std", derive(Debug))]
#[derive(Error)]
pub enum PoisonedErr {
	#[error("Node lock is poisoned")]
	Node,
	#[error("CAR lock is poisoned")]
	CAR,
	#[error("Storage lock is poisoned")]
	Storage,
}

#[cfg(feature = "vfs")]
impl From<PoisonedErr> for VfsError {
	fn from(p: PoisonedErr) -> Self {
		VfsErrorKind::Other(p.to_string()).into()
	}
}
