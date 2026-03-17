use std::io::{Read, Seek, Write};

pub mod arena;
pub use arena::{Arena, ArenaId, ArenaItem};
pub mod bounded_reader;
pub use bounded_reader::{BoundedReader, BoundedReaderErr};
pub mod car;
pub use car::ContentAddressableArchive;
pub mod config;
pub mod dag_pb;
pub mod error;
pub(crate) mod proto;

#[cfg(test)]
pub(crate) mod test_helpers;

/// Trait combining Seek and Read, return value for opening files
pub trait SeekAndRead: Seek + Read {}
impl<T> SeekAndRead for T where T: Seek + Read {}

/// Trait combining Seek and Write, return value for writing files
pub trait SeekAndWrite: Seek + Write {}
impl<T> SeekAndWrite for T where T: Seek + Write {}

pub trait ContextLen {
	fn data_len(&self) -> u64;
	fn dag_pb_len(&self) -> u64;
}

// Helper macros
// ============================================================================

#[macro_export]
macro_rules! fail {
	( $y:expr ) => {{
		return Err($y.into());
	}};
}

#[macro_export]
macro_rules! ensure {
	( $x:expr) => {{
		#[allow(clippy::neg_cmp_op_on_partial_ord)]
		if !$x {
			return false;
		}
	}};
	( $x:expr, $y:expr $(,)? ) => {{
		#[allow(clippy::neg_cmp_op_on_partial_ord)]
		if !$x {
			$crate::fail!($y);
		}
	}};
}
