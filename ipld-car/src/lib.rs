pub mod bounded_reader;
pub mod car;
pub use car::ContentAddressableArchive;
pub mod config;
pub use config::Config;
pub mod dag_pb;
pub mod error;
pub(crate) mod proto;
pub mod traits;

#[cfg(any(test, feature = "test_helpers"))]
pub mod test_helpers;

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
