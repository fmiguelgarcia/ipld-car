use crate::bounded_reader::error::BoundedReaderErr;

use std::ops::Range;

pub trait Bounded {
	/// Returns the absolute range this bounded reader is restricted to.
	fn bounds(&self) -> Range<u64>;

	/// Returns the length of the bounded range.
	fn bound_len(&self) -> u64;

	/// Creates a new bounded reader that is a sub-range of this one.
	fn sub<R: BoundedIndex<Self>>(&self, range: R) -> Result<Self, BoundedReaderErr>
	where
		Self: Sized;

	fn clamped_sub<R: BoundedIndex<Self>>(&self, range: R) -> Self
	where
		Self: Sized;
}

/// Trait for types that can be used as sub-ranges of a bounded reader.
pub trait BoundedIndex<T> {
	fn get(self, bounded: &T) -> Result<T, BoundedReaderErr>;
	fn clamped_get(self, bounded: &T) -> T;
}

pub trait CloneAndRewind {
	/// Clones this bounded reader and resets the read position to the start of the range.
	fn clone_and_rewind(&self) -> Self;
}
