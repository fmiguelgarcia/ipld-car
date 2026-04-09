use std::{io, ops::Range, sync::PoisonError};
use thiserror::Error;

use crate::bounded_reader::traits::Bounded;

/// Errors that can occur when working with bounded readers.
#[derive(Error, Debug)]
pub enum BoundedReaderErr {
	#[error("Bounded reader ({bounded_start}..{bounded_end}) len is to large with delta start `{delta_start}` and/or delta end `{delta_end}`")]
	FileTooLarge { bounded_start: u64, bounded_end: u64, delta_start: u64, delta_end: u64 },
	#[error("Sub bound start exceeds limits of original bounds")]
	SubBoundStartExceedLimits { bounded_start: u64, bounded_end: u64, delta_start: u64 },
	#[error("Sub bound end exceeds limits of original bounds")]
	SubBoundEndExceedLimits { bounded_start: u64, bounded_end: u64, delta_end: u64 },
	#[error("Range ({start}..{end}) is invalid, start MUST be less or equal to end")]
	InvalidRange { start: u64, end: u64 },
	#[error("Shared reader's mutex is poisoned")]
	ReaderPoisoned,
	#[error(transparent)]
	Io(#[from] io::Error),
	#[error("Range ({0}..{1}) is not supported by reader")]
	RangeNotSupportedByReader(u64, u64),
}

impl BoundedReaderErr {
	pub fn file_too_large<B: Bounded>(bounded: &B, delta_start: u64, delta_end: u64) -> Self {
		let b = bounded.bounds();
		Self::FileTooLarge { bounded_start: b.start, bounded_end: b.end, delta_start, delta_end }
	}

	pub fn sub_start_exceed<B: Bounded>(bounded: &B, delta_start: u64) -> Self {
		let b = bounded.bounds();
		Self::SubBoundStartExceedLimits { bounded_start: b.start, bounded_end: b.end, delta_start }
	}

	pub fn sub_end_exceed<B: Bounded>(bounded: &B, delta_end: u64) -> Self {
		let b = bounded.bounds();
		Self::SubBoundEndExceedLimits { bounded_start: b.start, bounded_end: b.end, delta_end }
	}

	#[inline]
	pub const fn invalid_range(r: Range<u64>) -> Self {
		Self::InvalidRange { start: r.start, end: r.end }
	}

	#[inline]
	pub const fn range_not_supp(s: u64, e: u64) -> Self {
		Self::RangeNotSupportedByReader(s, e)
	}
}

impl<T> From<PoisonError<T>> for BoundedReaderErr {
	#[inline]
	fn from(_: PoisonError<T>) -> Self {
		BoundedReaderErr::ReaderPoisoned
	}
}
