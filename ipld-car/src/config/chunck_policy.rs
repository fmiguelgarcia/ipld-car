use derive_more::From;
use std::num::NonZeroUsize;
use strum::{EnumIter, EnumString, IntoEnumIterator};

/// How chunks are defined.
///
/// Only fixed-size chunks are currently supported.
#[cfg_attr(feature = "std", derive(Debug))]
#[derive(Clone, Copy, From, PartialEq, Eq)]
pub enum ChunkPolicy {
	FixedSize(WellKnownChunkSize),
	// Rabin
}

impl ChunkPolicy {
	/// Returns the fixed chunk size in bytes if configured.
	pub fn fixed_size(&self) -> Option<usize> {
		match self {
			Self::FixedSize(well_known) => Some(*well_known as usize),
			// _ => None,
		}
	}
}

impl Default for ChunkPolicy {
	fn default() -> Self {
		Self::FixedSize(WellKnownChunkSize::default())
	}
}

/// Converts chunk policy to NonZeroUsize for APIs requiring non-zero sizes.
impl From<ChunkPolicy> for NonZeroUsize {
	fn from(policy: ChunkPolicy) -> NonZeroUsize {
		match policy {
			ChunkPolicy::FixedSize(size) => unsafe { NonZeroUsize::new_unchecked(size as usize) },
		}
	}
}

/// Well-known chunk sizes for fixed-size chunks.
///
/// Chunk sizes larger than `256KiB` need the feature `jumbo-chunks`.
#[cfg_attr(feature = "std", derive(Debug))]
#[derive(Default, Clone, Copy, PartialEq, Eq, strum::Display, EnumIter, EnumString)]
#[repr(u32)]
pub enum WellKnownChunkSize {
	#[strum(serialize = "32B", ascii_case_insensitive)]
	F32B = 32,
	#[strum(serialize = "512B", ascii_case_insensitive)]
	F512B = 512,
	#[strum(serialize = "1KiB", ascii_case_insensitive)]
	F1KiB = 1024,
	#[strum(serialize = "16KiB", ascii_case_insensitive)]
	F16KiB = 16 * 1024,
	#[default]
	#[strum(serialize = "256KiB", ascii_case_insensitive)]
	F256KiB = 256 * 1024,
	#[strum(serialize = "1MiB", ascii_case_insensitive)]
	F1MiB = 1_024 * 1_024,
	#[cfg(feature = "jumbo-chunks")]
	#[strum(serialize = "8MiB", ascii_case_insensitive)]
	F8MiB = 8 * 1_024 * 1_024,
	#[cfg(feature = "jumbo-chunks")]
	#[strum(serialize = "32MiB", ascii_case_insensitive)]
	F32MiB = 32 * 1_024 * 1_024,
	#[cfg(feature = "jumbo-chunks")]
	#[strum(serialize = "128MiB", ascii_case_insensitive)]
	F128MiB = 128 * 1_024 * 1_024,
	#[cfg(feature = "jumbo-chunks")]
	#[strum(serialize = "256MiB", ascii_case_insensitive)]
	F256MiB = 256 * 1_024 * 1_024,
	#[cfg(feature = "jumbo-chunks")]
	#[strum(serialize = "512MiB", ascii_case_insensitive)]
	F512MiB = 512 * 1_024 * 1_024,
}

fn valid_chunk_size_args() -> String {
	WellKnownChunkSize::iter().map(|wk| wk.to_string()).collect::<Vec<_>>().join(", ")
}

impl std::str::FromStr for ChunkPolicy {
	type Err = String;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		let wkcs: WellKnownChunkSize = s
			.parse()
			.map_err(|_| format!("Invalid `ChunkSize`, valid options are: {}", valid_chunk_size_args()))?;
		Ok(ChunkPolicy::FixedSize(wkcs))
	}
}
