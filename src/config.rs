use core::num::NonZeroUsize;
use derive_more::From;

/// Configuration for the DAG storage system.
#[derive(Clone, Copy)]
#[cfg_attr(feature = "std", derive(derive_builder::Builder))]
pub struct Config {
	#[cfg_attr(feature = "std", builder(default = "ChunkPolicy::FixedSize(WellKnownChunkSize::F256KiB)"))]
	pub chunk_policy: ChunkPolicy,
	#[cfg_attr(feature = "std", builder(default = "LeafPolicy::Raw"))]
	pub leaf_policy: LeafPolicy,
	#[cfg_attr(feature = "std", builder(default = "DAGLayout::Flat"))]
	pub layout: DAGLayout,
}

impl Default for Config {
	fn default() -> Self {
		Config {
			chunk_policy: ChunkPolicy::FixedSize(WellKnownChunkSize::F256KiB),
			leaf_policy: LeafPolicy::Raw,
			layout: DAGLayout::Flat,
		}
	}
}

/// Defines the layout of the Directed Acyclic Graph (DAG).
///
/// Only `Flat` layout is currently supported.
#[derive(Clone, Copy)]
pub enum DAGLayout {
	// Balanced(MaxChildren),
	// Trickle(MaxChildren, LayerRepeats),
	Flat,
}

/// Defines the configuration of leaf in a DAG.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum LeafPolicy {
	Raw,
	// UnixFs,
}

/// How chunks are defined.
///
/// Only fixed-size chunks are currently supported.
#[derive(Clone, Copy, From, PartialEq, Eq, Debug)]
pub enum ChunkPolicy {
	FixedSize(WellKnownChunkSize),
	// Rabin
}

impl From<ChunkPolicy> for NonZeroUsize {
	fn from(policy: ChunkPolicy) -> NonZeroUsize {
		match policy {
			ChunkPolicy::FixedSize(size) => unsafe { NonZeroUsize::new_unchecked(size as usize) },
		}
	}
}

/// Well-known chunk sizes for fixed-size chunks.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum WellKnownChunkSize {
	F32B = 32,
	F512B = 512,
	F1KiB = 1_024,
	F16KiB = 16_384,
	F256KiB = 262_144,
	#[cfg(feature = "jumbo-chunks")]
	F1MiB = 1_048_576,
	#[cfg(feature = "jumbo-chunks")]
	F8MiB = 8_388_608,
	#[cfg(feature = "jumbo-chunks")]
	F32MiB = 33_554_432,
	#[cfg(feature = "jumbo-chunks")]
	F128MiB = 134_217_728,
	#[cfg(feature = "jumbo-chunks")]
	F256MiB = 268_435_456,
	#[cfg(feature = "jumbo-chunks")]
	F512MiB = 536_870_912,
}
