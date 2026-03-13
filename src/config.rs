use derive_builder::Builder;
use derive_more::From;
use libipld::multihash::{self, Hasher, Sha2_256};
use nbytes::bytes;
use num_enum::TryFromPrimitive;
use std::{io::Write, num::NonZeroUsize};

/// Configuration of CID generation.
#[derive(Debug, Clone, Copy, Builder)]
pub struct Config {
	#[builder(default = "ChunkPolicy::FixedSize(WellKnownChunkSize::F256KiB)")]
	pub chunk_policy: ChunkPolicy,
	#[builder(default = "LeafPolicy::Raw")]
	pub leaf_policy: LeafPolicy,
	#[builder(default = "DAGLayout::Flat")]
	pub layout: DAGLayout,
	#[builder(default = "multihash::Code::Sha2_256")]
	pub hash_code: multihash::Code,
	#[builder(default = "CidCodec::DagPb")]
	pub cid_codec: CidCodec,
}

/// Trait combining hasher and write capabilities for streaming CID calculation.
pub trait HasherAndWrite: Hasher + Write + Send {}
impl<T: Hasher + Write + Send> HasherAndWrite for T {}

impl Config {
	/// Returns a hasher instance for the configured hash code.
	pub fn hasher(&self) -> Option<Box<dyn HasherAndWrite>> {
		match self.hash_code {
			multihash::Code::Sha2_256 => Some(Box::new(Sha2_256::default())),
			_unsupported => None,
		}
	}
}

impl Default for Config {
	fn default() -> Self {
		Config {
			chunk_policy: ChunkPolicy::FixedSize(WellKnownChunkSize::F256KiB),
			leaf_policy: LeafPolicy::Raw,
			layout: DAGLayout::Flat,
			hash_code: multihash::Code::Sha2_256,
			cid_codec: CidCodec::DagPb,
		}
	}
}

/// CID codec identifiers for IPLD content encoding.
#[cfg_attr(feature = "std", derive(Debug))]
#[derive(Default, Clone, Copy, PartialEq, Eq, TryFromPrimitive)]
#[repr(u64)]
pub enum CidCodec {
	#[default]
	Raw = 0x55,
	DagPb = 0x70,
	DagCbor = 0x71,
	DagJson = 0x0129,
}

/// Maximum children count for balanced/trickle DAG layouts (future use).
#[cfg_attr(feature = "std", derive(Debug))]
#[derive(Default, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum MaxChildren {
	C11 = 11,
	#[default]
	C44 = 44,
	C174 = 174,
}

/// Number of times a layer repeats in trickle DAG layout (future use).
#[cfg_attr(feature = "std", derive(Debug))]
#[derive(Clone, Copy, Default)]
#[repr(u8)]
pub enum LayerRepeats {
	LR1 = 1,
	LR4 = 4,
	#[default]
	LR16 = 16,
}
/// Defines the layout of the Directed Acyclic Graph (DAG).
///
/// Only `Flat` layout is currently supported.
#[cfg_attr(feature = "std", derive(Debug))]
#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub enum DAGLayout {
	#[default]
	Flat,
	// Balanced(MaxChildren),
	// Trickle(MaxChildren, LayerRepeats),
}

/// Defines the configuration of leaf in a DAG.
#[cfg_attr(feature = "std", derive(Debug))]
#[derive(Clone, Copy, PartialEq, Eq, Default)]
pub enum LeafPolicy {
	#[default]
	Raw,
	UnixFs,
}

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
#[derive(Default, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum WellKnownChunkSize {
	F32B = 32,
	F512B = 512,
	F1KiB = bytes!(1; KiB),
	F16KiB = bytes!(16; KiB),
	#[default]
	F256KiB = bytes!(256; KiB),
	F1MiB = bytes!(1; MiB),
	#[cfg(feature = "jumbo-chunks")]
	F8MiB = bytes!(8; MiB),
	#[cfg(feature = "jumbo-chunks")]
	F32MiB = bytes!(32; MiB),
	#[cfg(feature = "jumbo-chunks")]
	F128MiB = bytes!(128; MiB),
	#[cfg(feature = "jumbo-chunks")]
	F256MiB = bytes!(256; MiB),
	#[cfg(feature = "jumbo-chunks")]
	F512MiB = bytes!(512; MiB),
}
