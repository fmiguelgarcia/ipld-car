use crate::error::{CidErr, Error, InvalidErr};

use derivative::Derivative;
use derive_builder::Builder;
use derive_more::From;
use libipld::{
	multihash::{self, Hasher, Sha2_256},
	Cid,
};
use nbytes::bytes;
use num_enum::TryFromPrimitive;
use std::{io::Write, num::NonZeroUsize};

/// Configuration of CID generation.
#[derive(Debug, Clone, Copy, Builder, Derivative)]
#[derivative(Default)]
pub struct Config {
	#[builder(default = "ChunkPolicy::FixedSize(WellKnownChunkSize::F256KiB)")]
	pub chunk_policy: ChunkPolicy,
	#[builder(default = "LeafPolicy::Raw")]
	pub leaf_policy: LeafPolicy,
	#[builder(default = "DAGLayout::Flat")]
	pub layout: DAGLayout,
	#[builder(default = "multihash::Code::Sha2_256")]
	#[derivative(Default(value = "multihash::Code::Sha2_256"))]
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

impl TryFrom<&Cid> for Config {
	type Error = Error;

	fn try_from(cid: &Cid) -> Result<Self, Self::Error> {
		let cid_codec = CidCodec::try_from(cid.codec()).map_err(|_| CidErr::CodecNotSupported(cid.codec()))?;
		let hash_codec = multihash::Code::try_from(cid.hash().code())?;

		ConfigBuilder::default()
			.cid_codec(cid_codec)
			.hash_code(hash_codec)
			.build()
			.map_err(|build_err| Error::from(InvalidErr::ConfigBuilder(build_err.to_string())))
	}
}

/// CID codec identifiers for IPLD content encoding.
#[cfg_attr(feature = "std", derive(Debug))]
#[derive(Default, Clone, Copy, PartialEq, Eq, TryFromPrimitive)]
#[repr(u64)]
pub enum CidCodec {
	Raw = 0x55,
	#[default]
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
	C44 = 44,
	#[default]
	C174 = 174,
}

/// Number of times a layer repeats in trickle DAG layout (future use).
#[cfg_attr(feature = "std", derive(Debug))]
#[derive(Clone, Copy, Default, PartialEq, Eq)]
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
#[derive(Clone, Copy, PartialEq, Eq, Default)]
pub enum DAGLayout {
	#[default]
	Flat,
	Balanced(MaxChildren),
	Trickle(MaxChildren, LayerRepeats),
}

impl DAGLayout {
	pub fn max_children_per_layer(&self) -> u32 {
		match self {
			Self::Flat => u32::MAX,
			Self::Balanced(max) => *max as u8 as u32,
			Self::Trickle(max, _) => *max as u8 as u32,
		}
	}
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
