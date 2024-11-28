use derive_builder::Builder;
use derive_more::From;
use std::num::NonZeroUsize;

pub mod unixfs;
pub use unixfs::{FileSystemWriter, PbLink, PbNode, UnixFs};

mod flat_iterator;
pub use flat_iterator::{FlatIterErr, FlatIterator};
mod with_cid;
pub use with_cid::WithCid;

#[cfg(test)]
mod test_helpers;

#[derive(Clone, Copy)]
pub enum CidCodec {
	DagPb = 0x70,
	Raw = 0x55,
}

impl TryFrom<u64> for CidCodec {
	type Error = ();

	fn try_from(codec: u64) -> Result<Self, Self::Error> {
		match codec {
			0x70 => Ok(CidCodec::DagPb),
			0x55 => Ok(CidCodec::Raw),
			_ => Err(()),
		}
	}
}

impl From<CidCodec> for u64 {
	fn from(codec: CidCodec) -> u64 {
		match codec {
			CidCodec::DagPb => 0x70,
			CidCodec::Raw => 0x55,
		}
	}
}

#[derive(Clone, Copy)]
pub enum WellKnownChunkSize {
	F32B = 32,
	F512B = 512,
	F1KiB = 1024,
	F16KiB = 16384,
	F256KiB = 262144,
}

#[derive(Clone, Copy, From)]
pub enum ChunkPolicy {
	FixedSize(WellKnownChunkSize),
	// Rabin
}

impl From<ChunkPolicy> for NonZeroUsize {
	fn from(policy: ChunkPolicy) -> NonZeroUsize {
		match policy {
			ChunkPolicy::FixedSize(size) => unsafe { NonZeroUsize::new_unchecked(size as usize) },
			// ChunkPolicy::Rabin => 262144,
		}
	}
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum LeafPolicy {
	Raw,
	// UnixFs,
}

#[derive(Clone, Copy)]
pub enum DAGLayout {
	// Balanced(MaxChildren),
	// Trickle(MaxChildren, LayerRepeats),
	Flat,
}

#[derive(Clone, Copy)]
pub enum MaxChildren {
	C11 = 11,
	C44 = 44,
	C174 = 174,
}

#[derive(Clone, Copy)]
pub enum LayerRepeats {
	LR1 = 1,
	LR4 = 4,
	LR16 = 16,
}

#[derive(Builder, Clone, Copy)]
pub struct Config {
	#[builder(default = "ChunkPolicy::FixedSize(WellKnownChunkSize::F256KiB)")]
	pub chunk_policy: ChunkPolicy,
	#[builder(default = "LeafPolicy::Raw")]
	pub leaf_policy: LeafPolicy,
	#[builder(default = "DAGLayout::Flat")]
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
