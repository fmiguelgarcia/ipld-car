use crate::error::{Error, InvalidErr};

use derivative::Derivative;
use derive_builder::Builder;
use libipld::{
	multihash::{
		self, Blake2b256, Blake2b512, Blake2s128, Blake2s256, Blake3_256, Hasher, Keccak224, Keccak256, Keccak384,
		Keccak512, Sha2_256, Sha2_512, Sha3_224, Sha3_256, Sha3_384, Sha3_512,
	},
	Cid,
};
use std::io::Write;
use strum::FromRepr;

mod chunck_policy;
pub use chunck_policy::{ChunkPolicy, WellKnownChunkSize};
mod dag_layout;
pub use dag_layout::{DAGLayout, LayerRepeats, MaxChildren};

/// Configuration of CID generation.
#[derive(Debug, Clone, Copy, Builder, Derivative)]
#[derivative(Default)]
#[cfg_attr(feature = "cli", derive(clap::Args))]
pub struct Config {
	/// Chunk policy
	#[builder(default = "ChunkPolicy::FixedSize(WellKnownChunkSize::F256KiB)")]
	#[cfg_attr(feature = "cli", arg(long, value_name = "SIZE", default_value = "256KiB"))]
	pub chunk_policy: ChunkPolicy,

	/// Leaf encoding policy
	#[builder(default = "LeafPolicy::Raw")]
	#[cfg_attr(feature = "cli", arg(long, value_name = "LEAF", default_value = "raw"))]
	pub leaf_policy: LeafPolicy,

	/// DAG layout strategy (flat, balanced, trickle)
	#[builder(default = "DAGLayout::Flat")]
	#[cfg_attr(feature = "cli", arg(skip))]
	pub layout: DAGLayout,

	#[builder(default = "multihash::Code::Sha2_256")]
	#[derivative(Default(value = "multihash::Code::Sha2_256"))]
	#[cfg_attr(feature = "cli", arg(skip = multihash::Code::Sha2_256))]
	pub hash_code: multihash::Code,

	#[builder(default = "CidCodec::DagPb")]
	#[cfg_attr(feature = "cli", arg(long, value_name = "CID_CODEC", default_value = "dag-pb"))]
	pub cid_codec: CidCodec,
}

/// Trait combining hasher and write capabilities for streaming CID calculation.
pub trait HasherAndWrite: Hasher + Write + Send {}
impl<T: Hasher + Write + Send> HasherAndWrite for T {}

impl Config {
	/// Returns a hasher instance for the configured hash code.
	pub fn hasher(&self) -> Result<Box<dyn HasherAndWrite>, Error> {
		let hasher: Box<dyn HasherAndWrite> = match self.hash_code {
			multihash::Code::Sha2_256 => Box::new(Sha2_256::default()),
			multihash::Code::Sha2_512 => Box::new(Sha2_512::default()),
			multihash::Code::Sha3_224 => Box::new(Sha3_224::default()),
			multihash::Code::Sha3_256 => Box::new(Sha3_256::default()),
			multihash::Code::Sha3_384 => Box::new(Sha3_384::default()),
			multihash::Code::Sha3_512 => Box::new(Sha3_512::default()),
			multihash::Code::Keccak224 => Box::new(Keccak224::default()),
			multihash::Code::Keccak256 => Box::new(Keccak256::default()),
			multihash::Code::Keccak384 => Box::new(Keccak384::default()),
			multihash::Code::Keccak512 => Box::new(Keccak512::default()),
			multihash::Code::Blake2b256 => Box::new(Blake2b256::default()),
			multihash::Code::Blake2b512 => Box::new(Blake2b512::default()),
			multihash::Code::Blake2s128 => Box::new(Blake2s128::default()),
			multihash::Code::Blake2s256 => Box::new(Blake2s256::default()),
			multihash::Code::Blake3_256 => Box::new(Blake3_256::default()),
		};
		Ok(hasher)
	}
}

impl TryFrom<&Cid> for Config {
	type Error = Error;

	fn try_from(cid: &Cid) -> Result<Self, Self::Error> {
		let cid_codec_repr = cid.codec();
		let cid_codec = CidCodec::from_repr(cid_codec_repr).ok_or(Error::CodecNotSupported(cid_codec_repr))?;
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
#[derive(Default, Clone, Copy, PartialEq, Eq, FromRepr)]
#[cfg_attr(feature = "cli", derive(clap::ValueEnum))]
#[repr(u64)]
pub enum CidCodec {
	Raw = 0x55,
	#[default]
	#[cfg_attr(feature = "cli", value(name = "dag-pb"))]
	DagPb = 0x70,
	#[cfg_attr(feature = "cli", value(name = "dag-cbor"))]
	DagCbor = 0x71,
	#[cfg_attr(feature = "cli", value(name = "dag-json"))]
	DagJson = 0x0129,
}

/// Defines the configuration of leaf in a DAG.
#[derive(Clone, Copy, PartialEq, Eq, Default)]
#[cfg_attr(feature = "std", derive(Debug))]
#[cfg_attr(feature = "cli", derive(clap::ValueEnum))]
pub enum LeafPolicy {
	#[default]
	Raw,
	UnixFs,
}
