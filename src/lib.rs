pub mod config;
pub use config::{ChunkPolicy, Config, DAGLayout, LeafPolicy, WellKnownChunkSize};
#[cfg(feature = "std")]
pub use config::{ConfigBuilder, ConfigBuilderError};

pub mod unixfs;
use thiserror::Error;
pub use unixfs::{
	file_system_writer::from_links, FileSystemReader, FileSystemReaderError, FileSystemWriter, FileSystemWriterError,
	PbLink, PbNode, UnixFs,
};

mod flat_iterator;
pub use flat_iterator::{FlatIterErr, FlatIterator};
mod with_cid;
pub use with_cid::{cid_from_reader, WithCid};

#[cfg(test)]
mod test_helpers;

// Reexport libipld
pub use libipld::Cid;

#[derive(Clone, Copy, Debug)]
pub enum CidCodec {
	DagPb = 0x70,
	Raw = 0x55,
}

#[derive(Error, Debug)]
pub enum CidCodecError {
	#[error("Unsupported CID codec: {0}")]
	UnsupportedCodec(u64),
}

impl TryFrom<Cid> for CidCodec {
	type Error = CidCodecError;

	fn try_from(cid: Cid) -> Result<Self, Self::Error> {
		let raw_codec = cid.codec();
		Self::try_from(raw_codec).map_err(|_| CidCodecError::UnsupportedCodec(raw_codec))
	}
}

impl TryFrom<u64> for CidCodec {
	type Error = CidCodecError;

	fn try_from(codec: u64) -> Result<Self, Self::Error> {
		match codec {
			0x70 => Ok(CidCodec::DagPb),
			0x55 => Ok(CidCodec::Raw),
			raw_codec => Err(CidCodecError::UnsupportedCodec(raw_codec)),
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
