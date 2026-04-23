use crate::{
	bounded_reader::{sync::BoundedReader, traits::Bounded as _},
	car::{Block, BlockDef, CarHeader, ContentAddressableArchive},
	config::{CidCodec, Config},
	dag_pb::DagPb,
	error::{Error, Result},
	fail,
};

use std::io::{Read, Seek, SeekFrom};
use tracing::trace;

impl<F: Read + Seek> ContentAddressableArchive<F> {
	/// Loads a CAR from a reader, parsing the header and all blocks.
	pub fn load(reader: F) -> Result<Self> {
		let mut reader = BoundedReader::from_reader(reader)?;
		let mut this = Self::base_new(reader.clone(), Config::default());

		// Load header
		let header = CarHeader::load(&mut reader)?;
		this.car_overhead_byte_counter += reader.stream_position()?;
		trace!(?header, pos = this.car_overhead_byte_counter, "Header loaded");

		// load each blocka
		while let Some(block_def) = BlockDef::load(&mut reader)? {
			// Block elements: content & consolidation info from `reader`
			trace!(?block_def, "BlockDef loaded");
			this.car_overhead_byte_counter += block_def.car_overhead_byte_counter;
			let block_data = reader.sub(block_def.range.clone())?;

			// Load block based on its CID.
			let cid_codec = block_def.cid.codec();
			let codec = CidCodec::from_repr(cid_codec).ok_or(Error::CodecNotSupported(cid_codec))?;
			match codec {
				CidCodec::Raw => this.add_block(Block::new_raw(block_def.cid, block_data)),
				CidCodec::DagPb => DagPb::load(&mut this, block_def.cid, block_data)?,
				_other => fail!(Error::CodecNotSupported(cid_codec)),
			};
			reader.seek(SeekFrom::Start(block_def.range.end))?;
		}

		Ok(this)
	}
}
