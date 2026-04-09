use crate::{
	ensure,
	error::{Error, InvalidErr, Result},
};

use derive_more::Constructor;
use libipld::Cid;
use std::{
	io::{Read, Seek},
	ops::Range,
};

#[derive(Constructor, PartialEq, Eq, derive_more::Debug, Clone)]
pub struct BlockDef {
	#[debug("{}", cid.to_string())]
	pub cid: Cid,
	pub range: Range<u64>,
	pub car_overhead_byte_counter: u64,
}

impl BlockDef {
	pub fn load<R: Read + Seek>(reader: &mut R) -> Result<Option<Self>> {
		let block_def_start = reader.stream_position()?;
		let Ok(section_len) = leb128::read::unsigned(reader) else { return Ok(None) };

		let cid_start = reader.stream_position()?;
		let cid = Cid::read_bytes(&mut *reader).map_err(InvalidErr::from)?;

		let data_start = reader.stream_position()?;
		let encoded_cid_len = data_start - cid_start;
		ensure!(section_len >= encoded_cid_len, InvalidErr::BlockLen);

		let range = data_start..cid_start.checked_add(section_len).ok_or(Error::FileTooLarge)?;
		let car_overhead_byte_counter = data_start - block_def_start;
		Ok(Some(Self { cid, range, car_overhead_byte_counter }))
	}
}
