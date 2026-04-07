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
}

impl BlockDef {
	pub fn load<R: Read + Seek>(reader: &mut R) -> Result<Option<Self>> {
		let Ok(section_len) = leb128::read::unsigned(reader) else { return Ok(None) };

		let cid_start = reader.stream_position()?;
		let cid = Cid::read_bytes(&mut *reader).map_err(InvalidErr::from)?;

		let start = reader.stream_position()?;
		let encoded_cid_len = start - cid_start;
		ensure!(section_len >= encoded_cid_len, InvalidErr::BlockLen);

		let range = start..cid_start.checked_add(section_len).ok_or(Error::FileTooLarge)?;
		Ok(Some(Self { cid, range }))
	}
}
