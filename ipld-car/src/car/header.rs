use crate::{
	car::cbor_cid::CborCid,
	ensure,
	error::{InvalidErr, NotSupportedErr, Result},
};

use bytes::{BufMut, BytesMut};
use libipld::Cid;
use serde::{Deserialize, Serialize};
use std::io::{Read, Seek, Write};

/// CAR v1 file header.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CarHeader {
	/// Root CIDs of the DAG stored in this archive.
	pub roots: Vec<CborCid>,
	/// Format version — must be `1` for CAR v1.
	pub version: u64,
}

impl CarHeader {
	/// Build a v1 header from a list of root CIDs.
	pub fn new_v1<I>(roots: I) -> Self
	where
		I: IntoIterator<Item = Cid>,
	{
		Self { version: 1, roots: roots.into_iter().map(Into::into).collect() }
	}

	pub fn load<F: Read + Seek>(reader: &mut F) -> Result<CarHeader> {
		let header_len = leb128::read::unsigned(reader).map_err(|_| InvalidErr::HeaderLen)?;
		let mut header_reader = reader.take(header_len);
		let header: CarHeader = ciborium::de::from_reader(&mut header_reader).map_err(InvalidErr::from)?;
		ensure!(header.version == 1, NotSupportedErr::Version(header.version));

		Ok(header)
	}

	pub fn write<W: Write>(&self, writer: &mut W) -> Result<usize> {
		// Write header into `but` and calculate its len.
		let mut buf = BytesMut::new().writer();
		ciborium::ser::into_writer(&self, &mut buf)?;
		let buf = buf.into_inner().freeze();
		let buf_len = buf.len() as u64;

		// Write `len` + `encoded header`
		let len_written = leb128::write::unsigned(writer, buf_len)?;
		writer.write_all(&buf)?;
		Ok(buf.len() + len_written)
	}
}
