use bytes::{buf::Reader, Buf as _, Bytes};
use libipld::Cid;
use quick_protobuf::MessageWrite as _;
use std::io::{Chain, Read, Seek};

pub mod file_reader;
pub mod file_system_reader;
pub mod file_system_writer;
pub mod proto;
pub use file_reader::FileReader;
pub use file_system_reader::{Error as FileSystemReaderError, FileSystemReader};
pub use file_system_writer::{FileSystemWriter, FileSystemWriterError};
pub mod varint;
pub use varint::{VarintRead, VarintReaderError};

pub const SELF_PATH: &str = ".";

pub struct UnixFs<R> {
	pub cid: Cid,
	pub node: Option<PbNode>,
	package_len: u64,
	data: R,
}

impl<R> UnixFs<R> {
	pub fn new(cid: Cid, node: Option<PbNode>, data: R, data_len: u64) -> Option<Self> {
		let node_len = node.as_ref().map(|node| node.get_size()).unwrap_or_default() as u64;
		let package_len = data_len.checked_add(node_len)?;

		Some(Self { cid, node, package_len, data })
	}

	pub fn len(&self) -> u64 {
		self.package_len
	}

	pub fn is_empty(&self) -> bool {
		self.package_len == 0
	}
}

impl<R: Read + Seek> UnixFs<R> {
	pub fn package_reader(mut self) -> Chain<Reader<Bytes>, R> {
		self.data.rewind().expect("Rewind failed on data");
		let header = self.node.as_ref().map(|node| Bytes::from(pb::node::encode(node))).unwrap_or_default();
		header.reader().chain(self.data)
	}
}

pub use libipld::pb::{PbLink, PbNode};

pub(crate) mod pb {
	use crate::unixfs::{VarintRead, VarintReaderError};
	use thiserror::Error;

	#[derive(Debug, Error)]
	pub enum DecodeError {
		#[error(transparent)]
		VarintReader(#[from] VarintReaderError),
		#[error(transparent)]
		QuickProtobuf(#[from] quick_protobuf::Error),
		#[error("Unexpected bytes")]
		UnexpectedBytes,
	}

	pub(crate) mod link {
		use libipld::{pb::PbLink, Cid};

		/// NOTE: `name` is empty string (instead of just `None`), to become compatible with https://dag.ipfs.tech
		pub(crate) fn new(cid: Cid, size: u64) -> PbLink {
			PbLink { cid, size: Some(size), name: Some(String::new()) }
		}
	}

	pub(crate) mod node {
		use super::*;
		use bytes::Bytes;
		use libipld::pb::{PbLink, PbNode};
		use quick_protobuf::{reader::BytesReader as ProtoBytesReader, MessageRead, MessageWrite as _, Writer};
		use std::io::{ErrorKind, Read};

		pub(crate) fn new<D>(mut links: Vec<PbLink>, data: D) -> PbNode
		where
			Bytes: From<D>,
		{
			sort_links(&mut links);
			PbNode { links, data: Some(data.into()) }
		}

		pub(crate) fn encode(node: &PbNode) -> Vec<u8> {
			let mut buf = Vec::with_capacity(node.get_size());
			let mut writer = Writer::new(&mut buf);
			node.write_message(&mut writer).expect("Protobuf is valid .qed");
			buf
		}

		/// # Protobuf Strictness
		/// DAG-PB aims to have a canonical form for any given set of data. Therefore, in addition to the standard
		/// Protobuf parsing rules, DAG-PB decoders should enforce additional constraints to ensure canonical forms
		/// (where possible):
		///
		/// Fields in the PBLink message must appear in the order as defined by the Protobuf schema above, following the
		/// field numbers. Blocks with out-of-order PBLink fields should be rejected. (Note that it is common for
		/// Protobuf decoders to accept out-of-order field entries, which means the DAG-PB spec is somewhat stricter
		/// than may be seen as typical for other Protobuf-based formats.)
		///
		///Fields in the PBNode message must be encoded in the order as defined by the Protobuf schema above. Note that
		/// this order does not follow the field numbers. The decoder should accept either order, as IPFS data exists in
		/// both forms. Duplicate entries in the binary form are invalid; blocks with duplicate field values should be
		/// rejected. (Note that it is common for Protobuf decoders to accept repeated field values in the binary data,
		/// and interpret them as updates to fields that have already been set; DAG-PB is stricter than this.)
		/// Fields and wire types other than those that appear in the Protobuf schema above are invalid and blocks
		/// containing these should be rejected. (Note that it is common for Protobuf decoders to skip data in each
		/// message type that does not match the fields in the schema.)
		pub(crate) fn decode<R: Read>(reader: &mut R) -> Result<PbNode, DecodeError> {
			let mut var_reader = VarintRead::new(reader);
			let mut node = PbNode::default();

			loop {
				match var_reader.next_tag() {
					Ok(10) => {
						let data: Bytes = var_reader.read_bytes()?.into();
						node.data = Some(data);
						// Note: `Data` field is the last one.
						break;
					},
					Ok(18) => {
						let link_msg: Bytes = var_reader.read_message()?.into();
						let mut r = ProtoBytesReader::from_bytes(&link_msg);
						let link = PbLink::from_reader(&mut r, &link_msg)?;
						node.links.push(link);
						debug_assert!(node.data.is_none(), "Links must be encoded before `Data` field.");
					},
					Ok(_) => return Err(DecodeError::UnexpectedBytes),
					Err(err) => match err {
						VarintReaderError::Io(io_err) if io_err.kind() == ErrorKind::UnexpectedEof => break,
						_ => return Err(err.into()),
					},
				}
			}

			Ok(node)
		}

		// Links must be strictly sorted by name before encoding, leaving stable
		// ordering where the names are the same (or absent).
		fn sort_links(links: &mut [PbLink]) {
			links.sort_by(|a, b| {
				let a = a.name.as_ref().map(|s| s.as_bytes()).unwrap_or(&[][..]);
				let b = b.name.as_ref().map(|s| s.as_bytes()).unwrap_or(&[][..]);
				a.cmp(b)
			});
		}
	}
}

#[cfg(test)]
mod tests {
	use crate::{
		test_helpers::{checksum, raw_conf, test_file},
		unixfs::{FileSystemReader, FileSystemWriter},
		Config,
		WellKnownChunkSize::{F16KiB, F1KiB, F256KiB},
	};
	use libipld::multihash::Sha2_256;
	use std::path::Path;
	use test_case::test_case;

	#[test_case(raw_conf(F256KiB),"bitcoin.pdf", ""; "empty dir")]
	#[test_case(raw_conf(F16KiB),"bitcoin.pdf", ""; "empty dir with 16KiB chunk")]
	#[test_case(raw_conf(F1KiB),"bitcoin.pdf", ""; "empty dir with 1KiB chunk")]
	#[test_case(raw_conf(F256KiB),"bitcoin.pdf", "/"; "root dir")]
	#[test_case(raw_conf(F256KiB),"bitcoin.pdf", "./"; "current dir")]
	#[test_case(raw_conf(F256KiB),"bitcoin.pdf", "./.."; "current dir, parent")]
	fn file_system_tools<P, R>(conf: Config, file: P, read_dir_at: R)
	where
		P: AsRef<Path>,
		R: AsRef<Path> + std::fmt::Debug,
	{
		let original_fs_md5 = checksum::<Sha2_256, _>(test_file(&file));
		let reader = test_file(&file);
		let unixfs =
			FileSystemWriter::default().config(conf).add_data(reader, file).build().expect("Valid UnixFs .qed");

		let cid = unixfs.cid;
		let (header, data) = unixfs.package_reader().into_inner();

		let fs = FileSystemReader::load_from_parts(cid, data, header).expect("Valid package reader .qed");
		let file_reader = fs.read(read_dir_at).unwrap();

		let fs_md5 = checksum::<Sha2_256, _>(file_reader);
		assert_eq!(original_fs_md5, fs_md5);
	}
}
