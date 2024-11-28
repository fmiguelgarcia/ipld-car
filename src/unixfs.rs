use bytes::{buf::Reader, Buf as _, Bytes};
use libipld::Cid;
use quick_protobuf::MessageWrite as _;
use std::io::{Chain, Read};

pub mod file_system_reader;
pub mod file_system_writer;
pub mod proto;
pub use file_system_reader::FileSystemReader;
pub use file_system_writer::FileSystemWriter;
pub mod varint;
pub use varint::{VarintRead, VarintReaderError};

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

impl<R: Read> UnixFs<R> {
	pub fn package_reader(self) -> Chain<Reader<Bytes>, R> {
		let header = self.node.as_ref().map(|node| Bytes::from(pb::node::encode(node))).unwrap_or_default();
		header.reader().chain(self.data)
	}
}

// pub use proto::{PbLink, PbNode};
pub use libipld::pb::{PbLink, PbNode};

pub(crate) mod pb {
	use crate::unixfs::{VarintRead, VarintReaderError};
	use thiserror_no_std::Error;

	#[derive(Debug, Error)]
	pub enum DecodeError {
		VarintReader(#[from] VarintReaderError),
		QuickProtobuf(#[from] quick_protobuf::Error),
		DuplicateLinks,
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
		use quick_protobuf::{reader::BytesReader as ProtoBytesReader, MessageWrite as _, Writer};
		use std::io::Read;

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

		pub(crate) fn decode<R: Read>(reader: &mut R) -> Result<PbNode, DecodeError> {
			let mut var_reader = VarintRead::new(reader);
			let encoded = var_reader.read_message()?;
			let bytes = encoded.as_slice();
			let mut r = ProtoBytesReader::from_bytes(bytes);

			let mut node = PbNode::default();
			let mut links_before_data = false;
			while !r.is_eof() {
				match r.next_tag(bytes)? {
					18 => {
						// Links and data might be in any order, but they may not be interleaved.
						if links_before_data {
							return Err(DecodeError::DuplicateLinks);
						}
						node.links.push(r.read_message::<PbLink>(bytes)?)
					},
					10 => {
						node.data = Some(Bytes::copy_from_slice(r.read_bytes(bytes)?));
						if !node.links.is_empty() {
							links_before_data = true
						}
					},
					_ => {
						return Err(DecodeError::UnexpectedBytes);
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
		WellKnownChunkSize::{F16KiB, F256KiB},
	};
	use libipld::multihash::Sha2_256;
	use std::path::Path;
	use test_case::test_case;

	#[test_case(raw_conf(F256KiB),"bitcoin.pdf", ""; "empty dir")]
	#[test_case(raw_conf(F16KiB),"bitcoin.pdf", ""; "empty dir with 16KiB chunk")]
	#[test_case(raw_conf(F256KiB),"bitcoin.pdf", "/"; "root dir")]
	#[test_case(raw_conf(F256KiB),"bitcoin.pdf", "./"; "current dir")]
	#[test_case(raw_conf(F256KiB),"bitcoin.pdf", "./.."; "current dir, parent")]
	fn file_system_tools<P, R>(conf: Config, file: P, read_dir_at: R)
	where
		P: AsRef<Path>,
		R: AsRef<Path>,
	{
		let original_fs_md5 = checksum::<Sha2_256, _>(test_file(&file));
		let reader = test_file(&file);
		let unixfs =
			FileSystemWriter::default().config(conf).add_data(reader, file).build().expect("Valid UnixFs .qed");

		let cid = unixfs.cid;
		let (header, data) = unixfs.package_reader().into_inner();

		let fs = FileSystemReader::load(cid, header, data).expect("Valid package reader .qed");
		let paths = fs.read_dir(read_dir_at);
		assert_eq!(paths.as_slice(), &[&Path::new("")]);
		let path = paths.into_iter().next().cloned().expect("Path exists inside the FS .qed");
		let file_reader = fs.read(path).unwrap();

		let fs_md5 = checksum::<Sha2_256, _>(file_reader);
		assert_eq!(original_fs_md5, fs_md5);
	}
}
