use crate::{
	arena::{Arena, ArenaId},
	car::Block,
	config::{CidCodec, Config},
	ensure,
	error::{DagPbErr, DagPbResult, NotSupportedErr, UnixFsErr},
	fail,
	proto::{self, data::DataType},
	BoundedReader, ContextLen,
};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use derive_more::From;
use libipld::{
	multihash::MultihashDigest,
	pb::{PbLink, PbNode},
	Cid,
};
use nbytes::bytes;
use prost::Message;
use quick_protobuf::message::MessageWrite;
use std::{
	cmp::min,
	collections::BTreeMap,
	io::{copy, Read, Seek},
};

mod directory;
pub(crate) use directory::Directory;
mod multi_block_file;
pub(crate) use multi_block_file::MultiBlockFile;
mod single_block_file;
pub(crate) use single_block_file::{SBFContent, SingleBlockFile};
mod symlink;
use symlink::Symlink;
mod link;
pub use link::Link;

const BUF_LIMIT: usize = bytes!(2; MiB);
const BUF_CAPS: &[usize] = &[bytes!(4; KiB), bytes!(16; KiB), bytes!(128; KiB), bytes!(512; KiB), bytes!(1; MiB)];

#[derive(derive_more::Debug, From)]
pub enum DagPb<T> {
	Dir(Directory),
	SingleBlockFile(SingleBlockFile<T>),
	MultiBlockFile(MultiBlockFile<T>),
	Symlink(Symlink),
}

impl<T> Clone for DagPb<T> {
	fn clone(&self) -> Self {
		match self {
			Self::SingleBlockFile(sbf) => Self::SingleBlockFile(sbf.clone()),
			Self::MultiBlockFile(mbf) => Self::MultiBlockFile(mbf.clone()),
			Self::Dir(dir) => Self::Dir(dir.clone()),
			Self::Symlink(sym) => Self::Symlink(sym.clone()),
		}
	}
}

impl<T> ContextLen for DagPb<T> {
	fn data_len(&self) -> u64 {
		match self {
			Self::SingleBlockFile(sbf) => sbf.data_len(),
			Self::MultiBlockFile(mbf) => mbf.data_len(),
			Self::Dir(..) | Self::Symlink(..) => 0,
		}
	}

	fn dag_pb_len(&self) -> u64 {
		match self {
			Self::SingleBlockFile(sbf) => sbf.dag_pb_len(),
			Self::MultiBlockFile(mbf) => mbf.dag_pb_len(),
			Self::Dir(dir) => dir.dag_pb_len(),
			Self::Symlink(symlink) => symlink.dag_pb_len(),
		}
	}
}

// Load part
// ===========================================================================

impl<T: Read + Seek> DagPb<T> {
	pub fn load(arena: &mut Arena<Block<T>>, cid: Cid, mut reader: BoundedReader<T>) -> DagPbResult<ArenaId> {
		// Try to decode `PbNode` and rebounds to data.
		let decode_pb_node_max = reader.bound_len();
		let pb_node = decode_pb_node(&mut reader, decode_pb_node_max)?;
		tracing::debug!(?pb_node, "Load pb node");
		let pb_node_len = pb_node.get_size() as u64;
		debug_assert_eq!(pb_node_len, pb_node.clone().into_bytes().len() as u64);

		let bounded_data = reader.sub(pb_node_len..)?;

		// Decode Unixfs Data
		let enc_data = pb_node.data.clone().ok_or(UnixFsErr::MissingData)?;
		let unixfs = proto::Data::decode(enc_data).map_err(|_| UnixFsErr::InvalidData)?;
		tracing::debug!(?unixfs, "Load proto Data");
		let unixfs_type =
			DataType::try_from(unixfs.r#type).map_err(|_| UnixFsErr::DataTypeNotSupported(unixfs.r#type))?;
		let id = match unixfs_type {
			DataType::Directory => load_directory(arena, cid, pb_node.links)?,
			DataType::File => load_file(arena, cid, pb_node, unixfs, bounded_data)?,
			DataType::Raw => single_block_file(arena, cid, None, bounded_data)?,
			DataType::Symlink => load_symlink(arena, cid, &pb_node.links, unixfs)?,
			_ => fail!(UnixFsErr::DataTypeNotSupported(unixfs.r#type)),
		};

		Ok(id)
	}
}

fn load_symlink<T: Read + Seek>(
	arena: &mut Arena<Block<T>>,
	cid: Cid,
	links: &[PbLink],
	unixfs: proto::Data,
) -> DagPbResult<ArenaId> {
	ensure!(links.is_empty(), UnixFsErr::SymlinkWithChildren);

	let posix_path = unixfs.data.ok_or(UnixFsErr::MissingSymlinkInfo)?;
	let posix_path_utf8 = String::try_from(posix_path).map_err(|_| UnixFsErr::SymlinkPathUtf8)?;
	let block = Block::new(cid, DagPb::Symlink(Symlink::new(posix_path_utf8)));
	tracing::debug!(?block, "DagPb symlink loaded");

	Ok(arena.push(block))
}

fn load_file<T: Read + Seek>(
	arena: &mut Arena<Block<T>>,
	cid: Cid,
	pb_node: PbNode,
	unixfs: proto::Data,
	reader: BoundedReader<T>,
) -> DagPbResult<ArenaId> {
	if pb_node.links.is_empty() {
		return single_block_file(arena, cid, unixfs.data.map(Bytes::from), reader);
	}
	ensure!(
		unixfs.blocksizes.len() == pb_node.links.len(),
		UnixFsErr::BlocksizesLenDiffLinksLen(unixfs.blocksizes.len(), pb_node.links.len())
	);

	// Insert this node.
	let links = pb_node.links.into_iter().map(|pb_link| Link::from(pb_link).with_arena(arena)).collect::<Vec<_>>();
	let mbf = MultiBlockFile::new(links.clone(), reader.clone());
	let block = Block::new(cid, DagPb::from(mbf));
	tracing::debug!(?block, "DagPb file loaded");
	let id = arena.push(block);

	/*
	// Add children recursivelly.
	let mut acc_link_size = 0u64;
	for (link, _blocksize) in links.into_iter().zip(unixfs.blocksizes) {
		let link_cumulative_dag_size = link.cumulative_dag_size.unwrap_or_default();
		let new_acc_link_size =
			acc_link_size.checked_add(link_cumulative_dag_size).ok_or(UnixFsErr::LinkSizeOverflow)?;
		let link_data = data.sub(acc_link_size..new_acc_link_size)?;
		acc_link_size = new_acc_link_size;

		let cid_codec = link.cid.codec();
		let codec = CidCodec::try_from(cid_codec).map_err(|_| CidErr::CodecNotSupported(cid_codec))?;
		let _id = match codec {
			CidCodec::Raw => single_block_file(arena, link.cid, link_data)?,
			CidCodec::DagPb => DagPb::load(arena, link.cid, link_data)?,
			_ => fail!(CidErr::CodecNotSupported(cid_codec)),
		};
	}*/

	Ok(id)
}

fn single_block_file<T: Read + Seek>(
	arena: &mut Arena<Block<T>>,
	cid: Cid,
	data: Option<Bytes>,
	reader: BoundedReader<T>,
) -> DagPbResult<ArenaId> {
	let sbf_content = match data {
		Some(data) => {
			ensure!(reader.bound_len() == 0, UnixFsErr::FileWithDataAndReader);
			SBFContent::from(data)
		},
		None => SBFContent::from(reader),
	};
	let block = Block::new(cid, DagPb::SingleBlockFile(sbf_content.into()));
	tracing::debug!(?block, "DagPb single block loaded");
	Ok(arena.push(block))
}

/// Computes the DagPb CID for a directory with the given named links, using the hash
/// algorithm and codec defined in `config`.
pub(crate) fn dir_cid(dir: &Directory, config: &Config) -> DagPbResult<Cid> {
	use libipld::multihash::MultihashDigest as _;
	use std::io::Read as _;

	let ReaderWithLen { mut reader, .. } = directory_conent_writer(dir)?;
	let mut buf = Vec::new();
	reader.read_to_end(&mut buf)?;
	let digest = config.hash_code.digest(&buf);
	Ok(Cid::new_v1(config.cid_codec as u64, digest))
}

fn load_directory<T: Read + Seek>(
	arena: &mut Arena<Block<T>>,
	cid: Cid,
	pb_links: Vec<PbLink>,
) -> DagPbResult<ArenaId> {
	let dir_entries = pb_links
		.into_iter()
		.map(|mut pb_link| {
			let name = pb_link.name.take().ok_or(UnixFsErr::MissingLinkNameInDirectory)?;
			let link = Link::from(pb_link).with_arena(arena);
			Ok::<_, DagPbErr>((name, link))
		})
		.collect::<Result<BTreeMap<_, _>, _>>()?;

	let block = Block::new(cid, DagPb::Dir(Directory::from(dir_entries)));
	tracing::debug!(?block, "DagPb directory load");
	let id = arena.push(block);
	Ok(id)
}

/// It tries to decode a `pbNode` using a progressive increase on the buffer capacity, up to 2 `MiB`.
fn decode_pb_node<R: Read>(reader: &mut R, block_len: u64) -> DagPbResult<PbNode> {
	let caps = buf_caps(block_len);
	let init_capacity = caps.first().cloned().expect("At least one buffer len .qed");
	let mut buf = BytesMut::with_capacity(init_capacity);

	for cap in caps {
		// Reserve and real from `reader`, trying to fill `buf_len`
		let additional_cap = cap.saturating_sub(buf.capacity());
		if additional_cap > 0 {
			buf.reserve(additional_cap);
		}

		let buf_len = buf.len();
		let mut writer = buf.writer();
		let additional = cap.saturating_sub(buf_len);
		copy(&mut reader.take(additional as u64), &mut writer)?;

		// Try to decode the PbNode.
		let freezed_buf = writer.into_inner().freeze();
		if let Ok(pb_node) = PbNode::from_bytes(freezed_buf.clone()) {
			return Ok(pb_node);
		}
		buf = freezed_buf.try_into_mut().expect("Buffer is unique .qed");
	}

	Err(DagPbErr::ExceedBufLimitOnDecode)
}

fn buf_caps(block_len: u64) -> Vec<usize> {
	let max_buf_cap: usize = min(block_len.try_into().unwrap_or(usize::MAX), BUF_LIMIT);
	let mut buf_caps: Vec<usize> = BUF_CAPS.to_vec();
	buf_caps.push(max_buf_cap);

	buf_caps.sort();
	buf_caps.retain(|len| *len <= max_buf_cap);
	buf_caps
}

// Write part
// ===========================================================================

impl<T: Read + Seek + 'static> DagPb<T> {
	pub fn content_writer(&self, _arena: &Arena<Block<T>>) -> DagPbResult<ReaderWithLen> {
		match self {
			Self::Dir(directory) => directory_conent_writer(directory),
			Self::SingleBlockFile(sbf) => single_block_file_writer(sbf),
			Self::Symlink(symlink) => Ok(symlink_writer(symlink)),
			Self::MultiBlockFile(..) => unimplemented!("DagPb::content_writer"),
		}
	}
}

fn symlink_writer(s: &Symlink) -> ReaderWithLen {
	let pb_node = PbNode::from(s);
	tracing::debug!(?pb_node, "Write symlink");
	let enc_pb_node = Bytes::from(pb_node.into_bytes());
	let enc_pb_node_len = enc_pb_node.len() as u64;

	ReaderWithLen::new(enc_pb_node.reader(), enc_pb_node_len)
}

fn directory_conent_writer(dir: &Directory) -> DagPbResult<ReaderWithLen> {
	let pb_node = PbNode::from(dir);
	tracing::debug!(?pb_node, "Write directory");
	let enc_pb_node = Bytes::from(pb_node.into_bytes());
	let enc_pb_node_len = enc_pb_node.len() as u64;

	Ok(ReaderWithLen::new(enc_pb_node.reader(), enc_pb_node_len))
}

fn single_block_file_writer<T: Read + Seek + 'static>(sbf: &SingleBlockFile<T>) -> DagPbResult<ReaderWithLen> {
	let pb_node = PbNode::from(sbf);
	tracing::debug!(?pb_node, "Write SBF");
	let enc_pb_node = Bytes::from(pb_node.into_bytes());
	let enc_pb_node_len = enc_pb_node.len() as u64;

	match sbf.content() {
		SBFContent::Data(..) => Ok(ReaderWithLen::new(enc_pb_node.reader(), enc_pb_node_len)),
		SBFContent::Reader(reader) => {
			let chained_reader = enc_pb_node.reader().chain(reader.clone_and_rewind());
			let len = enc_pb_node_len.checked_add(reader.bound_len()).ok_or(DagPbErr::FileTooLarge)?;
			Ok(ReaderWithLen::new(chained_reader, len))
		},
	}
}

// Utility structs
// ===========================================================

pub struct ReaderWithLen {
	pub reader: Box<dyn Read>,
	pub len: u64,
}

impl ReaderWithLen {
	pub fn new<R: Read + 'static>(reader: R, len: u64) -> Self {
		Self { reader: Box::new(reader), len }
	}
}

pub trait BuildCid {
	fn build_cid(&self, config: &Config) -> crate::error::Result<Cid>;
}

impl<T> BuildCid for T
where
	PbNode: for<'a> From<&'a T>,
{
	fn build_cid(&self, config: &Config) -> crate::error::Result<Cid> {
		let mut hasher = config.hasher().ok_or(NotSupportedErr::Hasher(config.hash_code))?;
		let pb_node = PbNode::from(self).into_bytes();
		hasher.update(&*pb_node);
		drop(pb_node);

		let digest = config.hash_code.wrap(hasher.finalize())?;
		let cid = Cid::new_v1(CidCodec::DagPb as u64, digest);
		Ok(cid)
	}
}
