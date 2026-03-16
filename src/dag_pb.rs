use crate::{
	arena::{Arena, ArenaId},
	car::Block,
	ensure,
	error::{DagPbErr, DagPbResult, UnixFsErr},
	fail,
	proto::{self, data::DataType, new_pb_node},
	BoundedReader,
};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use libipld::{
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

mod single_block_file;
use single_block_file::SingleBlockFile;

const BUF_LIMIT: usize = bytes!(2; MiB);
const BUF_CAPS: &[usize] = &[bytes!(4; KiB), bytes!(16; KiB), bytes!(128; KiB), bytes!(512; KiB), bytes!(1; MiB)];

#[derive(derive_more::Debug)]
pub enum DagPb<T> {
	Dir(BTreeMap<String, Link>),
	SingleBlockFile(SingleBlockFile<T>),
	MultiBlockFile(Vec<Link>, BoundedReader<T>),
	Symlink(Symlink),
}

// Load part
// ===========================================================================

impl<T: Read + Seek> DagPb<T> {
	pub fn load(arena: &mut Arena<Block<T>>, cid: Cid, mut reader: BoundedReader<T>) -> DagPbResult<ArenaId> {
		// Try to decode `PbNode` and rebounds to data.
		let decode_pb_node_max = reader.bound_len();
		let pb_node = decode_pb_node(&mut reader, decode_pb_node_max)?;
		let pb_node_len = pb_node.get_size() as u64;
		debug_assert_eq!(pb_node_len, pb_node.clone().into_bytes().len() as u64);

		let bounded_data = reader.sub(pb_node_len..)?;

		// Decode Unixfs Data
		let enc_data = pb_node.data.clone().ok_or(UnixFsErr::MissingData)?;
		let unixfs = proto::Data::decode(enc_data).map_err(|_| UnixFsErr::InvalidData)?;
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
	/*
	let posix_path_str = posix_path_utf8.strip_prefix("/ipfs/").unwrap_or(posix_path_utf8.as_str());
	let target_cid_str = target_cid_str.split('/').next().unwrap_or(posix_path_utf8.as_str());
	let target_cid = target_cid_str.parse::<Cid>().map_err(CidErr::from)?;
	*/
	let block = Block::new(cid, DagPb::Symlink(Symlink::POSIXPath(posix_path_utf8)));
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
		return single_block_file(arena, cid, pb_node.data, reader);
	}
	ensure!(
		unixfs.blocksizes.len() == pb_node.links.len(),
		UnixFsErr::BlocksizesLenDiffLinksLen(unixfs.blocksizes.len(), pb_node.links.len())
	);

	// Insert this node.
	let links = pb_node
		.links
		.into_iter()
		.map(|pb_link| {
			let link_id = arena.get_id_by_index(&pb_link.cid);
			Link::new(pb_link.cid, pb_link.size, link_id)
		})
		.collect::<Vec<_>>();
	let content = DagPb::MultiBlockFile(links.clone(), reader.clone());
	let block = Block::new(cid, content);
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

fn single_block_file<T>(
	arena: &mut Arena<Block<T>>,
	cid: Cid,
	data: Option<Bytes>,
	reader: BoundedReader<T>,
) -> DagPbResult<ArenaId> {
	let sbf = match data {
		Some(data) => {
			ensure!(reader.bound_len() == 0, UnixFsErr::FileWithDataAndReader);
			SingleBlockFile::Data(data)
		},
		None => SingleBlockFile::Reader(reader),
	};
	let block = Block::new(cid, DagPb::SingleBlockFile(sbf));
	tracing::debug!(?block, "DagPb single block loaded");
	Ok(arena.push(block))
}

/// Computes the DagPb CID for a directory with the given named links.
pub(crate) fn dir_cid(named_links: &BTreeMap<String, Link>) -> DagPbResult<Cid> {
	use crate::config::CidCodec;
	use libipld::multihash::{Code, MultihashDigest as _};
	use std::io::Read as _;

	let ReaderWithLen { mut reader, .. } = directory_conent_writer(named_links)?;
	let mut buf = Vec::new();
	reader.read_to_end(&mut buf)?;
	let digest = Code::Sha2_256.digest(&buf);
	Ok(Cid::new_v1(CidCodec::DagPb as u64, digest))
}

fn load_directory<T>(arena: &mut Arena<Block<T>>, cid: Cid, pb_links: Vec<PbLink>) -> DagPbResult<ArenaId> {
	let links = pb_links
		.into_iter()
		.map(|pb_link| {
			let name = pb_link.name.ok_or(UnixFsErr::MissingLinkNameInDirectory)?;
			let link_id = arena.get_id_by_index(&pb_link.cid);
			let link = Link::new(pb_link.cid, pb_link.size, link_id);
			Ok::<_, DagPbErr>((name, link))
		})
		.collect::<Result<BTreeMap<_, _>, _>>()?;

	let block = Block::new(cid, DagPb::Dir(links));
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
			Self::Dir(named_links) => directory_conent_writer(named_links),
			Self::SingleBlockFile(sbf) => single_block_file_writer(sbf),
			Self::Symlink(symlink) => Ok(symlink_writer(symlink)),
			Self::MultiBlockFile(..) => unimplemented!("DagPb::content_writer"),
		}
	}
}

fn symlink_writer(s: &Symlink) -> ReaderWithLen {
	let unixfs = match s {
		Symlink::POSIXPath(path) => proto::Data::new_symlink(path.as_str()),
	};

	let data = Bytes::from(unixfs.encode_to_vec());
	let pb_node = Bytes::from(new_pb_node(vec![], data).into_bytes());
	let pb_node_len = pb_node.len() as u64;

	ReaderWithLen::new(pb_node.reader(), pb_node_len)
}

fn directory_conent_writer(named_links: &BTreeMap<String, Link>) -> DagPbResult<ReaderWithLen> {
	// Create PbNode
	let links = named_links
		.iter()
		.map(|(name, link)| PbLink { cid: link.cid, name: Some(name.clone()), size: link.cumulative_dag_size })
		.collect();
	let data = Bytes::from(proto::Data::new_directory().encode_to_vec());
	let pb_node = Bytes::from(new_pb_node(links, data).into_bytes());
	let pb_node_len = pb_node.len() as u64;

	Ok(ReaderWithLen::new(pb_node.reader(), pb_node_len))
}

fn single_block_file_writer<T: Read + Seek + 'static>(sbf: &SingleBlockFile<T>) -> DagPbResult<ReaderWithLen> {
	let (pb_node, reader) = match sbf {
		SingleBlockFile::Data(data) => {
			let pb_node = new_pb_node(vec![], data.clone());
			(pb_node, None)
		},
		SingleBlockFile::Reader(reader) => {
			let pb_node = new_pb_node(vec![], None);
			(pb_node, Some(reader.clone_and_rewind()))
		},
	};

	// Calculate total len.
	let pb_node_enc = Bytes::from(pb_node.into_bytes());
	let pb_node_enc_len = pb_node_enc.len() as u64;

	// Chain encoded pb_node and reader.
	let reader_with_len = if let Some(reader) = reader {
		let len = pb_node_enc_len.checked_add(reader.bound_len()).ok_or(DagPbErr::FileTooLarge)?;
		let chained = pb_node_enc.reader().chain(reader);

		ReaderWithLen::new(chained, len)
	} else {
		ReaderWithLen::new(pb_node_enc.reader(), pb_node_enc_len)
	};

	Ok(reader_with_len)
}

// Utility structs
// ===========================================================

#[derive(derive_more::Debug, Clone, Copy)]
pub struct Link {
	#[debug("{}", cid.to_string())]
	pub cid: Cid,
	pub cumulative_dag_size: Option<u64>,
	/// In-memory hint: the `ArenaId` of the block this link points to. Not serialized.
	/// Set when a link is created in-memory via [`create_dir`] to avoid CID-index
	/// collisions between distinct blocks that share the same content (and thus CID).
	pub(crate) arena_id: Option<ArenaId>,
}

impl Link {
	pub fn new<S, I>(cid: Cid, cumulative_dag_size: S, arena_id: I) -> Self
	where
		S: Into<Option<u64>>,
		I: Into<Option<ArenaId>>,
	{
		Self { cid, cumulative_dag_size: cumulative_dag_size.into(), arena_id: arena_id.into() }
	}
}

pub struct ReaderWithLen {
	pub reader: Box<dyn Read>,
	pub len: u64,
}

impl ReaderWithLen {
	pub fn new<R: Read + 'static>(reader: R, len: u64) -> Self {
		Self { reader: Box::new(reader), len }
	}
}

#[derive(Debug)]
pub enum Symlink {
	POSIXPath(String),
}
