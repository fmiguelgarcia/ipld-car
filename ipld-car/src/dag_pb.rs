use crate::{
	arena::{Arena, ArenaId},
	car::Block,
	ensure,
	error::{DagPbErr, DagPbResult, UnixFsErr},
	fail,
	proto::{self, data::DataType},
	reader_with_len::ReaderWithLen,
	BoundedReader, CIDBuilder, Config, ContextLen,
};

use bytes::{BufMut, Bytes, BytesMut};
use derivative::Derivative;
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

#[derive(derive_more::Debug, Derivative)]
#[derivative(Clone(bound = ""))]
pub enum DagPb<T> {
	Dir(Directory),
	SingleBlockFile(SingleBlockFile<T>),
	MultiBlockFile(MultiBlockFile<T>),
	Symlink(Symlink),
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

	fn invalidate(&mut self) {
		match self {
			Self::SingleBlockFile(sbf) => sbf.invalidate(),
			Self::MultiBlockFile(mbf) => mbf.invalidate(),
			Self::Dir(dir) => dir.invalidate(),
			Self::Symlink(symlink) => symlink.invalidate(),
		}
	}

	fn was_invalidated(&self) -> bool {
		match self {
			Self::SingleBlockFile(sbf) => sbf.was_invalidated(),
			Self::MultiBlockFile(mbf) => mbf.was_invalidated(),
			Self::Dir(dir) => dir.was_invalidated(),
			Self::Symlink(symlink) => symlink.was_invalidated(),
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

	Ok(arena.push(block, None))
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

	// Insert this node. Zip with unixfs.blocksizes to populate each Link's blocksize field,
	// which is needed for data_len() to return the correct total file size.
	let links = pb_node
		.links
		.into_iter()
		.zip(unixfs.blocksizes)
		.map(|(pb_link, blocksize)| {
			let cumulative_dag_size = pb_link.size.unwrap_or_default();
			Link::new(pb_link.cid, cumulative_dag_size, Some(blocksize), pb_link.name, None).with_arena(arena)
		})
		.collect::<Vec<_>>();
	let mbf = MultiBlockFile::new(links.clone(), reader.clone());
	let block = Block::new(cid, DagPb::MultiBlockFile(mbf));
	tracing::debug!(?block, "DagPb file loaded");
	let id = arena.push(block, None);

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
	Ok(arena.push(block, None))
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
	let id = arena.push(block, None);
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

// Ipld & CID related
// ===========================================================================

impl<T> From<&DagPb<T>> for PbNode {
	fn from(dag: &DagPb<T>) -> Self {
		match dag {
			DagPb::Dir(directory) => directory.into(),
			DagPb::SingleBlockFile(sbf) => sbf.into(),
			DagPb::Symlink(symlink) => symlink.into(),
			DagPb::MultiBlockFile(mbf) => mbf.into(),
		}
	}
}

impl<T: Read + Seek + 'static> DagPb<T> {
	pub fn as_reader_with_len(&self) -> Result<ReaderWithLen, crate::error::Error> {
		let this = match self {
			DagPb::Dir(directory) => PbNode::from(directory).into(),
			DagPb::SingleBlockFile(sbf) => sbf.as_reader_with_len()?,
			DagPb::Symlink(symlink) => PbNode::from(symlink).into(),
			DagPb::MultiBlockFile(mbf) => PbNode::from(mbf).into(),
		};
		Ok(this)
	}
}

impl<T: Read + Seek> CIDBuilder for DagPb<T> {
	fn cid(&self, config: &Config) -> Result<Cid, crate::error::Error> {
		match self {
			Self::Dir(directory) => directory.cid(config),
			Self::SingleBlockFile(sbf) => sbf.cid(config),
			Self::Symlink(symlink) => symlink.cid(config),
			Self::MultiBlockFile(mbf) => mbf.cid(config),
		}
	}
}
