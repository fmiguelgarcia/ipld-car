use crate::{
	bounded_reader::{
		functions::slice_ref,
		sync::BoundedReader,
		traits::{Bounded, CloneAndRewind},
	},
	car::{Block, BlockId, ContentAddressableArchive},
	error::{DagPbErr, DagPbResult, UnixFsErr},
	fail, proto,
	traits::ContextLen,
};

use bytes::{BufMut, BytesMut};
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
	io::{copy, Read, Seek},
};

mod multi_block_file;
pub(crate) use multi_block_file::MultiBlockFile;
mod symlink;
pub use symlink::Symlink;
mod link;
pub use link::{BlockLink, Link, NamedLink};

const BUF_LIMIT: usize = bytes!(2; MiB);
const BUF_CAPS: &[usize] = &[bytes!(4; KiB), bytes!(16; KiB), bytes!(128; KiB), bytes!(512; KiB), bytes!(1; MiB)];

#[derive(Debug, Clone)]
pub enum DagPbType {
	Dir,
	Symlink(Symlink),
	SingleBlockFile,
	MultiBlockFile(MultiBlockFile),
	MissingBlock(Box<PbLink>),
}

#[derive(derive_more::Debug, Derivative)]
#[derivative(Clone(bound = ""))]
pub struct DagPb<T> {
	pub r#type: DagPbType,
	pub data: BoundedReader<T>,
}

impl<T> DagPb<T> {
	pub fn directory() -> Self {
		Self::no_data(DagPbType::Dir)
	}

	pub fn symlink<S: Into<String>>(posix_path: S) -> Self {
		Self::no_data(DagPbType::Symlink(Symlink::new(posix_path)))
	}

	#[inline]
	pub fn single_block_file<D: Into<BoundedReader<T>>>(data: D) -> Self {
		Self { r#type: DagPbType::SingleBlockFile, data: data.into() }
	}

	#[inline]
	pub fn multi_block_file<BS, D>(blocksizes: BS, data: D) -> Self
	where
		BS: Into<MultiBlockFile>,
		D: Into<BoundedReader<T>>,
	{
		Self { r#type: DagPbType::MultiBlockFile(blocksizes.into()), data: data.into() }
	}

	#[inline]
	pub fn no_data(r#type: DagPbType) -> Self {
		Self { r#type, data: Default::default() }
	}

	pub fn as_sfb_data(&self) -> Option<BoundedReader<T>> {
		match &self.r#type {
			DagPbType::SingleBlockFile => Some(self.data.clone_and_rewind()),
			_ => None,
		}
	}
}

impl<T> From<DagPbType> for DagPb<T> {
	fn from(r#type: DagPbType) -> Self {
		Self { r#type, data: ().into() }
	}
}

impl<T> ContextLen for DagPb<T> {
	fn data_len(&self) -> u64 {
		match &self.r#type {
			DagPbType::Dir | DagPbType::MissingBlock(..) | DagPbType::Symlink(..) => 0u64,
			DagPbType::SingleBlockFile => self.data.bound_len(),
			DagPbType::MultiBlockFile(mbf) => mbf.block_sizes.iter().sum(),
		}
	}

	fn pb_data_len(&self) -> u64 {
		match &self.r#type {
			DagPbType::SingleBlockFile => 0u64,
			_ => self.data.bound_len(),
		}
	}
}

// Load part
// ===========================================================================

impl<T: Read + Seek> DagPb<T> {
	pub fn load(
		car: &mut ContentAddressableArchive<T>,
		cid: Cid,
		mut block_data: BoundedReader<T>,
	) -> DagPbResult<BlockId> {
		// Try to decode `PbNode` and rebounds to data.
		let decode_pb_node_max = block_data.bound_len();
		let pb_node = decode_pb_node(&mut block_data, decode_pb_node_max)?;
		let pb_node_len = pb_node.get_size() as u64;
		debug_assert_eq!(pb_node_len, pb_node.clone().into_bytes().len() as u64);
		let pb_data = block_data.sub(..pb_node_len)?;
		let data = block_data.sub(pb_node_len..)?;

		// Decode Unixfs Data
		let pb_node_data_enc = pb_node.data.clone().ok_or(UnixFsErr::MissingData)?;
		let unixfs = proto::Data::decode(pb_node_data_enc).map_err(|_| UnixFsErr::InvalidData)?;

		let unixfs_type = proto::data::DataType::try_from(unixfs.r#type)
			.map_err(|_| UnixFsErr::DataTypeNotSupported(unixfs.r#type))?;

		let id = match unixfs_type {
			proto::data::DataType::Directory => load_directory(car, cid, pb_data, &pb_node.links),
			proto::data::DataType::Symlink => load_symlink(car, cid, pb_data, unixfs)?,
			proto::data::DataType::Raw =>
				car.add_block(Block::new_dag_pb(cid, DagPb::single_block_file(data), pb_data)),
			proto::data::DataType::File => load_file(car, cid, pb_node, unixfs, pb_data, data)?,
			_ => fail!(UnixFsErr::DataTypeNotSupported(unixfs.r#type)),
		};

		Ok(id)
	}
}

fn load_directory<T>(
	car: &mut ContentAddressableArchive<T>,
	cid: Cid,
	pb_data: BoundedReader<T>,
	links: &[PbLink],
) -> BlockId {
	let block = Block::new_dag_pb(cid, DagPb::directory(), pb_data);
	car.add_directory(block, links)
}

fn load_symlink<T>(
	car: &mut ContentAddressableArchive<T>,
	cid: Cid,
	pb_data: BoundedReader<T>,
	unixfs: proto::Data,
) -> DagPbResult<BlockId> {
	let posix_path = unixfs.data.ok_or(UnixFsErr::MissingSymlinkInfo)?;
	let posix_path_utf8 = String::try_from(posix_path).map_err(|_| UnixFsErr::SymlinkPathUtf8)?;
	let block = Block::new_dag_pb(cid, DagPb::symlink(posix_path_utf8), pb_data);
	Ok(car.add_block(block))
}

fn load_file<T: Read + Seek>(
	car: &mut ContentAddressableArchive<T>,
	cid: Cid,
	pb_node: PbNode,
	unixfs: proto::Data,
	pb_data: BoundedReader<T>,
	data: BoundedReader<T>,
) -> DagPbResult<BlockId> {
	// Load as SBF
	if pb_node.links.is_empty() {
		let embedded_data = unixfs.data.and_then(|data| slice_ref(pb_data.clone_and_rewind(), &data));
		let data = embedded_data.unwrap_or(data);
		let sbf = DagPb::single_block_file(data);
		let block = Block::new_dag_pb(cid, sbf, pb_data);
		return Ok(car.add_block(block));
	}

	// Load as MBF
	let mbf = DagPb::multi_block_file(unixfs.blocksizes, data);
	let block = Block::new_dag_pb(cid, mbf, pb_data);
	Ok(car.add_multi_block_file(block, &pb_node.links))
}

/// It tries to decode a `pbNode` using a progressive increase on the buffer capacity, up to 2 `MiB`.
fn decode_pb_node<R: Read>(reader: &mut R, block_len: u64) -> DagPbResult<PbNode> {
	let caps = buffer_capacities_to_decode_pb_node(block_len);
	let init_capacity = caps.first().copied().unwrap_or(BUF_LIMIT);
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
		buf = freezed_buf.try_into_mut().map_err(|_| DagPbErr::ExceedBufLimitOnDecode)?;
	}

	Err(DagPbErr::ExceedBufLimitOnDecode)
}

fn buffer_capacities_to_decode_pb_node(block_len: u64) -> Vec<usize> {
	let max_buf_cap: usize = min(block_len.try_into().unwrap_or(usize::MAX), BUF_LIMIT);
	let mut buf_caps: Vec<usize> = BUF_CAPS.to_vec();
	buf_caps.push(max_buf_cap);

	buf_caps.sort();
	buf_caps.retain(|len| *len <= max_buf_cap);
	buf_caps
}
