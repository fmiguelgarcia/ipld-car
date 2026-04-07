use crate::{
	bounded_reader::{sync::BoundedReader, traits::Bounded},
	car::{Block, BlockId},
	config::{ChunkPolicy, DAGLayout, HasherAndWrite, LeafPolicy},
	dag_pb::DagPb,
	ensure,
	error::{Error, NotSupportedErr, Result},
	traits::ContextLen,
	ContentAddressableArchive,
};

use libipld::Cid;
use std::{
	collections::{vec_deque, VecDeque},
	io::{copy, Read, Seek},
	iter::Peekable,
};

/// Creates blocks based on the given configuration.
///
/// # TODO
/// - Empty file is failing.
pub struct BlockBuilder<'a, T> {
	car: &'a mut ContentAddressableArchive<T>,
	hasher: Box<dyn HasherAndWrite>,
	reader: BoundedReader<T>,
}

impl<'a, T: Read + Seek> BlockBuilder<'a, T> {
	pub fn new(car: &'a mut ContentAddressableArchive<T>, reader: BoundedReader<T>) -> Result<Self> {
		let hasher = car.config.hasher()?;
		Ok(Self { car, hasher, reader })
	}

	pub fn build(self) -> Result<BlockId> {
		let chunk_policy = self.car.config.chunk_policy;
		match chunk_policy {
			ChunkPolicy::FixedSize(chunk_size) => self.with_chunk(chunk_size as u64),
		}
	}

	fn with_chunk(mut self, chunk_size: u64) -> Result<BlockId> {
		let mut leaves = self.leaves_from_chunks(chunk_size)?;

		// Create tree if needed.
		let leaves_len = leaves.len();
		match leaves_len {
			0 => todo!("Create an empty file"),
			1 => Ok(leaves.pop_front().expect("One item exists .qed")),
			_ => self.tree_from_leaves(leaves),
		}
	}

	fn tree_from_leaves(&mut self, leaves: VecDeque<BlockId>) -> Result<BlockId> {
		match self.car.config.layout {
			DAGLayout::Balanced(..) => self.balanced_tree_from_leaves(leaves),
			DAGLayout::Trickle(..) | DAGLayout::Flat => self.trickle_tree_from_leaves(leaves),
		}
	}

	fn balanced_tree_from_leaves(&mut self, _leaves: VecDeque<BlockId>) -> Result<BlockId> {
		unimplemented!();
	}

	fn trickle_tree_from_leaves(&mut self, leaves: VecDeque<BlockId>) -> Result<BlockId> {
		let max_children = self.car.config.layout.max_children_per_layer();
		ensure!(max_children > 1, NotSupportedErr::DAGLayout(self.car.config.layout));

		self.recursive_trickle_tree_from_leaves(max_children as usize, leaves.into_iter().peekable())
	}

	fn recursive_trickle_tree_from_leaves(
		&mut self,
		max_children: usize,
		mut leaves: Peekable<vec_deque::IntoIter<BlockId>>,
	) -> Result<BlockId> {
		let mut leaf_ids = leaves.by_ref().take(max_children).collect::<Vec<_>>();

		if leaves.peek().is_some() {
			let sub_id = self.recursive_trickle_tree_from_leaves(max_children, leaves)?;
			leaf_ids.push(sub_id);
		}

		let block_sizes = leaf_ids
			.iter()
			.filter_map(|id| self.car.dag.node_weight(*id))
			.map(|block| block.data_len())
			.collect::<Vec<_>>();

		let dag_pb = DagPb::multi_block_file(block_sizes, ());
		let block = Block::new_dag_pb(Cid::default(), dag_pb, ());

		let block_id = self.car.add_block_without_cid(block);
		self.car.rebuild(block_id)?;
		self.car.link_children(block_id, &leaf_ids);

		Ok(block_id)
	}

	// Build leaves nodes
	// =========================================================================

	fn leaves_from_chunks(&mut self, chunk_size: u64) -> Result<VecDeque<BlockId>> {
		// How leaf's content is created: Raw or UnixFs
		let leaf_builder = match self.car.config.leaf_policy {
			LeafPolicy::Raw => |data| Block::new_raw(Cid::default(), data),
			LeafPolicy::UnixFs =>
				|data| Block::new_dag_pb(Cid::default(), DagPb::single_block_file(data), BoundedReader::empty()),
		};

		let mut offset = 0u64;
		let max_leaves_len = self.reader.bound_len() / chunk_size + 1;
		let mut leaves = VecDeque::with_capacity(max_leaves_len as usize);

		loop {
			let next_offset = offset.checked_add(chunk_size).ok_or(Error::FileTooLarge)?;
			let mut data_chunk = self.reader.clamped_sub(offset..next_offset);
			offset = match copy(&mut data_chunk, &mut self.hasher)? {
				0 if !leaves.is_empty() => break,
				_ => next_offset,
			};

			let block = leaf_builder(data_chunk);
			let block_id = self.car.add_block_without_cid(block);
			self.car.rebuild(block_id)?;
			leaves.push_back(block_id)
		}

		Ok(leaves)
	}
}
