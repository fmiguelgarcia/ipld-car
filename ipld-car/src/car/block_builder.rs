use crate::{
	car::{block_content::BlockContent, Block},
	config::{ChunkPolicy, CidCodec, Config, DAGLayout, HasherAndWrite, LeafPolicy},
	dag_pb::{DagPb, Link, MultiBlockFile, SBFContent, SingleBlockFile},
	ensure,
	error::{Error, NotSupportedErr, Result},
	BoundedReader, CIDBuilder, ContextLen,
};

use bytes::Bytes;
use libipld::{multihash::MultihashDigest, pb::PbNode, Cid};
use std::io::{copy, Read, Seek};

/// Creates blocks based on the given configuration.
///
/// # TODO
/// - Empty file is failing.
pub struct BlockBuilder<T> {
	config: Config,
	hasher: Box<dyn HasherAndWrite>,
	reader: BoundedReader<T>,
}

impl<T: Read + Seek> BlockBuilder<T> {
	pub fn new(reader: BoundedReader<T>, config: Config) -> Result<Self> {
		let hasher = config.hasher()?;
		Ok(Self { config, hasher, reader })
	}

	pub fn build(self) -> Result<Block<T>> {
		let chunk_policy = self.config.chunk_policy;
		match chunk_policy {
			ChunkPolicy::FixedSize(chunk_size) => self.with_chunk(chunk_size as u64),
		}
	}

	fn with_chunk(mut self, chunk_size: u64) -> Result<Block<T>> {
		let mut leaves = self.leaves_from_chunks(chunk_size)?;

		// Create tree if needed.
		let leaves_len = leaves.len();
		match leaves_len {
			0 => todo!("Create an empty file"),
			1 => Ok(leaves.pop().expect("One item exists .qed")),
			_ => self.tree_from_leaves(leaves),
		}
	}

	fn tree_from_leaves(&mut self, leaves: Vec<Block<T>>) -> Result<Block<T>> {
		match self.config.layout {
			DAGLayout::Balanced(..) => self.balanced_tree_from_leaves(leaves),
			DAGLayout::Trickle(..) | DAGLayout::Flat => self.trickle_tree_from_leaves(leaves),
		}
	}

	fn balanced_tree_from_leaves(&mut self, _leaves: Vec<Block<T>>) -> Result<Block<T>> {
		unimplemented!();
	}

	fn trickle_tree_from_leaves(&mut self, leaves: Vec<Block<T>>) -> Result<Block<T>> {
		let max_children = self.config.layout.max_children_per_layer();
		ensure!(max_children > 1, NotSupportedErr::DAGLayout(self.config.layout));

		self.recursive_trickle_tree_from_leaves(max_children as usize, 0, &leaves)
	}

	fn recursive_trickle_tree_from_leaves(
		&mut self,
		max_children: usize,
		offset: u64,
		leaves: &[Block<T>],
	) -> Result<Block<T>> {
		let chunk = &leaves[..max_children];
		let mut links = chunk
			.iter()
			.map(|leaf| {
				let cid = leaf.cid.expect("CID was created .qed");
				Link::new(cid, leaf.dag_pb_len(), leaf.data_len(), None, None)
			})
			.collect::<Vec<_>>();

		let next_chunks = &leaves[max_children..];
		if !next_chunks.is_empty() {
			let next_offset = links.iter().map(|l| l.cumulative_dag_size).sum();
			let block = self.recursive_trickle_tree_from_leaves(max_children, next_offset, next_chunks)?;
			let cid = block.cid.expect("Block CID is generated .qed");
			let link = Link::new(cid, block.dag_pb_len(), block.data_len(), None, None);
			links.push(link)
		}

		let acc_link_size = links.iter().map(|l| l.cumulative_dag_size).sum::<u64>();
		let sub_reader = self.reader.sub(offset..offset + acc_link_size).expect("Bounded sub range is valid .qed");
		let mbf = MultiBlockFile::new(links, sub_reader);
		let cid = mbf.cid(&self.config)?;

		Ok(Block::new(cid, DagPb::MultiBlockFile(mbf)))
	}

	// Build leaves nodes
	// =========================================================================
	fn leaves_from_chunks(&mut self, chunk_size: u64) -> Result<Vec<Block<T>>> {
		match self.config.leaf_policy {
			LeafPolicy::Raw => self.raw_leaves_from_chunks(chunk_size),
			LeafPolicy::UnixFs => self.unixfs_leaves_from_chunks(chunk_size),
		}
	}

	fn raw_leaves_from_chunks(&mut self, chunk_size: u64) -> Result<Vec<Block<T>>> {
		let mut offset = 0u64;
		let mut leaves = vec![];

		loop {
			let next_offset = offset.checked_add(chunk_size).ok_or(Error::FileTooLarge)?;
			let mut chunk = self.reader.clamped_sub(offset..next_offset);
			offset = match copy(&mut chunk, &mut self.hasher)? {
				0 if !leaves.is_empty() => break,
				_ => next_offset,
			};

			let digest = self.config.hash_code.wrap(self.hasher.finalize())?;
			let cid = Cid::new_v1(CidCodec::Raw as u64, digest);
			self.hasher.reset();

			let leaf = Block::new(cid, BlockContent::Raw(chunk));
			leaves.push(leaf);
		}

		Ok(leaves)
	}

	fn unixfs_leaves_from_chunks(&mut self, chunk_size: u64) -> Result<Vec<Block<T>>> {
		let mut offset = 0u64;
		let mut leaves = vec![];

		loop {
			let next_offset = offset.checked_add(chunk_size).ok_or(Error::FileTooLarge)?;
			let chunk = self.reader.clamped_sub(offset..next_offset);
			if chunk.bound_len() == 0 && !leaves.is_empty() {
				break;
			}
			offset = next_offset;

			let sbf_content = if chunk.bound_len() < chunk_size {
				let mut buf = Vec::with_capacity(chunk.bound_len() as usize);
				chunk.clone_and_rewind().read_to_end(&mut buf)?;
				SBFContent::from(Bytes::from(buf))
			} else {
				SBFContent::from(chunk)
			};
			let sbf = SingleBlockFile::from(sbf_content);

			// Calculate CID
			let pb_node = PbNode::from(&sbf).into_bytes();
			self.hasher.update(&pb_node);
			drop(pb_node);
			let digest = self.config.hash_code.wrap(self.hasher.finalize())?;
			self.hasher.reset();

			let cid = Cid::new_v1(CidCodec::DagPb as u64, digest);
			let leaf = Block::new(cid, BlockContent::DagPb(DagPb::SingleBlockFile(sbf)));
			leaves.push(leaf);
		}

		Ok(leaves)
	}
}
