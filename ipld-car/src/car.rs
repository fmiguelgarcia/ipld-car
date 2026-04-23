//! IPLD CAR v1 (Content Addressable aRchive) format.
//!
//! # Wire format
//!
//! ```text
//! <uvarint(header-len)> <dag-cbor-header> (<uvarint(section-len)> <cid-bytes> <block-data>)*
//! ```
//!
//! The header is a DAG-CBOR map:
//! ```json
//! { "version": 1, "roots": [<CID tag(42)>, ...] }
//! ```
//!
//! CIDs in the CBOR header are encoded as CBOR tag 42 over a byte string
//! with a leading `\x00` multibase-identity prefix.  In block sections the
//! CID bytes appear **without** any prefix.
//!
//! Reference: <https://ipld.io/specs/transport/car/carv1/>
use crate::{
	bounded_reader::{
		sync::{BoundedReader, ChainedBoundedReader},
		traits::{Bounded as _, CloneAndRewind as _},
	},
	car::tools::parent_or_root,
	config::{CidCodec, Config},
	dag_pb::{BlockLink, DagPb, DagPbType, Link, NamedLink},
	error::{InvalidErr, NotFoundErr, Result},
	fail, proto,
	traits::ContextLen,
};

use bytes::{Buf, Bytes};
use libipld::{
	multihash::MultihashDigest,
	pb::{PbLink, PbNode},
	Cid,
};
use petgraph::{
	graph::{EdgeReference, Graph, NodeIndex},
	visit::{Dfs, EdgeRef, Reversed, Walker},
	Direction,
};
use smallvec::SmallVec;
use std::{
	collections::HashMap,
	fs::File,
	io::{copy, BufWriter, Read, Seek},
	path::{Path, PathBuf},
};
use tempfile::tempfile;
use tracing::debug;

mod context_len;
mod load;
mod metadata;
mod tools;
mod write;
pub use metadata::{FileType, Metadata};
mod block;
pub use block::{Block, BlockType};
mod block_builder;
use block_builder::BlockBuilder;
mod block_def;
pub(crate) use block_def::BlockDef;
mod cbor_cid;
#[cfg(feature = "vfs")]
pub mod fs;
mod header;
pub(crate) use header::CarHeader;
mod as_block_finder;
mod as_cid_graph;
mod as_file_system;
#[cfg(test)]
mod tests;
pub mod traits;

pub type BlockId = NodeIndex<u32>;
pub type SmallBlockIds = SmallVec<[BlockId; 1]>;
static BLOCK_INSERTED_QED: &str = "Block just added .qed";

#[derive(derive_more::Debug)]
pub struct ContentAddressableArchive<T> {
	/// Configuration used only to generate consolidation info, like CIDs
	config: Config,

	/// Inner reader.
	pub content: BoundedReader<T>,

	/// Blocks DAG
	pub dag: Graph<Block<T>, Link>,

	/// Index block ID by CID.
	index_by_cid: HashMap<Cid, BlockId>,

	/// On MBF load process, this list stores any link (cid)
	/// that is referenced but it is not yet loaded.
	mbf_pending_links: HashMap<Cid, BlockId>,

	/// On loads, it register bytes used by CAR
	pub car_overhead_byte_counter: u64,
}

impl ContentAddressableArchive<BufWriter<File>> {
	pub fn tempfile(config: Config) -> Result<Self> {
		let content = BoundedReader::from_reader(BufWriter::new(tempfile()?))?;
		Ok(Self::base_new(content, config))
	}
}

impl<T> ContentAddressableArchive<T> {
	pub fn new(config: Config) -> Self {
		Self::base_new(BoundedReader::empty(), config)
	}

	fn base_new(content: BoundedReader<T>, config: Config) -> Self {
		let index_by_cid = HashMap::new();
		let mbf_pending_links = HashMap::new();
		let dag = Graph::new();

		Self { content, config, dag, index_by_cid, mbf_pending_links, car_overhead_byte_counter: 0u64 }
	}

	pub(crate) fn add_block_without_cid(&mut self, block: Block<T>) -> BlockId {
		let id = self.dag.add_node(block);
		let block = self.dag.node_weight(id).expect(BLOCK_INSERTED_QED);
		debug!(?id, ?block, "Added block without cid");
		id
	}

	pub(crate) fn add_block(&mut self, block: Block<T>) -> BlockId {
		// Check if block is a missing block
		let id = if let Some(id) = self.index_by_cid.get(&block.cid).copied() {
			if let Some(pre_block) = self.dag.node_weight_mut(id) {
				if let Some(DagPbType::MissingBlock(..)) = &pre_block.dag_pb_type() {
					*pre_block = block
				}
			}
			id
		} else {
			self.dag.add_node(block)
		};

		let (cid, pb_data_len) = {
			let block = self.dag.node_weight(id).expect(BLOCK_INSERTED_QED);
			debug!(?id, ?block, "Added block");
			(block.cid, block.pb_data_len())
		};

		// Double-check pending link list
		self.check_mbf_pending_link(id, cid, pb_data_len);

		// Add index by CID
		self.index_by_cid.insert(cid, id);
		id
	}

	pub(crate) fn link_children(&mut self, id: BlockId, children: &[BlockId]) {
		for child_id in children {
			let child_pb_len = self.dag.node_weight(*child_id).map(|block| block.pb_data_len()).unwrap_or_default();
			let link = BlockLink::new(child_pb_len).into();
			self.dag.add_edge(id, *child_id, link);
		}
	}

	fn check_mbf_pending_link(&mut self, id: BlockId, cid: Cid, pb_data_len: u64) {
		if let Some(parent_id) = self.mbf_pending_links.get(&cid) {
			let link = BlockLink::new(pb_data_len).into();
			debug!(?parent_id, cid = cid.to_string(), ?link, "MBF pending link found");
			self.dag.add_edge(*parent_id, id, link);
			self.mbf_pending_links.remove(&cid);
		}
	}

	pub(crate) fn add_multi_block_file(&mut self, block: Block<T>, links: &[PbLink]) -> BlockId {
		let dag_pb_len = block.pb_data_len();
		let id = self.add_block(block);

		for l in links {
			if let Some(link_id) = self.index_by_cid.get(&l.cid) {
				let link = BlockLink::new(dag_pb_len).into();
				self.dag.add_edge(id, *link_id, link);
			} else {
				self.mbf_pending_links.insert(l.cid, id);
			}
		}

		id
	}

	pub(crate) fn add_directory(&mut self, block: Block<T>, links: &[PbLink]) -> BlockId {
		let id = self.add_block(block);
		tracing::debug!(?links, "Add directory");

		for link in links {
			let link_id = self.index_by_cid.get(&link.cid).copied().unwrap_or_else(|| {
				let missing_block = Block::new_dag_pb(link.cid, DagPbType::MissingBlock(Box::new(link.clone())), ());
				self.add_block(missing_block)
			});
			let link = NamedLink::new(link.name.clone().unwrap_or_default()).into();
			self.dag.add_edge(id, link_id, link);
		}

		id
	}

	/// Get the `BlockId` of root nodes.
	///
	/// **Root Node** is defined by a node with no **incoming** edges from **another** node.
	/// It means that a root node can have a **incoming edge from itself**.
	pub(crate) fn root_ids(&self) -> Vec<NodeIndex> {
		self.dag
			.node_indices()
			.filter(|id| {
				self.dag
					.edges_directed(*id, petgraph::Direction::Incoming)
					.find(|edge| edge.source() != edge.target())
					.is_none()
			})
			.collect()
	}

	/// Returns the **unique** `Block` associated to `path`
	fn path_to_block<P: AsRef<Path>>(&self, path: P) -> Result<&'_ Block<T>> {
		let id = self.path_to_block_id(path)?;
		self.dag.node_weight(id).ok_or(NotFoundErr::BlockId(id).into())
	}

	pub fn path_to_cid<P: AsRef<Path>>(&self, path: P) -> Option<&Cid> {
		self.path_to_block(path).map(|block| &block.cid).ok()
	}

	fn outgoing_links_as_entries(&self, id: BlockId) -> Vec<PbLink> {
		let into_pb_link = |edge: EdgeReference<'_, Link>, name: &str| {
			let target_id = edge.target();
			let target = self.dag.node_weight(target_id)?;
			Some(proto::new_pb_link(target.cid, name.to_owned(), None))
		};

		// Dev: Only `edge` with proper `name`
		let mut named_links = self
			.dag
			.edges_directed(id, Direction::Outgoing)
			.filter_map(|edge| {
				let name = edge.weight().name()?;
				into_pb_link(edge, name)
			})
			.collect::<Vec<_>>();

		// NOTE: Links should be sorted by name, following the IPLD specs.
		named_links.sort_by(|a, b| {
			static LINK_WITH_NAME_QED: &str = "Links with `None` as name were filtered previously .qed";
			let a_name = a.name.as_ref().expect(LINK_WITH_NAME_QED);
			let b_name = b.name.as_ref().expect(LINK_WITH_NAME_QED);
			a_name.cmp(b_name)
		});
		named_links
	}

	fn outgoing_links_as_blocks(&self, id: BlockId) -> Vec<PbLink> {
		self.dag
			.edges_directed(id, Direction::Outgoing)
			.filter_map(|edge| {
				let cum_dag_size = edge.weight().cumulative_dag_size();
				let target_id = edge.target();
				let target = self.dag.node_weight(target_id)?;
				Some(proto::new_pb_link(target.cid, None, cum_dag_size))
			})
			.collect::<Vec<_>>()
	}

	pub fn block_count(&self) -> usize {
		self.dag.node_count()
	}

	pub fn into_inner_file(self) -> Option<T> {
		let unique_content = self.content.clone_and_rewind();
		drop(self);
		unique_content.into_inner_file()
	}
}

impl<T: Read + Seek> ContentAddressableArchive<T> {
	pub fn directory(config: Config) -> Result<Self> {
		let mut this = Self::new(config);

		// Add a root folder
		let root_folder = Block::new_dag_pb(Cid::default(), DagPb::directory(), ());
		let root_folder_id = this.add_block_without_cid(root_folder);
		this.rebuild(root_folder_id)?;
		Ok(this)
	}

	/// Recomputes consolidation info (like `CID`) for each block that was marked as dirty,
	/// then propagates upward through all ancestors.
	///
	/// Uses DFS on the reversed graph so `id` is rebuilt first and ancestors follow
	/// bottom-up. Cycle-safe: the DFS tracks visited nodes and never re-enters them.
	fn rebuild_ancestors(&mut self, id: BlockId) -> Result<()> {
		let rev_dag = Reversed(&self.dag);
		let ancestors = Dfs::new(&rev_dag, id).iter(&rev_dag).collect::<Vec<_>>();

		for ancestor_id in ancestors {
			self.rebuild(ancestor_id)?;
		}

		Ok(())
	}

	fn rebuild(&mut self, id: BlockId) -> Result<()> {
		let mut hasher = self.config.hasher()?;

		let cid = {
			let block = self.dag.node_weight(id).ok_or(NotFoundErr::BlockId(id))?;

			// Remove current CID from indexes
			self.index_by_cid.remove(&block.cid);

			// Rebuild CID
			let cid_codec = match &block.r#type {
				BlockType::Raw => {
					let _len = copy(&mut block.data.clone_and_rewind(), &mut hasher)?;
					CidCodec::Raw
				},
				BlockType::DagPb(dag_pb) => {
					let pb_node = self.as_pb_node(id, dag_pb)?;
					let dag_pb_data = Bytes::from(pb_node.into_bytes());
					let _len = copy(&mut dag_pb_data.reader(), &mut hasher)?;
					CidCodec::DagPb
				},
			};

			let digest = self.config.hash_code.wrap(hasher.finalize())?;
			Cid::new_v1(cid_codec as u64, digest)
		};

		// Calculate the cumulative_dag_size
		let cumulative_dag_size = self
			.dag
			.edges_directed(id, Direction::Incoming)
			.filter_map(|edge| (edge.target() != edge.source()).then_some(edge.weight().cumulative_dag_size()))
			.sum();
		let block_outgoing_edges = self
			.dag
			.edges_directed(id, Direction::Outgoing)
			.filter_map(|edge| match edge.weight() {
				Link::Block(..) if edge.source() != edge.target() => Some(edge.id()),
				_ => None,
			})
			.collect::<Vec<_>>();
		for edge_id in block_outgoing_edges {
			if let Some(Link::Block(block_link)) = self.dag.edge_weight_mut(edge_id) {
				block_link.cumulative_dag_size = cumulative_dag_size;
			}
		}

		// Update Block
		self.index_by_cid.insert(cid, id);
		let block = self.dag.node_weight_mut(id).ok_or(NotFoundErr::BlockId(id))?;
		tracing::debug!(block_id = ?id, prev_cid = block.cid.to_string(), cid = cid.to_string(), "CID updated on block" );
		block.cid = cid;

		Ok(())
	}

	fn as_pb_node(&self, block_id: BlockId, dag_pb: &DagPb<T>) -> Result<PbNode> {
		let pb_node = match &dag_pb.r#type {
			DagPbType::Dir => {
				let links = self.outgoing_links_as_entries(block_id);
				let pb_data: Bytes = proto::Data::new_directory().into();
				proto::new_pb_node(links, pb_data)
			},
			DagPbType::Symlink(s) => {
				let pb_data: Bytes = proto::Data::new_symlink(s.posix_path.clone()).into();
				proto::new_pb_node(vec![], pb_data)
			},
			DagPbType::SingleBlockFile => {
				let mut sbf_buf = Vec::with_capacity(dag_pb.data.bound_len() as usize);
				let _read_bytes = dag_pb.data.clone_and_rewind().read_to_end(&mut sbf_buf)?;
				let pb_data: Bytes = proto::Data::new_file_with_data(sbf_buf).into();
				proto::new_pb_node(vec![], pb_data)
			},
			DagPbType::MultiBlockFile(mbf) => {
				let links = self.outgoing_links_as_blocks(block_id);
				let pb_data: Bytes = proto::Data::new_file(mbf.block_sizes.clone()).into();
				proto::new_pb_node(links, pb_data)
			},
			DagPbType::MissingBlock(l) => fail!(InvalidErr::is_a_miss_block(format!("block_id={block_id:?}"), &l.cid)),
		};
		Ok(pb_node)
	}
}

// File System interface
// ===========================================================================

impl<T> ContentAddressableArchive<T> {
	fn resolve_open_symlink<PL: AsRef<Path>, PT: AsRef<Path>>(&self, link_path: PL, target_path: PT) -> PathBuf {
		let target_path = target_path.as_ref();
		if target_path.is_absolute() {
			return target_path.to_path_buf();
		}

		let mut link_path_parent = parent_or_root(link_path.as_ref());
		if link_path_parent.as_os_str().is_empty() {
			link_path_parent = Path::new("/");
		}

		link_path_parent.join(target_path)
	}

	fn open_multi_block_file(&self, id: BlockId) -> BoundedReader<T> {
		let dfs = Dfs::new(&self.dag, id);
		let part_readers = dfs
			.iter(&self.dag)
			.filter_map(|child_id| {
				let child = self.dag.node_weight(child_id)?;
				child.as_sfb_data()
			})
			.collect::<Vec<_>>();

		ChainedBoundedReader::new(part_readers).into()
	}
}

impl<T> Default for ContentAddressableArchive<T> {
	fn default() -> Self {
		Self::new(Config::default())
	}
}
