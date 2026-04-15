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
	config::{CidCodec, Config},
	dag_pb::{BlockLink, DagPb, DagPbType, Link, NamedLink},
	ensure,
	error::{Error, InvalidErr, LoopDetectedErr, NotFoundErr, NotSupportedErr, Result},
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
	visit::{Bfs, Dfs, EdgeRef, Reversed, Walker},
	Direction,
};
use smallvec::{smallvec, SmallVec};
use std::{
	collections::{HashMap, HashSet, VecDeque},
	fs::File,
	io::{copy, BufWriter, Read, Seek, SeekFrom, Write},
	path::{Component, Path, PathBuf},
};
use tempfile::tempfile;
use tracing::{debug, trace};

mod metadata;
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
#[cfg(test)]
mod tests;

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

	/// CAR root IDs.
	root_ids: SmallBlockIds,

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
		let root_ids = SmallBlockIds::new();
		let index_by_cid = HashMap::new();
		let mbf_pending_links = HashMap::new();
		let dag = Graph::new();

		Self { content, config, dag, index_by_cid, mbf_pending_links, root_ids, car_overhead_byte_counter: 0u64 }
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

	/// Returns the `BlockId`s associated to `path`.
	///
	/// Please note that it can be more than one because a CAR can contains multiple roots.
	fn path_to_block_ids<P: AsRef<Path>>(&self, path: P) -> Result<SmallBlockIds> {
		let path = path.as_ref();
		let not_found_path = || NotFoundErr::path(path);
		let mut levels = vec![self.root_ids.clone()];

		for path_component in path.components() {
			match path_component {
				Component::Normal(os_name) => {
					let name = os_name.to_str().ok_or_else(not_found_path)?;

					let mut new_level = SmallBlockIds::new();
					for block_id in levels.last().ok_or_else(not_found_path)? {
						let mut targets = self
							.dag
							.edges_directed(*block_id, Direction::Outgoing)
							.filter_map(|edge| (edge.weight().name() == Some(name)).then_some(edge.target()))
							.collect::<SmallBlockIds>();
						new_level.append(&mut targets);
					}

					levels.push(new_level)
				},
				Component::RootDir | Component::CurDir => {},
				Component::ParentDir => {
					levels.pop().ok_or_else(not_found_path)?;
				},
				Component::Prefix(..) => fail!(NotSupportedErr::Prefix),
			}
		}

		levels.pop().ok_or_else(|| not_found_path().into())
	}

	/// Returns the  **unique**`BlockId` associated to `path`.
	///
	/// If there is more that one `BlockId`, it will fail with an `Error::MoreThanOneMatchOnPath(..)`
	fn path_to_block_id<P: AsRef<Path>>(&self, path: P) -> Result<BlockId> {
		let path = path.as_ref();
		let ids = self.path_to_block_ids(path)?;
		ensure!(ids.len() < 2, Error::more_than_one(ids.len(), path));
		ids.first().copied().ok_or_else(|| NotFoundErr::path(path).into())
	}

	/// Returns the **unique** `Block` associated to `path`
	fn path_to_block<P: AsRef<Path>>(&self, path: P) -> Result<&'_ Block<T>> {
		let id = self.path_to_block_id(path)?;
		self.dag.node_weight(id).ok_or(NotFoundErr::BlockId(id).into())
	}

	pub fn path_to_cid<P: AsRef<Path>>(&self, path: P) -> Option<&Cid> {
		self.path_to_block(path).map(|block| &block.cid).ok()
	}

	fn outgoing_links(&self, id: BlockId) -> Vec<BlockId> {
		self.dag.edges_directed(id, Direction::Outgoing).map(|edge| edge.target()).collect()
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
		this.root_ids.push(root_folder_id);
		Ok(this)
	}

	/// Recomputes consolidation info (like `CID`) for each block that was marked as dirty,
	/// then propagates upward through all ancestors.
	///
	/// Uses pre-order DFS on the reversed graph so `id` is rebuilt first and ancestors follow
	/// bottom-up. Cycle-safe: the DFS tracks visited nodes and never re-enters them.
	fn rebuild_ancestors(&mut self, id: BlockId) -> Result<()> {
		let rev_dag = Reversed(&self.dag);
		let ancestors = Dfs::new(&rev_dag, id).iter(&rev_dag).collect::<Vec<_>>();

		for block_id in ancestors {
			self.rebuild(block_id)?;
		}

		Ok(())
	}

	fn rebuild(&mut self, id: BlockId) -> Result<()> {
		let mut hasher = self.config.hasher()?;

		let (cid, dag_pb_data) = {
			let block = self.dag.node_weight(id).ok_or(NotFoundErr::BlockId(id))?;

			// Remove current CID from indexes
			self.index_by_cid.remove(&block.cid);

			// Rebuild CID
			let (cid_codec, dag_pb_data) = match &block.r#type {
				BlockType::Raw => {
					let _len = copy(&mut block.data.clone_and_rewind(), &mut hasher)?;
					(CidCodec::Raw, Bytes::new())
				},
				BlockType::DagPb(dag_pb) => {
					let pb_node = self.as_pb_node(id, dag_pb)?;
					let dag_pb_data = Bytes::from(pb_node.into_bytes());
					let _len = copy(&mut dag_pb_data.clone().reader(), &mut hasher)?;
					(CidCodec::DagPb, dag_pb_data)
				},
			};

			let digest = self.config.hash_code.wrap(hasher.finalize())?;
			let cid = Cid::new_v1(cid_codec as u64, digest);
			(cid, dag_pb_data)
		};

		// Calculate the cumulative_dag_size
		let cumulative_dag_size = self
			.dag
			.edges_directed(id, Direction::Incoming)
			.map(|edge| edge.weight().cumulative_dag_size())
			.sum();
		let block_outgoing_edges = self
			.dag
			.edges_directed(id, Direction::Outgoing)
			.filter_map(|edge| match edge.weight() {
				Link::Block(..) => Some(edge.id()),
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
		if let BlockType::DagPb(dag_pb) = &mut block.r#type {
			dag_pb.data = BoundedReader::from(dag_pb_data);
		}
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
				let _ = dag_pb.data.clone_and_rewind().read_to_end(&mut sbf_buf)?;
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
	pub fn read_dir<P: AsRef<Path>>(&self, path: P) -> Result<impl Iterator<Item = &str>> {
		let block_id = self.path_to_block_id(path)?;
		let mut entries = self
			.dag
			.edges_directed(block_id, Direction::Outgoing)
			.filter_map(|edge| edge.weight().name())
			.collect::<Vec<_>>();
		entries.sort();

		Ok(entries.into_iter())
	}

	pub fn open_file<P: AsRef<Path>>(&self, path: P) -> Result<BoundedReader<T>> {
		self.open_file_with_loop_detector(path, smallvec![])
	}

	fn open_file_with_loop_detector<P: AsRef<Path>>(
		&self,
		path: P,
		mut open_block_ids: SmallVec<[BlockId; 1]>,
	) -> Result<BoundedReader<T>> {
		let id = self.path_to_block_id(path.as_ref())?;
		let block = self.dag.node_weight(id).ok_or(NotFoundErr::BlockId(id))?;
		match &block.r#type {
			BlockType::Raw => Ok(block.data.clone_and_rewind()),
			BlockType::DagPb(dag_pb) => match &dag_pb.r#type {
				DagPbType::SingleBlockFile => Ok(dag_pb.data.clone_and_rewind()),
				DagPbType::MultiBlockFile(_mbf) => Ok(self.open_multi_block_file(id)),
				DagPbType::Symlink(symlink) => {
					check_loop_and_update(&mut open_block_ids, path.as_ref(), id)?;
					let target_abs_path = self.resolve_open_symlink(path, &symlink.posix_path);
					self.open_file_with_loop_detector(target_abs_path, open_block_ids)
				},
				DagPbType::Dir => fail!(InvalidErr::is_a_dir(path)),
				DagPbType::MissingBlock(pb_link) => fail!(InvalidErr::is_a_miss_block(path, &pb_link.cid)),
			},
		}
	}

	fn resolve_open_symlink<PL: AsRef<Path>, PT: AsRef<Path>>(&self, link_path: PL, target_path: PT) -> PathBuf {
		let target_path = target_path.as_ref();
		if target_path.is_absolute() {
			return target_path.to_path_buf();
		}

		let root = Path::new("/");
		let mut link_path_parent = link_path.as_ref().parent().unwrap_or(root);
		if link_path_parent.as_os_str().is_empty() {
			link_path_parent = root;
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

	pub fn metadata<P: AsRef<Path>>(&self, path: P) -> Result<Metadata> {
		self.metadata_with_loop_detector(path, smallvec![])
	}

	fn metadata_with_loop_detector<P: AsRef<Path>>(
		&self,
		path: P,
		mut open_block_ids: SmallVec<[BlockId; 1]>,
	) -> Result<Metadata> {
		let block_id = self.path_to_block_id(path.as_ref())?;
		let block = self.dag.node_weight(block_id).ok_or(NotFoundErr::BlockId(block_id))?;

		let meta = match &block.r#type {
			BlockType::Raw => Metadata::file(block.data.bound_len()),
			BlockType::DagPb(dag_pb) => match &dag_pb.r#type {
				DagPbType::SingleBlockFile => Metadata::file(block.data_len()),
				DagPbType::MultiBlockFile(mbf) => {
					let acc_len = mbf.block_sizes.iter().sum::<u64>();
					Metadata::file(acc_len)
				},
				DagPbType::Dir => Metadata::directory(),
				DagPbType::Symlink(symlink) => {
					check_loop_and_update(&mut open_block_ids, path.as_ref(), block_id)?;
					let target_abs_path = self.resolve_open_symlink(path, &symlink.posix_path);
					let target_meta = self.metadata_with_loop_detector(target_abs_path, open_block_ids)?;
					Metadata::symlink(target_meta, &symlink.posix_path)
				},
				DagPbType::MissingBlock(link) => fail!(InvalidErr::is_a_miss_block(path, &link.cid)),
			},
		};

		Ok(meta)
	}

	pub fn exists<P: AsRef<Path>>(&self, path: P) -> bool {
		self.path_to_block_id(path).ok().is_some()
	}
}

impl<T: Read + Seek> ContentAddressableArchive<T> {
	pub fn with_dir<P: AsRef<Path>>(mut self, path: P) -> Result<Self> {
		self.create_dir(path)?;
		Ok(self)
	}

	/// Creates a new empty directory at `parent_path/dir_name`.
	pub fn create_dir<P: AsRef<Path>>(&mut self, path: P) -> Result<()> {
		let path = path.as_ref();
		let dir_name = path
			.file_name()
			.ok_or_else(|| InvalidErr::file_name(path))?
			.to_str()
			.ok_or_else(|| InvalidErr::not_utf8_path(path))?;
		let parent_path = path.parent().unwrap_or_else(|| Path::new("."));
		let parent_id = self.path_to_block_id(parent_path)?;

		// `dir_name` is not already used.
		let found_dir_name = self
			.dag
			.edges_directed(parent_id, Direction::Outgoing)
			.find(|edge| edge.weight().name() == Some(dir_name));
		ensure!(found_dir_name.is_none(), InvalidErr::exists(dir_name));

		let new_dir = Block::new_dag_pb(Cid::default(), DagPb::directory(), ());
		let new_dir_id = self.add_block(new_dir);
		self.dag.add_edge(parent_id, new_dir_id, NamedLink::new(dir_name).into());
		self.rebuild_ancestors(new_dir_id)
	}

	pub fn with_file<P: AsRef<Path>>(mut self, path: P, reader: T) -> Result<Self> {
		self.add_file(path, reader)?;
		Ok(self)
	}

	pub fn add_file<P: AsRef<Path>>(&mut self, path: P, reader: T) -> Result<()> {
		let path = path.as_ref();
		let os_name = path.file_name().ok_or_else(|| NotFoundErr::file_name(path))?;
		let name = os_name.to_str().ok_or_else(|| InvalidErr::not_utf8_path(os_name))?;

		// Create and add block.
		let bounded = BoundedReader::from_reader(reader)?;
		let block_id = BlockBuilder::new(self, bounded)?.build()?;

		if !self.root_ids.is_empty() {
			let parent_path = path.parent().unwrap_or(Path::new("."));
			let parent_id = self.path_to_block_id(parent_path)?;
			self.dag.add_edge(parent_id, block_id, NamedLink::new(name).into());
			self.rebuild_ancestors(block_id)
		} else {
			self.root_ids.push(block_id);
			self.dag.add_edge(block_id, block_id, NamedLink::new(name).into());
			Ok(())
		}
	}

	pub fn root_cids(&self) -> Result<Vec<Cid>> {
		self.root_ids
			.iter()
			.map(|id| {
				let block = self.dag.node_weight(*id).ok_or(NotFoundErr::BlockId(*id))?;
				Ok(block.cid)
			})
			.collect()
	}

	pub fn cids(&self) -> HashSet<Cid> {
		self.root_ids
			.iter()
			.flat_map(|root_id| {
				let bfs = Bfs::new(&self.dag, *root_id);
				bfs.iter(&self.dag)
					.filter_map(|node_id| self.dag.node_weight(node_id).map(|block| block.cid))
					.collect::<HashSet<Cid>>()
			})
			.collect::<HashSet<Cid>>()
	}

	pub fn block_from_cid(&self, cid: &Cid) -> Option<&Block<T>> {
		let block_id = self.index_by_cid.get(cid)?;
		self.dag.node_weight(*block_id)
	}
}

// Load functions
// ===========================================================================

impl<F: Read + Seek> ContentAddressableArchive<F> {
	pub fn load(reader: F) -> Result<Self> {
		let mut reader = BoundedReader::from_reader(reader)?;
		let mut this = Self::base_new(reader.clone(), Config::default());

		// Load header
		let header = CarHeader::load(&mut reader)?;
		this.car_overhead_byte_counter += reader.stream_position()?;
		trace!(?header, pos = this.car_overhead_byte_counter, "Header loaded");

		// load each blocka
		while let Some(block_def) = BlockDef::load(&mut reader)? {
			// Block elements: content & consolidation info from `reader`
			trace!(?block_def, "BlockDef loaded");
			this.car_overhead_byte_counter += block_def.car_overhead_byte_counter;
			let block_data = reader.sub(block_def.range.clone())?;

			// Load block based on its CID.
			let cid_codec = block_def.cid.codec();
			let codec = CidCodec::from_repr(cid_codec).ok_or(Error::CodecNotSupported(cid_codec))?;
			match codec {
				CidCodec::Raw => this.add_block(Block::new_raw(block_def.cid, block_data)),
				CidCodec::DagPb => DagPb::load(&mut this, block_def.cid, block_data)?,
				_other => fail!(Error::CodecNotSupported(cid_codec)),
			};
			reader.seek(SeekFrom::Start(block_def.range.end))?;
		}

		// Update roots.
		this.root_ids = header
			.roots
			.iter()
			.filter_map(|cid| this.index_by_cid.get(&cid.0))
			.cloned()
			.collect::<SmallBlockIds>();

		Ok(this)
	}
}

// Write functions
// ===========================================================================

impl<T: Read + Seek + 'static> ContentAddressableArchive<T> {
	pub fn write<W: Write>(&mut self, writer: &mut W) -> Result<u64> {
		// Write header
		let header = CarHeader::new_v1(self.root_cids()?);
		let header_written = header.write(writer)? as u64;
		// debug!(?header, pos = header_written, "Header written");

		// Write blocks in node insertion order, which preserves the original file block order
		// on round-trips. BFS would visit children in reverse-insertion order due to petgraph's
		// adjacency list being prepend-only.
		let mut acc_written = 0u64;

		for id in self.dag.node_indices() {
			let block = self.dag.node_weight(id).ok_or(NotFoundErr::BlockId(id))?;
			let cid = block.cid;
			let written_bytes = match &block.r#type {
				BlockType::Raw => {
					let len = block.data.bound_len();
					write_block(cid, len, &mut block.data.clone_and_rewind(), writer)?
				},
				BlockType::DagPb(dag_pb) => {
					if block.data.bound_len() > 0 {
						// Pass-through: write the original bytes from the loaded file.
						let len = block.data.bound_len();
						write_block(cid, len, &mut block.data.clone_and_rewind(), writer)?
					} else {
						// New block (no original bytes): encode from structure.
						let pb_node = Bytes::from(self.as_pb_node(id, dag_pb)?.into_bytes());
						let pb_node_len = pb_node.len() as u64;
						write_block(cid, pb_node_len, &mut pb_node.reader(), writer)?
					}
				},
			};
			acc_written = acc_written.checked_add(written_bytes).ok_or(Error::FileTooLarge)?;
		}

		header_written.checked_add(acc_written).ok_or(Error::FileTooLarge)
	}
}

fn write_block<R: Read, W: Write>(cid: Cid, reader_len: u64, reader: &mut R, w: &mut W) -> Result<u64> {
	let cid = cid.to_bytes();
	let section_len = reader_len.checked_add(cid.len() as u64).ok_or(Error::FileTooLarge)?;

	let leb_written = leb128::write::unsigned(w, section_len)? as u64;
	w.write_all(&cid)?;
	let copied = copy(reader, w)?;

	copied.checked_add(leb_written + cid.len() as u64).ok_or(Error::FileTooLarge)
}

impl<T> ContentAddressableArchive<T> {
	fn traverse_blocks<F>(&self, mut len_fn: F) -> u64
	where
		F: FnMut(&Block<T>, BlockId, &mut VecDeque<BlockId>, &HashSet<BlockId>) -> u64,
	{
		let mut acc_len = 0u64;
		let mut closed = HashSet::new();
		let mut open = VecDeque::from_iter(self.root_ids.iter().copied());

		while let Some(id) = open.pop_front() {
			let Some(block) = self.dag.node_weight(id) else { continue };
			let block_len = len_fn(block, id, &mut open, &closed);

			closed.insert(id);
			acc_len = acc_len.saturating_add(block_len);
		}

		acc_len
	}
}

impl<T> ContextLen for ContentAddressableArchive<T> {
	fn data_len(&self) -> u64 {
		self.traverse_blocks(|block, id, open, closed| match &block.r#type {
			BlockType::Raw => block.data.bound_len(),
			BlockType::DagPb(dag_pb) => match &dag_pb.r#type {
				DagPbType::Dir => {
					for entry_id in self.outgoing_links(id) {
						if entry_id != id && !open.contains(&entry_id) && !closed.contains(&entry_id) {
							open.push_back(entry_id);
						}
					}
					0u64
				},
				DagPbType::SingleBlockFile => dag_pb.data.bound_len(),
				DagPbType::MultiBlockFile(mbf) => mbf.block_sizes.iter().sum(),
				DagPbType::Symlink(..) | DagPbType::MissingBlock(..) => 0u64,
			},
		})
	}

	fn pb_data_len(&self) -> u64 {
		self.traverse_blocks(|block, id, open, closed| {
			if let BlockType::DagPb(dag_pb) = &block.r#type {
				if let DagPbType::Dir = &dag_pb.r#type {
					for entry_id in self.outgoing_links(id) {
						if entry_id != id && !open.contains(&entry_id) && !closed.contains(&entry_id) {
							open.push_back(entry_id);
						}
					}
				}
			}
			block.data.bound_len()
		})
	}
}

// Tools
// ===========================================================================

/// Uses `open_block_ids` to track visited block IDs, in order to detect loops during the
/// resolution of symbolic links.
fn check_loop_and_update<P: Into<PathBuf>>(
	open_block_ids: &mut SmallVec<[BlockId; 1]>,
	target_path: P,
	target_id: BlockId,
) -> Result<()> {
	ensure!(!open_block_ids.contains(&target_id), LoopDetectedErr::Symlink(target_path.into()));

	open_block_ids.push(target_id);
	Ok(())
}
