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
	arena::{Arena, ArenaId},
	car::block_builder::BlockBuilder,
	config::{CidCodec, Config},
	dag_pb::{DagPb, Directory},
	ensure,
	error::{CidErr, Error, InvalidErr, NotFoundErr, Result},
	fail, BoundedReader, CIDBuilder, ContextLen,
};

use libipld::{pb::PbNode, Cid};
use std::{
	collections::VecDeque,
	fs::File,
	io::{Read, Seek, SeekFrom, Write},
};
use tempfile::tempfile;
use tracing::{debug, trace};

mod block;
pub use block::Block;
mod block_builder;
mod block_content;
mod block_def;
pub(crate) use block_def::BlockDef;
mod cbor_cid;
#[cfg(feature = "vfs")]
pub mod fs;
mod header;
pub(crate) use header::CarHeader;
#[cfg(test)]
mod tests;

#[derive(derive_more::Debug)]
pub struct ContentAddressableArchive<T: Read + Seek> {
	pub content: BoundedReader<T>,
	roots: Vec<ArenaId>,
	arena: Arena<Block<T>>,
	config: Config,
}

impl ContentAddressableArchive<File> {
	pub fn new(config: Config) -> Result<Self> {
		let content = BoundedReader::from_reader(tempfile()?)?;
		let mut arena = Arena::default();
		let dir = Directory::default();
		let root_dir_cid = dir.cid(&config)?;
		let root_id = arena.push(Block::new(root_dir_cid, DagPb::Dir(dir)), None);

		Ok(Self { content, roots: vec![root_id], arena, config })
	}
}

impl<T: Read + Seek> ContentAddressableArchive<T> {
	pub fn add_file(&mut self, name: String, parent_id: ArenaId, parent_path: &Path, reader: T) -> Result<()> {
		// Create Tree of `reader`.
		let bounded = BoundedReader::from_reader(reader)?;
		let block_builder = BlockBuilder::new(bounded, self.config)?;
		let block = block_builder.build()?;
		let cid = block.cid.expect("Generated block has CID .qed");
		let link = Link::new(cid, block.dag_pb_len(), Some(block.data_len()), name.clone(), None);

		// Add block (recursivelly to arena).
		let id = self.arena.push(block, parent_id);

		{
			let parent = self.arena.get_mut(parent_id).ok_or(NotFoundErr::ArenaId(parent_id))?;
			parent.push_directory_entry(name, link.with_arena_id(id))?;
		}

		self.invalidate_cid_on_ancestors(parent_path);
		Ok(())
	}

	pub fn root_cids(&self) -> Result<Vec<Cid>> {
		let roots = self.roots.clone();

		roots
			.into_iter()
			.map(|id| {
				let block = self.arena.get(id).ok_or(NotFoundErr::ArenaId(id))?;
				let cid = block.cid.expect("Block SHOULD have CID until we add files");
				Ok(cid)
			})
			.collect()
	}

	#[inline]
	pub fn arena(&self) -> &Arena<Block<T>> {
		&self.arena
	}
}

// VFS support
// ===========================================================================

#[cfg(feature = "vfs")]
use crate::dag_pb::Link;
#[cfg(feature = "vfs")]
use crate::error::NotSupportedErr;
#[cfg(feature = "vfs")]
use block_content::BlockContent;
#[cfg(feature = "vfs")]
use std::path::{Component, Path};
#[cfg(feature = "vfs")]
use vfs::{
	error::{VfsErrorKind, VfsResult},
	VfsFileType, VfsMetadata,
};

#[cfg(feature = "vfs")]
impl<T: Read + Seek> ContentAddressableArchive<T> {
	pub fn path_to_block_ids(&self, path: &Path) -> Result<Vec<ArenaId>> {
		let mut levels = vec![self.roots.clone()];

		for path_component in path.components() {
			match path_component {
				Component::Normal(os_name) => {
					let name = os_name.to_str().ok_or(NotFoundErr::Path)?;

					let mut new_level = vec![];
					for block_id in levels.last().ok_or(NotFoundErr::Path)? {
						let block = self.arena.get(*block_id).expect("Invalid block ID");
						if let BlockContent::DagPb(DagPb::Dir(dir)) = &block.content {
							if let Some(link) = dir.entries().get(name).cloned() {
								let new_block_id = link
									.arena_id
									.or_else(|| self.arena.get_id_by_index(&link.cid))
									.ok_or(NotFoundErr::CidOnDirEntry)?;
								new_level.push(new_block_id);
							}
						}
					}
					levels.push(new_level)
				},
				Component::RootDir | Component::CurDir => {},
				Component::ParentDir => {
					levels.pop().ok_or(Error::NotFound(NotFoundErr::Path))?;
				},
				Component::Prefix(..) => fail!(NotSupportedErr::Prefix),
			}
		}

		levels.pop().ok_or(Error::NotFound(NotFoundErr::Path))
	}

	pub fn path_to_block_id(&self, path: &Path) -> Result<ArenaId> {
		let found_ids = self.path_to_block_ids(path)?;
		ensure!(found_ids.len() < 2, Error::more_than_one(found_ids.len(), path));
		found_ids.first().copied().ok_or_else(|| Error::from(NotFoundErr::Path))
	}

	pub fn path_to_cid(&self, path: &Path) -> Result<Option<Cid>> {
		let id = self.path_to_block_id(path)?;
		let cid = self.arena.get(id).and_then(|block| block.cid);
		Ok(cid)
	}

	fn path_to_mut_block(&mut self, path: &Path) -> Result<&'_ mut Block<T>> {
		let found_id = self.path_to_block_id(path)?;
		self.arena.get_mut(found_id).ok_or_else(|| Error::from(NotFoundErr::ArenaId(found_id)))
	}

	pub(crate) fn path_to_block(&self, path: &Path) -> Result<&'_ Block<T>> {
		let found_id = self.path_to_block_id(path)?;
		self.arena.get(found_id).ok_or_else(|| Error::from(NotFoundErr::ArenaId(found_id)))
	}

	/// Creates a new empty directory at `parent_path/dir_name`.
	pub fn create_dir(&mut self, parent_path: &Path, dir_name: &str) -> Result<()> {
		let parent_id = self.path_to_block_id(parent_path)?;

		// Verify the parent is a directory and the name is not already taken.
		{
			let parent = self.arena.get(parent_id).ok_or(NotFoundErr::ArenaId(parent_id))?;
			match &parent.content {
				BlockContent::DagPb(DagPb::Dir(dir)) =>
					if dir.entries().contains_key(dir_name) {
						fail!(InvalidErr::AlreadyExists(dir_name.to_string()));
					},
				_ => return Err(Error::invalid_path(parent_path)),
			}
		}

		// Compute the CID of the new empty directory and push it to the arena.
		// The returned ArenaId is stored in the Link so that path resolution can find this
		// specific block even when other empty directories share the same CID.
		let new_dir = Directory::default();
		let new_dir_cid = new_dir.cid(&self.config)?;
		let new_dir_pb_len = PbNode::from(&new_dir).into_bytes().len() as u64;
		let block = Block::new(new_dir_cid, DagPb::Dir(new_dir));
		tracing::trace!(?block, parent_id, "Directory block added under parent");
		let new_dir_arena_id = self.arena.push(block, parent_id);

		// Insert a Link to the new directory in the parent and invalidate its CID.
		let parent = self.arena.get_mut(parent_id).ok_or(NotFoundErr::ArenaId(parent_id))?;
		match &mut parent.content {
			BlockContent::DagPb(DagPb::Dir(dir)) => {
				let link = Link::new(new_dir_cid, new_dir_pb_len, None, dir_name.to_string(), new_dir_arena_id);
				dir.mut_entries().insert(dir_name.to_string(), link);

				self.invalidate_cid_on_ancestors(parent_path);
			},
			_ => unreachable!("already verified above"),
		}

		Ok(())
	}

	/// Invalidates `parent_path` and all its ancestors.
	fn invalidate_cid_on_ancestors(&mut self, parent_path: &Path) {
		let mut maybe_ancestor_path = Some(parent_path);
		while let Some(ancestor_path) = maybe_ancestor_path.take() {
			if let Ok(block) = self.path_to_mut_block(ancestor_path) {
				block.invalidate();
			}
			maybe_ancestor_path = ancestor_path.parent();
		}
	}

	pub(crate) fn metadata_by_ref(&self, block: &Block<T>) -> VfsResult<VfsMetadata> {
		use crate::ContextLen;

		let meta = match &block.content {
			BlockContent::Raw(reader) => metadata_new_file(reader.bound_len()),
			BlockContent::DagPb(dag_pb) => match dag_pb {
				DagPb::Dir(..) => metadata_new(VfsFileType::Directory, 0),
				DagPb::SingleBlockFile(sbf) => metadata_new_file(sbf.data_len()),
				DagPb::MultiBlockFile(mbf) => metadata_new_file(mbf.data_len()),
				DagPb::Symlink(..) => fail!(VfsErrorKind::NotSupported),
			},
		};

		Ok(meta)
	}
}

#[cfg(feature = "vfs")]
fn metadata_new(file_type: VfsFileType, len: u64) -> VfsMetadata {
	VfsMetadata { file_type, len, created: None, modified: None, accessed: None }
}

#[cfg(feature = "vfs")]
#[inline]
fn metadata_new_file(len: u64) -> VfsMetadata {
	metadata_new(VfsFileType::File, len)
}

// Load functions
// ===========================================================================

impl<F: Read + Seek> ContentAddressableArchive<F> {
	pub fn load(reader: F) -> Result<Self> {
		let mut reader = BoundedReader::from_reader(reader)?;
		debug!(?reader, "ContentAddressableArchive reader");
		let header = CarHeader::load(&mut reader)?;
		trace!(?header, pos = reader.stream_position()?, "Header loaded");

		// load each block
		let mut arena = Arena::with_capacity(header.roots.len());
		while let Some(block_def) = BlockDef::load(&mut reader)? {
			trace!(?block_def, "BlockDef loaded");
			let block_reader = reader.sub(block_def.range.clone())?;
			let cid_codec = block_def.cid.codec();
			let codec = CidCodec::from_repr(cid_codec).ok_or(CidErr::CodecNotSupported(cid_codec))?;
			let _id = match codec {
				CidCodec::Raw => {
					let block = Block::new(block_def.cid, block_reader);
					arena.push(block, None)
				},
				CidCodec::DagPb => DagPb::load(&mut arena, block_def.cid, block_reader)?,
				_other => fail!(CidErr::CodecNotSupported(cid_codec)),
			};

			debug!(pos = block_def.range.end, "CAR reader moved to next block");
			reader.seek(SeekFrom::Start(block_def.range.end))?;
		}

		// Get roots IDs.
		// TODO: Regenerate parent relations recursivelly.
		let roots = header
			.roots
			.iter()
			.filter_map(|cid| {
				let (idx, _) = arena.iter().enumerate().find(|(_idx, entry)| entry.cid.as_ref() == Some(&cid.0))?;
				Some(idx)
			})
			.collect::<Vec<_>>();
		ensure!(roots.len() == header.roots.len(), InvalidErr::HeaderLen);

		Ok(Self { content: reader, roots, arena, config: Config::default() })
	}
}

// Write functions
// ===========================================================================

impl<T: Read + Seek + 'static> ContentAddressableArchive<T> {
	pub fn write<W: Write>(&mut self, writer: &mut W) -> Result<u64> {
		self.rebuild_invalids()?;

		// Write header
		let header = CarHeader::new_v1(self.root_cids()?);
		let header_written = header.write(writer)? as u64;
		debug!(?header, pos = header_written, "Header written");

		// Write root entries.
		let mut acc_block_written = 0u64;
		for block in self.arena.iter() {
			let block_written = block.write(writer)?.checked_add(acc_block_written).ok_or(Error::FileTooLarge)?;
			acc_block_written = acc_block_written.checked_add(block_written).ok_or(Error::FileTooLarge)?;
			debug!(?block, acc_block_written, "Block written")
		}

		header_written.checked_add(acc_block_written).ok_or(Error::FileTooLarge)
	}

	/// Recomputes CIDs for all directory blocks whose CID has been invalidated (set to `None`),
	/// processing from highest arena index to lowest so children are finalized before parents.
	/// After recomputing a block's CID, updates the stale `link.cid` in every parent directory
	/// entry that points to that block (identified via `link.arena_id`).
	fn rebuild_invalids(&mut self) -> Result<()> {
		let mut invalidated_ids = self
			.arena
			.iter()
			.enumerate()
			.filter_map(|(id, block)| block.cid.is_none().then_some(id))
			.collect::<VecDeque<_>>();

		while let Some(id) = invalidated_ids.pop_back() {
			if let Some(block) = self.arena.get_mut(id) {
				if block.cid.is_none() {
					let new_cid = block.cid(&self.config)?;
					block.cid = Some(new_cid);
					let _ = block;

					// Update parents:
					// - link.cid of updated entry should be updated too.
					// - Mark this parent block as invalid, so we could rebuild it later.
					let parent_ids = self.arena.parent_of(id);
					for parent_id in parent_ids {
						if let Some(parent_block) = self.arena.get_mut(parent_id) {
							if let BlockContent::DagPb(DagPb::Dir(dir)) = &mut parent_block.content {
								// DEV: Could we have more that one entry pointing to updated block?
								// Maybe because we could create `symbolic links` to it.
								for link in dir.mut_entries().values_mut() {
									if link.arena_id == Some(id) {
										link.cid = new_cid;
									}
								}
							}

							parent_block.invalidate();
							if !invalidated_ids.contains(&parent_id) {
								invalidated_ids.push_front(parent_id);
							}
						}
					}
				}
			}
		}

		Ok(())
	}
}
