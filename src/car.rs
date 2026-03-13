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
	config::{CidCodec, Config},
	dag_pb::DagPb,
	ensure,
	error::{InvalidErr, CidErr, Error, NotFoundErr, Result},
	fail, BoundedReader,
};

use libipld::Cid;
use std::io::{Read, Seek, SeekFrom, Write};
use tracing::{debug, trace};

mod block;
pub use block::Block;
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
pub struct ContentAddressableArchive<T> {
	pub content: BoundedReader<T>,
	roots: Vec<ArenaId>,
	arena: Arena<Block<T>>,
}

// VFS support
// ===========================================================================

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
impl<T> ContentAddressableArchive<T> {
	pub(crate) fn path_to_block_ids(&self, path: &Path) -> Result<Vec<ArenaId>> {
		let mut levels = vec![self.roots.clone()];

		for path_component in path.components() {
			match path_component {
				Component::Normal(os_name) => {
					let name = os_name.to_str().ok_or(NotFoundErr::Path)?;

					let mut new_level = vec![];
					for block_id in levels.last().ok_or(NotFoundErr::Path)? {
						let block = self.arena.get(*block_id).expect("Invalid block ID");
						if let BlockContent::DagPb(DagPb::Dir(dir_entries)) = &block.content {
							if let Some(link) = dir_entries.get(name).cloned() {
								let new_block_id =
									self.arena.get_id_by_index(&link.cid).ok_or(NotFoundErr::CidOnDirEntry)?;
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

	fn path_to_block_id(&self, path: &Path) -> Result<ArenaId> {
		let found_ids = self.path_to_block_ids(path)?;
		ensure!(found_ids.len() < 2, Error::more_than_one(found_ids.len(), path));
		found_ids.first().copied().ok_or_else(|| Error::from(NotFoundErr::Path))
	}

	pub(crate) fn metadata_by_ref(&self, block: &Block<T>) -> VfsResult<VfsMetadata> {
		let meta = match &block.content {
			BlockContent::Raw(reader) => metadata_new_file(reader.bound_len()),
			BlockContent::DagPb(dag_pb) => match dag_pb {
				DagPb::Dir(..) => metadata_new(VfsFileType::Directory, 0),
				DagPb::SingleBlockFile(sbf) => metadata_new_file(sbf.len()),
				DagPb::MultiBlockFile(..) | DagPb::Symlink(..) => fail!(VfsErrorKind::NotSupported),
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
			let codec = CidCodec::try_from(cid_codec).map_err(|_| CidErr::CodecNotSupported(cid_codec))?;
			let _id = match codec {
				CidCodec::Raw => {
					let block = Block::new(block_def.cid, block_reader);
					arena.push(block)
				},
				CidCodec::DagPb => DagPb::load(&mut arena, block_def.cid, block_reader)?,
				_other => fail!(CidErr::CodecNotSupported(cid_codec)),
			};

			debug!(pos = block_def.range.end, "CAR reader moved to next block");
			reader.seek(SeekFrom::Start(block_def.range.end))?;
		}

		// Get roots IDs.
		let roots = header
			.roots
			.iter()
			.filter_map(|cid| {
				let (idx, _) = arena.iter().enumerate().find(|(_idx, entry)| entry.cid() == Some(&cid.0))?;
				Some(idx)
			})
			.collect::<Vec<_>>();
		ensure!(roots.len() == header.roots.len(), InvalidErr::HeaderLen);

		Ok(Self { content: reader, roots, arena })
	}
}

// Write functions
// ===========================================================================

impl<T: Read + Seek + 'static> ContentAddressableArchive<T> {
	pub fn write<W: Write>(&self, writer: &mut W, config: &Config) -> Result<u64> {
		// Write header
		let header = CarHeader::new_v1(self.root_cids(config)?);
		let header_written = header.write(writer)? as u64;
		debug!(?header, pos = header_written, "Header written");

		// Write root entries.
		let mut acc_block_written = 0u64;
		for block in self.arena.iter() {
			let block_written = block
				.write(&self.arena, writer, config)?
				.checked_add(acc_block_written)
				.ok_or(Error::FileTooLarge)?;
			acc_block_written = acc_block_written.checked_add(block_written).ok_or(Error::FileTooLarge)?;
			debug!(?block, acc_block_written, "Block written")
		}

		header_written.checked_add(acc_block_written).ok_or(Error::FileTooLarge)
	}
}

impl<T: Read + Seek> ContentAddressableArchive<T> {
	pub fn root_cids(&self, _config: &Config) -> Result<Vec<Cid>> {
		let roots = self.roots.clone();

		roots
			.into_iter()
			.map(|id| {
				let block = self.arena.get(id).ok_or(NotFoundErr::ArenaId(id))?;
				let cid = *block.cid().expect("Block SHOULD have CID until we add files");
				Ok(cid)
			})
			.collect()
	}
}
