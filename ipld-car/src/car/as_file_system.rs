use crate::{
	bounded_reader::{
		sync::BoundedReader,
		traits::{Bounded as _, CloneAndRewind as _},
	},
	car::{
		tools::{ensure_no_loop, parent_or_root},
		Block, BlockBuilder, BlockId, BlockType, ContentAddressableArchive, Metadata,
	},
	dag_pb::{DagPb, DagPbType, NamedLink},
	ensure,
	error::{Error, InvalidErr, NotFoundErr},
	fail,
	traits::{AsCIDGraph, AsFileSystem, ContextLen},
};

use libipld::Cid;
use petgraph::Direction;
use smallvec::{smallvec, SmallVec};
use std::{
	io::{Read, Seek},
	path::Path,
};

impl<T: Seek + Read> AsFileSystem for ContentAddressableArchive<T> {
	type Error = Error;
	type Metadata = Metadata;
	type Reader = T;
	type BoundedReader = BoundedReader<Self::Reader>;

	fn exists<P: AsRef<Path>>(&self, path: P) -> bool {
		self.path_to_block_id(path).ok().is_some()
	}

	fn metadata<P: AsRef<Path>>(&self, path: P) -> Result<Self::Metadata, Self::Error> {
		self.metadata_with_loop_detector(path, smallvec![])
	}

	fn open_file<P: AsRef<Path>>(&self, path: P) -> Result<Self::BoundedReader, Self::Error> {
		self.open_file_with_loop_detector(path, smallvec![])
	}

	fn add_file<P: AsRef<Path>>(&mut self, path: P, reader: Self::Reader) -> Result<(), Self::Error> {
		let path = path.as_ref();
		let os_name = path.file_name().ok_or_else(|| NotFoundErr::file_name(path))?;
		let name = os_name.to_str().ok_or_else(|| InvalidErr::not_utf8_path(os_name))?;

		// Create and add block.
		let bounded = BoundedReader::from_reader(reader)?;
		let block_id = BlockBuilder::new(self, bounded)?.build()?;

		if !self.is_root_empty() {
			let parent_id = self.path_to_block_id(parent_or_root(path))?;
			self.dag.add_edge(parent_id, block_id, NamedLink::new(name).into());
			self.rebuild_ancestors(block_id)
		} else {
			self.dag.add_edge(block_id, block_id, NamedLink::new(name).into());
			Ok(())
		}
	}

	fn create_dir<P: AsRef<Path>>(&mut self, path: P) -> Result<(), Self::Error> {
		let path = path.as_ref();
		let dir_name = path
			.file_name()
			.ok_or_else(|| InvalidErr::file_name(path))?
			.to_str()
			.ok_or_else(|| InvalidErr::not_utf8_path(path))?;
		let parent_id = self.path_to_block_id(parent_or_root(path))?;

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

	fn read_dir<P: AsRef<Path>>(&self, path: P) -> Result<impl Iterator<Item = &str>, Self::Error> {
		let block_id = self.path_to_block_id(path)?;
		let mut entries = self
			.dag
			.edges_directed(block_id, Direction::Outgoing)
			.filter_map(|edge| edge.weight().name())
			.collect::<Vec<_>>();
		entries.sort();

		Ok(entries.into_iter())
	}
}

impl<T> ContentAddressableArchive<T> {
	/// Retrieves metadata for the given path, tracking visited blocks to detect cycles.
	fn metadata_with_loop_detector<P: AsRef<Path>>(
		&self,
		path: P,
		mut visited: SmallVec<[BlockId; 1]>,
	) -> Result<Metadata, Error> {
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
					ensure_no_loop(&mut visited, block_id, path.as_ref())?;
					let target_abs_path = self.resolve_open_symlink(path, &symlink.posix_path);
					let target_meta = self.metadata_with_loop_detector(target_abs_path, visited)?;
					Metadata::symlink(target_meta, &symlink.posix_path)
				},
				DagPbType::MissingBlock(link) => fail!(InvalidErr::is_a_miss_block(path, &link.cid)),
			},
		};

		Ok(meta)
	}

	/// Opens a file at `path` for reading, tracking visited blocks to detect cycles.
	fn open_file_with_loop_detector<P: AsRef<Path>>(
		&self,
		path: P,
		mut visited: SmallVec<[BlockId; 1]>,
	) -> Result<BoundedReader<T>, Error> {
		let id = self.path_to_block_id(path.as_ref())?;
		let block = self.dag.node_weight(id).ok_or(NotFoundErr::BlockId(id))?;
		match &block.r#type {
			BlockType::Raw => Ok(block.data.clone_and_rewind()),
			BlockType::DagPb(dag_pb) => match &dag_pb.r#type {
				DagPbType::SingleBlockFile => Ok(dag_pb.data.clone_and_rewind()),
				DagPbType::MultiBlockFile(_mbf) => Ok(self.open_multi_block_file(id)),
				DagPbType::Symlink(symlink) => {
					ensure_no_loop(&mut visited, id, path.as_ref())?;
					let target_abs_path = self.resolve_open_symlink(path, &symlink.posix_path);
					self.open_file_with_loop_detector(target_abs_path, visited)
				},
				DagPbType::Dir => fail!(InvalidErr::is_a_dir(path)),
				DagPbType::MissingBlock(pb_link) => fail!(InvalidErr::is_a_miss_block(path, &pb_link.cid)),
			},
		}
	}
}
