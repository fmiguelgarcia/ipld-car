use crate::{
	unixfs::{
		pb,
		proto::{data::DataType, Data},
	},
	CidCodec,
};

use derive_more::{Constructor, From};
use libipld::Cid;
use prost::Message as _;
use std::{
	collections::HashMap as Map,
	fmt::Debug,
	io::{Read, Seek, SeekFrom, Take},
	path::{Component, Path, PathBuf},
};
use thiserror_no_std::Error;
use tracing::{trace, warn};

#[derive(Debug, Constructor, Clone)]
pub struct MetadataFile {
	offset: u64,
	unixfs: Data,
}

#[derive(From, Debug)]
pub enum FsEntry {
	File(MetadataFile),
	Dir(Map<PathBuf, FsEntry>),
}

impl FsEntry {
	pub fn get<P: AsRef<Path>>(&self, name: P) -> Option<&FsEntry> {
		match self {
			Self::Dir(dir) => dir.get(name.as_ref()),
			_ => None,
		}
	}

	pub fn as_file(&self) -> Option<&MetadataFile> {
		match self {
			Self::File(meta) => Some(meta),
			_ => None,
		}
	}

	pub fn as_dir(&self) -> Option<&Map<PathBuf, FsEntry>> {
		match self {
			Self::Dir(dir) => Some(dir),
			_ => None,
		}
	}
}

#[derive(Error, Debug)]
pub enum Error {
	UnsupportedCIDCodec,
	Io(#[from] std::io::Error),
	PbNode(#[from] pb::DecodeError),
	MissingPbNodeData,
	Prost(#[from] prost::DecodeError),
}

#[derive(Debug)]
pub struct FileSystemReader<R> {
	// cid: Cid,
	root: FsEntry,
	reader: Option<R>,
}

impl<R: Read + Seek> FileSystemReader<R> {
	pub fn load_from_parts<H: Read>(cid: Cid, mut header: H, mut reader: R) -> Result<Self, Error> {
		let codec = CidCodec::try_from(cid.codec()).map_err(|_| Error::UnsupportedCIDCodec)?;
		let metadata = match codec {
			CidCodec::Raw => {
				let file_size = reader.seek(SeekFrom::End(0))?;
				MetadataFile::new(0, Data::file(file_size))
			},
			CidCodec::DagPb => {
				let unixfs = Self::load_dag_pg(&mut header)?;
				debug_assert_eq!(unixfs.filesize, reader.seek(SeekFrom::End(0)).ok());
				debug_assert!(reader.rewind().is_ok());

				MetadataFile::new(0, unixfs)
			},
		};

		let root = Map::from([("".into(), metadata.into())]);
		Ok(Self { root: root.into(), reader: Some(reader) })
	}

	pub fn load(cid: Cid, mut reader: R) -> Result<Self, Error> {
		let codec = CidCodec::try_from(cid.codec()).map_err(|_| Error::UnsupportedCIDCodec)?;
		let metadata = match codec {
			CidCodec::Raw => {
				let file_size = reader.seek(SeekFrom::End(0))?;
				MetadataFile::new(0, Data::file(file_size))
			},
			CidCodec::DagPb => {
				let unixfs = Self::load_dag_pg(&mut reader)?;
				let offset = reader.stream_position()?;
				debug_assert_eq!(unixfs.filesize, reader.seek(SeekFrom::End(0)).ok());
				debug_assert!(reader.rewind().is_ok());

				MetadataFile::new(offset, unixfs)
			},
		};

		let root = Map::from([("".into(), metadata.into())]);
		Ok(Self { root: root.into(), reader: Some(reader) })
	}

	fn load_dag_pg<H: Read>(reader: &mut H) -> Result<Data, Error> {
		let node = pb::node::decode(reader)?;

		let unixfs_enc = node.data.as_ref().ok_or(Error::MissingPbNodeData)?;
		let unixfs = Data::decode(unixfs_enc.as_ref())?;

		debug_assert_eq!(unixfs.r#type, DataType::File as i32, "Only UnixFs file type is supported");
		Ok(unixfs)
	}

	fn get_fs_entry<P: AsRef<Path>>(&self, path: P) -> Option<&FsEntry> {
		let path = path.as_ref();
		let mut found = None;

		// Case where we have only one unamed file in root.
		if path.as_os_str().is_empty() {
			let root_dir = self.root.as_dir()?;
			return root_dir.get(path)
		}

		for path_fragment in path.components() {
			let curr_entry: &FsEntry = found.get_or_insert(&self.root);
			let dir = curr_entry.as_dir()?;

			match path_fragment {
				Component::Normal(fragment) => {
					let fragment: &Path = fragment.as_ref();
					found = Some(dir.get(fragment)?);
				},
				Component::RootDir | Component::Prefix(..) | Component::CurDir => {
					// We can ignore these components
					trace!(?path_fragment, "Ignored component");
				},
				Component::ParentDir => {
					warn!(?path, "ParentDir is ignored");
				},
			}
		}

		found
	}

	pub fn read_dir<P: AsRef<Path>>(&self, path: P) -> Vec<&PathBuf> {
		let path = path.as_ref();

		if path.as_os_str().is_empty() {
			return self.root.as_dir().map(|root_dir| root_dir.keys().collect()).unwrap_or_default()
		}

		if let Some(FsEntry::Dir(ref dir)) = self.get_fs_entry(path) {
			return dir.keys().collect()
		};

		vec![]
	}

	pub fn metadata<P: AsRef<Path>>(&self, path: P) -> Option<&MetadataFile> {
		let fs_entry = self.get_fs_entry(path.as_ref())?;
		match fs_entry {
			FsEntry::File(ref meta) => Some(meta),
			_ => None,
		}
	}

	#[allow(clippy::result_large_err)]
	pub fn read<P>(mut self, path: P) -> Result<FileReader<R>, Self>
	where
		P: AsRef<Path> + Debug,
	{
		if self.reader.is_none() {
			return Err(self);
		}

		let Some((offset, file_len)) =
			self.metadata(path).map(|meta| (meta.offset, meta.unixfs.filesize.unwrap_or(u64::MAX)))
		else {
			return Err(self);
		};

		let Some(curr_pos) = self.reader.as_mut().and_then(|r| r.seek(SeekFrom::Start(offset)).ok()) else {
			return Err(self);
		};
		debug_assert_eq!(curr_pos, offset);
		let Some(reader) = self.reader.take() else { return Err(self) };

		let bounded_reader = reader.take(file_len);
		Ok(FileReader::new(bounded_reader, self))
	}
}

impl<R> From<FileReader<R>> for FileSystemReader<R> {
	fn from(file: FileReader<R>) -> Self {
		file.into_file_system_reader()
	}
}

#[derive(Constructor)]
pub struct FileReader<R> {
	reader: Take<R>,
	partial_fs: FileSystemReader<R>,
}

impl<R> FileReader<R> {
	pub fn into_file_system_reader(self) -> FileSystemReader<R> {
		let Self { reader, mut partial_fs } = self;

		partial_fs.reader = Some(reader.into_inner());
		partial_fs
	}
}

impl<R: Read> Read for FileReader<R> {
	fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
		self.reader.read(buf)
	}
}
