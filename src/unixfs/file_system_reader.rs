use crate::{
	cid_from_reader, from_links,
	unixfs::{
		pb,
		proto::{data::DataType, Data},
		FileReader, SELF_PATH,
	},
	CidCodec, CidCodecError,
};

use derive_more::{Constructor, From};
use libipld::Cid;
use prost::Message as _;
use std::{
	collections::HashMap as Map,
	fmt::Debug,
	fs::File,
	io::{Read, Seek, SeekFrom, Take},
	path::{Component, Path, PathBuf},
};
use thiserror::Error;
use tracing::{trace, warn};

#[derive(Debug)]
pub struct FileSystemReader<R> {
	root: FsEntry,
	codec: CidCodec,
	pub(crate) reader: Option<R>,
}

impl<R: Read + Seek> FileSystemReader<R> {
	/// It loads metadata from the given `cid` and `reader`.
	fn load_metadata<H: Read>(codec: CidCodec, reader: &mut R, maybe_header: Option<H>) -> Result<MetadataFile, Error> {
		let metadata = match codec {
			CidCodec::Raw => {
				let file_size = reader.seek(SeekFrom::End(0))?;
				MetadataFile::new(0, Data::file(file_size))
			},
			CidCodec::DagPb => match maybe_header {
				Some(mut header) => {
					let unixfs = Self::load_dag_pg(&mut header)?;
					debug_assert_eq!(unixfs.filesize, reader.seek(SeekFrom::End(0)).ok());

					MetadataFile::new(0, unixfs)
				},
				None => {
					reader.rewind()?;
					let unixfs = Self::load_dag_pg(reader)?;
					let offset = reader.stream_position()?;

					MetadataFile::new(offset, unixfs)
				},
			},
		};

		Ok(metadata)
	}

	/// Loads a file system from the given `cid`, `header` and `reader`.
	pub fn load_from_parts<H: Read>(cid: Cid, mut reader: R, header: H) -> Result<Self, Error> {
		let codec = CidCodec::try_from(cid)?;
		let metadata = Self::load_metadata(codec, &mut reader, Some(header))?;
		let root = FsEntry::make_root(metadata);

		Ok(Self { root, reader: Some(reader), codec })
	}

	/// Loads a file system from the given `cid` and `reader`.
	pub fn load(cid: Cid, mut reader: R) -> Result<Self, Error> {
		let codec = CidCodec::try_from(cid)?;
		let metadata = Self::load_metadata::<File>(codec, &mut reader, None)?;
		let root = FsEntry::make_root(metadata);

		Ok(Self { root, reader: Some(reader), codec })
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

		// Walk through the path components to find the entry.
		let mut current = &self.root;
		for path_fragment in path.components() {
			let curr_dir = current.as_dir()?;

			match path_fragment {
				Component::Normal(fragment) => {
					if fragment == SELF_PATH {
						// We can ignore SELF_PATH, it is the root directory.
						trace!(?path_fragment, "Ignored SELF_PATH component");
						continue;
					}
					let fragment: &Path = fragment.as_ref();
					current = curr_dir.get(fragment)?;
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

		/*
		// NOTE: We point to first file ATM because we don't support more files yet.
		// It means that path `"."` or `""` should access to the first unique file instead of the
		// root.
		if std::ptr::eq(current,&self.root) {
			return self.root.as_dir()
				.and_then(|dir| dir.values().next())
				.map(|entry| entry)
		}
		*/

		Some(current)
	}

	/// # TODO:
	/// - Make recursive search.
	pub fn read_dir<P: AsRef<Path>>(&self, path: P) -> Vec<&Path> {
		let path = path.as_ref();
		let Some(FsEntry::Dir(dir)) = self.get_fs_entry(path) else { return vec![] };
		dir.keys().map(PathBuf::as_ref).collect()
	}

	/// # TODO:
	/// - Make recursive search.
	pub fn metadata<P: AsRef<Path>>(&self, path: P) -> Option<&MetadataFile> {
		let fs_entry = self.get_fs_entry(path.as_ref())?;
		match fs_entry {
			FsEntry::File(ref meta) => Some(meta),
			FsEntry::Dir(ref folder) =>
				if folder.len() == 1 {
					folder.values().next().and_then(|entry| entry.as_file())
				} else {
					None
				},
		}
	}

	/// # TODO:
	/// - Make recursive search.
	#[allow(clippy::result_large_err)]
	pub fn read<P>(mut self, path: P) -> Result<FileReader<R>, Error>
	where
		P: AsRef<Path> + Debug,
	{
		// Get metadata for the given path.
		let maybe_offset_size =
			self.metadata(path.as_ref()).map(|meta| (meta.offset, meta.unixfs.filesize.unwrap_or(u64::MAX)));
		let Some((offset, file_len)) = maybe_offset_size else {
			return Err(Error::MissingMetadataOnFile(path.as_ref().to_path_buf()));
		};

		// Take reader and create a bounded reader.
		let Some(mut reader) = self.reader.take() else { return Err(Error::MissingReader) };
		let curr_pos = reader.seek(SeekFrom::Start(offset))?;
		debug_assert_eq!(curr_pos, offset);

		let bounded_reader = reader.take(file_len);
		Ok(FileReader::new(bounded_reader, self))
	}

	pub fn take_reader<P>(&mut self, path: P) -> Result<Take<R>, Error>
	where
		P: AsRef<Path> + Debug,
	{
		let (offset, file_len) = self
			.metadata(path.as_ref())
			.map(|meta| (meta.offset, meta.unixfs.filesize.unwrap_or(u64::MAX)))
			.ok_or_else(|| Error::MissingMetadataOnFile(path.as_ref().to_path_buf()))?;

		// Move reader to the specified offset before take it.
		let curr_pos = self.reader.as_mut().map(|r| r.seek(SeekFrom::Start(offset))).transpose()?;
		debug_assert_eq!(curr_pos, Some(offset));

		let reader = self.reader.take().ok_or(Error::MissingReader)?;
		Ok(reader.take(file_len))
	}

	pub fn verify(&mut self, expected_cid: Cid) -> Result<bool, Error> {
		let actual_cid = self.calculate_cid()?;
		Ok(actual_cid == expected_cid)
	}

	pub fn calculate_cid(&mut self) -> Result<Cid, Error> {
		let Some(reader) = self.reader.as_mut() else {
			return Err(Error::MissingReader);
		};

		// 1. Check metadata.
		let actual_metadata = Self::load_metadata::<File>(self.codec, reader, None)?;
		let actual_root = FsEntry::make_root(actual_metadata);

		if self.root != actual_root {
			return Err(Error::MetadataNotMatched);
		}

		// 2. Check content.
		let self_root = self.root.clone();
		let self_root_dir = self_root.as_dir().ok_or(Error::MissingRootFolder)?;
		let file_cids = self_root_dir
			.iter()
			.map(|(path, entry)| -> Result<Cid, Error> {
				let file_entry = entry.as_file().ok_or_else(|| Error::MissingMetadataOnFile(path.clone()))?;

				let mut pb_links = Vec::new();
				let mut file_reader = self.take_reader(path.as_path())?;

				for block_size in file_entry.unixfs.blocksizes.iter() {
					let mut chunk_reader = file_reader.take(*block_size);
					let cid = cid_from_reader::<Error, _>(&mut chunk_reader)?;
					file_reader = chunk_reader.into_inner();

					pb_links.push(pb::link::new(cid, *block_size));
				}

				// Recover the original reader
				self.reader = Some(file_reader.into_inner());

				let (cid, _, _) = from_links(pb_links);
				Ok(cid)
			})
			.collect::<Result<Vec<_>, _>>();
		let file_cids = file_cids?;

		debug_assert_eq!(file_cids.len(), 1, "Only one file expected in root directory");
		file_cids.first().cloned().ok_or(Error::MissingRootFolder)
	}

	pub fn into_inner(self) -> R {
		self.reader.unwrap()
	}
}

#[derive(Debug, Constructor, Clone, PartialEq)]
pub struct MetadataFile {
	offset: u64,
	unixfs: Data,
}

#[derive(From, Debug, PartialEq, Clone)]
pub enum FsEntry {
	File(MetadataFile),
	Dir(Map<PathBuf, FsEntry>),
}

impl FsEntry {
	/// TODO: Only one file is supported in the root directory.
	pub fn make_root(file: MetadataFile) -> Self {
		let path = PathBuf::from(Path::new(SELF_PATH));
		Self::Dir(Map::from([(path, FsEntry::File(file))]))
	}

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

impl Default for FsEntry {
	fn default() -> Self {
		Self::Dir(Map::new())
	}
}

#[derive(Error, Debug)]
pub enum Error {
	#[error(transparent)]
	Io(#[from] std::io::Error),
	#[error(transparent)]
	PbNode(#[from] pb::DecodeError),
	#[cfg_attr(feature = "std", error(transparent))]
	#[cfg_attr(not(feature = "std"), error("Prost decode error: {0:?}"))]
	Prost(prost::DecodeError),
	#[error("Missing PbNode data field")]
	MissingPbNodeData,
	#[error("Unsupported CID Codec: {0}")]
	CIDCodec(#[from] CidCodecError),
	#[error("Reader is empty")]
	MissingReader,
	#[error("Root folder not found")]
	MissingRootFolder,
	#[error("Missing metadata on file: {0:?}")]
	MissingMetadataOnFile(PathBuf),
	#[error("Missing path: {0:?}")]
	MissingPath(PathBuf),
	#[error(transparent)]
	Multihash(#[from] libipld::multihash::Error),
	#[error("Extpected metadata doest not match with the actual metadata")]
	MetadataNotMatched,
}

impl From<prost::DecodeError> for Error {
	fn from(err: prost::DecodeError) -> Self {
		Error::Prost(err)
	}
}
