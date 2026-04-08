use derive_more::Constructor;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FileType {
	File,
	Dir,
	Symlink,
}

/// # TODO:
/// - Symlink is mapped to `VfsFileType::File`.
#[cfg(feature = "vfs")]
impl From<FileType> for vfs::VfsFileType {
	fn from(f: FileType) -> Self {
		match f {
			FileType::File | FileType::Symlink => vfs::VfsFileType::File,
			FileType::Dir => vfs::VfsFileType::Directory,
		}
	}
}

#[derive(Clone, Copy, Debug, Constructor)]
pub struct Metadata {
	pub file_type: FileType,
	pub len: u64,
}

impl Metadata {
	#[inline]
	pub const fn file(len: u64) -> Self {
		Self::new(FileType::File, len)
	}

	#[inline]
	pub const fn directory() -> Self {
		Self::new(FileType::Dir, 0)
	}

	#[inline]
	pub const fn symlink(target: Metadata) -> Self {
		Self::new(FileType::Symlink, target.len)
	}
}

#[cfg(feature = "vfs")]
impl From<Metadata> for vfs::VfsMetadata {
	fn from(m: Metadata) -> Self {
		vfs::VfsMetadata { file_type: m.file_type.into(), len: m.len, created: None, modified: None, accessed: None }
	}
}
