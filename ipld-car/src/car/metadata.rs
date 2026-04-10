use derive_more::Constructor;
use std::path::{Path, PathBuf};

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

#[derive(Clone, Debug, Constructor)]
pub struct Metadata {
	pub file_type: FileType,
	pub len: u64,
	pub target_path: Option<PathBuf>,
}

impl Metadata {
	#[inline]
	pub const fn file(len: u64) -> Self {
		Self::new(FileType::File, len, None)
	}

	#[inline]
	pub const fn directory() -> Self {
		Self::new(FileType::Dir, 0, None)
	}

	#[inline]
	pub fn symlink<P: AsRef<Path>>(target: Metadata, target_path: P) -> Self {
		let target_path = Some(target_path.as_ref().to_path_buf());
		Self::new(FileType::Symlink, target.len, target_path)
	}
}

#[cfg(feature = "vfs")]
impl From<Metadata> for vfs::VfsMetadata {
	fn from(m: Metadata) -> Self {
		vfs::VfsMetadata { file_type: m.file_type.into(), len: m.len, created: None, modified: None, accessed: None }
	}
}
