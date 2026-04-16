use libipld::Cid;
use std::path::Path;

pub trait ContextLen {
	fn data_len(&self) -> u64;
	fn pb_data_len(&self) -> u64;
}

pub trait AsCIDGraph {
	/// Returns the root cids, which are cid with no parents
	fn root_cids(&self) -> impl Iterator<Item = &Cid>;

	/// Returns `true` if there is not roots
	fn is_root_empty(&self) -> bool {
		self.root_cids().next().is_none()
	}

	/// Returns all cids
	fn cids(&self) -> impl Iterator<Item = &Cid>;

	fn descendants_of_cid(&self, cid: &Cid) -> Vec<&Cid>;
}

pub trait AsFileSystem {
	type Error;
	type Metadata;
	type Reader;
	type BoundedReader;

	fn exists<P: AsRef<Path>>(&self, path: P) -> bool;
	fn metadata<P: AsRef<Path>>(&self, path: P) -> Result<Self::Metadata, Self::Error>;

	fn open_file<P: AsRef<Path>>(&self, path: P) -> Result<Self::BoundedReader, Self::Error>;
	fn add_file<P: AsRef<Path>>(&mut self, path: P, reader: Self::Reader) -> Result<(), Self::Error>;

	/// Creates a new empty directory at `parent_path/dir_name`.
	fn create_dir<P: AsRef<Path>>(&mut self, path: P) -> Result<(), Self::Error>;
	fn read_dir<P: AsRef<Path>>(&self, path: P) -> Result<impl Iterator<Item = &str>, Self::Error>;

	// pub fn path_to_cid<P: AsRef<Path>>(&self, path: P) -> Option<&Cid>;
}

pub trait AsFileSystemBuilder: AsFileSystem
where
	Self: Sized,
{
	fn with_dir<P: AsRef<Path>>(mut self, path: P) -> Result<Self, Self::Error> {
		self.create_dir(path)?;
		Ok(self)
	}

	fn with_file<P: AsRef<Path>>(mut self, path: P, reader: Self::Reader) -> Result<Self, Self::Error> {
		self.add_file(path, reader)?;
		Ok(self)
	}
}

impl<T> AsFileSystemBuilder for T where T: AsFileSystem {}
