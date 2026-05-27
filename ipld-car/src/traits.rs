use libipld::Cid;
use std::{ops::Range, path::Path};

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

	/// Returns all descendants (direct and transitive) of the given `cid`
	fn descendants_of_cid(&self, cid: &Cid) -> Vec<&Cid>;

	/// Returns the direct descendants of the given `cid`
	fn direct_descendants_of_cid(&self, cid: &Cid) -> Vec<&Cid>;

	/// Returns the direct ascendant/parents of the given `cid`
	fn direct_parents_of_cid(&self, cid: &Cid) -> Vec<&Cid>;
}

pub trait AsBoundedContainer {
	fn bounds_of(&self, cid: &Cid) -> Option<Range<u64>>;

	fn bounds_len(&self, cid: &Cid) -> u64 {
		self.bounds_of(cid).map(|bounds| bounds.end.saturating_sub(bounds.start)).unwrap_or(0u64)
	}
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
