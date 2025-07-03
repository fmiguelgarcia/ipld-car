use crate::unixfs::FileSystemReader;

use derive_more::Constructor;
use std::io::{Read, Result as IOResult, Take};

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
	fn read(&mut self, buf: &mut [u8]) -> IOResult<usize> {
		self.reader.read(buf)
	}
}

impl<R> From<FileReader<R>> for FileSystemReader<R> {
	fn from(file: FileReader<R>) -> Self {
		file.into_file_system_reader()
	}
}
