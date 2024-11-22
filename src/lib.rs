use std::{path::Path, io::Read};

pub mod unixfs;

pub struct FilesystemWriter {
    root : unixfs::Data;
}

impl FilesystemWriter {
	pub fn push_file<P: AsRef<Path>>(&mut self, 
		reader: impl Read,
		path: P,
	) -> Result<(), u8> {
		unimplemented!()
	}

}

pub struct FilesystemReader {
}
