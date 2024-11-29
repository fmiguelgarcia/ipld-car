use crate::{Config, ConfigBuilder, LeafPolicy, WellKnownChunkSize};

use libipld::multihash::Hasher;
use std::{
	env,
	fs::File,
	io::{self, BufReader, Read, Write},
	path::Path,
};
use tracing::trace;

pub(crate) fn raw_conf(chunk_size: WellKnownChunkSize) -> Config {
	ConfigBuilder::default()
		.leaf_policy(LeafPolicy::Raw)
		.chunk_policy(chunk_size.into())
		.build()
		.unwrap()
}

pub(crate) fn test_file<P: AsRef<Path>>(relative_path: P) -> BufReader<File> {
	let manifest_dir = env::var("CARGO_MANIFEST_DIR").expect("Env variable CARGO_MANIFEST_DIR is missing");
	let path = Path::new(&manifest_dir).join("resources").join("tests").join(relative_path);
	BufReader::new(File::open(path).expect("Test file exists .qed"))
}

/// Calculate the checksum of a reader
pub(crate) fn checksum<H, R>(mut reader: R) -> Vec<u8>
where
	R: Read,
	H: Hasher + Write + Default,
{
	let mut hasher = H::default();
	let bytes = io::copy(&mut reader, &mut hasher).unwrap();
	trace!(?bytes);
	hasher.finalize().to_vec()
}
