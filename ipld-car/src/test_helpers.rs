use libipld::multihash::Hasher;
use std::{
	env,
	fs::File,
	io::{copy, BufRead, BufReader, Read, Write},
	path::{Path, PathBuf},
};

static MISSING_CARGO_MANIFEST: &str = "Env variable CARGO_MANIFEST_DIR is missing";
static FILE_OPEN_FAIL: &str = "File cannot be open";
static INVALID_LINE: &str = "Invalid line at test file";

/// Calculate the checksum of a reader
pub fn checksum<H, R>(reader: &mut R) -> Vec<u8>
where
	R: Read,
	H: Hasher + Write + Default,
{
	let mut hasher = H::default();
	let _bytes = copy(reader, &mut hasher).unwrap();
	hasher.finalize().to_vec()
}

/// It loads a file from `<project>/resources/tests/`
pub fn test_file<P: AsRef<Path>>(name: P) -> BufReader<File> {
	base_test_file(name, &[])
}

/// It loads the associated roots file (extension `.roots`) of the given test file.
pub fn roots_test_file<P: AsRef<Path>>(name: P) -> Vec<String> {
	base_test_file(name, &["exp", "roots"]).lines().collect::<Result<Vec<_>, _>>().expect(INVALID_LINE)
}

/// It loads the associated block IDs file (extension `.blockIds`) of the given test file.
pub fn block_ids_test_file<P: AsRef<Path>>(name: P) -> Vec<String> {
	base_test_file(name, &["exp", "block_ids"])
		.lines()
		.collect::<Result<Vec<_>, _>>()
		.expect(INVALID_LINE)
}

pub fn test_fixtures_path() -> PathBuf {
	let cargo_manifest_dir = env::var("CARGO_MANIFEST_DIR").expect(MISSING_CARGO_MANIFEST);
	Path::new(&cargo_manifest_dir).join("..").join("resources").join("tests").join("fixtures")
}

pub fn base_test_file<P: AsRef<Path>>(name: P, relative_paths: &[&str]) -> BufReader<File> {
	let mut path = test_fixtures_path();
	for sub in relative_paths {
		path = path.join(sub);
	}
	let file = File::open(path.join(name)).expect(FILE_OPEN_FAIL);
	BufReader::new(file)
}
