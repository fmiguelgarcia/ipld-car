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

/// It loads the file `<project>/resources/tests/<name>`
pub fn test_file<P: AsRef<Path>>(name: P) -> BufReader<File> {
	base_test_file(name, "")
}

/// It loads the file `<project>/resources/tests/fixtures/<name>`
pub fn test_fixtures_file<P: AsRef<Path>>(name: P) -> BufReader<File> {
	base_test_file(name, "fixtures/")
}

/// It loads the associated roots file (extension `.roots`) of the given test file.
pub fn roots_test_file<P: AsRef<Path>>(name: P) -> Vec<String> {
	let mut root_name = name.as_ref().to_path_buf();
	root_name.set_extension("roots");

	base_test_file(root_name, "exp/roots/").lines().collect::<Result<Vec<_>, _>>().expect(INVALID_LINE)
}

/// It loads the associated block IDs file (extension `.blockIds`) of the given test file.
pub fn block_ids_test_file<P: AsRef<Path>>(name: P) -> Vec<String> {
	let mut block_id_name = name.as_ref().to_path_buf();
	block_id_name.set_extension("blockIds");

	base_test_file(block_id_name, "exp/block_ids/")
		.lines()
		.collect::<Result<Vec<_>, _>>()
		.expect(INVALID_LINE)
}

/// Get the base path for resources used in test.
///
/// NOTE: On previous `cargo` versions (or nightly), `CARGO_MANIFEST_DIR` points to
/// workspace's folder instead of the individual crate's folder.
pub fn base_test_path() -> PathBuf {
	let cargo_manifest_dir = env::var("CARGO_MANIFEST_DIR").expect(MISSING_CARGO_MANIFEST);

	let on_crate_dir = Path::new(&cargo_manifest_dir).join("../resources/tests");
	if on_crate_dir.exists() {
		return on_crate_dir;
	}

	let on_workspace_dir = Path::new(&cargo_manifest_dir).join("resources/tests");
	on_workspace_dir
}

pub fn test_fixtures_path() -> PathBuf {
	base_test_path().join("fixtures")
}

pub fn base_test_file<P: AsRef<Path>>(name: P, relative_path: &str) -> BufReader<File> {
	let path = base_test_path().join(relative_path).join(name);
	let file = File::open(path).expect(FILE_OPEN_FAIL);
	BufReader::new(file)
}
