use libipld::multihash::Hasher;
use std::{
	env,
	ffi::OsStr,
	fs::File,
	io::{copy, BufRead, BufReader, Read, Result as IoResult, Write},
	path::Path,
};

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
pub fn test_file<P: AsRef<Path>>(file_name: P) -> BufReader<File> {
	base_test_file(file_name).expect("Test file exists .qed")
}

/// It loads the associated roots file (extension `.roots`) of the given test file.
pub fn roots_test_file<P: AsRef<Path>>(name: P) -> Vec<String> {
	test_file_with_ext(name, "roots")
}

/// It loads the associated block IDs file (extension `.blockIds`) of the given test file.
pub fn block_ids_test_file<P: AsRef<Path>>(name: P) -> Vec<String> {
	test_file_with_ext(name, "blockIds")
}

fn test_file_with_ext<P: AsRef<Path>, S: AsRef<OsStr>>(file_name: P, extension: S) -> Vec<String> {
	let mut roots_file_name = file_name.as_ref().to_path_buf();
	roots_file_name.set_extension(extension);

	if let Ok(reader) = base_test_file(roots_file_name) {
		reader.lines().map_while(Result::ok).collect()
	} else {
		vec![]
	}
}

fn base_test_file<P: AsRef<Path>>(file_name: P) -> IoResult<BufReader<File>> {
	let manifest_dir = env::var("CARGO_MANIFEST_DIR").expect("Env variable CARGO_MANIFEST_DIR is missing");
	let path = Path::new(&manifest_dir).join("..").join("resources").join("tests").join(file_name);
	Ok(BufReader::new(File::open(path)?))
}
