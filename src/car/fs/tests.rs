use crate::{
	car::{fs::CarFs, ContentAddressableArchive},
	test_helpers::test_file,
};

use anyhow::Result;
use test_case::test_case;
use vfs::FileSystem;

#[test_case("dir-with-files.car", "hello.txt", b"hello world\n".to_vec() )]
// #[test_case("dir-with-files.car", "multiblock.txt", vec![] )]
fn vfs_path_content(name: &str, path: &str, exp_content: Vec<u8>) -> Result<()> {
	let car = ContentAddressableArchive::load(test_file(name))?;
	let car_fs = CarFs::from(car);

	let mut content = Vec::new();
	let mut file = car_fs.open_file(path)?;
	file.read_to_end(&mut content)?;
	assert_eq!(content, exp_content);

	Ok(())
}

const EXP_DIR_421: [&str; 4] = ["ascii-copy.txt", "ascii.txt", "hello.txt", "multiblock.txt"];
fn exp_hamt_entries() -> Vec<String> {
	(1..=1000).map(|id| format!("{id}.txt")).collect()
}

/// Tests from [UnixFs Spec](https://specs.ipfs.tech/unixfs/)
#[test_case("dir-with-files.car", "/", EXP_DIR_421; "4.2.1 Simple Directory a" )]
#[test_case("dir-with-files.car", ".", EXP_DIR_421; "4.2.1 Simple Directory b" )]
#[test_case("dir-with-files.car", "", EXP_DIR_421; "4.2.1 Simple Directory c" )]
#[test_case("subdir-with-two-single-block-files.car", "/", ["subdir"]; "4.2.2-1 Nested Directories 1/2")]
#[test_case("subdir-with-two-single-block-files.car", "/subdir", ["ascii.txt", "hello.txt"]; "4.2.2-1 Nested Directories 2/2")]
#[test_case("dag-pb.car", "/", ["foo", "foo.txt"]; "4.2.2-2 Nested Directories 1/2" )]
#[test_case("dag-pb.car", "/foo", ["bar.txt"]; "4.2.2-2 Nested Directories 2/2" )]
#[test_case("fixtures.car", "/", ["api", "ipfs", "ipns", "ą"]; "4.2.3-1 Special characters in filenames 1/3" )]
#[test_case("fixtures.car", "/ą", ["ę"]; "4.2.3-1 Special characters in filenames 2/3" )]
#[test_case("fixtures.car", "/ą/ę", ["file-źł.txt"]; "4.2.3-1 Special characters in filenames 3/3" )]
#[test_case("dir-with-percent-encoded-filename.car", "/", ["Portugal%2C+España=Peninsula Ibérica.txt"]; "4.2.3-2 Special characters in filenames" )]
#[test_case("bafybeigcsevw74ssldzfwhiijzmg7a35lssfmjkuoj2t5qs5u5aztj47tq.dag-pb", "/", ["audio_only.m4a", "chat.txt", "playback.m3u", "zoom_0.mp4"]; "4.2.4 Directory with Missing blocks" )]
#[test_case("single-layer-hamt-with-multi-block-files.car", "/", exp_hamt_entries() => ignore["HAMT not yet supported"]; "4.2.5 HAMT Sharded Directory")]
#[test_case("symlink.car", "/", ["bar", "foo"]; "4.3.3 Symbolic links")]
#[test_case("subdir-with-mixed-block-files.car", "/", ["subdir"]; "4.3.4 Mixed Block Sizes 1/2")]
#[test_case("subdir-with-mixed-block-files.car", "/subdir", ["ascii.txt", "hello.txt", "multiblock.txt"]; "4.3.4 Mixed Block Sizes 2/2")]
#[test_case("dir-with-duplicate-files.car", "/", ["ascii-copy.txt","ascii.txt", "hello.txt", "multiblock.txt"]; "4.3.5 Deduplication")]
fn vfs_directory<I, S>(name: &str, dir_path: &str, exp_dir_entries: I) -> Result<()>
where
	I: IntoIterator<Item = S>,
	String: From<S>,
{
	let car = ContentAddressableArchive::load(test_file(name))?;
	let car_fs = CarFs::from(car);

	let dir_entries = car_fs.read_dir(dir_path)?.collect::<Vec<_>>();
	let exp_dir_entries = exp_dir_entries.into_iter().map(String::from).collect::<Vec<_>>();
	assert_eq!(exp_dir_entries, dir_entries);

	Ok(())
}

#[ignore = "Only for debug specific tests"]
#[test]
fn debug_vfs_directory() -> Result<()> {
	vfs_directory("fixtures.car", "/", ["ą"])
}

/*
fn empty_dag_pb_directory() {}
fn empty_dag_pb_file() {}
fn empty_raw_block() {}
*/
