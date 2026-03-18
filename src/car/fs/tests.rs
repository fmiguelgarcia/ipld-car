use crate::{
	car::{block_content::BlockContent, fs::CarFs, ContentAddressableArchive},
	config::{Config, ConfigBuilder, LeafPolicy},
	dag_pb::DagPb,
	fail,
	test_helpers::test_file,
	BoundedReader,
};

use anyhow::{anyhow, Result};
use libipld::{multihash::Code, Cid};
use std::path::Path;
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

#[test_case("bafybeigcsevw74ssldzfwhiijzmg7a35lssfmjkuoj2t5qs5u5aztj47tq", ["audio_only.m4a", "chat.txt", "playback.m3u", "zoom_0.mp4"]; "4.2.4 Directory with Missing blocks" )]
fn vfs_dag_directory<I, S>(name: &str, exp_dir_entries: I) -> Result<()>
where
	I: IntoIterator<Item = S>,
	String: From<S>,
{
	let mut arena = Default::default();
	let content = BoundedReader::from_reader(test_file(format!("{name}.dag-pb")))?;
	let cid = name.parse::<Cid>()?;
	let id = DagPb::load(&mut arena, cid, content)?;
	let BlockContent::DagPb(ref dag_pb) = arena.get(id).unwrap().content else { fail!(anyhow!("It is not a DagPb")) };
	let DagPb::Dir(directory) = dag_pb else { fail!(anyhow!("It is not a directory")) };

	let dir_entries = directory.entries().keys().cloned().collect::<Vec<_>>();
	let exp_dir_entries = exp_dir_entries.into_iter().map(String::from).collect::<Vec<_>>();
	assert_eq!(dir_entries, exp_dir_entries);

	Ok(())
}

/// Tests for `create_dir` — verifies that new directories appear correctly in `dir_path` after
/// creation. Mirror of `vfs_directory` cases, extended with a list of directories to create.
#[test_case("dir-with-files.car", "/", ["/new_dir"], ["ascii-copy.txt", "ascii.txt", "hello.txt", "multiblock.txt", "new_dir"]; "new dir in root")]
#[test_case("dir-with-files.car", "/", ["/a", "/a/b", "/a/b/c"], ["a", "ascii-copy.txt", "ascii.txt", "hello.txt", "multiblock.txt"]; "new nested dirs in root 1")]
#[test_case("dir-with-files.car", "/a", ["/a", "/a/b", "/a/c"], ["b", "c"]; "new nested dirs in root 2")]
#[test_case("dir-with-files.car", "/a/b", ["/a", "/a/b", "/a/b/c"], ["c"]; "new nested dirs in root 3")]
fn vfs_create_dir<I1, I2, S2>(name: &str, dir_path: &str, new_dirs: I1, exp_dir_entries: I2) -> Result<()>
where
	I1: IntoIterator<Item = &'static str>,
	I2: IntoIterator<Item = S2>,
	String: From<S2>,
{
	let car = ContentAddressableArchive::load(test_file(name))?;
	let car_fs = CarFs::from(car);

	for new_dir in new_dirs {
		car_fs.create_dir(new_dir)?;
	}

	let dir_entries = car_fs.read_dir(dir_path)?.collect::<Vec<_>>();
	let exp_dir_entries = exp_dir_entries.into_iter().map(String::from).collect::<Vec<_>>();
	assert_eq!(exp_dir_entries, dir_entries);

	Ok(())
}

#[ignore = "Only to debug test case"]
#[test]
fn debug_vfs_create_dir() -> Result<()> {
	let name = "dir-with-files.car";
	let dir_path = "/";
	let new_dirs = ["/a", "/a/b", "/a/b/c"];
	let exp_dir_entries = ["a", "ascii-copy.txt", "ascii.txt", "hello.txt", "multiblock.txt"];

	vfs_create_dir(name, dir_path, new_dirs, exp_dir_entries)
}

#[test_case( Config::default(), "bafybeiczsscdsbs7ffqz55asqdf3smv6klcw3gofszvwlyarci47bgf354"; "Default")]
#[test_case( ConfigBuilder::default().hash_code(Code::Sha2_256).build().unwrap(), "bafybeiczsscdsbs7ffqz55asqdf3smv6klcw3gofszvwlyarci47bgf354"; "Sha2_256")]
#[test_case( ConfigBuilder::default().hash_code(Code::Sha2_256).build().unwrap(), "bafybeiczsscdsbs7ffqz55asqdf3smv6klcw3gofszvwlyarci47bgf354"; "4.3.1 Empty dag-pb directory")]
#[test_case( ConfigBuilder::default().hash_code(Code::Sha2_512).build().unwrap(), "bafybgqe5nnhmihs4pga4k4xhbnfzs24sxuhxjdirpqamhpyx5zhst52pkrnfk7vjdmr7g7lopssextjnqmkc4ygusjcultofxzma3wsiifrxa"; "Sha2_512")]
#[test_case( ConfigBuilder::default().hash_code(Code::Sha3_224).build().unwrap(), "bafybohaxxsftjqf3b4znpqfahxmrfmbdqdtjs7ocpmrrricrng3a"; "Sha3_224")]
#[test_case( ConfigBuilder::default().hash_code(Code::Sha3_256).build().unwrap(), "bafybmialzx2tmwtnybodeampc4xrreu3qx72tmt66oc3gdcghdrffyobsi"; "Sha3_256")]
#[test_case( ConfigBuilder::default().hash_code(Code::Sha3_384).build().unwrap(), "bafybkmekctv2lplwpwyko474ghd3hdrdrcx24o6exea7x7y5ttd3k3derf6nnpylfajnl5nadbreax2fzllq"; "Sha3_384")]
#[test_case( ConfigBuilder::default().hash_code(Code::Sha3_512).build().unwrap(), "bafybiqex3teqzaa62yijyaplxks6legc2avg5uqelggjmuqpeb5dvbhcgbjrqni2w3ixpo2on3332ev5ubcpgeylgz6svhax2cb2uyjrfi3ii"; "Sha3_512")]
#[test_case( ConfigBuilder::default().hash_code(Code::Keccak224).build().unwrap(), "bafybuhachdpoyie3b6raejh7flxsudayunva35l7bzjrszweahwa"; "Keccak224")]
#[test_case( ConfigBuilder::default().hash_code(Code::Keccak256).build().unwrap(), "bafybwih5fpbfsmx433ipwqncxquogw5zcpjzvyitw2qzsnvtfmyqwo7lom"; "Keccak256")]
#[test_case( ConfigBuilder::default().hash_code(Code::Keccak384).build().unwrap(), "bafybyma26pxps62mxdpsvrc5u5iyjyoknkfoipawbus2fd2af74w5ujjob5cki4aiq2trvbidimesgaf4ehq"; "Keccak384")]
#[test_case( ConfigBuilder::default().hash_code(Code::Keccak512).build().unwrap(), "bafyb2qdkg7of5v7duv2qybyar3v5iuqyzo2dwtjdbqe4b3pw4ia46zijou4xlcipbjd32dntmp3kugqokmlxxtyrpwr23xtvby76wr3hrb65i"; "Keccak512")]
#[test_case( ConfigBuilder::default().hash_code(Code::Blake2b256).build().unwrap(), "bafykbzacebugfutjir6qie7apo5shpry32ruwfi762uytd5g3u2gk7tpscndq"; "Blake2b256")]
#[test_case( ConfigBuilder::default().hash_code(Code::Blake2b512).build().unwrap(), "bafymbzacicomke2wmycm7onwxnovyryjj2dyyegzh3z7gdrpmpjf3r5p5s7262oohdo27lujdbks4cog54atajyrt5e6ipq73tqfi62bsxfw3kad"; "Blake2b512")]
#[test_case( ConfigBuilder::default().hash_code(Code::Blake2s128).build().unwrap(), "bafynbzaccblku3vfxgszwvsyfufz6kqmkhnq"; "Blake2s128")]
#[test_case( ConfigBuilder::default().hash_code(Code::Blake2s256).build().unwrap(), "bafyobzacebxzx4f4amxpvjywriut5uzqpsbo4mkdfmpe7kgixhqqlhtmu5rhw"; "Blake2s256")]
#[test_case( ConfigBuilder::default().hash_code(Code::Blake3_256).build().unwrap(), "bafyb4igcwu4aknzxeunla7d6swhtgjcpcdpi7hk5twl4ubnlczrqh3gmme"; "Blake3_256")]
fn empty_dag_pb_directory(config: Config, exp_cid: &str) -> Result<()> {
	let car = ContentAddressableArchive::new(config)?;

	let root_cid = car.root_cids()?.first().map(Cid::to_string).unwrap_or_default();
	assert_eq!(&root_cid, exp_cid);

	Ok(())
}

#[test_case( Config::default(), "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku" ; "4.3.1 Empty RAW block" )]
#[test_case( ConfigBuilder::default().leaf_policy(LeafPolicy::UnixFs).build().unwrap(), "bafybeif7ztnhq65lumvvtr4ekcwd2ifwgm3awq4zfr3srh462rwyinlb4y"; "4.3.1 Empty dag-pb file" )]
#[test_log::test]
fn empty_dag_pb_file(config: Config, exp_cid: &str) -> Result<()> {
	const FILE_NAME: &str = "empty.txt";
	let car = CarFs::from(ContentAddressableArchive::new(config)?);

	let mut file = car.create_file(FILE_NAME)?;
	file.flush()?;
	drop(file);

	let file_cid = car.lock()?.path_to_cid(Path::new(FILE_NAME))?.map(|cid| cid.to_string()).unwrap_or_default();
	assert_eq!(file_cid, exp_cid);

	Ok(())
}

// fn empty_raw_block() {}
