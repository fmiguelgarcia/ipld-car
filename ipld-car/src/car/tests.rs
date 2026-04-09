use crate::{
	bounded_reader::traits::CloneAndRewind as _,
	car::ContentAddressableArchive,
	test_helpers::{block_ids_test_file, checksum, roots_test_file, test_fixtures_file},
};

use anyhow::Result;
use libipld::{multihash::Sha2_256, Cid};
use petgraph::visit::IntoNodeReferences;
use std::io::{BufReader, BufWriter, Seek, Write};
use tempfile::tempfile;
use test_case::test_case;

/// Loads test file `test_file_name` and check the expected CID roots, and blocks
#[test_case("dir-with-files.car")]
#[test_case("symlink.car")]
#[test_case("dir-with-percent-encoded-filename.car")]
fn load_and_check_cids(name: &str) -> Result<()> {
	let exp_roots = roots_test_file(name);
	let exp_block_ids = block_ids_test_file(name);
	let mut reader = test_fixtures_file(name);
	let car = ContentAddressableArchive::load(&mut reader)?;

	let roots = car.root_cids()?.iter().map(Cid::to_string).collect::<Vec<_>>();
	assert_eq!(exp_roots, roots);

	let block_ids = car.dag.node_references().map(|(_id, block)| block.cid.to_string()).collect::<Vec<_>>();
	assert_eq!(exp_block_ids, block_ids);

	Ok(())
}

/// Check that load and save using ContentAddressableArchive are the same.
#[test_case("symlink.car")]
#[test_case("dir-with-files.car")]
#[test_case("dir-with-percent-encoded-filename.car")]
fn load_and_save(car_path: &str) -> Result<()> {
	let mut car = ContentAddressableArchive::load(test_fixtures_file(car_path))?;

	let mut saved_car_file = BufWriter::new(tempfile()?);
	car.write(&mut saved_car_file)?;
	saved_car_file.rewind()?;

	// Check root CIDs
	let loaded_roots = car.root_cids()?;
	let saved_car_file = BufReader::new(saved_car_file.into_inner()?);
	let writen_car = ContentAddressableArchive::load(saved_car_file)?;
	let writen_roots = writen_car.root_cids()?;
	assert_eq!(loaded_roots, writen_roots);

	let mut writen_car_content = writen_car.content.clone_and_rewind();
	let writen_car_content_hash = checksum::<Sha2_256, _>(&mut writen_car_content);
	let car_path_hash = checksum::<Sha2_256, _>(&mut test_fixtures_file(car_path));
	if writen_car_content_hash != car_path_hash {
		let mut dbg_file = std::fs::File::create(format!("/tmp/dbg_{}", car_path).as_str())?;
		writen_car_content.rewind()?;
		std::io::copy(&mut writen_car_content, &mut dbg_file)?;
		dbg_file.flush()?;
	}
	assert_eq!(writen_car_content_hash, car_path_hash);

	Ok(())
}
