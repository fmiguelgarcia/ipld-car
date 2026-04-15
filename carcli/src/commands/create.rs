use ipld_car::{Config, ContentAddressableArchive};

use anyhow::{anyhow, Result};
use clap::Args;
use std::{
	fs::{self, File},
	io::{BufReader, BufWriter, Write},
	path::{Path, PathBuf},
};

/// Arguments for the `create` subcommand.
#[derive(Args)]
pub struct SubCmdCreate {
	/// Output CAR file path
	output: PathBuf,
	/// Source path to recursively add (directory or file)
	source: PathBuf,

	#[command(flatten)]
	config: Config,
}

impl SubCmdCreate {
	pub fn run(&self) -> Result<()> {
		let mut car = ContentAddressableArchive::directory(self.config)?;

		let source =
			self.source.canonicalize().map_err(|e| anyhow!("Cannot access `{}`: {e}", self.source.display()))?;
		let parent = source.parent().unwrap_or(Path::new(""));
		add_path(&mut car, &source, parent)?;

		let mut writer = BufWriter::new(File::create(&self.output)?);
		let bytes = car.write(&mut writer)?;
		writer.flush()?;
		drop(writer);
		println!("Written {} bytes to {}", bytes, self.output.display());

		Ok(())
	}
}

/// Recursively adds `path` into `fs`. `root` is the ancestor stripped to build dest paths.
fn add_path(car: &mut ContentAddressableArchive<BufReader<File>>, src_path: &Path, root: &Path) -> Result<()> {
	if src_path.is_dir() {
		add_directory(car, src_path, root)
	} else {
		add_file(car, src_path, root)
	}
}

fn add_file(car: &mut ContentAddressableArchive<BufReader<File>>, src_path: &Path, root: &Path) -> Result<()> {
	let car_path = dest_path(src_path, root)?;

	// Ensure parent directories exist
	if let Some(parent_car_path) = car_path.parent() {
		for ancestor in parent_car_path.ancestors().collect::<Vec<_>>().into_iter().rev() {
			if !ancestor.as_os_str().is_empty() && !car.exists(ancestor) {
				car.create_dir(ancestor)?;
			}
		}
	}

	let file = BufReader::new(File::open(src_path).map_err(|e| anyhow!("Cannot open `{src_path:?}`: {e}"))?);
	car.add_file(car_path, file).map_err(Into::into)
}

fn add_directory(car: &mut ContentAddressableArchive<BufReader<File>>, src_path: &Path, root: &Path) -> Result<()> {
	let car_path = dest_path(src_path, root)?;
	// Create target path if needed.
	if !car_path.as_path().as_os_str().is_empty() {
		car.create_dir(car_path.as_path()).map_err(|e| anyhow!("Cannot create dir `{car_path:?}`: {e}"))?;
	}

	// Recursively add entries of that directory.
	for entry in fs::read_dir(src_path).map_err(|e| anyhow!("Cannot read dir `{src_path:?}`: {e}"))? {
		let entry_path = entry?.path();
		add_path(car, &entry_path, root)?;
	}
	Ok(())
}

/// Converts an absolute `path` to a CAR-relative path by stripping `root`.
fn dest_path(path: &Path, root: &Path) -> Result<PathBuf> {
	path.strip_prefix(root)
		.map(Path::to_path_buf)
		.map_err(|_| anyhow!("Path `{:?}` is not under root `{:?}`", path, root))
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::commands::ls::SubCmdLs;
	use std::env;

	#[test]
	fn test_create() -> anyhow::Result<()> {
		let car_path = Path::new("/tmp/carcli_test_create.car");
		let cargo_manifest_dir = env::var("CARGO_MANIFEST_DIR").expect("Env variable CARGO_MANIFEST_DIR is missing");
		let source = Path::new(&cargo_manifest_dir).join("../resources/tests");

		let cmd_create = SubCmdCreate { output: car_path.into(), source, config: Config::default() };
		cmd_create.run()?;

		// Load
		let cmd_ls = SubCmdLs { file: car_path.into(), path: "/".into(), tree: true, binary: false, bytes: true };
		cmd_ls.run()?;

		// Clean tmp.
		fs::remove_file(car_path).map_err(Into::into)
	}
}
