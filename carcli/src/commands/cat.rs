use ipld_car::{car::fs::CarFs, ContentAddressableArchive};

use anyhow::{anyhow, Result};
use clap::Args;
use std::{fs::File, io::copy, path::PathBuf};
use vfs::FileSystem;

/// Arguments for the `cat` subcommand.
#[derive(Args)]
pub struct SubCmdCat {
	/// Path to the CAR file
	pub file: PathBuf,
	/// File path within the CAR to read
	pub path: String,
}

impl SubCmdCat {
	/// Reads the file at `self.path`, and streams it to stdout.
	pub fn run(&self) -> Result<()> {
		let file = File::open(&self.file)?;
		let fs = CarFs::from(ContentAddressableArchive::load(file)?);

		let mut reader = fs.open_file(&self.path).map_err(|e| anyhow!("Cannot open file: {e}"))?;
		let mut stdout = std::io::stdout().lock();
		copy(&mut reader, &mut stdout)?;

		Ok(())
	}
}
