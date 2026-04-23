use ipld_car::{traits::AsFileSystem as _, ContentAddressableArchive};

use anyhow::Result;
use clap::Args;
use std::{
	fs::File,
	io::{copy, BufReader},
	path::PathBuf,
};

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
		let file = BufReader::new(File::open(&self.file)?);
		let car = ContentAddressableArchive::load(file)?;

		let mut reader = car.open_file(&self.path)?;
		let mut stdout = std::io::stdout().lock();
		copy(&mut reader, &mut stdout)?;

		Ok(())
	}
}
