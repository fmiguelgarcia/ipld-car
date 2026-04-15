use ipld_car::{Config, ContentAddressableArchive};

use anyhow::{anyhow, Result};
use clap::Args;
use std::{
	fs::File,
	io::{BufReader, BufWriter},
	path::{Path, PathBuf},
};

/// Arguments for the `write` subcommand.
#[derive(Args)]
pub struct SubCmdWrite {
	/// Output CAR file path
	#[arg(short, long)]
	output: PathBuf,
	/// Files to add (format: <dest-path>=<src-file>)
	#[arg(short, long = "add", value_name = "DEST=SRC")]
	entries: Vec<String>,

	#[command(flatten)]
	config: Config,
}

impl SubCmdWrite {
	pub fn run(&self) -> Result<()> {
		let mut car = ContentAddressableArchive::directory(self.config)?;

		for entry in &self.entries {
			let (dest, src) =
				entry.split_once('=').ok_or_else(|| anyhow!("Invalid entry `{entry}`, expected DEST=SRC"))?;

			// Create parent directories if needed
			let dest_path = Path::new(dest);
			if let Some(parent) = dest_path.parent() {
				for ancestor in parent.ancestors().collect::<Vec<_>>().into_iter().rev() {
					if !ancestor.as_os_str().is_empty() && !car.exists(ancestor) {
						car.create_dir(ancestor)?;
					}
				}
			}

			let src_file = BufReader::new(File::open(src)?);
			car.add_file(dest_path, src_file)?;
		}

		let mut out_file = BufWriter::new(File::create(self.output.as_path())?);
		let bytes = car.write(&mut out_file)?;
		println!("Written {bytes} bytes to {}", self.output.display());

		Ok(())
	}
}
