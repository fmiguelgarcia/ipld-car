use ipld_car::{CarFs, Config, ContentAddressableArchive};

use anyhow::{anyhow, Result};
use clap::Args;
use std::{fs::File, io::BufWriter, path::PathBuf};
use vfs::FileSystem;

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
		let fs = CarFs::from(ContentAddressableArchive::new(self.config)?);

		for entry in &self.entries {
			let (dest, src) =
				entry.split_once('=').ok_or_else(|| anyhow!("Invalid entry `{entry}`, expected DEST=SRC"))?;

			// Create parent directories if needed
			let dest_path = std::path::Path::new(dest);
			if let Some(parent) = dest_path.parent() {
				for ancestor in parent.ancestors().collect::<Vec<_>>().into_iter().rev() {
					let s = ancestor.to_str().unwrap_or("");
					if !s.is_empty() && s != "/" && !fs.exists(s)? {
						fs.create_dir(s)?;
					}
				}
			}

			let mut src_file = File::open(src).map_err(|e| anyhow!("Cannot open `{src}`: {e}"))?;
			let mut writer = fs.create_file(dest)?;
			std::io::copy(&mut src_file, &mut *writer)?;
			drop(writer);
		}

		let mut car = fs.into_inner().ok_or_else(|| anyhow!("CAR is still referenced"))?;
		let out_file = File::create(self.output.as_path())?;
		let mut writer = BufWriter::new(out_file);
		let bytes = car.write(&mut writer)?;
		println!("Written {} bytes to {}", bytes, self.output.display());

		Ok(())
	}
}
