use ipld_car::{CarFs, Config, ContentAddressableArchive};

use anyhow::{anyhow, Result};
use clap::Args;
use std::{
	fs::File,
	io::BufWriter,
	path::{Path, PathBuf},
};
use vfs::FileSystem;

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
		let fs = CarFs::from(ContentAddressableArchive::new(self.config)?);

		let source =
			self.source.canonicalize().map_err(|e| anyhow!("Cannot access `{}`: {e}", self.source.display()))?;
		let parent = source.parent().unwrap_or(Path::new(""));

		add_path(&fs, &source, parent)?;

		let mut car = fs.into_inner().ok_or_else(|| anyhow!("CAR is still referenced"))?;
		let out_file = File::create(&self.output)?;
		let mut writer = BufWriter::new(out_file);
		let bytes = car.write(&mut writer)?;
		println!("Written {} bytes to {}", bytes, self.output.display());

		Ok(())
	}
}

/// Recursively adds `path` into `fs`. `root` is the ancestor stripped to build dest paths.
fn add_path(fs: &CarFs<File>, path: &Path, root: &Path) -> Result<()> {
	let dest = dest_path(path, root)?;

	if path.is_dir() {
		if !dest.is_empty() {
			fs.create_dir(&dest).map_err(|e| anyhow!("Cannot create dir `{dest}`: {e}"))?;
		}
		for entry in std::fs::read_dir(path).map_err(|e| anyhow!("Cannot read dir `{}`: {e}", path.display()))? {
			let entry = entry?;
			add_path(fs, &entry.path(), root)?;
		}
	} else {
		// Ensure parent directories exist
		let dest_path = Path::new(&dest);
		if let Some(parent_dest) = dest_path.parent() {
			for ancestor in parent_dest.ancestors().collect::<Vec<_>>().into_iter().rev() {
				let s = ancestor.to_str().unwrap_or("");
				if !s.is_empty() && s != "/" && !fs.exists(s)? {
					fs.create_dir(s)?;
				}
			}
		}

		let mut src_file = File::open(path).map_err(|e| anyhow!("Cannot open `{}`: {e}", path.display()))?;
		let mut writer = fs.create_file(&dest)?;
		std::io::copy(&mut src_file, &mut *writer)?;
		drop(writer);
	}

	Ok(())
}

/// Converts an absolute `path` to a CAR-relative dest string by stripping `root`.
fn dest_path(path: &Path, root: &Path) -> Result<String> {
	let rel = path
		.strip_prefix(root)
		.map_err(|_| anyhow!("Path `{}` is not under root `{}`", path.display(), root.display()))?;
	let s = rel.to_str().ok_or_else(|| anyhow!("Non-UTF-8 path: {}", rel.display()))?;
	if s.is_empty() {
		Ok(s.to_string())
	} else {
		Ok(format!("/{s}"))
	}
}
