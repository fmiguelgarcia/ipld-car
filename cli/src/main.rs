use anyhow::{anyhow, Result};
use clap::{Parser, Subcommand};
use ipfs_unixfs::{car::fs::CarFs, config::Config, ContentAddressableArchive};
use std::{fs::File, io::BufWriter, path::PathBuf};
use vfs::FileSystem;

#[derive(Parser)]
#[command(name = "ufs", about = "Interact with IPFS CAR files")]
struct Cli {
	#[command(subcommand)]
	command: Commands,
}

#[derive(Subcommand)]
enum Commands {
	/// Display information about a CAR file (roots, block count)
	Info {
		/// Path to the CAR file
		file: PathBuf,
	},

	/// List the contents of a directory within a CAR file
	Ls {
		/// Path to the CAR file
		file: PathBuf,
		/// Directory path to list within the CAR (default: root)
		#[arg(default_value = "/")]
		path: String,
	},

	/// Create a new CAR file from local files
	Write {
		/// Output CAR file path
		#[arg(short, long)]
		output: PathBuf,
		/// Files to add (format: <dest-path>=<src-file>)
		#[arg(short, long = "add", value_name = "DEST=SRC")]
		entries: Vec<String>,
	},
}

fn main() -> Result<()> {
	let cli = Cli::parse();
	match cli.command {
		Commands::Info { file } => cmd_info(file),
		Commands::Ls { file, path } => cmd_ls(file, path),
		Commands::Write { output, entries } => cmd_write(output, entries),
	}
}

fn cmd_info(path: PathBuf) -> Result<()> {
	let file = File::open(&path)?;
	let car = ContentAddressableArchive::load(file)?;
	let roots = car.root_cids()?;

	println!("File:  {}", path.display());
	println!("Roots: {}", roots.len());
	for (i, cid) in roots.iter().enumerate() {
		println!("  [{i}] {cid}");
	}

	Ok(())
}

fn cmd_ls(car_path: PathBuf, path: String) -> Result<()> {
	let file = File::open(&car_path)?;
	let car = ContentAddressableArchive::load(file)?;
	let fs: CarFs<File> = car.into();

	let entries = fs.read_dir(&path)?;
	for name in entries {
		println!("{name}");
	}

	Ok(())
}

fn cmd_write(output: PathBuf, entries: Vec<String>) -> Result<()> {
	let car = ContentAddressableArchive::new(Config::default())?;
	let fs: CarFs<File> = car.into();

	for entry in &entries {
		let (dest, src) = entry
			.split_once('=')
			.ok_or_else(|| anyhow!("Invalid entry `{entry}`, expected DEST=SRC"))?;

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

	let car = fs.into_inner().ok_or_else(|| anyhow!("CAR is still referenced"))?;
	let out_file = File::create(&output)?;
	let mut writer = BufWriter::new(out_file);
	let bytes = car.write(&mut writer)?;
	println!("Written {} bytes to {}", bytes, output.display());

	Ok(())
}
