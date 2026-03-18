use crate::commands::common::{fmt_size, pick_icon, SizeFormat};
use ipld_car::{car::fs::CarFs, ContentAddressableArchive};

use anyhow::{anyhow, Result};
use clap::Args;
use std::{fs::File, path::PathBuf};
use vfs::{error::VfsErrorKind, FileSystem, VfsFileType};

/// Arguments for the `ls` subcommand.
#[derive(Args)]
pub struct SubCmdLs {
	/// Path to the CAR file
	pub file: PathBuf,
	/// Directory path to list within the CAR (default: root)
	#[arg(default_value = "/")]
	pub path: String,
	/// Show recursive tree view
	#[arg(short = 'T', long = "tree")]
	pub tree: bool,
	/// List file sizes with binary prefixes (KiB, MiB, GiB)
	#[arg(short = 'b', long = "binary", conflicts_with = "bytes")]
	pub binary: bool,
	/// List file sizes in bytes, without any prefixes
	#[arg(short = 'B', long = "bytes")]
	pub bytes: bool,
}

impl SubCmdLs {
	fn size_format(&self) -> SizeFormat {
		if self.bytes {
			SizeFormat::Bytes
		} else if self.binary {
			SizeFormat::Binary
		} else {
			SizeFormat::Decimal
		}
	}

	pub fn run(&self) -> Result<()> {
		let file = File::open(&self.file)?;
		let car = ContentAddressableArchive::load(file)?;
		let fs: CarFs<File> = car.into();
		let size_format = self.size_format();

		if self.tree {
			let icon = pick_icon(&self.path, VfsFileType::Directory);
			println!("{icon} {}", self.path);
			print_tree(&fs, &self.path, "", size_format)?;
		} else {
			let entries: Vec<String> = fs.read_dir(&self.path)?.collect();
			println!(
				"{:<10}  {:>5}  {:<4}  {:<5}  {:<13}  Name",
				"Permissions", "Size", "User", "Group", "Date Modified"
			);
			for name in &entries {
				let (file_type, size_str, icon, suffix) = entry_info(&fs, &self.path, name, size_format)?;
				let perms = match (file_type, suffix) {
					(_, "@") => "lrwxrwxrwx",
					(VfsFileType::Directory, _) => "drwxr-xr-x",
					_ => ".rw-r--r--",
				};
				println!(
					"{:<10}  {:>5}  {:<4}  {:<5}  {:<13}  {} {}{}",
					perms, size_str, "-", "-", "-", icon, name, suffix
				);
			}
		}

		Ok(())
	}
}

fn entry_info(
	fs: &CarFs<File>,
	parent: &str,
	name: &str,
	size_format: SizeFormat,
) -> Result<(VfsFileType, String, char, &'static str)> {
	let child_path = if parent == "/" { format!("/{name}") } else { format!("{parent}/{name}") };
	match fs.metadata(&child_path) {
		Ok(meta) => {
			let size_str = match meta.file_type {
				VfsFileType::Directory => "-".to_string(),
				VfsFileType::File => fmt_size(meta.len, size_format),
			};
			let icon = pick_icon(name, meta.file_type);
			let suffix = if meta.file_type == VfsFileType::Directory { "/" } else { "" };
			Ok((meta.file_type, size_str, icon, suffix))
		},
		Err(e) if matches!(e.kind(), VfsErrorKind::NotSupported) =>
			Ok((VfsFileType::File, "-".to_string(), '\u{f0c1}', "@")),
		Err(e) => Err(anyhow!(e)),
	}
}

fn print_tree(fs: &CarFs<File>, path: &str, prefix: &str, size_format: SizeFormat) -> Result<()> {
	let entries: Vec<String> = fs.read_dir(path)?.collect();
	for (i, name) in entries.iter().enumerate() {
		let last = i == entries.len() - 1;
		let connector = if last { "└── " } else { "├── " };
		let (file_type, _, icon, suffix) = entry_info(fs, path, name, size_format)?;
		println!("{prefix}{connector}{icon} {name}{suffix}");
		if file_type == VfsFileType::Directory {
			let child_path = if path == "/" { format!("/{name}") } else { format!("{path}/{name}") };
			let extension = if last { "    " } else { "│   " };
			print_tree(fs, &child_path, &format!("{prefix}{extension}"), size_format)?;
		}
	}
	Ok(())
}
