use crate::commands::common::{fmt_size, pick_icon, SizeFormat};
use ipld_car::{car::fs::CarFs, ContentAddressableArchive};

use anyhow::{anyhow, Result};
use clap::Args;
use std::{fs::File, path::PathBuf};
use term_grid::{Direction, Filling, Grid, GridOptions};
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
			let mut rows: Vec<(&'static str, String, String)> = Vec::new();
			collect_tree(&fs, &self.path, "", size_format, &mut rows)?;
			print_table(build_cells(rows));
		} else {
			let entries: Vec<String> = fs.read_dir(&self.path)?.collect();
			let rows: Vec<(&'static str, String, String)> = entries
				.iter()
				.map(|name| {
					let (file_type, size_str, icon, suffix) = entry_info(&fs, &self.path, name, size_format)?;
					Ok((perms(file_type, suffix), size_str, format!("{icon} {name}{suffix}")))
				})
				.collect::<Result<_>>()?;
			print_table(build_cells(rows));
		}

		Ok(())
	}
}

/// Builds the flat cell list (header row + one row per entry) for the table grid.
fn build_cells(rows: Vec<(&'static str, String, String)>) -> Vec<String> {
	let mut cells: Vec<String> = vec![
		"Permissions".to_string(),
		"Size".to_string(),
		"User".to_string(),
		"Group".to_string(),
		"Date Modified".to_string(),
		"Name".to_string(),
	];
	for (p, size, name) in rows {
		cells.extend([p.to_string(), size, "-".to_string(), "-".to_string(), "-".to_string(), name]);
	}
	cells
}

/// Renders a 6-column table grid from `cells` built by [`build_cells`].
fn print_table(cells: Vec<String>) {
	const NCOLS: usize = 6;
	const SEP: usize = 2;

	// Compute the max display width per column (cell index % NCOLS).
	// Setting width = sum(col_max) + SEP*(NCOLS-1) + 1 guarantees the grid fits
	// exactly NCOLS columns but not NCOLS+1, preserving the table structure.
	let mut col_max = [0usize; NCOLS];
	for (i, cell) in cells.iter().enumerate() {
		col_max[i % NCOLS] = col_max[i % NCOLS].max(cell.len());
	}
	let table_width = col_max.iter().sum::<usize>() + SEP * (NCOLS - 1) + 1;

	let grid = Grid::new(
		cells,
		GridOptions { direction: Direction::LeftToRight, filling: Filling::Spaces(SEP), width: table_width },
	);
	print!("{grid}");
}

fn perms(file_type: VfsFileType, suffix: &'static str) -> &'static str {
	match (file_type, suffix) {
		(_, "@") => "lrwxrwxrwx",
		(VfsFileType::Directory, _) => "drwxr-xr-x",
		_ => ".rw-r--r--",
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

fn collect_tree(
	fs: &CarFs<File>,
	path: &str,
	prefix: &str,
	size_format: SizeFormat,
	rows: &mut Vec<(&'static str, String, String)>,
) -> Result<()> {
	let entries: Vec<String> = fs.read_dir(path)?.collect();
	for (i, name) in entries.iter().enumerate() {
		let last = i == entries.len() - 1;
		let connector = if last { "└── " } else { "├── " };
		let (file_type, size_str, icon, suffix) = entry_info(fs, path, name, size_format)?;
		rows.push((perms(file_type, suffix), size_str, format!("{prefix}{connector}{icon} {name}{suffix}")));
		if file_type == VfsFileType::Directory {
			let child_path = if path == "/" { format!("/{name}") } else { format!("{path}/{name}") };
			let extension = if last { "    " } else { "│   " };
			collect_tree(fs, &child_path, &format!("{prefix}{extension}"), size_format, rows)?;
		}
	}
	Ok(())
}
