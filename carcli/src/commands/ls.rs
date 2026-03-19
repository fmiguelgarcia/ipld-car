use crate::commands::common::{fmt_size, pick_icon, SizeFormat};
use ipld_car::{car::fs::CarFs, ContentAddressableArchive};

use anyhow::{anyhow, Result};
use clap::Args;
use std::{fs::File, path::PathBuf};
use term_grid::{Direction, Filling, Grid, GridOptions};
use users::{get_group_by_gid, get_user_by_uid};
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
	/// Resolves the active [`SizeFormat`] from the mutually-exclusive size flags.
	fn size_format(&self) -> SizeFormat {
		if self.bytes {
			SizeFormat::Bytes
		} else if self.binary {
			SizeFormat::Binary
		} else {
			SizeFormat::Decimal
		}
	}

	/// Runs the `ls` sub-command.
	pub fn run(&self) -> Result<()> {
		let (user, group) = get_car_file_owner(&self.file)?;
		let file = File::open(&self.file)?;
		let fs = CarFs::from(ContentAddressableArchive::load(file)?);

		if self.tree {
			self.print_tree(&fs, &user, &group)
		} else {
			self.print_list(&fs, &user, &group)
		}
	}

	/// Recursively collects all entries with tree connectors and renders a columnar table.
	fn print_tree(&self, fs: &CarFs<File>, user: &str, group: &str) -> Result<()> {
		let mut rows: Vec<(&'static str, String, String)> = Vec::new();
		collect_tree(fs, &self.path, "", self.size_format(), &mut rows)?;
		print_table(build_cells(rows, user, group));
		Ok(())
	}

	/// Lists direct children of `self.path` as a flat columnar table.
	fn print_list(&self, fs: &CarFs<File>, user: &str, group: &str) -> Result<()> {
		let size_format = self.size_format();
		let entries: Vec<String> = fs.read_dir(&self.path)?.collect();
		let rows: Vec<(&'static str, String, String)> = entries
			.iter()
			.map(|name| {
				let (file_type, size_str, icon, suffix) = entry_info(fs, &self.path, name, size_format)?;
				Ok((perms(file_type, suffix), size_str, format!("{icon} {name}{suffix}")))
			})
			.collect::<Result<_>>()?;
		print_table(build_cells(rows, user, group));
		Ok(())
	}
}

/// Returns the username and group name of the CAR file owner.
#[cfg(unix)]
fn get_car_file_owner(path: &PathBuf) -> Result<(String, String)> {
	use std::os::unix::fs::MetadataExt;
	let metadata = std::fs::metadata(path)?;
	let uid = metadata.uid();
	let gid = metadata.gid();
	let user = get_user_by_uid(uid)
		.map(|u| u.name().to_string_lossy().to_string())
		.unwrap_or_else(|| uid.to_string());
	let group = get_group_by_gid(gid)
		.map(|g| g.name().to_string_lossy().to_string())
		.unwrap_or_else(|| gid.to_string());
	Ok((user, group))
}

/// Returns the username and group name of the CAR file owner.
#[cfg(not(unix))]
fn get_car_file_owner(_path: &PathBuf) -> Result<(String, String)> {
	Ok(("unknown".to_string(), "unknown".to_string()))
}

/// Builds the flat cell list (header row + one row per entry) for the table grid.
fn build_cells(rows: Vec<(&'static str, String, String)>, user: &str, group: &str) -> Vec<String> {
	let mut cells: Vec<String> = vec![
		"Permissions".to_string(),
		"Size".to_string(),
		"User".to_string(),
		"Group".to_string(),
		"Date Modified".to_string(),
		"Name".to_string(),
	];
	for (p, size, name) in rows {
		cells.extend([p.to_string(), size, user.to_string(), group.to_string(), "-".to_string(), name]);
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

/// Returns a Unix-style permission string derived from the file type and suffix.
fn perms(file_type: VfsFileType, suffix: &'static str) -> &'static str {
	match (file_type, suffix) {
		(_, "@") => "lr-xr-xr-x",
		(VfsFileType::Directory, _) => "drwxr-xr-x",
		_ => ".r--r--r--",
	}
}

/// Resolves metadata for `name` under `parent`, returning file type, formatted size, icon, and suffix.
/// Symlinks, which VFS reports as `NotSupported`, are returned with a `"@"` suffix.
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

/// Recursively appends rows to `rows` for all entries under `path`, decorating each
/// name with tree connector art (`├──` / `└──`) and indentation carried in `prefix`.
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
