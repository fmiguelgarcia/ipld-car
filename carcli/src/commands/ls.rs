use crate::commands::common::{fmt_size, format_modified_time, pick_icon, SizeFormat};
use ipld_car::{car::FileType, ContentAddressableArchive};

use anyhow::Result;
use clap::Args;
use std::{
	fs::File,
	io::BufReader,
	path::{Path, PathBuf},
};
use term_grid::{Direction, Filling, Grid, GridOptions};
use users::{get_group_by_gid, get_user_by_uid};

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
	/// Runs the `ls` sub-command.
	pub fn run(&self) -> Result<()> {
		let (user, group, modified) = get_car_file_info(&self.file)?;
		let file = BufReader::new(File::open(&self.file)?);
		let car = ContentAddressableArchive::load(file)?;

		if self.tree {
			self.print_tree(&car, &user, &group, &modified)
		} else {
			self.print_list(&car, &user, &group, &modified)
		}
	}

	/// Recursively collects all entries with tree connectors and renders a columnar table.
	fn print_tree<T>(&self, car: &ContentAddressableArchive<T>, user: &str, group: &str, modified: &str) -> Result<()> {
		let path = Path::new(&self.path);
		let mut rows: Vec<(&'static str, String, String)> = Vec::new();
		collect_tree(car, path, "", SizeFormat::from(self), &mut rows)?;
		print_table(build_cells(rows, user, group, modified));
		Ok(())
	}

	/// Lists direct children of `self.path` as a flat columnar table.
	fn print_list<T>(&self, car: &ContentAddressableArchive<T>, user: &str, group: &str, modified: &str) -> Result<()> {
		let self_path = Path::new(&self.path);
		let size_format = SizeFormat::from(self);
		let entries = car.read_dir(self_path)?.collect::<Vec<_>>();
		let rows: Vec<(&'static str, String, String)> = entries
			.iter()
			.map(|name| {
				let (file_type, size_str, icon, suffix) = entry_info(car, self_path, Path::new(name), size_format)?;
				Ok((perms(file_type), size_str, format!("{icon} {name}{suffix}")))
			})
			.collect::<Result<_>>()?;
		print_table(build_cells(rows, user, group, modified));
		Ok(())
	}
}

impl From<&SubCmdLs> for SizeFormat {
	fn from(cmd: &SubCmdLs) -> Self {
		if cmd.bytes {
			SizeFormat::Bytes
		} else if cmd.binary {
			SizeFormat::Binary
		} else {
			SizeFormat::Decimal
		}
	}
}

/// Returns the username, group name, and formatted modified date of the CAR file.
#[cfg(unix)]
fn get_car_file_info(path: &PathBuf) -> Result<(String, String, String)> {
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
	let modified = format_modified_time(&metadata);
	Ok((user, group, modified))
}

/// Returns the username, group name, and formatted modified date of the CAR file.
#[cfg(not(unix))]
fn get_car_file_info(_path: &PathBuf) -> Result<(String, String, String)> {
	Ok(("unknown".to_string(), "unknown".to_string(), "-".to_string()))
}

/// Builds the flat cell list (header row + one row per entry) for the table grid.
fn build_cells(rows: Vec<(&'static str, String, String)>, user: &str, group: &str, modified: &str) -> Vec<String> {
	let mut cells: Vec<String> = vec![
		"Permissions".to_string(),
		"Size".to_string(),
		"User".to_string(),
		"Group".to_string(),
		"Date Modified".to_string(),
		"Name".to_string(),
	];
	for (p, size, name) in rows {
		cells.extend([p.to_string(), size, user.to_string(), group.to_string(), modified.to_string(), name]);
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
fn perms(file_type: FileType) -> &'static str {
	match file_type {
		FileType::Symlink => "lr-xr-xr-x",
		FileType::Dir => "drwxr-xr-x",
		FileType::File => ".r--r--r--",
	}
}

/// Resolves metadata for `name` under `parent`, returning file type, formatted size, icon, and suffix.
/// Symlinks, which VFS reports as `NotSupported`, are returned with a `"@"` suffix.
fn entry_info<T>(
	car: &ContentAddressableArchive<T>,
	parent: &Path,
	name: &Path,
	size_format: SizeFormat,
) -> Result<(FileType, String, char, String)> {
	let child_path = parent.join(name);
	let meta = car.metadata(child_path.as_path())?;
	let (size_str, suffix) = match meta.file_type {
		FileType::Dir => ("-".to_string(), "/".to_string()),
		FileType::Symlink => {
			let target_link = meta.target_path.as_ref().map(|p| p.as_os_str().to_string_lossy()).unwrap_or_default();
			("-".to_string(), format!("@ -> {target_link}"))
		},
		FileType::File => (fmt_size(meta.len, size_format), "".to_string()),
	};
	let icon = pick_icon(name, meta.file_type);
	Ok((meta.file_type, size_str, icon, suffix))
}

/// Recursively appends rows to `rows` for all entries under `path`, decorating each
/// name with tree connector art (`├──` / `└──`) and indentation carried in `prefix`.
fn collect_tree<T>(
	car: &ContentAddressableArchive<T>,
	path: &Path,
	prefix: &str,
	size_format: SizeFormat,
	rows: &mut Vec<(&'static str, String, String)>,
) -> Result<()> {
	let entries = car.read_dir(path)?.collect::<Vec<_>>();
	for (i, name) in entries.iter().enumerate() {
		let last = i == entries.len() - 1;
		let connector = if last { "└── " } else { "├── " };
		let name_path = Path::new(name);
		let (file_type, size_str, icon, suffix) = entry_info(car, path, name_path, size_format)?;
		rows.push((perms(file_type), size_str, format!("{prefix}{connector}{icon} {name}{suffix}")));
		if file_type == FileType::Dir {
			let child_path = path.join(name_path);
			let extension = if last { "    " } else { "│   " };
			collect_tree(car, &child_path, &format!("{prefix}{extension}"), size_format, rows)?;
		}
	}
	Ok(())
}
