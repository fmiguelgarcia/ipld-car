use ipld_car::car::FileType;

use std::{ffi::os_str::OsStr, fs::Metadata, os::unix::ffi::OsStrExt, path::Path};
use unit_prefix::NumberPrefix;

/// Controls how file sizes are displayed in `ls` output.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SizeFormat {
	/// Binary prefixes (KiB, MiB, GiB); powers of 1 024.
	Binary,
	/// Decimal prefixes (kB, MB, GB); powers of 1 000.
	Decimal,
	/// Raw byte count with no prefix.
	Bytes,
}

/// Returns a Nerd Font icon character for `name` based on its extension and `file_type`.
pub fn pick_icon(name: &Path, file_type: FileType) -> char {
	match file_type {
		FileType::Dir => '\u{f115}',
		FileType::Symlink => '',
		FileType::File => {
			let default_ext = OsStr::new("");
			let ext = name.extension().unwrap_or(default_ext);
			match ext.as_bytes() {
				b"txt" => '\u{f15c}',
				b"md" | b"markdown" => '\u{e73e}',
				b"rs" => '\u{e7a8}',
				b"toml" | b"json" | b"yaml" | b"yml" => '\u{e60b}',
				b"pdf" => '\u{f1c1}',
				b"car" => '\u{f187}',
				_ => '\u{f15b}',
			}
		},
	}
}

/// Formats `bytes` according to `format`; returns `"-"` for zero.
pub fn fmt_size(bytes: u64, format: SizeFormat) -> String {
	if bytes == 0 {
		return "-".to_string();
	}
	match format {
		SizeFormat::Bytes => format!("{bytes}B"),
		SizeFormat::Binary => format_prefix(NumberPrefix::binary(bytes as f64)),
		SizeFormat::Decimal => format_prefix(NumberPrefix::decimal(bytes as f64)),
	}
}

/// Converts a `NumberPrefix` value into a human-readable size string.
fn format_prefix(n: NumberPrefix<f64>) -> String {
	match n {
		NumberPrefix::Standalone(n) => format!("{n:.0}B"),
		NumberPrefix::Prefixed(prefix, n) => format!("{n:.1}{prefix}B"),
	}
}

/// Formats the file's modification time for display.
pub(crate) fn format_modified_time(metadata: &Metadata) -> String {
	metadata
		.modified()
		.ok()
		.and_then(|t| {
			use time::{format_description::well_known::Rfc3339, OffsetDateTime};
			let duration = t.duration_since(std::time::UNIX_EPOCH).ok()?;
			let dt = OffsetDateTime::from_unix_timestamp(duration.as_secs().try_into().ok()?).ok()?;
			dt.format(&Rfc3339).ok()
		})
		.unwrap_or_else(|| "-".to_string())
}
