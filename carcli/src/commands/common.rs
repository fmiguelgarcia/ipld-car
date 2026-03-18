use unit_prefix::NumberPrefix;
use vfs::VfsFileType;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SizeFormat {
	Binary,
	Decimal,
	Bytes,
}

pub fn pick_icon(name: &str, file_type: VfsFileType) -> char {
	if file_type == VfsFileType::Directory {
		return '\u{f115}';
	}
	let ext = name.rsplit_once('.').map(|(_, e)| e).unwrap_or("").to_ascii_lowercase();
	match ext.as_str() {
		"txt" => '\u{f15c}',
		"md" | "markdown" => '\u{e73e}',
		"rs" => '\u{e7a8}',
		"toml" | "json" | "yaml" | "yml" => '\u{e60b}',
		"pdf" => '\u{f1c1}',
		"car" => '\u{f187}',
		_ => '\u{f15b}',
	}
}

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

fn format_prefix(n: NumberPrefix<f64>) -> String {
	match n {
		NumberPrefix::Standalone(n) => format!("{n:.0}B"),
		NumberPrefix::Prefixed(prefix, n) => format!("{n:.1}{prefix}B"),
	}
}
