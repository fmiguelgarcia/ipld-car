use vfs::VfsFileType;

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

pub fn fmt_size(bytes: u64) -> String {
	if bytes == 0 {
		return "-".to_string();
	}
	if bytes < 1_000 {
		return format!("{bytes}B");
	}
	if bytes < 1_000_000 {
		return fmt_decimal(bytes, 1_000, 'k');
	}
	if bytes < 1_000_000_000 {
		return fmt_decimal(bytes, 1_000_000, 'M');
	}
	fmt_decimal(bytes, 1_000_000_000, 'G')
}

fn fmt_decimal(bytes: u64, divisor: u64, suffix: char) -> String {
	let whole = bytes / divisor;
	let frac = (bytes % divisor) * 10 / divisor;
	if frac == 0 {
		format!("{whole}{suffix}")
	} else {
		format!("{whole}.{frac}{suffix}")
	}
}
