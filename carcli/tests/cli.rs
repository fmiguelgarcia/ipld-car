//! Integration tests for the `carcli` binary.
//!
//! Each test runs a subcommand against a fixture `.car` file and compares stdout
//! with the corresponding `.output` file under `resources/tests/exp/cli/`.  The `ls`
//! output normalises the User/Group columns — which reflect the OS owner of the
//! `.car` file — so that the expected files are portable across machines.
use handlebars::Handlebars;
use ipld_car::{car::fs::CarFs, test_helpers::test_fixtures_path, ContentAddressableArchive};
use std::{
	collections::BTreeMap,
	fs::{metadata, File, Metadata},
	path::Path,
	process::Command,
};
use tempfile::tempdir;
use vfs::{FileSystem, VfsFileType};

use test_case::test_case;

// ── helpers ──────────────────────────────────────────────────────────────────

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
/// Resolves OS-level owner (user, group) of `car_file` inside `resources/tests/fixtures/`,
/// mirroring the same logic used by `get_car_file_owner` in `ls.rs`.
#[cfg(unix)]
fn car_file_owner(car_file: &str) -> (String, String, String) {
	use std::os::unix::fs::MetadataExt;

	let path = test_fixtures_path().join(car_file);
	let meta = metadata(&path).expect("metadata");
	let user = users::get_user_by_uid(meta.uid())
		.map(|u| u.name().to_string_lossy().into_owned())
		.unwrap_or_else(|| meta.uid().to_string());
	let group = users::get_group_by_gid(meta.gid())
		.map(|g| g.name().to_string_lossy().into_owned())
		.unwrap_or_else(|| meta.gid().to_string());
	let modified = format_modified_time(&meta);

	(user, group, modified)
}

#[cfg(not(unix))]
fn car_file_owner(_car_file: &str) -> (String, String, String) {
	("unknown".into(), "unknown".into(), "-".to_string())
}

/// Runs `carcli <cmd> fixtures/<car_file>` with `current_dir = resources/tests/` and
/// returns stdout as a `String`.  Passing `fixtures/<file>` produces a stable relative
/// path in the `info` "File:" line regardless of the machine's workspace location.
fn run_cli(car_file: &str, cmd: &str, extra_args: &[&str]) -> String {
	let ws = test_fixtures_path();

	let mut args: Vec<&str> = cmd.split_whitespace().collect();
	args.push(car_file);
	extra_args.iter().for_each(|extra_arg| args.push(extra_arg));

	let out = Command::new(env!("CARGO_BIN_EXE_carcli"))
		.args(&args)
		.current_dir(&ws)
		.output()
		.expect("failed to run carcli");

	String::from_utf8(out.stdout).expect("stdout is not valid UTF-8")
}

/// Reads the expected output file at `resources/tests/exp/cli/<car_file_stem>.<ext>`.
fn output_test_file(car_file_name: &str, ext: &str) -> String {
	let (user, group, date_modified) = car_file_owner(car_file_name);
	let template_data = [("user", user.as_str()), ("group", group.as_str()), ("date_modified", date_modified.as_str())]
		.into_iter()
		.collect::<BTreeMap<&str, &str>>();

	// Path to output file
	let mut output_file_name = Path::new(car_file_name).to_path_buf();
	output_file_name.set_extension(ext);
	let output_file_path = test_fixtures_path().join("../exp/cli").join(&output_file_name);

	// Render it
	let mut handlebars = Handlebars::new();
	handlebars.register_template_file(car_file_name, output_file_path).expect("Valid template");
	handlebars.render(car_file_name, &template_data).expect("Valid render")
}

// ── info tests ────────────────────────────────────────────────────────────────

#[test_case("dag-pb.car", "info -B")]
#[test_case("dir-with-duplicate-files.car", "info -B")]
#[test_case("dir-with-files.car", "info -B")]
#[test_case("dir-with-percent-encoded-filename.car", "info -B")]
#[test_case("fixtures.car", "info -B")]
#[test_case("subdir-with-mixed-block-files.car", "info -B")]
#[test_case("subdir-with-two-single-block-files.car", "info -B")]
#[test_case("symlink.car", "info -B")]
fn info_test(car_file: &str, cmd: &str) {
	let expected_content = output_test_file(car_file, "info.output");
	let output = run_cli(car_file, cmd, &[]);
	assert_eq!(output, expected_content);
}

// ── ls tests ──────────────────────────────────────────────────────────────────

#[test_case("dir-with-files.car", "ls -B")]
#[test_case("dir-with-duplicate-files.car", "ls -B")]
#[test_case("dir-with-percent-encoded-filename.car", "ls -B")]
#[test_case("fixtures.car", "ls -T -B")]
#[test_case("subdir-with-mixed-block-files.car", "ls -T -B")]
#[test_case("subdir-with-two-single-block-files.car", "ls -T -B")]
#[test_case("symlink.car", "ls -B")]
fn ls_test(car_file: &str, cmd: &str) {
	let expected_content = output_test_file(car_file, "ls.output");
	let output = run_cli(car_file, cmd, &[]);
	assert_eq!(output, expected_content);
}

// ── cat tests ──────────────────────────────────────────────────────────────────

#[test_case("dir-with-files.car", "cat", "hello.txt", "hello world\n")]
#[test_case("dir-with-files.car", "cat", "ascii.txt", "hello application/vnd.ipld.car\n")]
#[test_case("dir-with-files.car", "cat", "multiblock.txt", MULTIBLOCK_CONTENT)]
#[test_case("symlink.car", "cat", "foo", "content\n")]
#[test_case("symlink.car", "cat", "bar", "content\n")]
#[test_case("dag-pb.car", "cat", "foo/bar.txt", "Hello, world!\n")]
#[test_case("fixtures.car", "cat", "ą/ę/file-źł.txt", "I am a txt file on path with utf8\n")]
fn cat_test(car_file: &str, cmd: &str, file: &str, exp_file_content: &str) {
	let output = run_cli(car_file, cmd, &[file]);
	assert_eq!(output, exp_file_content);
}

const MULTIBLOCK_CONTENT :&str = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Nunc non imperdiet nunc. Proin ac quam ut nibh eleifend aliquet. Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia curae; Sed ligula dolor, imperdiet sagittis arcu et, semper tincidunt urna. Donec et tempor augue, quis sollicitudin metus. Curabitur semper ullamcorper aliquet. Mauris hendrerit sodales lectus eget fermentum. Proin sollicitudin vestibulum commodo. Vivamus nec lectus eu augue aliquet dignissim nec condimentum justo. In hac habitasse platea dictumst. Mauris vel sem neque.

Vivamus finibus, enim at lacinia semper, arcu erat gravida lacus, sit amet gravida magna orci sit amet est. Sed non leo lacus. Nullam viverra ipsum a tincidunt dapibus. Nulla pulvinar ligula sit amet ante ultrices tempus. Proin purus urna, semper sed lobortis quis, gravida vitae ipsum. Aliquam mi urna, pulvinar eu bibendum quis, convallis ac dolor. In gravida justo sed risus ullamcorper, vitae luctus massa hendrerit. Pellentesque habitant amet.";

// ── create extract and re-pack  test ────────────────────────────────────────────────────

/// Recursively extracts all files and directories from `car_path` (VFS) into `dest` on disk.
fn extract_car_to_dir(fs: &CarFs<File>, car_path: &str, dest: &Path) -> anyhow::Result<()> {
	for name in fs.read_dir(car_path).map_err(|e| anyhow::anyhow!("{e}"))? {
		let child_car_path = if car_path == "/" { format!("/{name}") } else { format!("{car_path}/{name}") };
		let child_dest = dest.join(&name);
		let meta = fs.metadata(&child_car_path).map_err(|e| anyhow::anyhow!("{e}"))?;
		match meta.file_type {
			VfsFileType::Directory => {
				std::fs::create_dir_all(&child_dest)?;
				extract_car_to_dir(fs, &child_car_path, &child_dest)?;
			},
			VfsFileType::File => {
				let mut reader = fs.open_file(&child_car_path).map_err(|e| anyhow::anyhow!("{e}"))?;
				std::io::copy(&mut reader, &mut File::create(&child_dest)?)?;
			},
		}
	}
	Ok(())
}

/// Replaces ISO 8601 timestamps (`YYYY-MM-DDTHH:MM:SSZ`) with `<DATE>` so that
/// two `ls -T` outputs from different CAR files can be compared structurally.
fn remove_dates(s: &str) -> String {
	let re = regex::Regex::new(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z").unwrap();
	re.replace_all(s, "<DATE>").into()
}

/// Runs `carcli ls -T -B <car_path>` and returns stdout.
fn ls_tree_bytes(car_path: &Path) -> String {
	let out = Command::new(env!("CARGO_BIN_EXE_carcli"))
		.args(["ls", "-T", "-B", car_path.to_str().unwrap()])
		.output()
		.expect("failed to run carcli ls");
	String::from_utf8(out.stdout).expect("stdout is not valid UTF-8")
}

/// Extracts `car_file_name` to a temp directory, re-creates it
/// with `carcli create`, then asserts that `ls -T -B` on both CARs produces the same output
#[test_case("subdir-with-two-single-block-files.car")]
fn extract_and_repack(car_file_name: &str) -> anyhow::Result<()> {
	let tmp = tempdir()?;
	let car_file_path = test_fixtures_path().join(car_file_name);

	// 1. Extract the fixture CAR to a temporary directory.
	{
		let fs = CarFs::from(ContentAddressableArchive::load(File::open(&car_file_path)?)?);
		extract_car_to_dir(&fs, "/", tmp.path())?;
	}

	// 2. Re-create a new CAR from the extracted `subdir/`.
	let output_car = tmp.path().join("recreated.car");
	let source = tmp.path().join("subdir");
	let status = Command::new(env!("CARGO_BIN_EXE_carcli"))
		.args(["create", output_car.to_str().unwrap(), source.to_str().unwrap()])
		.status()?;
	assert!(status.success(), "carcli create failed");

	// 3. Compare `ls -T -B` output, ignoring Date Modified.
	let original_ls = ls_tree_bytes(&car_file_path);
	let created_ls = ls_tree_bytes(&output_car);
	assert_eq!(remove_dates(&original_ls), remove_dates(&created_ls));

	Ok(())
}
