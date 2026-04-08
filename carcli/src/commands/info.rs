use crate::commands::common::{fmt_size, SizeFormat};
use ipld_car::{traits::ContextLen, ContentAddressableArchive};

use anyhow::Result;
use clap::Args;
use std::{fs::File, io::BufReader, path::PathBuf};

/// Arguments for the `info` subcommand.
#[derive(Args)]
pub struct SubCmdInfo {
	/// Path to the CAR file
	pub file: PathBuf,
	/// List file sizes with binary prefixes (KiB, MiB, GiB)
	#[arg(short = 'b', long = "binary", conflicts_with = "bytes")]
	pub binary: bool,
	/// List file sizes in bytes, without any prefixes
	#[arg(short = 'B', long = "bytes")]
	pub bytes: bool,
}

impl SubCmdInfo {
	/// Prints a summary: block counts, total sizes, and root CIDs.
	pub fn run(&self) -> Result<()> {
		let file = BufReader::new(File::open(&self.file)?);
		let car = ContentAddressableArchive::load(file)?;
		let roots = car.root_cids()?;
		let block_count = car.block_count();
		let non_roots = block_count.saturating_sub(roots.len());
		let total_dag_pb_size = car.pb_data_len();
		let total_data_size = car.data_len();
		let size_format = SizeFormat::from(self);

		println!("File:            {}", self.file.display());
		println!("Blocks:          {}", block_count);
		println!("  Roots:         {}", roots.len());
		println!("  Non-roots:     {}", non_roots);
		println!("Total DAG-PB:    {}", fmt_size(total_dag_pb_size, size_format));
		println!("Total Data:      {}", fmt_size(total_data_size, size_format));

		for (i, cid) in roots.iter().enumerate() {
			println!("  [{i}] {cid}");
		}

		Ok(())
	}
}

impl From<&SubCmdInfo> for SizeFormat {
	fn from(cmd: &SubCmdInfo) -> Self {
		if cmd.bytes {
			SizeFormat::Bytes
		} else if cmd.binary {
			SizeFormat::Binary
		} else {
			SizeFormat::Decimal
		}
	}
}
