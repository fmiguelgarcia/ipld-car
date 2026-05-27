use crate::commands::common::{fmt_size, SizeFormat};
use ipld_car::{
	traits::{AsCIDGraph as _, ContextLen as _},
	ContentAddressableArchive,
};

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
		let roots = car.root_cids().collect::<Vec<_>>();
		let block_count = car.block_count();
		let non_roots = block_count.saturating_sub(roots.len());
		let size_format = SizeFormat::from(self);
		let car_data_len = car.data_len();
		let car_dag_pg_only_len = car.pb_data_len() - car_data_len;
		let total_car_size = car.car_overhead_byte_counter + car_dag_pg_only_len + car_data_len;

		println!("File:            {}", self.file.display());
		println!("Blocks:          {}", block_count);
		println!("  Roots:         {}", roots.len());
		println!("  Non-roots:     {}", non_roots);

		println!("Storage space:");
		println!("  CAR:           {}", fmt_size(car.car_overhead_byte_counter, size_format));
		println!("  DAG-PB:        {}", fmt_size(car_dag_pg_only_len, size_format));
		println!("  Data:          {}", fmt_size(car_data_len, size_format));
		println!("  Total:         {}", fmt_size(total_car_size, size_format));

		println!("Root CIDs:");
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
