use crate::commands::common::{fmt_size, SizeFormat};
use ipld_car::{ContentAddressableArchive, ContextLen};

use anyhow::Result;
use clap::Args;
use std::{
	fs::File,
	io::{Read, Seek},
	path::PathBuf,
};

/// Arguments for the `info` subcommand.
#[derive(Args)]
pub struct SubCmdInfo {
	/// Path to the CAR file
	pub file: PathBuf,
}

impl SubCmdInfo {
	pub fn run(&self) -> Result<()> {
		let file = File::open(&self.file)?;
		let car = ContentAddressableArchive::load(file)?;
		let roots = car.root_cids()?;
		let block_count = block_count(&car);
		let non_roots = block_count.saturating_sub(roots.len());
		let total_dag_pb_size = total_dag_pb_size(&car);
		let total_data_size = total_data_size(&car);

		println!("File:            {}", self.file.display());
		println!("Blocks:          {}", block_count);
		println!("  Roots:         {}", roots.len());
		println!("  Non-roots:     {}", non_roots);
		println!("Total DAG-PB:    {}", fmt_size(total_dag_pb_size, SizeFormat::Decimal));
		println!("Total Data:      {}", fmt_size(total_data_size, SizeFormat::Decimal));

		for (i, cid) in roots.iter().enumerate() {
			println!("  [{i}] {cid}");
		}

		Ok(())
	}
}

pub fn block_count<T: Read + Seek>(car: &ContentAddressableArchive<T>) -> usize {
	car.arena().iter().count()
}

pub fn total_dag_pb_size<T: Read + Seek>(car: &ContentAddressableArchive<T>) -> u64 {
	car.arena().iter().map(|block| block.dag_pb_len()).sum()
}

pub fn total_data_size<T: Read + Seek>(car: &ContentAddressableArchive<T>) -> u64 {
	car.arena().iter().map(|block| block.data_len()).sum()
}
