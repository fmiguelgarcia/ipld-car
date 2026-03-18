use anyhow::Result;
use clap::Subcommand;

pub(crate) mod common;
mod info;
use info::SubCmdInfo;
mod ls;
use ls::SubCmdLs;
mod write;
use write::SubCmdWrite;

#[derive(Subcommand)]
pub enum Commands {
	/// Display information about a CAR file (roots, block count)
	Info(SubCmdInfo),

	/// List the contents of a directory within a CAR file
	Ls(SubCmdLs),

	/// Create a new CAR file from local files
	Write(SubCmdWrite),
}

impl Commands {
	pub fn run(&self) -> Result<()> {
		match self {
			Commands::Info(cmd) => cmd.run(),
			Commands::Ls(cmd) => cmd.run(),
			Commands::Write(cmd) => cmd.run(),
		}
	}
}
