use anyhow::Result;
use clap::Subcommand;

mod cat;
pub(crate) mod common;
use cat::SubCmdCat;
mod create;
use create::SubCmdCreate;
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

	/// Print the content of a file within a CAR file
	Cat(SubCmdCat),

	/// Create a new CAR file by recursively adding a directory or file
	Create(SubCmdCreate),
}

impl Commands {
	/// Dispatches to the selected subcommand.
	pub fn run(&self) -> Result<()> {
		match self {
			Commands::Info(cmd) => cmd.run(),
			Commands::Ls(cmd) => cmd.run(),
			Commands::Write(cmd) => cmd.run(),
			Commands::Cat(cmd) => cmd.run(),
			Commands::Create(cmd) => cmd.run(),
		}
	}
}
