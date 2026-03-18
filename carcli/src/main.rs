use anyhow::Result;
use clap::Parser;

mod commands;
use commands::Commands;

#[derive(Parser)]
#[command(name = "ufs", about = "Interact with IPFS CAR files")]
struct Cli {
	#[command(subcommand)]
	command: Commands,
}

fn main() -> Result<()> {
	let cli = Cli::parse();
	cli.command.run()
}
