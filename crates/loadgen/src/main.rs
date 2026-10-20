//! Workload generator and client-machine frontend (workstream E). Today this
//! only loads and checks the configuration.

use clap::Parser;
use proto::config::Config;
use std::path::PathBuf;
use std::process::ExitCode;

#[derive(Parser)]
#[command(about = "RescueMesh workload generator")]
struct Args {
    #[arg(long)]
    config: PathBuf,
    /// Number of this client machine (1, 2 or 3).
    #[arg(long)]
    machine: u16,
}

fn main() -> ExitCode {
    let args = Args::parse();
    match Config::load(&args.config) {
        Ok(cfg) => {
            println!("client machine {}: run {}", args.machine, cfg.run.run_id);
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("{e}");
            ExitCode::FAILURE
        }
    }
}
