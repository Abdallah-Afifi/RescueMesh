//! Operator tool (workstream A): cluster status and targeted fault injection
//! (`docs/protocol-v2.md` Section 8). Commands are parsed and validated here;
//! sending them is added with the transport.

use clap::{Parser, Subcommand};
use proto::config::Config;
use std::path::PathBuf;
use std::process::ExitCode;

#[derive(Parser)]
#[command(about = "RescueMesh operator tool")]
struct Args {
    #[arg(long)]
    config: PathBuf,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Show each node's view, epoch and commit position.
    Status,
    /// Silence a node: a node id, "coordinator" or "rank1".
    Silence {
        target: String,
        #[arg(long, default_value_t = 10_000)]
        duration_ms: u64,
    },
}

fn main() -> ExitCode {
    let args = Args::parse();
    if let Err(e) = Config::load(&args.config) {
        eprintln!("{e}");
        return ExitCode::FAILURE;
    }
    match args.cmd {
        Cmd::Status => eprintln!("status: not implemented yet"),
        Cmd::Silence {
            target,
            duration_ms,
        } => {
            if duration_ms == 0 || duration_ms > 20_000 {
                eprintln!("duration must be 1..=20000 ms");
                return ExitCode::FAILURE;
            }
            eprintln!("silence {target} for {duration_ms} ms: not implemented yet");
        }
    }
    ExitCode::FAILURE
}
