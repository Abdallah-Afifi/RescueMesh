//! `rm-loadgen`: the workload generator and per-machine frontend.
//!
//! ```text
//! rm-loadgen run --config run.toml --machine 1 --processes 4 --users 50 \
//!                --mode open --rate 800 --duration-secs 60 --warmup-secs 10
//! ```
//!
//! `run` starts the worker processes itself; `worker` is the hidden
//! subcommand it starts them with.

use clap::{Parser, Subcommand};
use loadgen::frontend::{self, FrontendArgs};
use loadgen::params::LoadParams;
use loadgen::worker::{self, WorkerArgs};
use proto::config::Config;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

#[derive(Parser)]
#[command(about = "RescueMesh workload generator")]
struct Args {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Run the load on this client machine: start the workers, wait, summarize.
    Run {
        #[arg(long)]
        config: PathBuf,
        /// Number of this client machine (1, 2 or 3).
        #[arg(long)]
        machine: u16,
        /// Wall-clock start (Unix milliseconds) shared by all client machines,
        /// so their warm-up and measured windows coincide. Default: in 3 s.
        #[arg(long)]
        start_at_ms: Option<u64>,
        #[command(flatten)]
        params: LoadParams,
    },
    /// One client process. Started by `run`; not meant to be run by hand.
    #[command(hide = true)]
    Worker {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        machine: u16,
        #[arg(long)]
        process: u16,
        #[arg(long)]
        start_at_ms: u64,
        #[command(flatten)]
        params: LoadParams,
    },
}

fn load(path: &Path, params: &LoadParams) -> Result<Config, String> {
    let cfg = Config::load(path).map_err(|e| e.to_string())?;
    params.validate()?;
    Ok(cfg)
}

#[tokio::main]
async fn main() -> ExitCode {
    let result = match Args::parse().cmd {
        Cmd::Run {
            config,
            machine,
            start_at_ms,
            params,
        } => match load(&config, &params) {
            Ok(cfg) => frontend::run(FrontendArgs {
                config_path: config,
                cfg,
                machine,
                params,
                start_at_ms,
            })
            .await
            .map(|_| ())
            .map_err(|e| e.to_string()),
            Err(e) => Err(e),
        },
        Cmd::Worker {
            config,
            machine,
            process,
            start_at_ms,
            params,
        } => match load(&config, &params) {
            Ok(cfg) => worker::run(WorkerArgs {
                cfg,
                machine,
                process,
                params,
                start_at_ms,
            })
            .await
            .map(|_| ())
            .map_err(|e| e.to_string()),
            Err(e) => Err(e),
        },
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("{e}");
            ExitCode::FAILURE
        }
    }
}
