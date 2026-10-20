//! Cloud node (workstreams A–D). Today this only loads and checks the
//! configuration; the runtime, replication, selection and planning modules
//! are added by their owners against the traits in `proto`.

use clap::Parser;
use proto::NodeId;
use proto::config::Config;
use std::path::PathBuf;
use std::process::ExitCode;

#[derive(Parser)]
#[command(about = "RescueMesh cloud node")]
struct Args {
    /// Shared run configuration.
    #[arg(long)]
    config: PathBuf,
    /// This node's id: 1, 2 or 3.
    #[arg(long)]
    id: u8,
}

fn main() -> ExitCode {
    let args = Args::parse();
    let cfg = match Config::load(&args.config) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
    };
    let id = NodeId(args.id);
    let Some(me) = cfg.node(id) else {
        eprintln!("node id {} is not in the cluster config", args.id);
        return ExitCode::FAILURE;
    };
    println!(
        "{id}: run {} on {}:{} (heartbeats on udp {}), policy {:?}",
        cfg.run.run_id, me.host, me.port, me.heartbeat_port, cfg.policy.ranker
    );
    ExitCode::SUCCESS
}
