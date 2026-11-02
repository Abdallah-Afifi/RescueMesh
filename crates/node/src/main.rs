//! `rm-node`: one RescueMesh cloud node (workstreams A–D).
//!
//! ```sh
//! rm-node --config config/local.toml --id 1
//! ```
//!
//! Starts the transport, heartbeats and failure detector (workstream B), the
//! core task with replication and selection (A, C), and the planning pool.
//! Until workstream D's incident service is merged the pool is the
//! placeholder in `node::dev`; `--dev-spin-us` makes each plan burn CPU.
//! Events go to `<run.log_dir>/<run.run_id>/cloud<id>-node.jsonl`.

use clap::Parser;
use client::EventLog;
use node::dev::{DevPool, DevState};
use node::runtime::{self, NodeOpts};
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
    /// Placeholder planner only: busy CPU time per plan, microseconds.
    #[arg(long, default_value_t = 0)]
    dev_spin_us: u64,
}

/// Random enough to differ between restarts of the same node (DL-021).
fn incarnation() -> u32 {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let mut x = nanos ^ (u64::from(std::process::id()) << 32);
    // SplitMix64 finalizer.
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    (x ^ (x >> 31)) as u32
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
    let Some(me) = cfg.node(id).cloned() else {
        eprintln!("node id {} is not in the cluster config", args.id);
        return ExitCode::FAILURE;
    };
    let log = match EventLog::create(
        &cfg.run.log_dir,
        &cfg.run.run_id,
        &format!("cloud{}", id.0),
        "node",
        Some(id),
    ) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("cannot create the event log: {e}");
            return ExitCode::FAILURE;
        }
    };
    let threads = cfg.planner.worker_threads.unwrap_or_else(|| {
        std::thread::available_parallelism()
            .map(|n| n.get().saturating_sub(1).max(1))
            .unwrap_or(1)
    });
    let pool = DevPool::factory(
        threads,
        cfg.planner.queue_capacity,
        cfg.timeouts.max_replans,
        args.dev_spin_us,
    );
    let inc = incarnation();
    eprintln!(
        "{id}: run {} on {}:{} (heartbeats on udp {}), policy {:?}, incarnation {inc:08x}, \
         {threads} planning threads",
        cfg.run.run_id, me.host, me.port, me.heartbeat_port, cfg.policy.ranker
    );
    let opts = NodeOpts::new(id, cfg, inc, Some(log.clone()));
    match runtime::start(opts, Box::new(DevState::new()), pool) {
        Ok(node) => node.join(),
        Err(e) => {
            eprintln!("{id}: cannot start: {e}");
            log.finish();
            return ExitCode::FAILURE;
        }
    }
    log.finish();
    ExitCode::SUCCESS
}
