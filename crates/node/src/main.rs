//! `rm-node`: one RescueMesh cloud node (workstreams A–D).
//!
//! ```sh
//! rm-node --config config/local.toml --id 1
//! ```
//!
//! Starts the transport, heartbeats and failure detector (workstream B), the
//! core task with replication and selection (A, C), and workstream D's
//! incident planner on its worker pool (DL-065). `--dev-spin-us` swaps in
//! the placeholder planner of `node::dev` for runtime stress tests only.
//! Events go to `<run.log_dir>/<run.run_id>/cloud<id>-node.jsonl`.
//!
//! SIGTERM or SIGINT flushes the event log and exits with status 0, so the
//! experiment runner loses no events when it stops the node (DL-067). The
//! exit status is non-zero if the core thread panicked.

use clap::Parser;
use client::EventLog;
use incident::{IncidentPlanner, State};
use node::dev::DevPool;
use node::runtime::{self, NodeOpts, PlanSink, PoolFactory};
use node::{PlanPool, WorkerPool};
use proto::NodeId;
use proto::Planner;
use proto::config::Config;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

#[derive(Parser)]
#[command(about = "RescueMesh cloud node")]
struct Args {
    /// Shared run configuration.
    #[arg(long)]
    config: PathBuf,
    /// This node's id: 1, 2 or 3.
    #[arg(long)]
    id: u8,
    /// Run the placeholder planner instead of the incident service, burning
    /// this many microseconds of CPU per plan. For stress-testing the runtime
    /// only; never report numbers measured this way. 0 (default): the real
    /// planner.
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
    let (capacity, max_replans) = (cfg.planner.queue_capacity, cfg.timeouts.max_replans);
    let (pool, planner_name): (PoolFactory, &str) = if args.dev_spin_us > 0 {
        (
            DevPool::factory(threads, capacity, max_replans, args.dev_spin_us),
            "placeholder",
        )
    } else {
        // Workstream D's planner on workstream D's worker pool; finished plans
        // go back to the core task through the runtime's sink (DL-065).
        let planner: Arc<dyn Planner> = Arc::new(IncidentPlanner::from_config(&cfg));
        let factory: PoolFactory = Box::new(move |sink: PlanSink| {
            let on_planned = Arc::new(move |req, plan, us| sink.done(req, plan, us));
            Box::new(WorkerPool::with_parts(
                threads,
                capacity,
                max_replans,
                planner,
                on_planned,
            )) as Box<dyn PlanPool>
        });
        (factory, "incident")
    };
    let inc = incarnation();
    eprintln!(
        "{id}: run {} on {}:{} (heartbeats on udp {}), policy {:?}, incarnation {inc:08x}, \
         {threads} planning threads, {planner_name} planner",
        cfg.run.run_id, me.host, me.port, me.heartbeat_port, cfg.policy.ranker
    );
    let opts = NodeOpts::new(id, cfg, inc, Some(log.clone()));
    let node = match runtime::start(opts, Box::new(State::new()), pool) {
        Ok(node) => node,
        Err(e) => {
            eprintln!("{id}: cannot start: {e}");
            log.finish();
            return ExitCode::FAILURE;
        }
    };
    stop_on_signal(id, log.clone());
    let clean = node.join();
    log.finish();
    if clean {
        ExitCode::SUCCESS
    } else {
        eprintln!("{id}: the core thread panicked");
        ExitCode::FAILURE
    }
}

/// On SIGTERM or SIGINT: flush the event log and exit 0. Runs a one-thread
/// runtime on its own thread; the node itself uses no async code.
fn stop_on_signal(id: NodeId, log: Arc<EventLog>) {
    let spawned = std::thread::Builder::new()
        .name("signals".into())
        .spawn(move || {
            let Ok(rt) = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            else {
                return;
            };
            rt.block_on(async {
                #[cfg(unix)]
                {
                    use tokio::signal::unix::{SignalKind, signal};
                    let (Ok(mut term), Ok(mut int)) = (
                        signal(SignalKind::terminate()),
                        signal(SignalKind::interrupt()),
                    ) else {
                        return;
                    };
                    tokio::select! {
                        _ = term.recv() => {}
                        _ = int.recv() => {}
                    }
                }
                #[cfg(not(unix))]
                let _ = tokio::signal::ctrl_c().await;
            });
            eprintln!("{id}: stopping on signal");
            log.finish();
            std::process::exit(0);
        });
    if let Err(e) = spawned {
        eprintln!("{id}: cannot watch for signals: {e}");
    }
}
