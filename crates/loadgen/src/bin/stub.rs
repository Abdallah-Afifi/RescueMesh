//! `rm-stub`: a stand-in for the three cloud nodes, for developing and
//! testing the load generator and the analysis scripts before the real nodes
//! exist. It listens on the three addresses of the configuration, serves
//! requests with a fixed service time, and can silence a node on the same
//! seeded schedule the real fault injection uses (`proto::hash`). It writes
//! node-side events so the analysis can be tried end to end.
//!
//! It is NOT a model of the real system. Never report its numbers.

use clap::Parser;
use client::EventLog;
use client::testkit::{StubCluster, StubOpts};
use proto::NodeId;
use proto::config::Config;
use proto::hash::{injection_duration_ms, injection_victim};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

#[derive(Parser)]
#[command(about = "Stand-in cloud for testing the client side")]
struct Args {
    #[arg(long)]
    config: PathBuf,
    /// Service time per request, milliseconds.
    #[arg(long, default_value_t = 5)]
    service_ms: u64,
    /// Requests each node serves at once.
    #[arg(long, default_value_t = 4)]
    workers: usize,
    #[arg(long, default_value_t = 256)]
    queue_cap: usize,
    /// How long after a node goes silent the others notice, milliseconds.
    #[arg(long, default_value_t = 500)]
    detect_ms: u64,
    /// Silence one node every this many seconds (0 = never). Victim and
    /// duration come from the run seed, as in the real fault injection.
    #[arg(long, default_value_t = 0)]
    silence_every_s: u64,
    #[arg(long, default_value_t = 10_000)]
    silence_max_ms: u64,
}

#[tokio::main]
async fn main() -> ExitCode {
    let args = Args::parse();
    let cfg = match Config::load(&args.config) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
    };
    let mut bind = Vec::new();
    let mut logs = Vec::new();
    for id in NodeId::ALL {
        let n = cfg.node(id).expect("validated");
        match format!("{}:{}", n.host, n.port).parse::<SocketAddr>() {
            Ok(a) => bind.push(a),
            Err(e) => {
                eprintln!(
                    "node {id}: {}:{} is not a socket address: {e}",
                    n.host, n.port
                );
                return ExitCode::FAILURE;
            }
        }
        match EventLog::create(
            &cfg.run.log_dir,
            &cfg.run.run_id,
            "cloud-stub",
            &id.to_string(),
            Some(id),
        ) {
            Ok(l) => logs.push(l),
            Err(e) => {
                eprintln!("cannot create log: {e}");
                return ExitCode::FAILURE;
            }
        }
    }
    let opts = StubOpts {
        service: Duration::from_millis(args.service_ms),
        workers: args.workers,
        queue_cap: args.queue_cap,
        detect_delay: Duration::from_millis(args.detect_ms),
        format: cfg.run.wire_format,
        ..StubOpts::default()
    };
    let logs: [Arc<EventLog>; 3] = logs.try_into().ok().expect("three logs");
    let cluster = match StubCluster::start(
        bind.try_into().expect("three addresses"),
        opts,
        Some(logs.clone()),
    )
    .await
    {
        Ok(c) => c,
        Err(e) => {
            eprintln!("cannot listen: {e}");
            return ExitCode::FAILURE;
        }
    };
    eprintln!("rm-stub listening on {:?}", cluster.addrs());

    let schedule = async {
        if args.silence_every_s == 0 {
            std::future::pending::<()>().await;
        }
        for k in 1u64..=u64::MAX {
            tokio::time::sleep(Duration::from_secs(args.silence_every_s)).await;
            let victim = injection_victim(cfg.run.seed, k);
            let ms = injection_duration_ms(cfg.run.seed, k, 1_000, args.silence_max_ms);
            eprintln!("silencing {victim} for {ms} ms");
            cluster.silence(victim, Duration::from_millis(ms));
            tokio::time::sleep(Duration::from_millis(ms + 1_000)).await;
        }
    };
    tokio::select! {
        () = schedule => {}
        _ = tokio::signal::ctrl_c() => {}
    }
    for l in &logs {
        l.finish();
    }
    ExitCode::SUCCESS
}
