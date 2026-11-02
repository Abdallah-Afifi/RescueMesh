//! Operator tool (workstream A; transport by workstream B): cluster status
//! and targeted fault injection (`docs/protocol-v2.md` Section 8).
//!
//! Talks to a node's normal TCP port with the `RMO1` preamble and one text
//! command per line (`node::net`):
//!
//! ```sh
//! rm-ctl --config config/local.toml status
//! rm-ctl --config config/local.toml silence coordinator --duration-ms 10000
//! rm-ctl --config config/local.toml silence 2 --duration-ms 5000
//! rm-ctl --config config/local.toml silence rank1
//! rm-ctl --config config/local.toml load 2 --threads 8 --duration-ms 30000
//! rm-ctl --config config/local.toml load 2 --threads 0
//! ```
//!
//! `silence` goes to the current coordinator, found with `status`, which
//! commits it as an `INJECT` entry or says why it declines; if it cannot be
//! reached, any other node accepts it and forwards it. `--via N` picks the
//! node. `load` goes to the named node itself: it burns
//! CPU inside that node's process for the load-imbalance demo (DL-067). The
//! exit status is 0 only if a node accepted.

use clap::{Parser, Subcommand};
use proto::NodeId;
use proto::config::Config;
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

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
    /// Show each node's role, view, epoch and commit position.
    Status,
    /// Silence a node: a node id, "coordinator" or "rank1".
    Silence {
        target: String,
        #[arg(long, default_value_t = 10_000)]
        duration_ms: u64,
        /// Send the command to this node instead of the first that answers.
        #[arg(long)]
        via: Option<u8>,
    },
    /// Raise one node's CPU load artificially (load-imbalance demo).
    Load {
        /// Node id: 1, 2 or 3.
        node: u8,
        /// Burner threads; 0 stops the load. Capped at the node's cores
        /// minus one. On a machine with many cores, use enough threads to
        /// move its CPU share noticeably (about 8 or more).
        #[arg(long)]
        threads: usize,
        /// Busy share of each burner thread, in thousandths (capped at 900).
        #[arg(long, default_value_t = 900)]
        duty: u16,
        /// Stop after this many milliseconds; 0 runs until stopped.
        #[arg(long, default_value_t = 0)]
        duration_ms: u64,
    },
}

/// Sends one command line to `node` and returns its one-line answer.
fn ask(cfg: &Config, node: NodeId, line: &str) -> Result<String, String> {
    let n = cfg
        .node(node)
        .ok_or_else(|| format!("{node} is not in the config"))?;
    let addr = (n.host.as_str(), n.port)
        .to_socket_addrs()
        .map_err(|e| e.to_string())?
        .next()
        .ok_or_else(|| format!("{}:{} did not resolve", n.host, n.port))?;
    let mut s =
        TcpStream::connect_timeout(&addr, Duration::from_secs(2)).map_err(|e| e.to_string())?;
    s.set_read_timeout(Some(Duration::from_secs(4)))
        .map_err(|e| e.to_string())?;
    s.write_all(b"RMO1").map_err(|e| e.to_string())?;
    writeln!(s, "{line}").map_err(|e| e.to_string())?;
    let mut out = String::new();
    BufReader::new(s)
        .read_line(&mut out)
        .map_err(|e| e.to_string())?;
    if out.is_empty() {
        return Err("connection closed without an answer".into());
    }
    Ok(out.trim().to_owned())
}

/// The coordinator named by the first node that answers `status`.
fn coordinator(cfg: &Config) -> Option<NodeId> {
    NodeId::ALL.iter().find_map(|&id| {
        let s = ask(cfg, id, "status").ok()?;
        let rest = &s[s.find("\"coordinator\":")? + "\"coordinator\":".len()..];
        let end = rest.find([',', '}'])?;
        let n: u8 = rest[..end].trim().parse().ok()?;
        Some(NodeId(n)).filter(|n| n.is_valid())
    })
}

fn valid_target(t: &str) -> bool {
    matches!(t, "coordinator" | "rank1" | "1" | "2" | "3")
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
    match args.cmd {
        Cmd::Status => {
            let mut any = false;
            for id in NodeId::ALL {
                match ask(&cfg, id, "status") {
                    Ok(s) => {
                        any = true;
                        println!("{s}");
                    }
                    Err(e) => println!(
                        "{{\"node\":{},\"error\":\"{}\"}}",
                        id.0,
                        e.replace('"', "'")
                    ),
                }
            }
            if any {
                ExitCode::SUCCESS
            } else {
                ExitCode::FAILURE
            }
        }
        Cmd::Silence {
            target,
            duration_ms,
            via,
        } => {
            if duration_ms == 0 || duration_ms > 20_000 {
                eprintln!("duration must be 1..=20000 ms");
                return ExitCode::FAILURE;
            }
            if !valid_target(&target) {
                eprintln!("target must be 1, 2, 3, coordinator or rank1");
                return ExitCode::FAILURE;
            }
            let line = format!("silence {target} {duration_ms}");
            // Ask the coordinator first: it decides at once and says why if
            // it declines, while a follower only forwards the request.
            let order: Vec<NodeId> = match via {
                Some(v) => vec![NodeId(v)],
                None => {
                    let mut order = NodeId::ALL.to_vec();
                    if let Some(c) = coordinator(&cfg) {
                        order.retain(|&n| n != c);
                        order.insert(0, c);
                    }
                    order
                }
            };
            for id in order {
                match ask(&cfg, id, &line) {
                    Ok(answer) if answer.starts_with("ok") => {
                        println!("{answer}");
                        return ExitCode::SUCCESS;
                    }
                    // The coordinator declined: asking another node would
                    // only forward the same request to it and hide the
                    // refusal behind that node's "ok".
                    Ok(answer)
                        if answer.contains("injection refused")
                            && !answer.contains("no coordinator") =>
                    {
                        eprintln!("{id}: {answer}");
                        return ExitCode::FAILURE;
                    }
                    Ok(answer) => eprintln!("{id}: {answer}"),
                    Err(e) => eprintln!("{id}: {e}"),
                }
            }
            ExitCode::FAILURE
        }
        Cmd::Load {
            node,
            threads,
            duty,
            duration_ms,
        } => {
            let id = NodeId(node);
            if !id.is_valid() {
                eprintln!("node must be 1, 2 or 3");
                return ExitCode::FAILURE;
            }
            if duty > 1000 {
                eprintln!("duty must be 0..=1000 permille");
                return ExitCode::FAILURE;
            }
            match ask(&cfg, id, &format!("load {threads} {duty} {duration_ms}")) {
                Ok(answer) if answer.starts_with("ok") => {
                    println!("{answer}");
                    ExitCode::SUCCESS
                }
                Ok(answer) => {
                    eprintln!("{id}: {answer}");
                    ExitCode::FAILURE
                }
                Err(e) => {
                    eprintln!("{id}: {e}");
                    ExitCode::FAILURE
                }
            }
        }
    }
}
