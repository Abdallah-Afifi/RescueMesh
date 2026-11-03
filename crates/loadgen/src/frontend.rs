//! The per-machine frontend: starts K worker processes, samples this
//! machine's CPU and network once a second, and merges the workers'
//! summaries when they finish. Ctrl-C stops the run: the workers see it too
//! (it reaches the whole foreground process group), stop issuing requests,
//! drain and write their logs; a second Ctrl-C kills them.

use crate::params::{LoadParams, Mode};
use crate::sampler::{MachineSampler, log_clock_offset, log_machine_info};
use crate::summary::{Counts, LatencyMs, MachineSummary, WorkerSummary, merge_hists, render};
use client::{EventLog, now_ms};
use proto::config::Config;
use proto::event::EventKind;
use std::io;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::process::{Child, Command};
use tokio::time;

pub struct FrontendArgs {
    pub config_path: PathBuf,
    pub cfg: Config,
    pub machine: u16,
    pub params: LoadParams,
    /// Wall-clock start shared by all client machines; `None` starts in 3 s.
    pub start_at_ms: Option<u64>,
}

pub async fn run(args: FrontendArgs) -> io::Result<MachineSummary> {
    let FrontendArgs {
        config_path,
        cfg,
        machine,
        params,
        start_at_ms,
    } = args;
    let machine_name = format!("client{machine}");
    let start_at = start_at_ms.unwrap_or_else(|| now_ms() + 3_000);
    let log = EventLog::create(
        &cfg.run.log_dir,
        &cfg.run.run_id,
        &machine_name,
        "frontend",
        None,
    )?;
    log_machine_info(&log);
    log_clock_offset(&log, "start");

    let exe = std::env::current_exe()?;
    let mut children: Vec<Child> = Vec::new();
    for p in 0..params.processes {
        let child = Command::new(&exe)
            .arg("worker")
            .arg("--config")
            .arg(&config_path)
            .args(["--machine", &machine.to_string()])
            .args(["--process", &p.to_string()])
            .args(["--start-at-ms", &start_at.to_string()])
            .args(params.to_cli())
            .kill_on_drop(true)
            .spawn()?;
        children.push(child);
    }
    eprintln!(
        "client machine {machine}: {} workers started, load begins at {start_at} ms (in {} ms)",
        children.len(),
        start_at.saturating_sub(now_ms())
    );

    // Machine-wide CPU and network, once a second.
    let cpu_samples: Arc<Mutex<Vec<(u64, u32)>>> = Arc::new(Mutex::new(Vec::new()));
    let sampler = {
        let (log, cpu_samples) = (log.clone(), cpu_samples.clone());
        let alive = children.len() as u64;
        tokio::spawn(async move {
            let mut s = MachineSampler::new();
            let mut tick = time::interval(Duration::from_secs(1));
            tick.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
            tick.tick().await;
            loop {
                tick.tick().await;
                let m = s.sample();
                cpu_samples
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .push((now_ms(), m.cpu_permille));
                log.emit(
                    log.event(EventKind::ResourceSample)
                        .field("scope", "machine")
                        .field("cpu_permille", m.cpu_permille)
                        .field("mem_used_bytes", m.mem_used_bytes)
                        .field("net_rx_bytes", m.net_rx_bytes)
                        .field("net_tx_bytes", m.net_tx_bytes)
                        .field("workers", alive),
                );
            }
        })
    };

    let mut failed = 0;
    let mut interrupted = false;
    let worker_pids: Vec<u32> = children.iter().filter_map(|c| c.id()).collect();
    for child in &mut children {
        let status = tokio::select! {
            s = child.wait() => s?,
            _ = crate::stop_signal(), if !interrupted => {
                interrupted = true;
                // Pass the stop on: a SIGTERM reaches only this process (a
                // terminal's Ctrl-C reaches the whole group), and a worker
                // with no end time would otherwise run on.
                for pid in &worker_pids {
                    let _ = std::process::Command::new("kill")
                        .args(["-TERM", &pid.to_string()])
                        .status();
                }
                eprintln!("stopping: workers are draining (Ctrl-C again to kill them)");
                tokio::select! {
                    s = child.wait() => s?,
                    _ = crate::stop_signal() => {
                        let _ = child.kill().await;
                        child.wait().await?
                    }
                }
            }
        };
        if !status.success() {
            failed += 1;
        }
    }
    sampler.abort();
    log_clock_offset(&log, "end");
    log.finish();

    let dir = log.path().parent().map(PathBuf::from).unwrap_or_default();
    let mut workers: Vec<WorkerSummary> = Vec::new();
    for p in 0..params.processes {
        let path = dir.join(format!("{machine_name}-{p}.summary.json"));
        match std::fs::read(&path)
            .ok()
            .and_then(|b| serde_json::from_slice(&b).ok())
        {
            Some(w) => workers.push(w),
            None => eprintln!("no summary from worker {machine}.{p} ({})", path.display()),
        }
    }

    let mut measure = Counts::default();
    let mut total = Counts::default();
    for w in &workers {
        measure.add(&w.measure);
        total.add(&w.total);
    }
    let hist = merge_hists(&workers);
    let (win_start, win_end) = (
        start_at + params.warmup_secs * 1000,
        start_at + (params.warmup_secs + params.duration_secs) * 1000,
    );
    let window_cpu: Vec<u32> = cpu_samples
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .iter()
        .filter(|(t, _)| *t >= win_start && (params.duration_secs == 0 || *t <= win_end))
        .map(|(_, c)| *c)
        .collect();
    let summary = MachineSummary {
        run_id: cfg.run.run_id.clone(),
        machine,
        mode: params.mode.as_str().to_owned(),
        processes: params.processes,
        users_per_process: params.users,
        rate: (params.mode == Mode::Open).then_some(params.rate),
        seed: params.seed,
        mix: params.mix.clone(),
        duration_secs: params.duration_secs,
        warmup_secs: params.warmup_secs,
        start_at_ms: start_at,
        workers_reported: workers.len(),
        throughput_ops: if params.duration_secs > 0 {
            measure.ok as f64 / params.duration_secs as f64
        } else {
            f64::NAN
        },
        latency_ms: LatencyMs::of(&hist),
        hist: hist.snapshot(),
        machine_cpu_avg_permille: (!window_cpu.is_empty()).then(|| {
            (window_cpu.iter().map(|&c| u64::from(c)).sum::<u64>() / window_cpu.len() as u64) as u32
        }),
        machine_cpu_max_permille: window_cpu.iter().copied().max(),
        events_dropped: workers.iter().map(|w| w.events_dropped).sum::<u64>() + log.dropped(),
        send_dropped: workers.iter().map(|w| w.send_dropped).sum(),
        reconnects: workers.iter().map(|w| w.reconnects).sum(),
        workers_not_connected_at_start: workers.iter().filter(|w| !w.connected_at_start).count(),
        measure,
        total,
    };
    std::fs::write(
        dir.join(format!("{machine_name}-summary.json")),
        serde_json::to_vec_pretty(&summary)?,
    )?;
    print!("{}", render(&summary));
    if failed > 0 || workers.len() != usize::from(params.processes) {
        return Err(io::Error::other(format!(
            "{failed} workers failed, {} of {} summaries found",
            workers.len(),
            params.processes
        )));
    }
    Ok(summary)
}
