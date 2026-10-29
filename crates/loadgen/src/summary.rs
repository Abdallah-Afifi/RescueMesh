//! Run summaries. The raw event logs are the source of every reported number;
//! these summaries are for the operator watching a run, and the frontend
//! prints one at the end of each run.

use client::hist::{HistSnapshot, LatencyHist};
use serde::{Deserialize, Serialize};
use std::fmt::Write;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Counts {
    /// Requests started.
    pub started: u64,
    /// Requests that got an `Ok` reply (including application-level refusals).
    pub ok: u64,
    /// Requests that exhausted their retries without an `Ok` reply.
    pub gave_up: u64,
    /// Of `ok`, those whose result was `Refused`.
    pub refused: u64,
    /// Resends beyond the first send, summed over requests.
    pub retries: u64,
    /// Open loop: arrivals not sent because the process was at `max_inflight`.
    /// Nonzero means the client machine, not the cloud, limited the load.
    pub dropped_client: u64,
}

impl Counts {
    pub fn add(&mut self, o: &Counts) {
        self.started += o.started;
        self.ok += o.ok;
        self.gave_up += o.gave_up;
        self.refused += o.refused;
        self.retries += o.retries;
        self.dropped_client += o.dropped_client;
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerSummary {
    pub machine: u16,
    pub process: u16,
    pub users: u32,
    pub mode: String,
    pub incarnation: u32,
    /// All three connections were up before the run started.
    pub connected_at_start: bool,
    /// Counts over the whole run, warm-up and drain included.
    pub total: Counts,
    /// Counts for requests that started inside the measured window.
    pub measure: Counts,
    /// Latency of measured-window requests that succeeded.
    pub hist: HistSnapshot,
    pub events_dropped: u64,
    pub send_dropped: u64,
    pub reconnects: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LatencyMs {
    pub mean: f64,
    pub p50: f64,
    pub p95: f64,
    pub p99: f64,
    pub max: f64,
}

impl LatencyMs {
    pub fn of(h: &LatencyHist) -> LatencyMs {
        let ms = |us: Option<u64>| us.map_or(f64::NAN, |v| v as f64 / 1000.0);
        LatencyMs {
            mean: h.mean_us().map_or(f64::NAN, |v| v / 1000.0),
            p50: ms(h.quantile_us(0.50)),
            p95: ms(h.quantile_us(0.95)),
            p99: ms(h.quantile_us(0.99)),
            max: ms(h.max_us()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MachineSummary {
    pub run_id: String,
    pub machine: u16,
    pub mode: String,
    pub processes: u16,
    pub users_per_process: u32,
    pub rate: Option<f64>,
    pub seed: u64,
    pub mix: String,
    pub duration_secs: u64,
    pub warmup_secs: u64,
    pub start_at_ms: u64,
    pub workers_reported: usize,
    pub measure: Counts,
    pub total: Counts,
    pub throughput_ops: f64,
    pub latency_ms: LatencyMs,
    pub hist: HistSnapshot,
    /// Machine-wide CPU over the measured window, thousandths.
    pub machine_cpu_avg_permille: Option<u32>,
    pub machine_cpu_max_permille: Option<u32>,
    pub events_dropped: u64,
    pub send_dropped: u64,
    pub reconnects: u64,
    pub workers_not_connected_at_start: usize,
}

pub fn merge_hists(workers: &[WorkerSummary]) -> LatencyHist {
    let mut h = LatencyHist::new();
    for w in workers {
        h.merge(&LatencyHist::from_snapshot(&w.hist));
    }
    h
}

pub fn render(s: &MachineSummary) -> String {
    let mut o = String::new();
    let users = u64::from(s.processes) * u64::from(s.users_per_process);
    let _ = writeln!(
        o,
        "client machine {}  run {}  {} loop{}  {} processes x {} users = {users} users",
        s.machine,
        s.run_id,
        s.mode,
        s.rate.map_or(String::new(), |r| format!(" at {r:.0}/s")),
        s.processes,
        s.users_per_process
    );
    let m = &s.measure;
    let _ = writeln!(
        o,
        "  measured window {} s: started {}  ok {}  gave up {}  retries {}  client-dropped {}",
        s.duration_secs, m.started, m.ok, m.gave_up, m.retries, m.dropped_client
    );
    let l = &s.latency_ms;
    let _ = writeln!(
        o,
        "  throughput {:.1} ops/s   latency ms: mean {:.2}  p50 {:.2}  p95 {:.2}  p99 {:.2}  max {:.2}",
        s.throughput_ops, l.mean, l.p50, l.p95, l.p99, l.max
    );
    if let (Some(avg), Some(max)) = (s.machine_cpu_avg_permille, s.machine_cpu_max_permille) {
        let _ = writeln!(
            o,
            "  this machine's CPU: avg {:.0}%  max {:.0}%",
            f64::from(avg) / 10.0,
            f64::from(max) / 10.0
        );
    }
    if s.events_dropped > 0 || s.send_dropped > 0 || s.workers_not_connected_at_start > 0 {
        let _ = writeln!(
            o,
            "  WARNING: events dropped {}, frames dropped {}, workers not connected at start {}",
            s.events_dropped, s.send_dropped, s.workers_not_connected_at_start
        );
    }
    if m.dropped_client > 0 {
        let _ = writeln!(
            o,
            "  WARNING: the client machine could not offer the requested load"
        );
    }
    o
}
