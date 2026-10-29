//! Resource and clock measurements for the event log: per-process CPU and
//! memory, machine-wide CPU and network, hardware description, and the clock
//! offset against the time source. The project description asks for CPU,
//! memory, network and queue behaviour (p. 5); logging the client machines'
//! CPU is what lets the report show that a measured limit is on the server
//! side and not in the load generators (Phase 1 plan, Section 1.4).

use client::EventLog;
use proto::event::EventKind;
use sysinfo::{Networks, Pid, ProcessRefreshKind, ProcessesToUpdate, System, get_current_pid};

pub struct ProcSample {
    /// 1000 means one core fully busy; can exceed 1000 on a multi-core machine.
    pub cpu_core_permille: u32,
    /// Share of the whole machine, 0..=1000.
    pub cpu_permille: u32,
    pub rss_bytes: u64,
}

pub struct ProcSampler {
    sys: System,
    pid: Pid,
    cores: u32,
}

impl Default for ProcSampler {
    fn default() -> Self {
        ProcSampler::new()
    }
}

impl ProcSampler {
    pub fn new() -> ProcSampler {
        let pid = get_current_pid().unwrap_or_else(|_| Pid::from_u32(std::process::id()));
        let cores = std::thread::available_parallelism()
            .map(|n| n.get() as u32)
            .unwrap_or(1);
        let mut s = ProcSampler {
            sys: System::new(),
            pid,
            cores,
        };
        // The first reading only establishes a baseline.
        s.refresh();
        s
    }

    fn refresh(&mut self) {
        self.sys.refresh_processes_specifics(
            ProcessesToUpdate::Some(&[self.pid]),
            true,
            ProcessRefreshKind::nothing().with_cpu().with_memory(),
        );
    }

    pub fn sample(&mut self) -> ProcSample {
        self.refresh();
        let (cpu, rss) = self
            .sys
            .process(self.pid)
            .map(|p| (p.cpu_usage(), p.memory()))
            .unwrap_or((0.0, 0));
        let core = (cpu * 10.0).round().max(0.0) as u32;
        ProcSample {
            cpu_core_permille: core,
            cpu_permille: (core / self.cores).min(1000),
            rss_bytes: rss,
        }
    }
}

pub struct MachineSample {
    pub cpu_permille: u32,
    pub mem_used_bytes: u64,
    /// Cumulative bytes over all interfaces; take differences.
    pub net_rx_bytes: u64,
    pub net_tx_bytes: u64,
}

pub struct MachineSampler {
    sys: System,
    nets: Networks,
}

impl Default for MachineSampler {
    fn default() -> Self {
        MachineSampler::new()
    }
}

impl MachineSampler {
    pub fn new() -> MachineSampler {
        let mut sys = System::new();
        sys.refresh_cpu_usage();
        MachineSampler {
            sys,
            nets: Networks::new_with_refreshed_list(),
        }
    }

    pub fn sample(&mut self) -> MachineSample {
        self.sys.refresh_cpu_usage();
        self.sys.refresh_memory();
        self.nets.refresh(true);
        let (rx, tx) = self.nets.iter().fold((0u64, 0u64), |(r, t), (_, n)| {
            (r + n.total_received(), t + n.total_transmitted())
        });
        MachineSample {
            cpu_permille: (self.sys.global_cpu_usage() * 10.0)
                .round()
                .clamp(0.0, 1000.0) as u32,
            mem_used_bytes: self.sys.used_memory(),
            net_rx_bytes: rx,
            net_tx_bytes: tx,
        }
    }
}

/// Logs the `machine_info` event (hardware, once per machine per run).
pub fn log_machine_info(log: &EventLog) {
    let mut sys = System::new();
    sys.refresh_cpu_usage();
    sys.refresh_memory();
    let model = sys
        .cpus()
        .first()
        .map(|c| c.brand().trim().to_owned())
        .unwrap_or_default();
    let os = System::long_os_version()
        .or_else(System::name)
        .unwrap_or_else(|| std::env::consts::OS.to_owned());
    log.emit(
        log.event(EventKind::MachineInfo)
            .field("cpu_model", model)
            .field("cores", sys.cpus().len() as u64)
            .field("mem_bytes", sys.total_memory())
            .field("os", os),
    );
}

/// Clock offset against the time source, in nanoseconds, positive when this
/// machine's clock is ahead (so `corrected = local - offset`), and where it
/// came from. Reads `chronyc tracking`; `RM_CLOCK_OFFSET_NS` overrides it for
/// machines without chrony. `None` means unknown, and the analysis then
/// treats that machine's cross-machine timings as uncorrected.
pub fn clock_offset() -> (Option<i64>, &'static str) {
    if let Some(n) = std::env::var("RM_CLOCK_OFFSET_NS")
        .ok()
        .and_then(|v| v.trim().parse().ok())
    {
        return (Some(n), "manual");
    }
    match std::process::Command::new("chronyc")
        .arg("tracking")
        .output()
    {
        Ok(o) if o.status.success() => match parse_chrony(&String::from_utf8_lossy(&o.stdout)) {
            Some(n) => (Some(n), "chrony"),
            None => (None, "chrony_unparsed"),
        },
        _ => (None, "unavailable"),
    }
}

/// Parses the `System time` line of `chronyc tracking`, for example
/// `System time     : 0.000012345 seconds fast of NTP time`.
pub fn parse_chrony(text: &str) -> Option<i64> {
    let line = text.lines().find(|l| l.starts_with("System time"))?;
    let rest = line.split_once(':')?.1;
    let mut parts = rest.split_whitespace();
    let secs: f64 = parts.next()?.parse().ok()?;
    parts.next()?; // "seconds"
    let sign = match parts.next()? {
        "fast" => 1.0,
        "slow" => -1.0,
        _ => return None,
    };
    Some((sign * secs * 1e9).round() as i64)
}

/// Logs a `clock_offset` event; `when` is `"start"` or `"end"` of the run.
pub fn log_clock_offset(log: &EventLog, when: &str) {
    let (offset, source) = clock_offset();
    let mut ev = log
        .event(EventKind::ClockOffset)
        .field("source", source)
        .field("when", when);
    if let Some(n) = offset {
        ev = ev.field("offset_ns", n);
    }
    log.emit(ev);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chrony_output_is_parsed_with_sign() {
        let fast = "Reference ID    : A9FEA9FE\nSystem time     : 0.000012345 seconds fast of NTP time\nLast offset     : +0.0\n";
        assert_eq!(parse_chrony(fast), Some(12_345));
        let slow = "System time     : 0.250000000 seconds slow of NTP time\n";
        assert_eq!(parse_chrony(slow), Some(-250_000_000));
        assert_eq!(parse_chrony("nothing useful"), None);
        assert_eq!(
            parse_chrony("System time     : abc seconds fast of NTP time"),
            None
        );
    }

    #[test]
    fn samplers_return_plausible_values() {
        let mut p = ProcSampler::new();
        let s = p.sample();
        assert!(s.rss_bytes > 0);
        assert!(s.cpu_permille <= 1000);
        let mut m = MachineSampler::new();
        let s = m.sample();
        assert!(s.cpu_permille <= 1000);
        assert!(s.mem_used_bytes > 0);
    }
}
