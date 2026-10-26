//! Command-line parameters shared by the frontend and its workers. The
//! frontend passes them on unchanged, so they are defined once.

use clap::{Args, ValueEnum};

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Mode {
    /// Poisson arrivals at a fixed rate, whatever the system does. Use for
    /// latency curves: a slow system cannot slow the offered load down.
    Open,
    /// Each user sends its next request after the previous reply (plus think
    /// time). Use for peak throughput.
    Closed,
}

impl Mode {
    pub fn as_str(self) -> &'static str {
        match self {
            Mode::Open => "open",
            Mode::Closed => "closed",
        }
    }
}

#[derive(Debug, Clone, Args)]
pub struct LoadParams {
    /// Client processes started on this machine (K).
    #[arg(long, default_value_t = 4)]
    pub processes: u16,
    /// Simulated users in each process (M).
    #[arg(long, default_value_t = 50)]
    pub users: u32,
    #[arg(long, value_enum, default_value_t = Mode::Closed)]
    pub mode: Mode,
    /// Open loop: arrival rate for the whole machine, requests per second.
    #[arg(long, default_value_t = 100.0)]
    pub rate: f64,
    /// Closed loop: mean think time between a reply and the next request, in
    /// milliseconds (exponentially distributed). 0 means none.
    #[arg(long, default_value_t = 0)]
    pub think_ms: u64,
    /// Length of the measured window in seconds; 0 runs until Ctrl-C.
    #[arg(long, default_value_t = 60)]
    pub duration_secs: u64,
    /// Seconds of load before the measured window, discarded by the analysis.
    #[arg(long, default_value_t = 10)]
    pub warmup_secs: u64,
    /// After the window, how long to wait for outstanding requests.
    #[arg(long, default_value_t = 30)]
    pub drain_secs: u64,
    /// Seed of the operation mix. The same seed gives every user the same
    /// sequence of operations in every run.
    #[arg(long, default_value_t = 1)]
    pub seed: u64,
    /// Operation mix as weights, e.g. `report=60,offer=20,update=15,query=5`.
    #[arg(long, default_value = "report=60,offer=20,update=15,query=5")]
    pub mix: String,
    /// Open loop: new arrivals are dropped (and counted) while this many
    /// requests are outstanding in one process, so a saturated system cannot
    /// exhaust the client machine's memory.
    #[arg(long, default_value_t = 20_000)]
    pub max_inflight: usize,
}

impl LoadParams {
    /// The same parameters as command-line arguments for a worker.
    pub fn to_cli(&self) -> Vec<String> {
        vec![
            "--processes".into(),
            self.processes.to_string(),
            "--users".into(),
            self.users.to_string(),
            "--mode".into(),
            self.mode.as_str().into(),
            "--rate".into(),
            self.rate.to_string(),
            "--think-ms".into(),
            self.think_ms.to_string(),
            "--duration-secs".into(),
            self.duration_secs.to_string(),
            "--warmup-secs".into(),
            self.warmup_secs.to_string(),
            "--drain-secs".into(),
            self.drain_secs.to_string(),
            "--seed".into(),
            self.seed.to_string(),
            "--mix".into(),
            self.mix.clone(),
            "--max-inflight".into(),
            self.max_inflight.to_string(),
        ]
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.processes == 0 || self.users == 0 {
            return Err("--processes and --users must be positive".into());
        }
        if self.mode == Mode::Open && !(self.rate.is_finite() && self.rate > 0.0) {
            return Err("--rate must be positive in open-loop mode".into());
        }
        if self.max_inflight == 0 {
            return Err("--max-inflight must be positive".into());
        }
        crate::workload::Mix::parse(&self.mix).map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[derive(Parser)]
    struct Wrap {
        #[command(flatten)]
        p: LoadParams,
    }

    #[test]
    fn parameters_survive_the_trip_to_a_worker() {
        let a = Wrap::parse_from(["x", "--mode", "open", "--rate", "250.5", "--users", "7"]);
        let b = Wrap::parse_from(std::iter::once("x".to_owned()).chain(a.p.to_cli()));
        assert_eq!(b.p.mode, Mode::Open);
        assert_eq!(b.p.rate, 250.5);
        assert_eq!(b.p.users, 7);
        assert_eq!(b.p.mix, a.p.mix);
        assert_eq!(b.p.max_inflight, a.p.max_inflight);
        a.p.validate().unwrap();
    }

    #[test]
    fn bad_values_are_rejected() {
        let mut p = Wrap::parse_from(["x"]).p;
        p.users = 0;
        assert!(p.validate().is_err());
        let mut p = Wrap::parse_from(["x", "--mode", "open"]).p;
        p.rate = 0.0;
        assert!(p.validate().is_err());
        let mut p = Wrap::parse_from(["x"]).p;
        p.mix = "report=0,offer=0".into();
        assert!(p.validate().is_err());
    }
}
