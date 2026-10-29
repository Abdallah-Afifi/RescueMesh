//! The run configuration: one TOML file shared by all six machines.
//! Each process is told its own identity on the command line.
//!
//! Defaults are the agreed starting values (design log DL-024). Every field
//! has a default except the cluster addresses, so a config file only needs to
//! state what differs. [`Config::validate`] rejects combinations that would
//! break the protocol's assumptions; every binary must call it at start-up.

use crate::codec::WireFormat;
use crate::ids::NodeId;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub run: RunCfg,
    pub cluster: ClusterCfg,
    #[serde(default)]
    pub timeouts: Timeouts,
    #[serde(default)]
    pub score: ScoreCfg,
    #[serde(default)]
    pub policy: PolicyCfg,
    #[serde(default)]
    pub planner: PlannerCfg,
    #[serde(default)]
    pub inject: InjectCfg,
    #[serde(default)]
    pub client: ClientCfg,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RunCfg {
    pub run_id: String,
    pub seed: u64,
    pub log_dir: PathBuf,
    pub wire_format: WireFormat,
}

impl Default for RunCfg {
    fn default() -> Self {
        RunCfg {
            run_id: "dev".into(),
            seed: 1,
            log_dir: "logs".into(),
            wire_format: WireFormat::Postcard,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterCfg {
    pub nodes: Vec<NodeAddr>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeAddr {
    pub id: NodeId,
    pub host: String,
    /// TCP port for clients and peers.
    pub port: u16,
    /// UDP port for heartbeats.
    pub heartbeat_port: u16,
}

/// All durations in milliseconds unless the name says otherwise.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Timeouts {
    pub heartbeat_ms: u64,
    pub fd_phi_threshold: f64,
    pub fd_min_ms: u64,
    pub fd_max_ms: u64,
    pub claim_timeout_min_ms: u64,
    pub claim_timeout_rtt_multiple: u32,
    pub candidacy_base_ms: u64,
    pub candidacy_jitter_ms: u64,
    pub election_backoff_min_ms: u64,
    pub election_backoff_max_ms: u64,
    pub lease_ms: u64,
    pub propose_retry_ms: u64,
    pub max_replans: u32,
    pub retransmit_ms: u64,
    pub ack_delay_ms: u64,
}

impl Default for Timeouts {
    fn default() -> Self {
        Timeouts {
            heartbeat_ms: 100,
            fd_phi_threshold: 8.0,
            fd_min_ms: 300,
            fd_max_ms: 1000,
            claim_timeout_min_ms: 20,
            claim_timeout_rtt_multiple: 3,
            candidacy_base_ms: 150,
            candidacy_jitter_ms: 150,
            election_backoff_min_ms: 150,
            election_backoff_max_ms: 300,
            lease_ms: 250,
            propose_retry_ms: 200,
            max_replans: 3,
            retransmit_ms: 100,
            ack_delay_ms: 20,
        }
    }
}

/// Score parameters (Section 5.1). Weights are in thousandths and must sum
/// to 1000, so the score is integer arithmetic on every node.
///
/// `w_missed` defaults to 0: each node measures missed heartbeats itself, so
/// a non-zero weight makes nodes rank from different inputs (design log
/// DL-047).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ScoreCfg {
    pub w_queue: u16,
    pub w_cpu: u16,
    pub w_service: u16,
    pub w_missed: u16,
    pub w_rtt: u16,
    pub queue_cap: u32,
    pub service_cap_ms: u32,
    pub rtt_cap_ms: u32,
    pub quantum_permille: u16,
}

impl Default for ScoreCfg {
    fn default() -> Self {
        ScoreCfg {
            w_queue: 250,
            w_cpu: 250,
            w_service: 250,
            w_missed: 0,
            w_rtt: 250,
            queue_cap: 64,
            service_cap_ms: 500,
            rtt_cap_ms: 50,
            quantum_permille: 50,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RankerKind {
    /// MWBE with scores from heartbeats plus the local correction (primary).
    #[default]
    MwbeCached,
    /// MWBE with a fresh bid round per request (optional comparison).
    MwbeBids,
    Random,
    Hash,
    RoundRobin,
    TwoChoices,
    HighestId,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PolicyCfg {
    pub ranker: RankerKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PlannerCfg {
    pub zones: u16,
    /// Cost knob: how many pending incidents enter the assignment problem.
    /// The assignment step grows roughly with its cube.
    pub top_k: u32,
    pub worker_threads: Option<usize>,
    pub queue_capacity: usize,
}

impl Default for PlannerCfg {
    fn default() -> Self {
        PlannerCfg {
            zones: 30,
            top_k: 100,
            worker_threads: None,
            queue_capacity: 256,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct InjectCfg {
    pub enabled: bool,
    pub interval_min_ms: u64,
    pub interval_max_ms: u64,
    pub duration_min_ms: u64,
    pub duration_max_ms: u64,
}

impl Default for InjectCfg {
    fn default() -> Self {
        InjectCfg {
            enabled: false,
            interval_min_ms: 45_000,
            interval_max_ms: 75_000,
            duration_min_ms: 1_000,
            duration_max_ms: 20_000,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ClientCfg {
    pub timeout_initial_ms: u64,
    pub timeout_p99_multiple: u32,
    pub max_retries: u32,
    pub backoff_factor: f64,
}

impl Default for ClientCfg {
    fn default() -> Self {
        ClientCfg {
            timeout_initial_ms: 2_000,
            timeout_p99_multiple: 3,
            max_retries: 4,
            backoff_factor: 2.0,
        }
    }
}

#[derive(Debug)]
pub enum ConfigError {
    Read(PathBuf, std::io::Error),
    Parse(String),
    Invalid(Vec<String>),
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigError::Read(p, e) => write!(f, "cannot read {}: {e}", p.display()),
            ConfigError::Parse(e) => write!(f, "cannot parse config: {e}"),
            ConfigError::Invalid(errs) => write!(f, "invalid config:\n  - {}", errs.join("\n  - ")),
        }
    }
}

impl std::error::Error for ConfigError {}

impl Config {
    /// Read, parse and validate.
    pub fn load(path: &Path) -> Result<Config, ConfigError> {
        let text =
            std::fs::read_to_string(path).map_err(|e| ConfigError::Read(path.to_path_buf(), e))?;
        let cfg = Config::parse(&text)?;
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn parse(text: &str) -> Result<Config, ConfigError> {
        toml::from_str(text).map_err(|e| ConfigError::Parse(e.to_string()))
    }

    pub fn node(&self, id: NodeId) -> Option<&NodeAddr> {
        self.cluster.nodes.iter().find(|n| n.id == id)
    }

    /// Reports every problem at once rather than the first.
    pub fn validate(&self) -> Result<(), ConfigError> {
        let mut e = Vec::new();
        let t = &self.timeouts;
        let s = &self.score;

        let mut ids: Vec<u8> = self.cluster.nodes.iter().map(|n| n.id.0).collect();
        ids.sort_unstable();
        if ids != [1, 2, 3] {
            e.push(format!(
                "cluster.nodes must list ids 1, 2 and 3 exactly once (found {ids:?})"
            ));
        }
        for n in &self.cluster.nodes {
            if n.port == 0 || n.heartbeat_port == 0 {
                e.push(format!(
                    "node {} needs non-zero port and heartbeat_port",
                    n.id
                ));
            }
            if n.port == n.heartbeat_port {
                e.push(format!(
                    "node {} uses the same port for TCP and heartbeats",
                    n.id
                ));
            }
        }
        if self.run.run_id.trim().is_empty() {
            e.push("run.run_id must not be empty".into());
        }

        if t.heartbeat_ms == 0 {
            e.push("timeouts.heartbeat_ms must be positive".into());
        }
        if t.fd_min_ms < 2 * t.heartbeat_ms {
            e.push("timeouts.fd_min_ms must be at least two heartbeat intervals".into());
        }
        if t.fd_min_ms > t.fd_max_ms {
            e.push("timeouts.fd_min_ms must not exceed fd_max_ms".into());
        }
        if !(t.fd_phi_threshold.is_finite() && t.fd_phi_threshold > 0.0) {
            e.push("timeouts.fd_phi_threshold must be a positive number".into());
        }
        // A cut-off coordinator must stop acting before anyone can suspect it (Section 7, step 8).
        if t.lease_ms >= t.fd_min_ms {
            e.push("timeouts.lease_ms must be shorter than fd_min_ms".into());
        }
        if t.election_backoff_min_ms == 0 || t.election_backoff_min_ms > t.election_backoff_max_ms {
            e.push(
                "timeouts.election_backoff_min_ms must be positive and not exceed the max".into(),
            );
        }
        if t.claim_timeout_min_ms == 0 || t.claim_timeout_rtt_multiple == 0 {
            e.push("claim timeout settings must be positive".into());
        }
        if t.retransmit_ms == 0 || t.ack_delay_ms >= t.retransmit_ms {
            e.push(
                "timeouts.ack_delay_ms must be shorter than retransmit_ms, which must be positive"
                    .into(),
            );
        }
        if t.propose_retry_ms == 0 {
            e.push("timeouts.propose_retry_ms must be positive".into());
        }

        let wsum = u32::from(s.w_queue)
            + u32::from(s.w_cpu)
            + u32::from(s.w_service)
            + u32::from(s.w_missed)
            + u32::from(s.w_rtt);
        if wsum != 1000 {
            e.push(format!("score weights must sum to 1000 (found {wsum})"));
        }
        if s.queue_cap == 0 || s.service_cap_ms == 0 || s.rtt_cap_ms == 0 {
            e.push("score caps must be positive".into());
        }
        if s.quantum_permille == 0 || s.quantum_permille > 1000 || 1000 % s.quantum_permille != 0 {
            e.push("score.quantum_permille must divide 1000".into());
        }

        if self.planner.zones < 2 || self.planner.top_k == 0 || self.planner.queue_capacity == 0 {
            e.push("planner needs at least 2 zones, top_k > 0 and queue_capacity > 0".into());
        }
        if self.planner.worker_threads == Some(0) {
            e.push("planner.worker_threads must be positive when set".into());
        }

        let i = &self.inject;
        if i.interval_min_ms > i.interval_max_ms || i.duration_min_ms > i.duration_max_ms {
            e.push("inject min values must not exceed max values".into());
        }
        if i.duration_max_ms > 20_000 {
            e.push("inject.duration_max_ms must not exceed 20000 (spec p. 3)".into());
        }
        if i.duration_min_ms == 0 || i.interval_min_ms == 0 {
            e.push("inject durations and intervals must be positive".into());
        }
        // The previous silence must end, and the node rejoin, before the next starts.
        if i.interval_min_ms <= i.duration_max_ms {
            e.push("inject.interval_min_ms must exceed duration_max_ms".into());
        }

        let c = &self.client;
        if c.timeout_initial_ms == 0 || c.timeout_p99_multiple == 0 {
            e.push("client timeouts must be positive".into());
        }
        if !(c.backoff_factor.is_finite() && c.backoff_factor >= 1.0) {
            e.push("client.backoff_factor must be at least 1".into());
        }

        if e.is_empty() {
            Ok(())
        } else {
            Err(ConfigError::Invalid(e))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const EXAMPLE: &str = include_str!("../../../config/cluster.example.toml");

    #[test]
    fn example_config_is_valid() {
        let cfg = Config::parse(EXAMPLE).unwrap();
        cfg.validate().unwrap();
        assert_eq!(cfg.cluster.nodes.len(), 3);
        assert_eq!(cfg.node(NodeId(2)).unwrap().port, 7000);
    }

    #[test]
    fn defaults_are_valid() {
        let cfg = Config::parse(
            r#"
            [[cluster.nodes]]
            id = 1
            host = "127.0.0.1"
            port = 7001
            heartbeat_port = 7101
            [[cluster.nodes]]
            id = 2
            host = "127.0.0.1"
            port = 7002
            heartbeat_port = 7102
            [[cluster.nodes]]
            id = 3
            host = "127.0.0.1"
            port = 7003
            heartbeat_port = 7103
            "#,
        )
        .unwrap();
        cfg.validate().unwrap();
    }

    #[test]
    fn bad_values_are_all_reported() {
        let mut cfg = Config::parse(EXAMPLE).unwrap();
        cfg.timeouts.lease_ms = cfg.timeouts.fd_min_ms;
        cfg.score.w_rtt = 0;
        cfg.inject.duration_max_ms = 30_000;
        cfg.cluster.nodes.pop();
        let Err(ConfigError::Invalid(errs)) = cfg.validate() else {
            panic!("expected invalid")
        };
        assert!(errs.len() >= 4, "{errs:?}");
    }

    #[test]
    fn unknown_fields_are_rejected() {
        let text = EXAMPLE.replace("[timeouts]", "[timeouts]\nheartbeat_msec = 5");
        assert!(Config::parse(&text).is_err());
    }
}
