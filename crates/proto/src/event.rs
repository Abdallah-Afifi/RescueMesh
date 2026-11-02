//! Event log schema. Every process writes one JSON object per line to
//! `<log_dir>/<run_id>/<machine>-<process>.jsonl`; the analysis scripts read
//! only these files, so every number in the report must be derivable from
//! them.
//!
//! Each line has the common fields of [`Event`] plus kind-specific fields
//! flattened into the same object. Add new kinds at the end of [`EventKind`]
//! and document their fields here; never rename or remove one.

use crate::ids::{NodeId, ReqId};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Event {
    /// Wall-clock time, nanoseconds since the Unix epoch. Comparable across
    /// machines only after correcting by the logged clock offset.
    pub ts_ns: u64,
    /// Monotonic time since process start. Use for durations within one process.
    pub mono_ns: u64,
    pub run_id: String,
    pub machine: String,
    /// Set for cloud-node processes, absent for clients.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub node: Option<NodeId>,
    pub kind: EventKind,
    /// [`ReqId`] in its display form, e.g. `2.7.41/00000009/1234`.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub req: Option<String>,
    #[serde(flatten)]
    pub fields: Map<String, Value>,
}

/// Kinds and their extra fields.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    // Client side
    /// `attempt`, `op` (operation name)
    ClientSend,
    /// `attempt`, `from` (node), `outcome` (ok / busy / error), `latency_us`
    ClientReply,
    /// `attempt`, `timeout_ms`
    ClientRetry,

    // Request path on a node
    /// `op`
    ReqRecv,
    /// `rank` (list of node ids), `policy`
    Ranked,
    /// `score_step`, `tie`
    Claim,
    PlanStart,
    /// `service_us`, `read_set_len`
    PlanEnd,
    /// `attempt`
    Propose,
    /// `current` (seq)
    Replan,
    /// `seq`
    Commit,
    /// `outcome`
    Reply,
    /// `queue_len`
    Busy,
    /// `seq`; emitted by the coordinator when it returns a stored result
    DuplicateSuppressed,

    // Failure detection, membership and election
    /// `peer`, `phi`
    Suspect,
    /// `view`, `active` (list)
    ViewChange,
    /// `epoch`
    ElectionStart,
    /// `epoch`, `votes`
    ElectionWon,
    /// `epoch`, `reason`
    StepDown,

    // Fault injection and recovery
    /// `k`, `victim`, `duration_ms`, `by_operator`
    InjectStart,
    /// `k`
    InjectEnd,
    /// `last_seq`, `commit`
    SyncStart,
    /// `applied`, `view`
    Active,

    // Periodic samples, once per second per process
    /// `cpu_permille`, `rss_bytes`, `net_rx_bytes`, `net_tx_bytes`, `queue_len`
    ResourceSample,
    /// `offset_ns`, `source` (e.g. chrony); logged at start and end of a run
    ClockOffset,
    /// `cpu_model`, `cores`, `mem_bytes`, `os`; logged once at start
    MachineInfo,
    /// `reason`, `target`; a fault injection the coordinator declined
    InjectRefused,
    /// `threads`, `duty_permille`, `duration_ms` (0: until stopped), `on`;
    /// the artificial CPU load of the load-imbalance demo started or stopped
    LoadKnob,
}

impl Event {
    /// An event stamped with the current time.
    pub fn now(
        run_id: &str,
        machine: &str,
        node: Option<NodeId>,
        kind: EventKind,
        start: std::time::Instant,
    ) -> Event {
        let ts_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        Event {
            ts_ns,
            mono_ns: start.elapsed().as_nanos() as u64,
            run_id: run_id.to_owned(),
            machine: machine.to_owned(),
            node,
            kind,
            req: None,
            fields: Map::new(),
        }
    }

    pub fn req(mut self, req: &ReqId) -> Event {
        self.req = Some(req.to_string());
        self
    }

    pub fn field(mut self, key: &str, value: impl Into<Value>) -> Event {
        self.fields.insert(key.to_owned(), value.into());
        self
    }

    /// One line of JSON, without the trailing newline.
    pub fn to_line(&self) -> String {
        serde_json::to_string(self).expect("events always serialize")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::ClientId;

    #[test]
    fn event_line_is_flat_json() {
        let r = ReqId {
            client: ClientId {
                machine: 2,
                process: 7,
                user: 41,
            },
            incarnation: 9,
            seq: 1234,
        };
        let e = Event::now(
            "run-1",
            "cloud-a",
            Some(NodeId(2)),
            EventKind::PlanEnd,
            std::time::Instant::now(),
        )
        .req(&r)
        .field("service_us", 1530);
        let v: Value = serde_json::from_str(&e.to_line()).unwrap();
        assert_eq!(v["kind"], "plan_end");
        assert_eq!(v["req"], "2.7.41/00000009/1234");
        assert_eq!(v["service_us"], 1530);
        assert_eq!(v["node"], 2);
        let back: Event = serde_json::from_value(v).unwrap();
        assert_eq!(back, e);
    }
}
