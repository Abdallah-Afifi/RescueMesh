//! Committed incidents, resources and assignments (design log DL-029), changed
//! only by applying committed log entries in `seq` order.
//!
//! The planning snapshot is shared with planning threads through an `Arc`, so
//! applying an entry while a plan holds the old snapshot copies it. The
//! snapshot is therefore kept small: every resource, but only open incidents
//! and no assignments (DL-055). Full state lives beside it for queries.

use proto::{
    Assignment, Entry, EntryKind, Incident, IncidentId, IncidentStatus, OpResult, Query, Request,
    ResourceId, Seq, Snapshot, StateMachine, Version, Write,
};
use std::collections::BTreeMap;
use std::sync::Arc;

/// Expired items are dropped from the snapshot this long after they expire,
/// measured on the clients' clocks, which may disagree by this much.
pub const PRUNE_SLACK_MS: u64 = 60_000;
/// Pruning runs at every entry whose `seq` is a multiple of this, so every
/// node prunes at the same log positions.
pub const PRUNE_EVERY: u64 = 1_024;

#[derive(Debug, Default)]
pub struct State {
    snap: Arc<Snapshot>,
    incidents: BTreeMap<IncidentId, Incident>,
    assignments: BTreeMap<(IncidentId, ResourceId), Assignment>,
    /// Latest client timestamp seen in a committed write. Pruning compares
    /// against this, never against a local clock, so it is deterministic.
    latest_ms: u64,
}

impl State {
    pub fn new() -> State {
        State::default()
    }

    pub fn assignments(&self) -> &BTreeMap<(IncidentId, ResourceId), Assignment> {
        &self.assignments
    }

    fn prune(&mut self) {
        let cut = self.latest_ms.saturating_sub(PRUNE_SLACK_MS);
        let s = Arc::make_mut(&mut self.snap);
        s.incidents
            .retain(|_, (i, _)| i.reported_at_ms.saturating_add(i.ttl_ms) > cut);
        s.resources.retain(|_, (r, _)| r.expires_at_ms > cut);
    }
}

impl StateMachine for State {
    fn apply(&mut self, entry: &Entry) {
        let version = Version::from(entry.seq);
        let s = Arc::make_mut(&mut self.snap);
        s.applied = entry.seq;
        if let EntryKind::Result { writes, .. } = &entry.kind {
            for w in writes {
                match w {
                    Write::PutIncident(i) => {
                        self.latest_ms = self.latest_ms.max(i.reported_at_ms);
                        if i.status == IncidentStatus::Open {
                            s.incidents.insert(i.id, (i.clone(), version));
                        } else {
                            s.incidents.remove(&i.id);
                        }
                        self.incidents.insert(i.id, i.clone());
                    }
                    Write::PutResource(r) => {
                        s.resources.insert(r.id, (r.clone(), version));
                    }
                    Write::PutAssignment(a) => {
                        self.latest_ms = self.latest_ms.max(a.created_at_ms);
                        self.assignments.insert((a.incident, a.resource), a.clone());
                    }
                }
            }
        }
        if entry.seq.0.is_multiple_of(PRUNE_EVERY) {
            self.prune();
        }
    }

    fn snapshot(&self) -> Arc<Snapshot> {
        Arc::clone(&self.snap)
    }

    fn version_of(&self, id: ResourceId) -> Option<Version> {
        let s = &self.snap;
        s.resources
            .get(&id)
            .map(|(_, v)| *v)
            .or_else(|| s.incidents.get(&id).map(|(_, v)| *v))
    }

    fn applied(&self) -> Seq {
        self.snap.applied
    }

    fn query(&self, req: &Request) -> OpResult {
        match &req.op {
            proto::Op::Query(Query::Incident(id)) => {
                OpResult::Incident(self.incidents.get(id).cloned())
            }
            proto::Op::Query(Query::IncidentsInZone(z)) => OpResult::Incidents(
                self.incidents
                    .values()
                    .filter(|i| {
                        i.zone == *z && i.reported_at_ms.saturating_add(i.ttl_ms) > req.sent_at_ms
                    })
                    .cloned()
                    .collect(),
            ),
            proto::Op::Query(Query::Resource(id)) => {
                OpResult::Resource(self.snap.resources.get(id).map(|(r, _)| r.clone()))
            }
            _ => OpResult::Refused("not a query".into()),
        }
    }
}
