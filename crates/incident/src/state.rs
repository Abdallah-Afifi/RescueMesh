//! Committed incidents, resources and assignments (design log DL-029), changed
//! only by applying committed log entries in `seq` order.
//!
//! The planning snapshot is shared with planning threads through an `Arc`, so
//! applying an entry while a plan holds the old snapshot copies it. The
//! snapshot is therefore kept small: every resource, but only open incidents
//! and no assignments (DL-058). Full state lives beside it for queries.
//!
//! Applying an entry only stores what the plan wrote. Time-dependent views,
//! an incident past its TTL or the units of an expired hold, are worked out
//! when read: queries from the request's `sent_at_ms`, plans from theirs,
//! kept near the committed clients' time (`planner::plan_time`). That time,
//! [`State::clients_now_ms`], is the median client timestamp of the last
//! [`CLOCK_WINDOW`] committed entries, so one client with a wrong clock
//! cannot move it; the snapshot carries it for planning. Pruning is the one
//! change apply makes on its own, and it removes only what expired well
//! before that time. A plan computed before a prune and committed after it
//! must not bring pruned items back, so apply ignores writes of anything
//! that expired by the last prune's cut. Every node therefore holds the same
//! state after the same entries, with no local clock involved.

use crate::planner::{expiry, release_expired};
use proto::{
    Assignment, Entry, EntryKind, Incident, IncidentId, IncidentStatus, OpResult, Query, Request,
    ResourceId, Seq, Snapshot, StateMachine, Version, Write, ZoneId,
};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;

/// Expired items are dropped this long after they expire, measured on the
/// clients' clocks, which may disagree by this much.
pub const PRUNE_SLACK_MS: u64 = 60_000;
/// Pruning runs at every entry whose `seq` is a multiple of this, so every
/// node prunes at the same log positions.
pub const PRUNE_EVERY: u64 = 1_024;
/// Committed entries whose client timestamps give the clients' clock (odd,
/// so the median is one of them). Moving the median takes 16 of the last 31
/// entries from wrong clocks. A longer window resists longer bursts but
/// lags further behind real time, by about half the window's span: 31
/// entries lag 1.5 s at 10 requests/s, inside the planner's
/// `CLOCK_SLACK_MS`, where 255 entries lagged 13 s and held back every
/// hold's release by about that much.
pub const CLOCK_WINDOW: usize = 31;

#[derive(Debug, Default)]
pub struct State {
    snap: Arc<Snapshot>,
    incidents: BTreeMap<IncidentId, Incident>,
    /// The incidents of each zone, so a zone query reads only its own.
    by_zone: BTreeMap<ZoneId, BTreeSet<IncidentId>>,
    assignments: BTreeMap<(IncidentId, ResourceId), Assignment>,
    /// The client timestamp of each of the last [`CLOCK_WINDOW`] committed
    /// entries that carried one (the latest of its writes). Pruning compares
    /// against their median, never against a local clock, so it is
    /// deterministic, and a minority of clients with wrong clocks cannot
    /// move it.
    recent_ms: VecDeque<u64>,
    /// The last prune's cut. Anything that expired by then is gone, and
    /// writes of it from plans computed before the prune are ignored.
    pruned_cut: u64,
    /// Last applied entry. The snapshot's own `applied` is the last entry
    /// that changed it: entries without writes leave the snapshot alone, so
    /// they never force a copy while planning jobs hold it.
    applied: Seq,
}

/// `i` as seen at `now`: `Expired` once its TTL has run out, unless it was
/// resolved first.
fn as_of(i: &Incident, now: u64) -> Incident {
    let mut i = i.clone();
    if expiry(&i) <= now && i.status != IncidentStatus::Resolved {
        i.status = IncidentStatus::Expired;
    }
    i
}

/// The median of `recent`, 0 if it is empty.
fn median(recent: &VecDeque<u64>) -> u64 {
    let mut v: Vec<u64> = recent.iter().copied().collect();
    if v.is_empty() {
        return 0;
    }
    let mid = v.len() / 2;
    *v.select_nth_unstable(mid).1
}

impl State {
    /// The clients' time as committed: the median of the recent entries'
    /// client timestamps (0 before any). Also `Snapshot::clients_now_ms`.
    pub fn clients_now_ms(&self) -> u64 {
        median(&self.recent_ms)
    }

    pub fn new() -> State {
        State::default()
    }

    /// Every incident not yet pruned, as last written.
    pub fn incidents(&self) -> &BTreeMap<IncidentId, Incident> {
        &self.incidents
    }

    /// Every assignment not yet pruned.
    pub fn assignments(&self) -> &BTreeMap<(IncidentId, ResourceId), Assignment> {
        &self.assignments
    }

    /// Drops incidents, assignments and resources that expired more than
    /// [`PRUNE_SLACK_MS`] before the latest committed client timestamp.
    /// Resource holds are never touched here: their units go back only in
    /// a plan's writes (see `planner`), and a pruned resource takes its holds
    /// with it.
    fn prune(&mut self, seq: Seq) {
        if self.recent_ms.is_empty() {
            return;
        }
        let cut = self.clients_now_ms().saturating_sub(PRUNE_SLACK_MS);
        self.pruned_cut = self.pruned_cut.max(cut);
        let gone = |i: &Incident| expiry(i) <= cut;
        // Copy the snapshot only if something in it will go.
        let stale = self.snap.incidents.values().any(|(i, _)| gone(i))
            || self
                .snap
                .resources
                .values()
                .any(|(r, _)| r.expires_at_ms <= cut);
        if stale {
            let s = Arc::make_mut(&mut self.snap);
            s.applied = seq;
            s.incidents.retain(|_, (i, _)| !gone(i));
            s.resources.retain(|_, (r, _)| r.expires_at_ms > cut);
        }
        let by_zone = &mut self.by_zone;
        self.incidents.retain(|id, i| {
            let keep = !gone(i);
            if !keep && let Some(ids) = by_zone.get_mut(&i.zone) {
                ids.remove(id);
                if ids.is_empty() {
                    by_zone.remove(&i.zone);
                }
            }
            keep
        });
        self.assignments.retain(|_, a| a.expires_at_ms > cut);
    }
}

impl StateMachine for State {
    fn apply(&mut self, entry: &Entry) {
        let version = Version::from(entry.seq);
        self.applied = entry.seq;
        if let EntryKind::Result { writes, .. } = &entry.kind
            && !writes.is_empty()
        {
            // Copy-on-write: copies the snapshot only if a job still holds it.
            let s = Arc::make_mut(&mut self.snap);
            s.applied = entry.seq;
            let mut stamp = 0;
            let cut = self.pruned_cut;
            for w in writes {
                match w {
                    // Already pruned: a plan from before the prune.
                    Write::PutIncident(i) if expiry(i) <= cut => {
                        stamp = stamp.max(i.reported_at_ms);
                    }
                    Write::PutResource(r) if r.expires_at_ms <= cut => {}
                    Write::PutAssignment(a) if a.expires_at_ms <= cut => {
                        stamp = stamp.max(a.created_at_ms);
                    }
                    Write::PutIncident(i) => {
                        stamp = stamp.max(i.reported_at_ms);
                        if i.status == IncidentStatus::Open {
                            s.incidents.insert(i.id, (i.clone(), version));
                        } else {
                            s.incidents.remove(&i.id);
                        }
                        // Zones never change, but keep the index right if one did.
                        if let Some(old) = self.incidents.insert(i.id, i.clone())
                            && old.zone != i.zone
                            && let Some(ids) = self.by_zone.get_mut(&old.zone)
                        {
                            ids.remove(&i.id);
                        }
                        self.by_zone.entry(i.zone).or_default().insert(i.id);
                    }
                    Write::PutResource(r) => {
                        s.resources.insert(r.id, (r.clone(), version));
                    }
                    Write::PutAssignment(a) => {
                        stamp = stamp.max(a.created_at_ms);
                        self.assignments.insert((a.incident, a.resource), a.clone());
                    }
                }
            }
            if stamp > 0 {
                if self.recent_ms.len() == CLOCK_WINDOW {
                    self.recent_ms.pop_front();
                }
                self.recent_ms.push_back(stamp);
                s.clients_now_ms = median(&self.recent_ms);
            }
        }
        if entry.seq.0.is_multiple_of(PRUNE_EVERY) {
            self.prune(entry.seq);
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
        self.applied
    }

    /// Answers as of the request's own timestamp: an incident past its TTL is
    /// `Expired`, a zone lists only incidents still live, and a resource
    /// shows the units free at that time, expired holds released. An
    /// incident or resource already pruned is unknown (`None`).
    fn query(&self, req: &Request) -> OpResult {
        let now = req.sent_at_ms;
        match &req.op {
            proto::Op::Query(Query::Incident(id)) => {
                OpResult::Incident(self.incidents.get(id).map(|i| as_of(i, now)))
            }
            proto::Op::Query(Query::IncidentsInZone(z)) => OpResult::Incidents(
                self.by_zone
                    .get(z)
                    .into_iter()
                    .flatten()
                    .filter_map(|id| self.incidents.get(id))
                    .filter(|i| expiry(i) > now)
                    .cloned()
                    .collect(),
            ),
            proto::Op::Query(Query::Resource(id)) => OpResult::Resource(
                self.snap
                    .resources
                    .get(id)
                    .map(|(r, _)| release_expired(r.clone(), now)),
            ),
            _ => OpResult::Refused("not a query".into()),
        }
    }
}
