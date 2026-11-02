//! The planner's promises, checked through the state machine: plans are
//! deterministic, never hand out more units than exist, favour the more
//! urgent incident, respect responder capability, and pick up an incident
//! that found no help when it was reported.

use incident::planner::{capable, units_needed};
use incident::{IncidentPlanner, State};
use proto::{
    Category, ClientId, Entry, EntryKind, IncidentStatus, NodeId, Op, OpResult, PlanOutput,
    Planner, Query, ReqId, Request, ResourceKind, Seq, Severity, Snapshot, StateMachine, Version,
    Write, ZoneId,
};
use std::collections::BTreeMap;

fn rid(user: u32, seq: u64) -> ReqId {
    ReqId {
        client: ClientId {
            machine: 1,
            process: 1,
            user,
        },
        incarnation: 7,
        seq,
    }
}

fn req(id: ReqId, at: u64, op: Op) -> Request {
    Request {
        id,
        sent_at_ms: at,
        op,
    }
}

fn offer(kind: ResourceKind, capacity: u32, zone: u16) -> Op {
    Op::OfferResource {
        kind,
        capacity,
        zone: ZoneId(zone),
        lease_ms: 300_000,
    }
}

fn report(category: Category, severity: u8, zone: u16) -> Op {
    Op::ReportIncident {
        category,
        zone: ZoneId(zone),
        severity: Severity(severity),
        ttl_ms: 600_000,
        attachment: None,
    }
}

/// A single node: plans and commits each request in turn, as the
/// coordinator would when nothing runs concurrently.
struct Node {
    planner: IncidentPlanner,
    state: State,
    seq: u64,
    /// Version of every object, as the coordinator tracks it.
    versions: BTreeMap<ReqId, Version>,
}

impl Node {
    fn new(top_k: u32) -> Node {
        Node {
            planner: IncidentPlanner::new(30, top_k),
            state: State::new(),
            seq: 0,
            versions: BTreeMap::new(),
        }
    }

    /// The coordinator's read-set check (protocol step 7).
    fn is_current(&self, out: &PlanOutput) -> bool {
        out.read_set
            .iter()
            .all(|(id, v)| self.versions.get(id).copied().unwrap_or(Version(0)) == *v)
    }

    fn commit(&mut self, r: &Request, out: &PlanOutput) {
        assert!(self.is_current(out), "plan was stale");
        self.seq += 1;
        for w in &out.writes {
            let id = match w {
                Write::PutIncident(i) => Some(i.id),
                Write::PutResource(r) => Some(r.id),
                Write::PutAssignment(_) => None,
            };
            if let Some(id) = id {
                self.versions.insert(id, Version(self.seq));
            }
        }
        self.state.apply(&Entry {
            seq: Seq(self.seq),
            epoch: proto::Epoch(1),
            kind: EntryKind::Result {
                req: r.id,
                executor: NodeId(1),
                writes: out.writes.clone(),
                result: out.result.clone(),
            },
        });
    }

    fn run(&mut self, r: Request) -> PlanOutput {
        let out = self.planner.plan(&self.state.snapshot(), &r);
        self.commit(&r, &out);
        out
    }
}

fn planned(out: &PlanOutput) -> &proto::Plan {
    match &out.result {
        OpResult::Planned(p) => p,
        other => panic!("expected a plan, got {other:?}"),
    }
}

#[test]
fn the_same_snapshot_and_request_always_give_the_same_plan() {
    let mut n = Node::new(50);
    for u in 0..40 {
        let kind = [
            ResourceKind::Medic,
            ResourceKind::Technician,
            ResourceKind::Volunteer,
        ][u % 3];
        n.run(req(
            rid(u as u32, 1),
            1_000,
            offer(kind, 2, (u * 7 % 30) as u16),
        ));
    }
    for u in 0..60 {
        n.run(req(
            rid(100 + u, 1),
            2_000 + u64::from(u),
            report(Category::Flooding, 5, (u % 30) as u16),
        ));
    }
    let r = req(
        rid(999, 1),
        9_000,
        report(Category::MedicalAssistance, 4, 3),
    );
    let snap = n.state.snapshot();
    let first = n.planner.plan(&snap, &r);
    // A different planner instance, and a deep copy of the snapshot.
    let other = IncidentPlanner::new(30, 50);
    let copy: Snapshot = (*snap).clone();
    for _ in 0..5 {
        assert_eq!(other.plan(&copy, &r), first);
    }
}

#[test]
fn no_resource_ever_gives_out_more_units_than_it_has() {
    let mut n = Node::new(20);
    for u in 0..10 {
        n.run(req(
            rid(u, 1),
            0,
            offer(ResourceKind::Medic, 3, (u * 3) as u16),
        ));
    }
    for u in 0..200 {
        n.run(req(
            rid(1_000 + u, 1),
            u64::from(u) * 50,
            report(
                Category::MedicalAssistance,
                (1 + u % 5) as u8,
                (u % 30) as u16,
            ),
        ));
    }
    let snap = n.state.snapshot();
    let mut given: BTreeMap<ReqId, u32> = BTreeMap::new();
    for a in n.state.assignments().values() {
        *given.entry(a.resource).or_default() += a.units;
    }
    for (id, (r, _)) in &snap.resources {
        assert_eq!(
            given.get(id).copied().unwrap_or(0) + r.available,
            r.capacity,
            "{id}"
        );
    }
    // Every assigned incident got exactly one responder.
    let mut per_incident: BTreeMap<ReqId, u32> = BTreeMap::new();
    for (i, _) in n.state.assignments().keys() {
        *per_incident.entry(*i).or_default() += 1;
    }
    assert!(per_incident.values().all(|&c| c == 1));
}

#[test]
fn when_two_incidents_want_one_responder_the_more_urgent_wins() {
    let mut n = Node::new(10);
    // A minor incident waits with no one available.
    let minor = rid(1, 1);
    n.run(req(minor, 0, report(Category::NetworkOutage, 1, 0)));
    n.run(req(rid(2, 1), 100, offer(ResourceKind::Technician, 1, 0)));
    // A critical one arrives far away: it gets the only technician.
    let critical = req(rid(3, 1), 200, report(Category::NetworkOutage, 5, 15));
    let out = n.run(critical.clone());
    let p = planned(&out);
    assert_eq!(p.assignments.len(), 1);
    assert_eq!(p.priority, 5_000);
    let q = n
        .state
        .query(&req(rid(4, 1), 300, Op::Query(Query::Incident(minor))));
    assert!(matches!(q, OpResult::Incident(Some(i)) if i.status == IncidentStatus::Open));
}

#[test]
fn a_waiting_incident_is_picked_up_by_a_later_plan() {
    let mut n = Node::new(10);
    let waiting = rid(1, 1);
    n.run(req(waiting, 0, report(Category::PowerOutage, 3, 4)));
    n.run(req(rid(2, 1), 100, offer(ResourceKind::Generator, 4, 4)));
    // An unrelated report re-dispatches, and the generator goes to the waiting incident.
    let out = n.run(req(rid(3, 1), 200, report(Category::LabIncident, 1, 9)));
    assert!(out.writes.iter().any(|w| matches!(w, Write::PutIncident(i) if i.id == waiting && i.status == IncidentStatus::Assigned)));
    let a = &n.state.assignments()[&(waiting, rid(2, 1))];
    assert_eq!(a.units, units_needed(Severity(3)));
}

#[test]
fn only_capable_kinds_are_assigned_and_the_route_ends_at_the_incident() {
    let mut n = Node::new(10);
    n.run(req(rid(1, 1), 0, offer(ResourceKind::Generator, 5, 0)));
    // Only technicians fix a network outage: the generator is no use.
    let none = n.run(req(rid(2, 1), 10, report(Category::NetworkOutage, 2, 0)));
    assert!(planned(&none).assignments.is_empty());
    n.run(req(rid(3, 1), 20, offer(ResourceKind::FirstAidKit, 5, 20)));
    let out = n.run(req(
        rid(4, 1),
        30,
        report(Category::MedicalAssistance, 2, 11),
    ));
    let p = planned(&out);
    assert_eq!(p.assignments[0].resource, rid(3, 1));
    // The responder list leads with the one assigned and never lists the generator.
    assert_eq!(p.responders, vec![rid(3, 1)]);
    assert!(capable(Category::MedicalAssistance).contains(&ResourceKind::FirstAidKit));
    assert_eq!(
        (p.route[0], *p.route.last().unwrap()),
        (ZoneId(20), ZoneId(11))
    );
}

#[test]
fn two_plans_from_one_snapshot_that_share_a_resource_cannot_both_commit() {
    let mut n = Node::new(10);
    n.run(req(rid(1, 1), 0, offer(ResourceKind::Technician, 1, 0)));
    let snap = n.state.snapshot();
    let a = req(rid(2, 1), 10, report(Category::NetworkOutage, 2, 0));
    let b = req(rid(3, 1), 10, report(Category::NetworkOutage, 2, 1));
    let (pa, pb) = (n.planner.plan(&snap, &a), n.planner.plan(&snap, &b));
    n.commit(&a, &pa);
    assert!(
        !n.is_current(&pb),
        "the second plan must be rejected as stale"
    );
    // Re-planned from fresh state it commits, and finds the technician taken.
    let pb = n.planner.plan(&n.state.snapshot(), &b);
    assert!(planned(&pb).assignments.is_empty());
    n.commit(&b, &pb);
}

#[test]
fn bad_requests_are_refused_not_planned() {
    let n = Node::new(10);
    let snap = n.state.snapshot();
    let refused = |op| {
        matches!(
            n.planner.plan(&snap, &req(rid(1, 1), 0, op)).result,
            OpResult::Refused(_)
        )
    };
    assert!(refused(report(Category::Flooding, 0, 1)));
    assert!(refused(report(Category::Flooding, 6, 1)));
    assert!(refused(report(Category::Flooding, 3, 30)));
    assert!(refused(offer(ResourceKind::Medic, 0, 1)));
    assert!(refused(Op::UpdateResource {
        resource: rid(9, 9),
        available: 1,
        extend_lease_ms: None
    }));
    assert!(refused(Op::Query(Query::Resource(rid(9, 9)))));
}

#[test]
fn only_the_owner_updates_a_resource_and_availability_never_exceeds_capacity() {
    let mut n = Node::new(10);
    let r = rid(1, 1);
    n.run(req(r, 0, offer(ResourceKind::Vehicle, 3, 2)));
    let update = |available| Op::UpdateResource {
        resource: r,
        available,
        extend_lease_ms: Some(1_000),
    };
    let theirs = n
        .planner
        .plan(&n.state.snapshot(), &req(rid(2, 1), 10, update(1)));
    assert!(matches!(theirs.result, OpResult::Refused(_)));
    n.run(req(rid(1, 2), 10, update(50)));
    let q = n
        .state
        .query(&req(rid(1, 3), 20, Op::Query(Query::Resource(r))));
    assert!(
        matches!(q, OpResult::Resource(Some(o)) if o.available == 3 && o.expires_at_ms == 301_000)
    );
}

#[test]
fn the_snapshot_holds_only_open_incidents_and_drops_expired_ones() {
    let mut n = Node::new(10);
    n.run(req(rid(1, 1), 0, offer(ResourceKind::Medic, 9, 0)));
    n.run(req(rid(2, 1), 0, report(Category::MedicalAssistance, 1, 0))); // assigned
    n.run(req(rid(3, 1), 0, report(Category::NetworkOutage, 1, 0))); // waits
    let snap = n.state.snapshot();
    assert_eq!(
        snap.incidents.keys().copied().collect::<Vec<_>>(),
        vec![rid(3, 1)]
    );
    assert_eq!(n.state.version_of(rid(3, 1)), Some(Version(3)));
    // Much later, past expiry plus slack, the log reaches a pruning position.
    let late = 600_000 + incident::state::PRUNE_SLACK_MS + 1;
    while n.seq + 1 < incident::state::PRUNE_EVERY {
        n.run(req(
            rid(4, n.seq),
            late,
            offer(ResourceKind::Volunteer, 1, 1),
        ));
    }
    n.run(req(rid(5, 1), late, report(Category::Flooding, 1, 1)));
    assert!(!n.state.snapshot().incidents.contains_key(&rid(3, 1)));
    assert!(!n.state.snapshot().resources.contains_key(&rid(1, 1)));
    // Queries still answer from full state.
    let q = n
        .state
        .query(&req(rid(6, 1), late, Op::Query(Query::Incident(rid(3, 1)))));
    assert!(matches!(q, OpResult::Incident(Some(_))));
}

/// The cost knob: planning time grows with `top_k`. Run with
/// `cargo test -p incident --release -- --ignored --nocapture`.
#[test]
#[ignore]
fn service_time_against_top_k() {
    for k in [10u32, 25, 50, 100, 200] {
        let mut n = Node::new(k);
        for u in 0..300 {
            let kind = [
                ResourceKind::Medic,
                ResourceKind::Technician,
                ResourceKind::Volunteer,
                ResourceKind::Vehicle,
            ][u % 4];
            n.run(req(rid(u as u32, 1), 0, offer(kind, 1, (u % 30) as u16)));
        }
        for u in 0..400 {
            // Only generators help a power outage, and there are none, so these wait.
            n.run(req(
                rid(10_000 + u, 1),
                0,
                report(Category::PowerOutage, (1 + u % 5) as u8, (u % 30) as u16),
            ));
        }
        let snap = n.state.snapshot();
        let r = req(rid(99_999, 1), 1, report(Category::Flooding, 3, 7));
        let t = std::time::Instant::now();
        let reps = 20;
        for _ in 0..reps {
            std::hint::black_box(n.planner.plan(&snap, &r));
        }
        println!(
            "top_k {k:>3}: {:>8.2} ms per plan",
            t.elapsed().as_secs_f64() * 1e3 / f64::from(reps)
        );
    }
}
