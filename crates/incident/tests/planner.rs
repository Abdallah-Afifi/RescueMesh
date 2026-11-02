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
        // Nothing has expired yet, so every assignment still holds its units.
        let held: u32 = r.holds.iter().map(|h| h.units).sum();
        assert_eq!(given.get(id).copied().unwrap_or(0), held, "{id}");
        assert_eq!(held + r.available, r.capacity, "{id}");
        for h in &r.holds {
            assert_eq!(n.state.assignments()[&(h.incident, *id)].units, h.units);
        }
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
    // Both wait with no technician; the minor one has waited longer.
    let minor = rid(1, 1);
    let critical = rid(3, 1);
    n.run(req(minor, 0, report(Category::NetworkOutage, 1, 0)));
    n.run(req(critical, 100, report(Category::NetworkOutage, 5, 15)));
    // One technician appears near the minor incident: the critical one gets it.
    let out = n.run(req(rid(2, 1), 200, offer(ResourceKind::Technician, 1, 0)));
    assert!(out.writes.iter().any(
        |w| matches!(w, Write::PutIncident(i) if i.id == critical && i.status == IncidentStatus::Assigned)
    ));
    let q = n
        .state
        .query(&req(rid(4, 1), 300, Op::Query(Query::Incident(minor))));
    assert!(matches!(q, OpResult::Incident(Some(i)) if i.status == IncidentStatus::Open));
}

#[test]
fn a_waiting_incident_is_picked_up_when_capacity_is_offered() {
    let mut n = Node::new(10);
    let waiting = rid(1, 1);
    n.run(req(waiting, 0, report(Category::PowerOutage, 3, 4)));
    // The offer itself dispatches the generator to the waiting incident.
    let out = n.run(req(rid(2, 1), 100, offer(ResourceKind::Generator, 4, 4)));
    assert!(out.writes.iter().any(|w| matches!(w, Write::PutIncident(i) if i.id == waiting && i.status == IncidentStatus::Assigned)));
    let a = &n.state.assignments()[&(waiting, rid(2, 1))];
    assert_eq!(a.units, units_needed(Severity(3)));
}

#[test]
fn a_report_writes_only_its_own_incident_and_resource() {
    let mut n = Node::new(10);
    // Two incidents wait for a generator nobody has offered.
    n.run(req(rid(1, 1), 0, report(Category::PowerOutage, 2, 4)));
    n.run(req(rid(1, 2), 10, report(Category::PowerOutage, 4, 5)));
    n.run(req(rid(2, 1), 20, offer(ResourceKind::Medic, 2, 9)));
    // A new medical report: it may only touch itself and the medic, so it
    // never collides with another report over the waiting incidents.
    let new = rid(3, 1);
    let out = n.run(req(new, 30, report(Category::MedicalAssistance, 2, 9)));
    for w in &out.writes {
        match w {
            Write::PutIncident(i) => assert_eq!(i.id, new),
            Write::PutResource(r) => assert_eq!(r.id, rid(2, 1)),
            Write::PutAssignment(a) => assert_eq!(a.incident, new),
        }
    }
}

#[test]
fn an_update_never_frees_units_that_are_assigned() {
    let mut n = Node::new(10);
    let vehicle = rid(1, 1);
    n.run(req(vehicle, 0, offer(ResourceKind::Vehicle, 3, 2)));
    // A critical road block takes all three units.
    n.run(req(rid(2, 1), 10, report(Category::BlockedRoad, 5, 2)));
    // The owner says it can offer 3 again: the 3 assigned units stay assigned.
    n.run(req(
        rid(1, 2),
        20,
        Op::UpdateResource {
            resource: vehicle,
            available: 3,
            extend_lease_ms: None,
        },
    ));
    let out = n.run(req(rid(3, 1), 30, report(Category::BlockedRoad, 5, 2)));
    assert!(
        !out.writes
            .iter()
            .any(|w| matches!(w, Write::PutAssignment(a) if a.resource == vehicle)),
        "the second incident must not get units already given out"
    );
    let given: u32 = n
        .state
        .assignments()
        .values()
        .filter(|a| a.resource == vehicle)
        .map(|a| a.units)
        .sum();
    assert!(given <= 3, "{given} units handed out from a capacity of 3");
}

#[test]
fn an_update_that_frees_capacity_dispatches_waiting_incidents() {
    let mut n = Node::new(10);
    let vehicle = rid(1, 1);
    n.run(req(vehicle, 0, offer(ResourceKind::Vehicle, 3, 2)));
    // The owner withdraws all units; a flood then has to wait.
    n.run(req(
        rid(1, 2),
        10,
        Op::UpdateResource {
            resource: vehicle,
            available: 0,
            extend_lease_ms: None,
        },
    ));
    let flood = rid(2, 1);
    n.run(req(flood, 20, report(Category::Flooding, 3, 2)));
    // Units come back: the update itself assigns them to the flood.
    let out = n.run(req(
        rid(1, 3),
        30,
        Op::UpdateResource {
            resource: vehicle,
            available: 3,
            extend_lease_ms: None,
        },
    ));
    assert!(out.writes.iter().any(|w| matches!(w, Write::PutIncident(i) if i.id == flood && i.status == IncidentStatus::Assigned)));
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
    // The owner now offers 50 units in total; none were assigned.
    assert!(
        matches!(q, OpResult::Resource(Some(o)) if o.available == 50 && o.capacity == 50 && o.expires_at_ms == 301_000)
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
    // Much later, past expiry plus slack, the log nears a pruning position.
    let late = 600_000 + incident::state::PRUNE_SLACK_MS + 1;
    while n.seq + 2 < incident::state::PRUNE_EVERY {
        n.run(req(
            rid(4, n.seq),
            late,
            offer(ResourceKind::Volunteer, 1, 1),
        ));
    }
    let live = rid(5, 1);
    n.run(req(live, late, report(Category::NetworkOutage, 2, 0)));
    // Not a pruning position yet: everything is still there.
    assert!(n.state.incidents().contains_key(&rid(3, 1)));
    assert!(n.state.snapshot().incidents.contains_key(&rid(3, 1)));
    n.run(req(rid(5, 2), late, report(Category::Flooding, 1, 1)));
    assert_eq!(n.seq, incident::state::PRUNE_EVERY);
    // Pruned everywhere: the snapshot, full state and the zone index.
    assert!(!n.state.snapshot().incidents.contains_key(&rid(3, 1)));
    assert!(!n.state.snapshot().resources.contains_key(&rid(1, 1)));
    assert!(!n.state.incidents().contains_key(&rid(2, 1)));
    assert!(!n.state.incidents().contains_key(&rid(3, 1)));
    assert!(n.state.assignments().keys().all(|(i, _)| *i != rid(2, 1)));
    let q = |op| n.state.query(&req(rid(6, 1), late, Op::Query(op)));
    assert_eq!(q(Query::Incident(rid(3, 1))), OpResult::Incident(None));
    assert!(
        matches!(q(Query::Incident(live)), OpResult::Incident(Some(i)) if i.status == IncidentStatus::Open)
    );
    assert!(
        matches!(q(Query::IncidentsInZone(ZoneId(0))), OpResult::Incidents(v) if v.len() == 1 && v[0].id == live)
    );
}

#[test]
fn one_client_with_a_fast_clock_cannot_make_the_nodes_prune_live_data() {
    // Pruning follows the median client timestamp of recent entries, not the
    // latest: a few writes stamped a day ahead must not expire everything.
    let mut n = Node::new(10);
    let live = rid(1, 1);
    n.run(req(live, 1_000, report(Category::NetworkOutage, 2, 0)));
    let day = 86_400_000;
    while n.seq < incident::state::PRUNE_EVERY {
        let fast = n.seq.is_multiple_of(10);
        let at = if fast { day } else { 2_000 + n.seq };
        n.run(req(
            rid(2, n.seq),
            at,
            report(Category::Flooding, 1, 1 + (n.seq % 5) as u16),
        ));
    }
    assert!(
        n.state.clients_now_ms() < 10_000,
        "{}",
        n.state.clients_now_ms()
    );
    assert!(n.state.incidents().contains_key(&live));
    assert!(n.state.snapshot().incidents.contains_key(&live));
}

/// A report with ttl `ttl_ms`, otherwise like [`report`].
fn report_for(category: Category, severity: u8, zone: u16, ttl_ms: u64) -> Op {
    Op::ReportIncident {
        category,
        zone: ZoneId(zone),
        severity: Severity(severity),
        ttl_ms,
        attachment: None,
    }
}

#[test]
fn a_client_with_a_fast_clock_cannot_release_a_live_hold() {
    let mut n = Node::new(10);
    let medic = rid(1, 1);
    n.run(req(medic, 0, offer(ResourceKind::Medic, 1, 0)));
    // Held until 61 000 by the committed clients' clock.
    let a = rid(2, 1);
    n.run(req(
        a,
        1_000,
        report_for(Category::MedicalAssistance, 1, 0, 60_000),
    ));
    // Honest clients carry the clock to about 2 000.
    for k in 0..20 {
        n.run(req(
            rid(3, k),
            1_000 + k * 50,
            report(Category::NetworkOutage, 1, 5),
        ));
    }
    // A client whose clock reads 62 000 must not see the hold as expired.
    let b = rid(4, 1);
    let out = n.run(req(
        b,
        62_000,
        report_for(Category::MedicalAssistance, 1, 0, 60_000),
    ));
    assert!(
        planned(&out).assignments.is_empty(),
        "{:?}",
        planned(&out).assignments
    );
    let r = &n.state.snapshot().resources[&medic].0;
    assert_eq!(r.holds.len(), 1);
    assert_eq!(r.holds[0].incident, a);
}

#[test]
fn a_client_with_a_slow_clock_cannot_dispatch_to_expired_incidents() {
    let mut n = Node::new(10);
    // Expired at 1 000 by everyone's clock.
    let w = rid(1, 1);
    n.run(req(w, 0, report_for(Category::NetworkOutage, 2, 0, 1_000)));
    // Honest clients carry the clock to 100 000 (technicians can't help them).
    for k in 0..20 {
        n.run(req(
            rid(2, k),
            100_000 + k,
            report(Category::MedicalAssistance, 1, 1),
        ));
    }
    // A technician offered by a client whose clock reads 500.
    let out = n.run(req(rid(3, 1), 500, offer(ResourceKind::Technician, 1, 0)));
    assert!(
        !out.writes
            .iter()
            .any(|x| matches!(x, Write::PutAssignment(a) if a.incident == w)),
        "{:?}",
        out.writes
    );
}

#[test]
fn a_stale_plan_committed_after_a_prune_does_not_bring_back_pruned_items() {
    let mut n = Node::new(10);
    let w = rid(1, 1);
    n.run(req(
        w,
        100,
        report_for(Category::NetworkOutage, 2, 0, 1_000),
    ));
    // Planned while `w` was still waiting, at time 500.
    let snap = n.state.snapshot();
    let tech = rid(5, 1);
    let o = req(tech, 500, offer(ResourceKind::Technician, 1, 0));
    let stale = n.planner.plan(&snap, &o);
    assert!(
        stale
            .writes
            .iter()
            .any(|x| matches!(x, Write::PutIncident(i) if i.id == w))
    );
    // Much later the log reaches a pruning position and `w` is pruned.
    while n.seq < incident::state::PRUNE_EVERY {
        n.run(req(
            rid(4, n.seq),
            100_000 + n.seq,
            report(Category::Flooding, 1, 1),
        ));
    }
    assert!(!n.state.incidents().contains_key(&w));
    // The stale plan still passes the version check (nothing it read
    // changed), but committing it must not bring `w` back.
    assert!(n.is_current(&stale));
    n.commit(&o, &stale);
    assert!(!n.state.incidents().contains_key(&w));
    assert!(!n.state.snapshot().incidents.contains_key(&w));
    assert!(n.state.assignments().keys().all(|(i, _)| *i != w));
    // The live technician is kept.
    assert!(n.state.snapshot().resources.contains_key(&tech));
}

#[test]
fn settled_resources_go_to_the_incidents_the_solve_paired_them_with() {
    let mut n = Node::new(10);
    let (vol, veh) = (rid(1, 1), rid(2, 1));
    n.run(req(vol, 0, offer(ResourceKind::Volunteer, 1, 0)));
    n.run(req(veh, 0, offer(ResourceKind::Vehicle, 1, 0)));
    // Both held until 1 000.
    n.run(req(
        rid(3, 1),
        0,
        report_for(Category::MedicalAssistance, 1, 0, 1_000),
    ));
    n.run(req(
        rid(3, 2),
        0,
        report_for(Category::BlockedRoad, 1, 0, 1_000),
    ));
    assert!(n.state.assignments().contains_key(&(rid(3, 1), vol)));
    assert!(n.state.assignments().contains_key(&(rid(3, 2), veh)));
    // Only the volunteer can help the medical incident; the road block can
    // use either, and is the more urgent.
    let (med, road) = (rid(4, 1), rid(4, 2));
    n.run(req(med, 100, report(Category::MedicalAssistance, 3, 0)));
    n.run(req(road, 100, report(Category::BlockedRoad, 5, 0)));
    // A report nobody can serve, after both holds expired: its solve pairs
    // the vehicle with the road block and the volunteer with the medical
    // incident, and the dispatch must follow that pairing.
    n.run(req(rid(5, 1), 2_000, report(Category::PowerOutage, 1, 0)));
    assert!(
        n.state.assignments().contains_key(&(med, vol)),
        "medical incident starved"
    );
    assert!(n.state.assignments().contains_key(&(road, veh)));
}

#[test]
fn returned_units_reach_a_waiting_incident_outside_the_top_rows() {
    // top_k 3: the two rows after the new incident would go to two critical
    // power outages that nothing on campus can serve.
    let mut n = Node::new(3);
    let medic = rid(1, 1);
    n.run(req(medic, 0, offer(ResourceKind::Medic, 1, 0)));
    n.run(req(
        rid(2, 1),
        0,
        report_for(Category::MedicalAssistance, 1, 0, 1_000),
    ));
    let old = rid(3, 1);
    n.run(req(old, 100, report(Category::MedicalAssistance, 4, 0))); // waits
    n.run(req(rid(4, 1), 200, report(Category::PowerOutage, 5, 3)));
    n.run(req(rid(4, 2), 200, report(Category::PowerOutage, 5, 4)));
    // After the hold expires, an unrelated report dispatches the medic to
    // the waiting medical incident...
    n.run(req(rid(5, 1), 2_000, report(Category::NetworkOutage, 1, 9)));
    assert!(n.state.assignments().contains_key(&(old, medic)));
    // ...so a newer, less urgent medical report does not get it first.
    let out = n.run(req(
        rid(6, 1),
        3_000,
        report(Category::MedicalAssistance, 1, 0),
    ));
    assert!(planned(&out).assignments.is_empty());
}

#[test]
fn at_top_k_1_returned_units_still_reach_waiting_incidents() {
    // With one row, the solve never sees a waiting incident at all.
    let mut n = Node::new(1);
    let medic = rid(1, 1);
    n.run(req(medic, 0, offer(ResourceKind::Medic, 1, 0)));
    n.run(req(
        rid(2, 1),
        0,
        report_for(Category::MedicalAssistance, 1, 0, 1_000),
    ));
    let old = rid(3, 1);
    n.run(req(old, 100, report(Category::MedicalAssistance, 4, 0))); // waits
    n.run(req(rid(5, 1), 2_000, report(Category::NetworkOutage, 1, 9)));
    assert!(n.state.assignments().contains_key(&(old, medic)));
}

#[test]
fn a_query_reports_an_incident_past_its_ttl_as_expired() {
    let mut n = Node::new(10);
    n.run(req(rid(1, 1), 0, offer(ResourceKind::Medic, 9, 0)));
    let ttl = |op: Op| match op {
        Op::ReportIncident {
            category,
            zone,
            severity,
            attachment,
            ..
        } => Op::ReportIncident {
            category,
            zone,
            severity,
            ttl_ms: 1_000,
            attachment,
        },
        other => other,
    };
    let (assigned, waiting) = (rid(2, 1), rid(3, 1));
    n.run(req(
        assigned,
        0,
        ttl(report(Category::MedicalAssistance, 1, 0)),
    ));
    n.run(req(waiting, 0, ttl(report(Category::NetworkOutage, 1, 0))));
    let status = |id, at| match n
        .state
        .query(&req(rid(9, 1), at, Op::Query(Query::Incident(id))))
    {
        OpResult::Incident(Some(i)) => i.status,
        other => panic!("{other:?}"),
    };
    assert_eq!(status(assigned, 999), IncidentStatus::Assigned);
    assert_eq!(status(waiting, 999), IncidentStatus::Open);
    assert_eq!(status(assigned, 1_000), IncidentStatus::Expired);
    assert_eq!(status(waiting, 1_000), IncidentStatus::Expired);
    // The zone lists only incidents still live at the query's time.
    let zone = |at| {
        n.state.query(&req(
            rid(9, 2),
            at,
            Op::Query(Query::IncidentsInZone(ZoneId(0))),
        ))
    };
    assert!(matches!(zone(999), OpResult::Incidents(v) if v.len() == 2));
    assert_eq!(zone(1_000), OpResult::Incidents(vec![]));
}

#[test]
fn units_come_back_when_an_assignment_expires() {
    let mut n = Node::new(10);
    let medic = rid(1, 1);
    n.run(req(medic, 0, offer(ResourceKind::Medic, 2, 0)));
    let short = |sev| Op::ReportIncident {
        category: Category::MedicalAssistance,
        zone: ZoneId(0),
        severity: Severity(sev),
        ttl_ms: 1_000,
        attachment: None,
    };
    // Takes both units until time 1 000.
    let first = n.run(req(rid(2, 1), 0, short(3)));
    assert_eq!(planned(&first).assignments[0].units, 2);
    let none = n.run(req(rid(3, 1), 500, short(3)));
    assert!(planned(&none).assignments.is_empty());
    // Applying entries never moves units: the stored resource still holds them.
    let stored = |n: &Node| n.state.snapshot().resources[&medic].0.clone();
    assert_eq!((stored(&n).available, stored(&n).holds.len()), (0, 1));
    // A query at time 1 000 sees them free.
    let q = n
        .state
        .query(&req(rid(9, 1), 1_000, Op::Query(Query::Resource(medic))));
    assert!(
        matches!(q, OpResult::Resource(Some(r)) if r.available == 2 && r.holds.is_empty() && r.capacity == 2)
    );
    // A less urgent report at 1 000: the solve gives the medic to the
    // waiting incident, so this plan dispatches it there, releasing the old
    // hold in the same write, and the new report waits.
    let minor = n.run(req(rid(4, 1), 1_000, short(1)));
    assert!(planned(&minor).assignments.is_empty());
    let r = stored(&n);
    assert_eq!((r.capacity, r.available), (2, 0));
    assert_eq!(r.holds.len(), 1);
    assert_eq!((r.holds[0].incident, r.holds[0].units), (rid(3, 1), 2));
    assert!(minor.read_set.contains(&(medic, Version(2))));
    // After that hold expires too (at 1 500), a report gets the medic itself.
    let again = n.run(req(rid(5, 1), 1_500, short(3)));
    assert_eq!(planned(&again).assignments[0].resource, medic);
    assert_eq!(stored(&n).holds[0].incident, rid(5, 1));
}

#[test]
fn units_back_from_expiry_go_to_the_most_urgent_waiting_incident() {
    let mut n = Node::new(10);
    let tech = rid(1, 1);
    n.run(req(tech, 0, offer(ResourceKind::Technician, 1, 0)));
    let outage = |sev, ttl_ms| Op::ReportIncident {
        category: Category::NetworkOutage,
        zone: ZoneId(0),
        severity: Severity(sev),
        ttl_ms,
        attachment: None,
    };
    n.run(req(rid(2, 1), 0, outage(1, 1_000))); // holds the technician until 1 000
    let urgent = rid(3, 1);
    n.run(req(urgent, 100, outage(5, 60_000))); // waits
    // A flood report the technician could also serve: it must not take the
    // returned unit from the critical outage, and its plan dispatches it.
    let flood = n.run(req(rid(4, 1), 2_000, report(Category::Flooding, 1, 0)));
    assert!(planned(&flood).assignments.is_empty());
    assert!(flood.writes.iter().any(
        |w| matches!(w, Write::PutIncident(i) if i.id == urgent && i.status == IncidentStatus::Assigned)
    ));
    assert!(n.state.assignments().contains_key(&(urgent, tech)));
}

#[test]
fn one_plan_dispatching_several_resources_serves_each_incident_once() {
    let mut n = Node::new(10);
    let outage = |sev, zone, ttl_ms| Op::ReportIncident {
        category: Category::NetworkOutage,
        zone: ZoneId(zone),
        severity: Severity(sev),
        ttl_ms,
        attachment: None,
    };
    // Two technicians, each held until 1 000; then three outages wait.
    for u in 0..2u32 {
        n.run(req(
            rid(u, 1),
            0,
            offer(ResourceKind::Technician, 1, u as u16 * 10),
        ));
        n.run(req(rid(10 + u, 1), 0, outage(1, u as u16 * 10, 1_000)));
    }
    for u in 0..3u32 {
        n.run(req(rid(20 + u, 1), 100, outage(3, u as u16 * 5, 60_000)));
    }
    // A report nobody can serve: its solve gives both technicians to waiting
    // outages, and the plan dispatches both.
    let out = n.run(req(rid(30, 1), 2_000, report(Category::PowerOutage, 1, 0)));
    let mut served: Vec<ReqId> = out
        .writes
        .iter()
        .filter_map(|w| match w {
            Write::PutAssignment(a) => Some(a.incident),
            _ => None,
        })
        .collect();
    assert_eq!(served.len(), 2);
    served.dedup();
    assert_eq!(served.len(), 2, "an incident was dispatched twice");
    let read: Vec<ReqId> = out.read_set.iter().map(|(id, _)| *id).collect();
    for id in [rid(0, 1), rid(1, 1)] {
        assert!(
            read.contains(&id),
            "{id} is written, so its version is checked"
        );
    }
}

#[test]
fn released_units_are_version_checked_like_any_other_write() {
    let mut n = Node::new(10);
    let medic = rid(1, 1);
    n.run(req(medic, 0, offer(ResourceKind::Medic, 1, 0)));
    let short = Op::ReportIncident {
        category: Category::MedicalAssistance,
        zone: ZoneId(0),
        severity: Severity(1),
        ttl_ms: 1_000,
        attachment: None,
    };
    n.run(req(rid(2, 1), 0, short.clone()));
    // Two reports planned from one snapshot after the hold expired both see
    // the unit free; only the first to commit may have it.
    let snap = n.state.snapshot();
    let a = req(rid(3, 1), 2_000, short.clone());
    let b = req(rid(4, 1), 2_000, short);
    let (pa, pb) = (n.planner.plan(&snap, &a), n.planner.plan(&snap, &b));
    assert_eq!(planned(&pa).assignments[0].resource, medic);
    assert_eq!(planned(&pb).assignments[0].resource, medic);
    n.commit(&a, &pa);
    assert!(
        !n.is_current(&pb),
        "the second plan must be rejected as stale"
    );
    let pb = n.planner.plan(&n.state.snapshot(), &b);
    assert!(planned(&pb).assignments.is_empty());
}

#[test]
fn an_update_counts_expired_holds_as_free() {
    let mut n = Node::new(10);
    let vehicle = rid(1, 1);
    n.run(req(vehicle, 0, offer(ResourceKind::Vehicle, 3, 2)));
    n.run(req(
        rid(2, 1),
        0,
        Op::ReportIncident {
            category: Category::BlockedRoad,
            zone: ZoneId(2),
            severity: Severity(5),
            ttl_ms: 1_000,
            attachment: None,
        },
    ));
    let update = |at, available| {
        req(
            rid(1, at),
            at,
            Op::UpdateResource {
                resource: vehicle,
                available,
                extend_lease_ms: None,
            },
        )
    };
    // While the hold lasts, the owner cannot shrink below the 3 held units.
    n.run(update(500, 1));
    let r = &n.state.snapshot().resources[&vehicle].0;
    assert_eq!((r.capacity, r.available), (3, 0));
    // Once it has expired, the units are the owner's again.
    n.run(update(1_000, 1));
    let r = &n.state.snapshot().resources[&vehicle].0;
    assert_eq!((r.capacity, r.available, r.holds.len()), (1, 1, 0));
}

/// Over a long run with short-lived incidents, units return and are reused,
/// and no resource ever has more units out at once than its capacity,
/// counted from every assignment ever committed, not from the holds.
#[test]
fn resources_do_not_drain_over_a_long_run() {
    let mut n = Node::new(10);
    for u in 0..4 {
        n.run(req(
            rid(u, 1),
            0,
            Op::OfferResource {
                kind: ResourceKind::Medic,
                capacity: 2,
                zone: ZoneId((u * 7) as u16),
                lease_ms: 10_000_000,
            },
        ));
    }
    // Every assignment ever committed: (resource, from, until, units).
    let mut ledger: Vec<(ReqId, u64, u64, u32)> = Vec::new();
    let mut units_late = 0;
    for u in 0..3_000u32 {
        let at = u64::from(u) * 100;
        let out = n.run(req(
            rid(100 + u % 50, u64::from(u) + 10),
            at,
            Op::ReportIncident {
                category: Category::MedicalAssistance,
                zone: ZoneId((u % 30) as u16),
                severity: Severity((1 + u % 5) as u8),
                ttl_ms: 2_000,
                attachment: None,
            },
        ));
        for w in &out.writes {
            if let Write::PutAssignment(a) = w {
                ledger.push((a.resource, a.created_at_ms, a.expires_at_ms, a.units));
                if u >= 2_900 {
                    units_late += a.units;
                }
            }
        }
        for (r, _) in n.state.snapshot().resources.values() {
            let held: u32 = r.holds.iter().map(|h| h.units).sum();
            assert_eq!(r.available + held, r.capacity, "{}", r.id);
            let out_now: u32 = ledger
                .iter()
                .filter(|(id, from, until, _)| *id == r.id && *from <= at && at < *until)
                .map(|e| e.3)
                .sum();
            assert!(
                out_now <= r.capacity,
                "{} units of {} out",
                out_now,
                r.capacity
            );
        }
    }
    // 8 units, each held 2 s, so up to 40 unit-assignments in the last 10 s.
    // Had expired units never come back, there would be none.
    assert!(
        units_late >= 30,
        "{units_late} units handed out in the last 10 s"
    );
    // The full state was pruned as the run went on.
    assert!(n.state.incidents().len() < 3_000);
    assert!(n.state.assignments().len() < 1_000);
}

#[test]
fn a_zone_query_matches_a_scan_of_every_incident() {
    let mut n = Node::new(5);
    for u in 0..400u32 {
        n.run(req(
            rid(u, 1),
            u64::from(u) * 10,
            Op::ReportIncident {
                category: ALL_CATEGORIES[(u % 7) as usize],
                zone: ZoneId((u * 13 % 30) as u16),
                severity: Severity((1 + u % 5) as u8),
                ttl_ms: 1_000 + u64::from(u % 9) * 500,
                attachment: None,
            },
        ));
    }
    for (z, at) in [
        (0u16, 0u64),
        (3, 2_000),
        (7, 3_500),
        (29, 4_000),
        (12, 9_000),
    ] {
        let want: Vec<proto::Incident> = n
            .state
            .incidents()
            .values()
            .filter(|i| i.zone == ZoneId(z) && i.reported_at_ms + i.ttl_ms > at)
            .cloned()
            .collect();
        let got = n.state.query(&req(
            rid(999, 1),
            at,
            Op::Query(Query::IncidentsInZone(ZoneId(z))),
        ));
        assert_eq!(got, OpResult::Incidents(want), "zone {z} at {at}");
    }
}

#[test]
fn a_plan_keeps_only_the_best_few_responders() {
    let mut n = Node::new(50);
    for u in 0..30u32 {
        n.run(req(
            rid(u, 1),
            0,
            offer(ResourceKind::Medic, 2, (u % 30) as u16),
        ));
    }
    let out = n.run(req(
        rid(100, 1),
        10,
        report(Category::MedicalAssistance, 2, 4),
    ));
    let p = planned(&out);
    // Nothing else waits, so each candidate's total is its own walking
    // distance: the list is the nearest ten of all 30, ties to the lower ID,
    // led by the one assigned.
    let campus = n.planner.campus();
    let mut by_cost: Vec<(u32, ReqId)> = (0..30u32)
        .map(|u| (campus.distance(ZoneId(u as u16), ZoneId(4)), rid(u, 1)))
        .collect();
    by_cost.sort();
    let nearest: Vec<ReqId> = by_cost
        .iter()
        .take(incident::planner::RESPONDERS_KEPT)
        .map(|&(_, id)| id)
        .collect();
    assert_eq!(p.responders, nearest);
    assert_eq!(p.responders[0], p.assignments[0].resource);
}

#[test]
fn bounding_the_columns_gives_the_same_plans_on_a_small_campus() {
    // Few resources of two kinds in three zones, and short-lived incidents,
    // so units keep coming back and reports dispatch them. With every
    // resource a column the answer is known; the bounded planner must find
    // the same plans, even at one column per row. (The dual check itself is
    // exercised by the next test.)
    let mut n = Node::new(12);
    for u in 0..10u32 {
        let kind = [ResourceKind::Medic, ResourceKind::Volunteer][(u % 2) as usize];
        n.run(req(rid(u, 1), 0, offer(kind, 1 + u % 2, (u % 3) as u16)));
    }
    let categories = [
        Category::MedicalAssistance,
        Category::Flooding,
        Category::TechnicalAssistance,
        Category::LabIncident,
    ];
    let mut assigned = 0;
    for u in 0..150u32 {
        let r = req(
            rid(1_000 + u, 1),
            100 + u64::from(u) * 200,
            Op::ReportIncident {
                category: categories[(u % 4) as usize],
                zone: ZoneId((u * 7 % 3) as u16),
                severity: Severity((1 + u * 3 % 5) as u8),
                ttl_ms: 3_000,
                attachment: None,
            },
        );
        let snap = n.state.snapshot();
        let every = n.planner.clone().with_columns_per_row(usize::MAX);
        let want = every.plan(&snap, &r);
        for c in [1, 3] {
            let bounded = n.planner.clone().with_columns_per_row(c);
            assert_eq!(bounded.plan(&snap, &r), want, "report {u}, c {c}");
        }
        if !planned(&want).assignments.is_empty() {
            assigned += 1;
        }
        n.commit(&r, &want);
    }
    assert!(assigned > 30, "the run must keep assigning: {assigned}");
}

/// The incidents a plan serves, with the units each gets.
fn served(out: &PlanOutput) -> BTreeMap<ReqId, u32> {
    out.writes
        .iter()
        .filter_map(|w| match w {
            Write::PutAssignment(a) => Some((a.incident, a.units)),
            _ => None,
        })
        .collect()
}

#[test]
fn bounding_the_columns_serves_the_same_incidents_under_contention() {
    // Thirty one-unit medics in two zones, all free, and eight medical
    // incidents waiting in one zone with distinct priorities (as when units
    // have just come back). The new report needs a technician, so it brings
    // technicians, and every medical row brings the same equally cheap medic:
    // at one column per row they all want one column, and only the dual
    // check finds them the others. Medics tie, so which medic each gets may
    // differ from the full matrix; which incidents are served, and how much,
    // may not.
    let mut n = Node::new(9);
    for u in 0..30u32 {
        n.run(req(
            rid(u, 1),
            0,
            offer(ResourceKind::Medic, 1, (u % 2) as u16),
        ));
    }
    for u in 0..2u32 {
        n.run(req(
            rid(100 + u, 1),
            0,
            offer(ResourceKind::Technician, 1, 7),
        ));
    }
    let writes = (0..8u32)
        .map(|u| {
            let id = rid(200 + u, 1);
            Write::PutIncident(proto::Incident {
                id,
                owner: id.client,
                category: Category::MedicalAssistance,
                zone: ZoneId(0),
                severity: Severity((1 + u % 5) as u8),
                reported_at_ms: 1_000 + u64::from(u) * 3_000,
                status: IncidentStatus::Open,
                ttl_ms: 600_000,
                attachment: None,
            })
        })
        .collect();
    let load = PlanOutput {
        read_set: vec![],
        writes,
        result: OpResult::Accepted,
    };
    n.commit(
        &req(rid(300, 1), 30_000, report(Category::Flooding, 1, 29)),
        &load,
    );
    let r = req(rid(400, 1), 40_000, report(Category::NetworkOutage, 2, 5));
    let snap = n.state.snapshot();
    let want = served(
        &n.planner
            .clone()
            .with_columns_per_row(usize::MAX)
            .plan(&snap, &r),
    );
    assert_eq!(
        want.len(),
        9,
        "the new incident and all eight waiting are served"
    );
    for c in [1, 2, 3] {
        let bounded = n.planner.clone().with_columns_per_row(c).plan(&snap, &r);
        assert_eq!(served(&bounded), want, "c {c}");
    }
}

const ALL_KINDS: [ResourceKind; 7] = [
    ResourceKind::Medic,
    ResourceKind::Technician,
    ResourceKind::SecurityOfficer,
    ResourceKind::Vehicle,
    ResourceKind::Generator,
    ResourceKind::FirstAidKit,
    ResourceKind::Volunteer,
];
const ALL_CATEGORIES: [Category; 7] = [
    Category::BlockedRoad,
    Category::PowerOutage,
    Category::LabIncident,
    Category::Flooding,
    Category::NetworkOutage,
    Category::MedicalAssistance,
    Category::TechnicalAssistance,
];

/// A busy campus at time 100 s: `resources` live offers of every kind with
/// 1–3 units, a tenth of them withdrawn to 0, and `incidents` open incidents
/// of every category and severity. The waiting incidents compete with a new
/// report for the same responders, as they do in a stress run.
fn busy_campus(top_k: u32, resources: u32, incidents: u32) -> Node {
    let mut n = Node::new(top_k);
    for u in 0..resources {
        let id = rid(u, 1);
        let kind = ALL_KINDS[(u % 7) as usize];
        n.run(req(id, 0, offer(kind, 1 + u % 3, (u * 7 % 30) as u16)));
        if u % 10 == 9 {
            let withdraw = Op::UpdateResource {
                resource: id,
                available: 0,
                extend_lease_ms: None,
            };
            n.run(req(rid(u, 2), 0, withdraw));
        }
    }
    // Waiting incidents are written directly: planning each one would also
    // assign it, and the point is a snapshot with many open incidents.
    let writes = (0..incidents)
        .map(|u| {
            let id = rid(1_000_000 + u, 1);
            Write::PutIncident(proto::Incident {
                id,
                owner: id.client,
                category: ALL_CATEGORIES[(u % 7) as usize],
                zone: ZoneId((u * 11 % 30) as u16),
                severity: Severity((1 + u % 5) as u8),
                reported_at_ms: u64::from(u) * 37 % 100_000,
                status: IncidentStatus::Open,
                ttl_ms: 600_000,
                attachment: None,
            })
        })
        .collect();
    let load = PlanOutput {
        read_set: vec![],
        writes,
        result: OpResult::Accepted,
    };
    let at = req(rid(999_998, 1), 100_000, report(Category::Flooding, 1, 0));
    n.commit(&at, &load);
    n
}

/// Every committed plan is stored and sent to every node, so a report's log
/// entry must stay small even at `top_k` 100 (DL-059 kept 100 responders,
/// about 2.5 KB a plan).
#[test]
fn a_report_entry_stays_small_in_the_log() {
    let n = busy_campus(100, 3_000, 0);
    let r = req(rid(999_999, 1), 100_000, report(Category::Flooding, 3, 7));
    let out = n.planner.plan(&n.state.snapshot(), &r);
    assert_eq!(
        planned(&out).responders.len(),
        incident::planner::RESPONDERS_KEPT
    );
    let entry = Entry {
        seq: Seq(1),
        epoch: proto::Epoch(1),
        kind: EntryKind::Result {
            req: r.id,
            executor: NodeId(1),
            writes: out.writes.clone(),
            result: out.result.clone(),
        },
    };
    let bytes = proto::codec::encode(proto::codec::WireFormat::Postcard, &entry).unwrap();
    println!("report entry: {} bytes", bytes.len());
    assert!(bytes.len() < 600, "{} bytes", bytes.len());
}

/// Milliseconds per plan of a new flood report on `n`'s snapshot.
fn ms_per_plan(n: &Node, reps: u32) -> f64 {
    let snap = n.state.snapshot();
    let r = req(rid(999_999, 1), 100_000, report(Category::Flooding, 3, 7));
    let t = std::time::Instant::now();
    for _ in 0..reps {
        std::hint::black_box(n.planner.plan(&snap, &r));
    }
    t.elapsed().as_secs_f64() * 1e3 / f64::from(reps)
}

/// The cost knob: planning time grows with `top_k`. The waiting incidents can
/// use the same responders as the new one, so every row of the assignment has
/// real columns to choose from. Run with
/// `cargo test -p incident --release -- --ignored --nocapture`.
#[test]
#[ignore]
fn service_time_against_top_k() {
    for k in [10u32, 25, 50, 100, 200] {
        let mut n = busy_campus(k, 300, 400);
        let bounded = ms_per_plan(&n, 20);
        n.planner = n.planner.clone().with_columns_per_row(usize::MAX);
        let every = ms_per_plan(&n, 20);
        println!(
            "top_k {k:>3}: {bounded:>8.2} ms per plan ({every:.2} ms with every resource a column)"
        );
    }
}

/// Planning time must not grow with the number of live resources: at a fixed
/// `top_k`, a hundred times the resources should cost about the same. The
/// time at `top_k` 1 is the part that still grows with them: one pass over
/// the resources and the open incidents.
#[test]
#[ignore]
fn service_time_against_resource_count() {
    let sizes = [
        (300, 300),
        (3_000, 2_000),
        (10_000, 5_000),
        (30_000, 10_000),
    ];
    for (resources, incidents) in sizes {
        let mut n = busy_campus(100, resources, incidents);
        let full = ms_per_plan(&n, 5);
        n.planner = IncidentPlanner::new(30, 1);
        println!(
            "top_k 100, {resources:>6} resources, {incidents:>6} open incidents: \
             {full:>8.2} ms per plan ({:.2} ms at top_k 1)",
            ms_per_plan(&n, 20)
        );
    }
}
