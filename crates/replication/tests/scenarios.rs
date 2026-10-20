//! One test per protocol behaviour, on a network without loss so each
//! scenario is easy to follow. `tests/simulation.rs` adds loss and chaos.

mod common;

use common::{INITIAL_AVAILABLE, Params, Sim, request_id, resource_id};
use proto::event::EventKind;
use proto::{EntryKind, NodeId, ProposeOutcome, SilenceTarget, Version};
use replication::{InjectSchedule, Role, Status};

const N1: NodeId = NodeId(1);
const N2: NodeId = NodeId(2);
const N3: NodeId = NodeId(3);

fn started(seed: u64) -> Sim {
    let mut sim = Sim::new(Params::calm(seed));
    sim.run_until(3_000, "first coordinator", |s| {
        s.coordinator().is_some()
            && s.committed.len() == 1
            && NodeId::ALL
                .iter()
                .all(|&n| s.core(n).is_active() && s.core(n).applied_seq().0 == 1)
    });
    sim
}

fn last_outcome(sim: &Sim, n: NodeId, req: proto::ReqId) -> Option<ProposeOutcome> {
    sim.replies
        .get(&req)?
        .iter()
        .rev()
        .find(|(m, _)| *m == n)
        .map(|(_, o)| o.clone())
}

#[test]
fn highest_id_is_elected_first_and_everyone_is_active() {
    for seed in 0..20 {
        let sim = started(seed);
        assert_eq!(sim.coordinator(), Some(N3), "seed {seed}");
        // The first entry is the new coordinator's view of all three nodes.
        assert!(matches!(&sim.committed[0].kind, EntryKind::View(v) if v.active().len() == 3));
        sim.check_safety();
        sim.check_converged();
    }
}

#[test]
fn a_proposal_is_committed_and_applied_on_every_node() {
    let mut sim = started(1);
    let req = request_id(1);
    sim.execute(N1, req);
    sim.run_until(1_000, "applied everywhere", |s| {
        NodeId::ALL.iter().all(|&n| s.core(n).applied_seq().0 == 2)
    });
    assert!(matches!(
        last_outcome(&sim, N1, req),
        Some(ProposeOutcome::Committed(_))
    ));
    let r = resource_id((proto::hash::request(&req) % 4) as u32);
    for n in NodeId::ALL {
        assert_eq!(sim.node(n).state[&r], (INITIAL_AVAILABLE - 1, Version(2)));
    }
    sim.check_safety();
}

#[test]
fn competing_executors_get_one_result() {
    let mut sim = started(2);
    let req = request_id(7);
    sim.execute(N1, req);
    sim.execute(N2, req);
    sim.run_until(1_000, "both answered", |s| {
        s.replies.get(&req).is_some_and(|r| r.len() == 2)
    });
    let results = sim
        .committed
        .iter()
        .filter(|e| matches!(&e.kind, EntryKind::Result { req: r, .. } if *r == req))
        .count();
    assert_eq!(results, 1);
    let outcomes: Vec<_> = sim.replies[&req].iter().map(|(_, o)| o.clone()).collect();
    assert_eq!(
        outcomes
            .iter()
            .filter(|o| matches!(o, ProposeOutcome::Committed(_)))
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|o| matches!(o, ProposeOutcome::Duplicate(_)))
            .count(),
        1
    );
    sim.check_safety();
}

#[test]
fn a_plan_from_a_stale_snapshot_is_rejected() {
    let mut sim = started(3);
    // Two requests on the same resource; the second is planned before the first commits.
    let a = request_id(1);
    let b = (2..)
        .map(request_id)
        .find(|r| proto::hash::request(r) % 4 == proto::hash::request(&a) % 4)
        .unwrap();
    let stale = sim.plan(N2, b);
    sim.execute(N1, a);
    sim.run_until(1_000, "first commit applied on n2", |s| {
        s.core(N2).applied_seq().0 == 2
    });
    sim.propose_raw(N2, stale);
    sim.run_until(1_000, "answer", |s| s.replies.contains_key(&b));
    assert!(matches!(sim.replies[&b][0].1, ProposeOutcome::Stale { .. }));
    // The simulated executor plans again from fresh state and succeeds.
    sim.run_until(1_000, "replanned", |s| {
        s.replies[&b]
            .iter()
            .any(|(_, o)| matches!(o, ProposeOutcome::Committed(_)))
    });
    sim.check_safety();
}

#[test]
fn a_node_outside_the_view_cannot_commit_results() {
    let mut sim = started(4);
    sim.isolated[N1.index()] = true;
    sim.run_until(3_000, "n1 removed", |s| {
        !s.core(N3).view().contains(N1) && s.core(N3).commit_seq() == s.core(N3).last_seq()
    });
    // n2 forwards a proposal that names n1 as executor.
    let req = request_id(50);
    let mut p = sim.plan(N2, req);
    p.executor = N1;
    sim.propose_raw(N2, p);
    sim.run_until(1_000, "answer", |s| s.replies.contains_key(&req));
    assert_eq!(sim.replies[&req][0].1, ProposeOutcome::NotInView);
    sim.isolated[N1.index()] = false;
    sim.settle_and_check();
}

#[test]
fn coordinator_failover_keeps_committing_and_the_old_coordinator_is_fenced() {
    let mut sim = started(5);
    sim.workload = true;
    sim.run_for(2_000);
    let old_epoch = sim.core(N3).epoch();
    let before = sim.committed.len();

    sim.isolated[N3.index()] = true;
    sim.run_until(3_000, "new coordinator", |s| {
        s.coordinator().is_some_and(|c| c != N3)
    });
    // The isolated coordinator gave up its role on its own (lease).
    assert_eq!(sim.core(N3).role(), Role::Follower);
    let new = sim.coordinator().unwrap();
    assert!(sim.core(new).epoch() > old_epoch);
    sim.run_for(3_000);
    assert!(
        sim.committed.len() > before + 100,
        "service continued during the outage"
    );
    assert!(!sim.core(new).view().contains(N3));

    sim.isolated[N3.index()] = false;
    sim.run_until(5_000, "n3 back", |s| s.core(N3).is_active());
    assert_eq!(
        sim.coordinator(),
        Some(new),
        "the returning node did not take the role back"
    );
    sim.settle_and_check();
}

#[test]
fn an_isolated_coordinator_steps_down_within_its_lease() {
    let mut sim = started(6);
    sim.isolated[N3.index()] = true;
    let t0 = sim.now;
    sim.run_until(1_000, "step down", |s| {
        s.core(N3).role() != Role::Coordinator
    });
    // Lease 250 ms, checked every tick.
    assert!(
        sim.now - t0 <= 260,
        "stepped down after {} ms",
        sim.now - t0
    );
    sim.isolated[N3.index()] = false;
    sim.settle_and_check();
}

#[test]
fn a_returning_node_does_not_disrupt_a_healthy_coordinator() {
    let mut sim = started(7);
    sim.workload = true;
    sim.isolated[N1.index()] = true;
    sim.run_for(4_000);
    let (c, epoch) = (sim.coordinator().unwrap(), sim.core(N3).epoch());
    sim.isolated[N1.index()] = false;
    sim.run_until(5_000, "n1 active", |s| s.core(N1).is_active());
    sim.run_for(2_000);
    assert_eq!(sim.coordinator(), Some(c));
    assert_eq!(sim.core(c).epoch(), epoch, "pre-vote must stop an election");
    sim.settle_and_check();
}

#[test]
fn entries_only_the_old_coordinator_held_are_replaced() {
    let mut sim = started(8);
    // n3 is coordinator. Cut it off, then let it accept its own proposals,
    // which it can append but never commit.
    sim.isolated[N3.index()] = true;
    let mine = request_id(900);
    sim.execute(N3, mine); // appended locally; nothing it sends arrives
    assert!(sim.core(N3).last_seq() > sim.core(N3).commit_seq());
    sim.workload = true;
    sim.run_until(3_000, "new coordinator", |s| {
        s.coordinator().is_some_and(|c| c != N3)
    });
    sim.run_for(2_000);
    sim.isolated[N3.index()] = false;
    // n3's uncommitted entry is overwritten, and its proposal is retried
    // through the new coordinator and answered.
    sim.settle_and_check();
    assert!(sim.replies.contains_key(&mine));
}

#[test]
fn a_short_silence_needs_no_view_change() {
    let mut sim = started(9);
    sim.workload = true;
    sim.run_for(1_000);
    let view = sim.core(N3).view().no;
    // 150 ms is below the 300 ms detection timeout.
    sim.operator_silence(N2, SilenceTarget::Node(N1), 150);
    sim.run_until(1_000, "n1 silenced", |s| s.node(N1).silenced);
    sim.run_until(2_000, "n1 back", |s| !s.node(N1).silenced);
    assert_eq!(sim.core(N1).status(), Status::Syncing);
    sim.run_until(2_000, "n1 active", |s| s.core(N1).is_active());
    // Only the injection itself was added; nobody was removed.
    assert_eq!(sim.core(N3).view().no, view);
    sim.settle_and_check();
}

#[test]
fn an_operator_can_silence_the_coordinator() {
    let mut sim = started(10);
    sim.workload = true;
    sim.run_for(1_000);
    sim.operator_silence(N1, SilenceTarget::Coordinator, 3_000);
    sim.run_until(1_000, "coordinator silenced", |s| s.node(N3).silenced);
    sim.run_until(3_000, "new coordinator", |s| {
        s.coordinator().is_some_and(|c| c != N3)
    });
    sim.run_until(8_000, "n3 rejoined", |s| {
        !s.node(N3).silenced && s.core(N3).is_active()
    });
    assert_eq!(sim.count_events(EventKind::InjectStart), 1);
    sim.settle_and_check();
}

#[test]
fn injection_is_refused_while_a_node_is_out() {
    let mut sim = started(11);
    sim.isolated[N1.index()] = true;
    sim.run_until(3_000, "n1 removed", |s| !s.core(N3).view().contains(N1));
    sim.operator_silence(N3, SilenceTarget::Node(N2), 5_000);
    assert_eq!(sim.count_events(EventKind::InjectRefused), 1);
    sim.run_for(500);
    assert!(!sim.node(N2).silenced);
    sim.isolated[N1.index()] = false;
    sim.settle_and_check();
}

#[test]
fn scheduled_injections_silence_one_node_at_a_time_and_it_recovers() {
    let mut params = Params::calm(12);
    params.inject = Some(InjectSchedule {
        seed: 12,
        interval_min_ms: 4_000,
        interval_max_ms: 6_000,
        duration_min_ms: 500,
        duration_max_ms: 3_000,
    });
    let mut sim = Sim::new(params);
    sim.workload = true;
    sim.run_for(40_000);
    let injections = sim.count_events(EventKind::InjectStart);
    assert!(injections >= 4, "only {injections} injections");
    assert_eq!(sim.max_silenced_at_once, 1);
    sim.settle_and_check();
    // Every silenced node went through SYNCING and came back.
    let injections = sim.count_events(EventKind::InjectStart);
    assert_eq!(sim.count_events(EventKind::InjectEnd), injections);
    assert!(sim.count_events(EventKind::SyncStart) >= injections);
}
