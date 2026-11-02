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
fn after_a_short_silence_on_an_idle_cluster_the_node_becomes_active() {
    // With no load the commit position never moves after the node wakes, so
    // it must become current from the first `Append` alone (DL-066).
    let mut sim = started(19);
    sim.operator_silence(N2, SilenceTarget::Node(N1), 150);
    sim.run_until(1_000, "n1 silenced", |s| s.node(N1).silenced);
    sim.run_until(2_000, "n1 back", |s| !s.node(N1).silenced);
    sim.run_until(500, "n1 active without new commits", |s| {
        s.core(N1).is_active()
    });
    sim.settle_and_check();
}

#[test]
fn a_silenced_follower_gets_probes_not_the_same_entries_again_and_again() {
    let mut sim = started(20);
    sim.workload = true; // one request every 10 ms
    sim.run_for(1_000);
    sim.operator_silence(N2, SilenceTarget::Node(N1), 5_000);
    sim.run_until(1_000, "n1 silenced", |s| s.node(N1).silenced);
    let commit_before = sim.core(N3).commit_seq().0;
    sim.run_until(6_000, "n1 back", |s| !s.node(N1).silenced);
    let committed_meanwhile = sim.core(N3).commit_seq().0 - commit_before;
    let sent = sim.entries_to_silenced[N1.index()];
    eprintln!("{sent} entries sent to the silenced node; {committed_meanwhile} committed");
    // Before DL-066 the coordinator resent a full batch every commit flush:
    // about 28,000 entries for about 500 committed. Now only what went out
    // before the silence was noticed (200 ms of traffic) and nothing more.
    assert!(
        sent <= committed_meanwhile / 10,
        "{sent} entries sent to the silenced node while {committed_meanwhile} were committed"
    );
    sim.run_until(5_000, "n1 caught up and active", |s| {
        s.core(N1).is_active() && s.core(N1).view().contains(N1)
    });
    sim.settle_and_check();
}

#[test]
fn a_woken_old_coordinator_is_sent_only_what_it_missed() {
    // Regression: the new coordinator starts with `matched = 0`, and a probe
    // placed at `matched` always matched, so the whole log was resent to the
    // old coordinator when it woke (found in review of DL-066).
    for seed in [1, 2, 3] {
        let mut sim = started(30 + seed);
        sim.workload = true;
        sim.run_for(20_000);
        let old = sim.coordinator().unwrap();
        let at_silence = sim.core(old).last_seq().0;
        sim.operator_silence(old, SilenceTarget::Coordinator, 2_000);
        sim.run_until(1_000, "silenced", |s| s.node(old).silenced);
        sim.run_until(3_000, "back", |s| !s.node(old).silenced);
        let sent_before = sim.entries_sent[old.index()];
        let new = sim.coordinator().unwrap();
        let missed = sim.core(new).last_seq().0 - at_silence;
        sim.run_until(5_000, "old coordinator active in the view", |s| {
            s.core(old).is_active() && s.core(old).view().contains(old)
        });
        let sent = sim.entries_sent[old.index()] - sent_before;
        eprintln!("seed {seed}: missed {missed}, sent after wake {sent}, log {at_silence}");
        assert!(
            sent <= 3 * missed + 256,
            "seed {seed}: {sent} entries sent after waking for {missed} missed (log {at_silence})"
        );
        sim.settle_and_check();
    }
}

#[test]
fn a_retransmitted_append_from_before_a_silence_does_not_end_the_catch_up() {
    // Regression (review of DL-066): the first `Append` after waking may be a
    // link retransmission from before the silence, carrying an old commit
    // position the node has already applied. It must not make the node
    // ACTIVE while newer commits exist.
    let mut sim = started(40);
    sim.workload = true;
    sim.run_for(1_000);
    let c = sim.coordinator().unwrap();
    let f = NodeId::ALL.into_iter().find(|&n| n != c).unwrap();
    sim.operator_silence(c, SilenceTarget::Node(f), 150);
    sim.run_until(1_000, "silenced", |s| s.node(f).silenced);
    // What `f` already holds when it goes quiet: an `Append` carrying this
    // commit position is what a retransmission from before the silence
    // looks like.
    let old = sim.core(f).applied_seq();
    let old_epoch = sim.core(f).entry(old).unwrap().epoch;
    sim.run_until(2_000, "back", |s| !s.node(f).silenced);
    let woke = sim.now;
    let commit_at_wake = sim.core(c).commit_seq();
    assert!(commit_at_wake > old, "commits happened during the silence");
    sim.deliver(
        f,
        c,
        proto::NodeMsg::Append {
            epoch: sim.core(c).epoch(),
            prev_seq: old,
            prev_epoch: old_epoch,
            entries: vec![],
            commit: old,
        },
    );
    assert!(
        !sim.core(f).is_active(),
        "a stale target ended the catch-up"
    );
    // Within two heartbeat intervals of waking it stays SYNCING whatever
    // `Append`s arrive, and becomes ACTIVE only at the newest target.
    sim.run_until(1_000, "active", |s| s.core(f).is_active());
    assert!(sim.now - woke >= 200, "active after {} ms", sim.now - woke);
    assert!(sim.core(f).applied_seq() >= commit_at_wake);
    sim.settle_and_check();
}

#[test]
fn a_coordinator_whose_peers_are_heard_keeps_its_lease_while_acks_queue() {
    // The runtime reports transport-level contact through `heard_from`; it
    // stands in for acknowledgments the core has not processed yet.
    let mut sim = started(21);
    let c = sim.coordinator().unwrap();
    sim.isolated[c.index()] = true;
    let t0 = sim.now;
    for t in 0..240 {
        sim.step();
        if t % 20 == 0 {
            let now = sim.now;
            for p in NodeId::ALL.into_iter().filter(|&p| p != c) {
                sim.heard_from(c, p, now);
            }
        }
    }
    assert_eq!(
        sim.core(c).role(),
        Role::Coordinator,
        "lease kept by contact"
    );
    // Without contact the lease runs out as before.
    sim.run_until(1_000, "step down", |s| {
        s.core(c).role() != Role::Coordinator
    });
    assert!(
        sim.now - t0 <= 240 + 260,
        "stepped down after {} ms",
        sim.now - t0
    );
    sim.isolated[c.index()] = false;
    sim.settle_and_check();
}

#[test]
fn a_new_coordinator_keeps_the_injection_margin_for_a_woken_node() {
    // Regression (DL-074): the margin after an injection was kept only as
    // "when the last injection ends", which a new coordinator does not know.
    // The silenced old coordinator could be silenced again right after
    // waking, before it was ever ACTIVE.
    let mut sim = started(102);
    sim.workload = true;
    sim.run_for(1_000);
    let old = sim.coordinator().unwrap();
    sim.operator_silence(old, SilenceTarget::Coordinator, 3_000);
    sim.run_until(1_000, "silenced", |s| s.node(old).silenced);
    sim.run_until(4_000, "woke", |s| !s.node(old).silenced);
    let woke = sim.now;
    let mut accepted = None;
    for _ in 0..6_000 {
        if let Some(c) = sim.coordinator().filter(|&c| c != old) {
            let refused = sim.count_events(EventKind::InjectRefused);
            sim.operator_silence(c, SilenceTarget::Node(old), 1_000);
            if sim.count_events(EventKind::InjectRefused) == refused {
                accepted = Some((sim.now - woke, sim.core(old).is_active()));
                break;
            }
        }
        sim.step();
    }
    let (after, was_active) = accepted.expect("an injection is accepted eventually");
    assert!(
        after >= 2_000,
        "accepted {after} ms after waking (margin 2000)"
    );
    assert!(
        was_active,
        "the woken node was not ACTIVE when silenced again"
    );
    sim.settle_and_check();
}

#[test]
fn no_injection_while_a_reconnected_node_is_still_catching_up() {
    // Regression (DL-074): "responsive" is not "ACTIVE". A node cut off long
    // enough to leave the view must be back in it and caught up, for the
    // whole margin, before anyone is silenced.
    let mut sim = started(300);
    sim.workload = true;
    sim.run_for(1_000);
    let c = sim.coordinator().unwrap();
    let f = NodeId::ALL.into_iter().find(|&n| n != c).unwrap();
    sim.isolated[f.index()] = true;
    sim.run_until(3_000, "f removed", |s| !s.core(c).view().contains(f));
    sim.isolated[f.index()] = false;
    let starts = sim.count_events(EventKind::InjectStart);
    let back = sim.now;
    while sim.now < back + 1_500 {
        if let Some(co) = sim.coordinator() {
            sim.operator_silence(co, SilenceTarget::Coordinator, 5_000);
        }
        sim.step();
    }
    assert_eq!(
        sim.count_events(EventKind::InjectStart),
        starts,
        "an injection was accepted while a node was rejoining"
    );
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

#[test]
fn two_plans_writing_one_incident_cannot_both_commit() {
    // DL-057: incidents are versioned like resources, so a waiting incident
    // assigned by one committed plan makes a concurrent plan for it stale.
    use proto::{Category, Incident, IncidentStatus, OpResult, Severity, Version, Write, ZoneId};
    let mut sim = started(4);
    let incident = Incident {
        id: request_id(900),
        owner: request_id(900).client,
        category: Category::Flooding,
        zone: ZoneId(0),
        severity: Severity(3),
        reported_at_ms: 0,
        status: IncidentStatus::Assigned,
        ttl_ms: 60_000,
        attachment: None,
    };
    let writing_incident = |sim: &Sim, n: NodeId, req| {
        let mut p = sim.plan(n, req);
        p.read_set = vec![(incident.id, Version(0))];
        p.writes = vec![Write::PutIncident(incident.clone())];
        p.result = OpResult::Accepted;
        p
    };
    let (a, b) = (request_id(1), request_id(2));
    let first = writing_incident(&sim, N1, a);
    let second = writing_incident(&sim, N2, b);
    sim.propose_raw(N1, first);
    sim.run_until(1_000, "first committed", |s| {
        last_outcome(s, N1, a).is_some_and(|o| matches!(o, ProposeOutcome::Committed(_)))
    });
    sim.propose_raw(N2, second);
    sim.run_until(1_000, "second answered", |s| s.replies.contains_key(&b));
    assert!(matches!(sim.replies[&b][0].1, ProposeOutcome::Stale { .. }));
}
