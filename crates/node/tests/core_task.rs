//! The core task end to end: three nodes with replication and selection,
//! fake planning pools and a simulated network.

mod common;

use common::{Params, Sim};
use proto::event::EventKind;
use proto::{NodeId, OpResult, ReplyBody};
use replication::InjectSchedule;
use replication::rng::SplitMix64;

const N1: NodeId = NodeId(1);

fn started(params: Params) -> Sim {
    let mut sim = Sim::new(params);
    sim.run_until(3_000, "a coordinator and three active nodes", |s| {
        s.coordinator().is_some() && NodeId::ALL.iter().all(|&n| s.task(n).core().is_active())
    });
    sim
}

fn ok_replies(sim: &Sim, req: &proto::ReqId) -> usize {
    sim.replies.get(req).map_or(0, |r| {
        r.iter()
            .filter(|(_, b)| matches!(b, ReplyBody::Ok(_)))
            .count()
    })
}

#[test]
fn requests_flow_end_to_end_and_every_client_gets_the_committed_result() {
    let mut sim = started(Params::calm(1));
    sim.workload = true;
    sim.run_for(5_000);
    sim.settle();
    assert!(
        sim.submitted.len() >= 450,
        "{} requests",
        sim.submitted.len()
    );
    assert!(sim.pending.is_empty());
    // Every request was executed by exactly one node in the normal case.
    let executed: u64 = NodeId::ALL
        .iter()
        .map(|&n| sim.pool(n).attempts_seen.get(&0).copied().unwrap_or(0))
        .sum();
    assert!(
        executed <= sim.submitted.len() as u64 * 11 / 10,
        "{executed} executions"
    );
}

#[test]
fn a_full_pool_answers_busy_before_selection() {
    let mut sim = started(Params::calm(2));
    sim.pool(N1).capacity = 0; // node 1 is always full
    let req = sim.new_request();
    sim.run_until(2_000, "answered", |s| ok_replies(s, &req) > 0);
    // Node 1 answered Busy and never let the request into selection.
    let n1: Vec<_> = sim.replies[&req].iter().filter(|(n, _)| *n == N1).collect();
    assert!(n1.iter().all(|(_, b)| *b == ReplyBody::Busy), "{n1:?}");
    assert!(!n1.is_empty());
    assert!(
        !sim.events
            .iter()
            .any(|(_, n, k, r)| *n == N1 && *k == EventKind::Ranked && *r == Some(req)),
        "node 1 must not rank a request it refused"
    );
    assert!(sim.count(N1, EventKind::Busy) >= 1);
    sim.pool(N1).capacity = 64;
    sim.settle();
}

#[test]
fn a_request_refused_busy_never_enters_the_selector() {
    let mut sim = started(Params::calm(3));
    sim.pool(N1).capacity = 0;
    let req = sim.new_request();
    // Before any commit is applied on node 1, its table has no row.
    sim.run_until(500, "busy sent", |s| s.count(N1, EventKind::Busy) >= 1);
    assert_eq!(sim.task(N1).selector().row_state(&req), None);
    sim.pool(N1).capacity = 64;
    sim.settle();
}

#[test]
fn an_execute_is_never_dropped_even_when_the_pool_is_full() {
    let mut sim = started(Params::calm(4));
    // Stall every pool so the request stays with its first executor.
    for n in NodeId::ALL {
        sim.pool(n).paused = true;
    }
    let req = sim.new_request();
    sim.run_until(1_000, "claimed and queued", |s| {
        NodeId::ALL.iter().any(|&n| s.task(n).executing() > 0)
    });
    let executor = NodeId::ALL
        .into_iter()
        .find(|&n| sim.task(n).executing() > 0)
        .unwrap();
    // The other two pools are now full for new requests, and running again.
    let others: Vec<NodeId> = NodeId::ALL.into_iter().filter(|&n| n != executor).collect();
    for &n in &others {
        let mut p = sim.pool(n);
        p.capacity = 0;
        p.paused = false;
    }
    let submitted_before: u64 = others.iter().map(|&n| sim.pool(n).submitted).sum();

    // The executor drops out; a survivor takes the request over.
    sim.isolated[executor.index()] = true;
    sim.run_until(5_000, "answered by a survivor", |s| ok_replies(s, &req) > 0);
    let submitted_after: u64 = others.iter().map(|&n| sim.pool(n).submitted).sum();
    assert!(
        submitted_after > submitted_before,
        "the takeover Execute reached a full pool"
    );
    // A new request is still refused there: the pool really was full.
    let probe = sim.new_request();
    sim.run_until(1_000, "busy", |s| {
        s.replies.get(&probe).is_some_and(|r| {
            r.iter()
                .any(|(n, b)| others.contains(n) && *b == ReplyBody::Busy)
        })
    });
    for n in NodeId::ALL {
        let mut p = sim.pool(n);
        p.capacity = 64;
        p.paused = false;
    }
    sim.settle();
}

#[test]
fn stale_plans_are_planned_again_and_capped() {
    let mut params = Params::calm(5);
    params.resources = 1; // every request competes for the same resource
    params.request_every_ms = 2;
    let mut sim = started(params);
    sim.workload = true;
    sim.run_for(3_000);
    sim.settle();
    let replanned: u64 = NodeId::ALL
        .iter()
        .map(|&n| {
            sim.pool(n)
                .attempts_seen
                .iter()
                .filter(|(a, _)| **a > 0)
                .map(|(_, c)| c)
                .sum::<u64>()
        })
        .sum();
    assert!(replanned > 0, "contention must cause re-planning");
    let replans: usize = NodeId::ALL
        .iter()
        .map(|&n| sim.count(n, EventKind::Replan))
        .sum();
    assert!(replans > 0);
    // Some requests may end Refused after the cap; every one is answered.
    let refused = sim
        .replies
        .values()
        .flatten()
        .filter(|(_, b)| matches!(b, ReplyBody::Ok(OpResult::Refused(_))))
        .count();
    assert!(
        sim.pending.is_empty(),
        "refused or not, every request is answered ({refused} refused)"
    );
}

#[test]
fn a_retry_of_a_committed_request_is_answered_with_the_same_result() {
    let mut sim = started(Params::calm(6));
    let req = sim.new_request();
    sim.run_until(2_000, "answered", |s| ok_replies(s, &req) > 0);
    sim.run_for(300);
    let before = ok_replies(&sim, &req);
    let again = sim.client_request(req.seq);
    sim.send_to_all(again);
    sim.run_for(300);
    assert!(ok_replies(&sim, &req) > before, "the retry was answered");
    sim.check_answers();
}

fn chaos(seed: u64, ms: u64) -> Sim {
    chaos_with(seed, ms, None)
}

fn chaos_with(seed: u64, ms: u64, planner: Option<incident::IncidentPlanner>) -> Sim {
    let mut r = SplitMix64::new(seed ^ 0xc0de);
    let params = Params {
        seed,
        drop: r.range(0, 3) as f64 / 100.0,
        delay_max: r.range(2, 10),
        request_every_ms: r.range(5, 15),
        resources: r.range(2, 16) as u32,
        pool_capacity: r.range(2, 32) as usize,
        workers: r.range(1, 3) as usize,
        service_ms: (2, r.range(3, 20)),
        client_retry_ms: 600,
        inject: Some(InjectSchedule {
            seed,
            interval_min_ms: 4_000,
            interval_max_ms: 7_000,
            duration_min_ms: 300,
            duration_max_ms: 3_000,
        }),
    };
    let mut sim = match planner {
        Some(p) => Sim::with_real_planner(params, p),
        None => Sim::new(params),
    };
    sim.workload = true;
    let mut until = [0u64; 3];
    let end = sim.now + ms;
    while sim.now < end {
        if sim.now.is_multiple_of(500) {
            for n in NodeId::ALL {
                if sim.isolated[n.index()] && sim.now >= until[n.index()] {
                    sim.isolated[n.index()] = false;
                }
            }
            let out = (0..3)
                .filter(|&i| sim.isolated[i] || sim.nodes[i].silenced)
                .count();
            if out == 0 && r.chance(0.1) {
                let n = NodeId::ALL[r.range(0, 2) as usize];
                sim.isolated[n.index()] = true;
                until[n.index()] = sim.now + r.range(100, 2_500);
            }
        }
        sim.step();
    }
    sim
}

#[test]
fn under_faults_and_overload_every_request_gets_one_consistent_answer() {
    let seeds = std::env::var("SIM_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(4u64);
    for seed in 0..seeds {
        let mut sim = chaos(seed, 25_000);
        sim.settle();
        assert!(sim.submitted.len() > 1_000, "seed {seed}");
    }
}

/// Workstream D under the same faults: the real planner and state machine on
/// all three nodes, with competing incidents and scarce responders. Rebuilds
/// the state from the committed log and checks no responder ever gave out
/// more units than it had, and no incident got two responders.
#[test]
fn the_real_planner_keeps_assignments_consistent_under_faults() {
    use incident::State;
    use proto::{EntryKind, StateMachine, Write};
    use std::collections::BTreeMap;
    let seeds = std::env::var("SIM_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2u64);
    for seed in 0..seeds {
        let mut sim = chaos_with(seed, 15_000, Some(incident::IncidentPlanner::new(30, 20)));
        sim.settle();
        assert!(sim.submitted.len() > 500, "seed {seed}");
        // Nodes may learn the final commit position at different times, so
        // compare the common committed prefix and audit the longest log.
        let logs: Vec<_> = NodeId::ALL.iter().map(|&n| sim.committed(n)).collect();
        let log = logs.iter().max_by_key(|l| l.len()).unwrap().clone();
        for l in &logs {
            assert_eq!(
                l[..],
                log[..l.len()],
                "seed {seed}: committed logs disagree"
            );
        }
        let mut state = State::new();
        let mut capacity = BTreeMap::new();
        let mut assigned = 0;
        let (mut results, mut gave_up) = (0usize, 0usize);
        for e in &log {
            if let EntryKind::Result { result, .. } = &e.kind {
                results += 1;
                if matches!(result, proto::OpResult::Refused(why) if why.contains("kept changing"))
                {
                    gave_up += 1;
                }
            }
            if let EntryKind::Result { writes, .. } = &e.kind {
                for w in writes {
                    match w {
                        Write::PutResource(r) => {
                            capacity.insert(r.id, r.capacity);
                        }
                        Write::PutAssignment(_) => assigned += 1,
                        Write::PutIncident(_) => {}
                    }
                }
            }
            state.apply(e);
        }
        assert!(
            assigned > 50,
            "seed {seed}: the workload must exercise assignment"
        );
        let snap = state.snapshot();
        let mut given: BTreeMap<proto::ReqId, u32> = BTreeMap::new();
        let mut per_incident: BTreeMap<proto::ReqId, u32> = BTreeMap::new();
        for a in state.assignments().values() {
            *given.entry(a.resource).or_default() += a.units;
            *per_incident.entry(a.incident).or_default() += 1;
        }
        for (id, (r, _)) in &snap.resources {
            assert_eq!(
                given.get(id).copied().unwrap_or(0) + r.available,
                capacity[id],
                "seed {seed}"
            );
        }
        assert!(
            per_incident.values().all(|&c| c == 1),
            "seed {seed}: an incident got two responders"
        );
        // Concurrent plans must rarely collide: a request refused after the
        // re-plan cap is lost to its client (DL-062). Typically under 1%; the
        // worst seed seen was 6.7%, at 200 requests/s with one planning
        // thread, where queued jobs plan from old snapshots (DL-063).
        assert!(
            gave_up * 10 <= results,
            "seed {seed}: {gave_up} of {results} requests gave up after re-planning"
        );
    }
}
