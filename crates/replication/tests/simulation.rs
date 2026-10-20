//! Randomized runs: message loss, duplication and delay, competing executors,
//! scheduled injections and extra isolations, over many seeds. Each run ends
//! with a quiet period after which the cluster must have converged; safety is
//! checked over the whole history.
//!
//! `SIM_SEEDS=500 cargo test --release -p replication --test simulation`
//! runs more seeds than the default.

mod common;

use common::{Params, Sim};
use proto::NodeId;
use replication::InjectSchedule;
use replication::rng::SplitMix64;

fn seeds(default: u64) -> u64 {
    std::env::var("SIM_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn chaos_params(seed: u64) -> Params {
    let mut r = SplitMix64::new(seed ^ 0x51);
    Params {
        seed,
        drop: r.range(0, 5) as f64 / 100.0,
        dup: r.range(0, 2) as f64 / 100.0,
        delay_min: 1,
        delay_max: r.range(2, 15),
        heartbeat_ms: 100,
        fd_timeout_ms: 300,
        request_every_ms: r.range(4, 15),
        competing: 0.15,
        resources: r.range(2, 8) as u32,
        inject: Some(InjectSchedule {
            seed,
            interval_min_ms: 4_000,
            interval_max_ms: 8_000,
            duration_min_ms: 200,
            duration_max_ms: 3_500,
        }),
    }
}

/// Run `ms` of chaos. With `overlap`, isolations may hit a second node while
/// another is out, which removes the majority for a while.
fn chaos(sim: &mut Sim, ms: u64, seed: u64, overlap: bool) {
    let mut r = SplitMix64::new(seed ^ 0xc4a05);
    let mut isolated_until = [0u64; 3];
    let end = sim.now + ms;
    sim.workload = true;
    while sim.now < end {
        if sim.now.is_multiple_of(500) {
            for n in NodeId::ALL {
                if sim.isolated[n.index()] && sim.now >= isolated_until[n.index()] {
                    sim.isolated[n.index()] = false;
                }
            }
            let out = (0..3)
                .filter(|&i| sim.isolated[i] || sim.nodes[i].silenced)
                .count();
            if r.chance(0.15) && (out == 0 || overlap) {
                let n = NodeId::ALL[r.range(0, 2) as usize];
                sim.isolated[n.index()] = true;
                isolated_until[n.index()] = sim.now + r.range(100, 3_000);
            }
        }
        sim.step();
    }
}

#[test]
fn random_faults_never_break_safety_and_the_cluster_recovers() {
    let n = seeds(40);
    for seed in 0..n {
        let mut sim = Sim::new(chaos_params(seed));
        chaos(&mut sim, 30_000, seed, false);
        sim.settle_and_check();
        assert!(
            sim.committed.len() > 300,
            "seed {seed}: too little progress ({} entries)",
            sim.committed.len()
        );
    }
}

#[test]
fn losing_the_majority_stalls_but_never_corrupts() {
    let n = seeds(20);
    for seed in 1000..1000 + n {
        let mut sim = Sim::new(chaos_params(seed));
        chaos(&mut sim, 30_000, seed, true);
        sim.settle_and_check();
    }
}

/// Prints what the randomized runs actually exercised. Run with
/// `cargo test -p replication --test simulation -- --ignored --nocapture`.
#[test]
#[ignore]
fn report_coverage() {
    use proto::event::EventKind as K;
    let n = seeds(40);
    let mut totals = [0usize; 9];
    for seed in 0..n {
        let mut sim = Sim::new(chaos_params(seed));
        chaos(&mut sim, 30_000, seed, false);
        sim.settle_and_check();
        let stale = sim
            .replies
            .values()
            .flatten()
            .filter(|(_, o)| matches!(o, proto::ProposeOutcome::Stale { .. }))
            .count();
        let dup = sim
            .replies
            .values()
            .flatten()
            .filter(|(_, o)| matches!(o, proto::ProposeOutcome::Duplicate(_)))
            .count();
        let row = [
            sim.committed.len(),
            sim.submitted.len(),
            sim.count_events(K::ElectionWon),
            sim.count_events(K::StepDown),
            sim.count_events(K::InjectStart),
            sim.count_events(K::ViewChange) / 3,
            sim.count_events(K::SyncStart),
            stale,
            dup,
        ];
        for (t, v) in totals.iter_mut().zip(row) {
            *t += v;
        }
    }
    println!(
        "{n} runs: committed entries {}, requests {}, elections won {}, step-downs {}, injections {}, \
         view changes {}, resyncs {}, stale rejections {}, duplicate answers {}",
        totals[0],
        totals[1],
        totals[2],
        totals[3],
        totals[4],
        totals[5],
        totals[6],
        totals[7],
        totals[8]
    );
}
