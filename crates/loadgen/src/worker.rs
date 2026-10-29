//! One client process: M simulated users as tasks, one shared middleware,
//! one event log (design log DL-018).
//!
//! Timeline, identical in every process of every machine because the
//! frontend hands out one wall-clock `start_at_ms`:
//!
//! ```text
//! start_at          +warmup                +duration              +drain
//!   |  warm-up phase   |   measure phase     |  cooldown: no new   |
//!   |  (discarded)     |   (analysed)        |  requests, drain    |
//! ```
//!
//! Requests are classified by the phase in which they *start*.

use crate::params::{LoadParams, Mode};
use crate::sampler::ProcSampler;
use crate::summary::{Counts, WorkerSummary};
use crate::workload::{Kind, Mix, UserGen};
use client::hist::LatencyHist;
use client::rng::{Rng, seed_from};
use client::{Client, EventLog, Middleware, Phase, SubmitError, fresh_incarnation, now_ms};
use proto::config::Config;
use proto::event::EventKind;
use proto::{ClientId, Op, OpResult};
use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::{self, Instant};

pub struct WorkerArgs {
    pub cfg: Config,
    pub machine: u16,
    pub process: u16,
    pub params: LoadParams,
    pub start_at_ms: u64,
}

#[derive(Clone, Copy)]
enum Field {
    Started,
    Ok,
    GaveUp,
    Refused,
    Retries,
    Dropped,
}

#[derive(Default)]
struct AtomicCounts([AtomicU64; 6]);

impl AtomicCounts {
    fn bump(&self, f: Field, by: u64) {
        self.0[f as usize].fetch_add(by, Ordering::Relaxed);
    }

    fn snapshot(&self) -> Counts {
        let g = |f: Field| self.0[f as usize].load(Ordering::Relaxed);
        Counts {
            started: g(Field::Started),
            ok: g(Field::Ok),
            gave_up: g(Field::GaveUp),
            refused: g(Field::Refused),
            retries: g(Field::Retries),
            dropped_client: g(Field::Dropped),
        }
    }
}

struct Run {
    end_gen: AtomicBool,
    inflight: AtomicUsize,
    total: AtomicCounts,
    measure: AtomicCounts,
    hist: Mutex<LatencyHist>,
}

impl Run {
    fn bump(&self, measuring: bool, f: Field, by: u64) {
        self.total.bump(f, by);
        if measuring {
            self.measure.bump(f, by);
        }
    }
}

struct User {
    client: Client,
    ops: Mutex<UserGen>,
}

impl User {
    fn next(&self) -> (Op, Kind) {
        self.ops.lock().unwrap_or_else(|e| e.into_inner()).next_op()
    }
}

/// Sends one operation and records the outcome.
/// Issues one request. `scheduled` is its planned arrival in open-loop mode,
/// so latency includes any delay before the client got to it.
async fn issue(
    run: &Run,
    mw: &Middleware,
    user: &User,
    op: Op,
    kind: Kind,
    scheduled: Option<Instant>,
) {
    let measuring = mw.phase() == Phase::Measure;
    run.bump(measuring, Field::Started, 1);
    let result = match scheduled {
        Some(at) => user.client.submit_at(op, at).await,
        None => user.client.submit(op).await,
    };
    match result {
        Ok(done) => {
            run.bump(measuring, Field::Ok, 1);
            run.bump(measuring, Field::Retries, u64::from(done.attempts - 1));
            if matches!(done.result, OpResult::Refused(_)) {
                run.bump(measuring, Field::Refused, 1);
            } else {
                user.ops
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .note(kind, done.req);
            }
            if measuring {
                run.hist
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .record(done.latency.as_micros() as u64);
            }
        }
        Err(SubmitError::GaveUp { attempts, .. }) => {
            run.bump(measuring, Field::GaveUp, 1);
            run.bump(measuring, Field::Retries, u64::from(attempts - 1));
        }
        Err(SubmitError::Encode(e)) => {
            eprintln!("cannot encode request: {e}");
            run.bump(measuring, Field::GaveUp, 1);
        }
    }
}

async fn closed_user(run: Arc<Run>, mw: Middleware, user: Arc<User>, think_ms: u64, mut rng: Rng) {
    // Spread the users' first requests over a second instead of one burst.
    time::sleep(Duration::from_millis(rng.below(1000))).await;
    while !run.end_gen.load(Ordering::Relaxed) {
        let (op, kind) = user.next();
        run.inflight.fetch_add(1, Ordering::Relaxed);
        issue(&run, &mw, &user, op, kind, None).await;
        run.inflight.fetch_sub(1, Ordering::Relaxed);
        if think_ms > 0 {
            time::sleep(Duration::from_secs_f64(rng.exp(think_ms as f64 / 1000.0))).await;
        }
    }
}

/// Poisson arrivals at `rate` per second, spread over the users in turn.
/// Arrival times follow a schedule that does not drift when the process is
/// briefly late: missed arrivals are issued at once, in a burst.
async fn open_loop(
    run: Arc<Run>,
    mw: Middleware,
    users: Vec<Arc<User>>,
    rate: f64,
    max_inflight: usize,
    mut rng: Rng,
) {
    let mean_gap = 1.0 / rate;
    let mut next = Instant::now();
    let mut turn = 0usize;
    while !run.end_gen.load(Ordering::Relaxed) {
        let now = Instant::now();
        while next <= now {
            let measuring = mw.phase() == Phase::Measure;
            if run.inflight.load(Ordering::Relaxed) >= max_inflight {
                run.bump(measuring, Field::Dropped, 1);
            } else {
                let user = users[turn % users.len()].clone();
                let (op, kind) = user.next();
                run.inflight.fetch_add(1, Ordering::Relaxed);
                let (run, mw) = (run.clone(), mw.clone());
                let scheduled = next;
                tokio::spawn(async move {
                    issue(&run, &mw, &user, op, kind, Some(scheduled)).await;
                    run.inflight.fetch_sub(1, Ordering::Relaxed);
                });
            }
            turn += 1;
            next += Duration::from_secs_f64(rng.exp(mean_gap));
        }
        // Wake at the next arrival, but at least every 100 ms to notice the end.
        time::sleep_until(next.min(now + Duration::from_millis(100))).await;
    }
}

async fn sample_loop(log: Arc<EventLog>, mw: Middleware, run: Arc<Run>) {
    let mut sampler = ProcSampler::new();
    let mut tick = time::interval(Duration::from_secs(1));
    tick.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
    tick.tick().await;
    loop {
        tick.tick().await;
        let p = sampler.sample();
        let s = mw.stats();
        let net = mw.net().stats();
        let c = run.total.snapshot();
        log.emit(
            log.event(EventKind::ResourceSample)
                .field("cpu_permille", p.cpu_permille)
                .field("cpu_core_permille", p.cpu_core_permille)
                .field("rss_bytes", p.rss_bytes)
                .field("net_rx_bytes", net.rx_bytes.load(Ordering::Relaxed))
                .field("net_tx_bytes", net.tx_bytes.load(Ordering::Relaxed))
                .field("queue_len", run.inflight.load(Ordering::Relaxed) as u64)
                .field("submitted", s.submitted)
                .field("completed", s.completed)
                .field("gave_up", s.gave_up)
                .field("dropped_client", c.dropped_client)
                .field("phase", mw.phase().as_str()),
        );
    }
}

/// Wall-clock end of the warm-up and of the measure phase.
fn phase_bounds(start_at_ms: u64, warmup_secs: u64, duration_secs: u64) -> (u64, u64) {
    let warm_end = start_at_ms + warmup_secs * 1000;
    (warm_end, warm_end + duration_secs * 1000)
}

async fn sleep_until_ms(at_ms: u64) {
    let now = now_ms();
    if at_ms > now {
        time::sleep(Duration::from_millis(at_ms - now)).await;
    }
}

/// Runs one worker to completion and returns (and writes) its summary.
pub async fn run(args: WorkerArgs) -> io::Result<WorkerSummary> {
    let WorkerArgs {
        cfg,
        machine,
        process,
        params,
        start_at_ms,
    } = args;
    let mix =
        Mix::parse(&params.mix).map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
    let machine_name = format!("client{machine}");
    let log = EventLog::create(
        &cfg.run.log_dir,
        &cfg.run.run_id,
        &machine_name,
        &process.to_string(),
        None,
    )?;
    let incarnation = fresh_incarnation((u64::from(machine) << 16) | u64::from(process));
    let mw = Middleware::start(&cfg, Some(log.clone()), incarnation);
    let connected = mw.wait_connected(Duration::from_secs(10)).await;
    if !connected {
        eprintln!(
            "worker {machine}.{process}: not all nodes reachable yet; requests will be retried"
        );
    }

    let run = Arc::new(Run {
        end_gen: AtomicBool::new(false),
        inflight: AtomicUsize::new(0),
        total: AtomicCounts::default(),
        measure: AtomicCounts::default(),
        hist: Mutex::new(LatencyHist::new()),
    });
    let users: Vec<Arc<User>> = (0..params.users)
        .map(|u| {
            let id = ClientId {
                machine,
                process,
                user: u,
            };
            Arc::new(User {
                client: mw.client(id),
                ops: Mutex::new(UserGen::new(params.seed, id, cfg.planner.zones, mix)),
            })
        })
        .collect();

    let sampler = tokio::spawn(sample_loop(log.clone(), mw.clone(), run.clone()));
    {
        let run = run.clone();
        tokio::spawn(async move {
            if tokio::signal::ctrl_c().await.is_ok() {
                run.end_gen.store(true, Ordering::Relaxed);
            }
        });
    }

    // Phase boundaries are wall-clock times derived from the shared
    // `start_at_ms`, so a worker that starts late (a slow connect, a busy
    // machine) still measures exactly the same window as every other worker.
    let (warm_end, measure_end) =
        phase_bounds(start_at_ms, params.warmup_secs, params.duration_secs);
    sleep_until_ms(start_at_ms).await;
    mw.set_phase(Phase::Warmup);
    let phases = {
        let (run, mw) = (run.clone(), mw.clone());
        let duration = params.duration_secs;
        tokio::spawn(async move {
            sleep_until_ms(warm_end).await;
            mw.set_phase(Phase::Measure);
            if duration > 0 {
                sleep_until_ms(measure_end).await;
                mw.set_phase(Phase::Cooldown);
                run.end_gen.store(true, Ordering::Relaxed);
            }
        })
    };

    let drain = Duration::from_secs(params.drain_secs);
    match params.mode {
        Mode::Closed => {
            let mut tasks = Vec::new();
            for (i, u) in users.iter().enumerate() {
                let rng = Rng::new(seed_from(&[
                    params.seed,
                    u64::from(machine),
                    u64::from(process),
                    i as u64,
                    0xC105ED,
                ]));
                tasks.push(tokio::spawn(closed_user(
                    run.clone(),
                    mw.clone(),
                    u.clone(),
                    params.think_ms,
                    rng,
                )));
            }
            let all = async {
                for t in tasks {
                    let _ = t.await;
                }
            };
            // Users stop after their current request, so most of the drain is
            // spent waiting for the last replies or retry timeouts.
            let end = async {
                while !run.end_gen.load(Ordering::Relaxed) {
                    time::sleep(Duration::from_millis(50)).await;
                }
                time::sleep(drain).await;
            };
            tokio::select! {
                () = all => {}
                () = end => eprintln!("worker {machine}.{process}: drain period expired with requests outstanding"),
            }
        }
        Mode::Open => {
            let rate = params.rate / f64::from(params.processes);
            let rng = Rng::new(seed_from(&[
                params.seed,
                u64::from(machine),
                u64::from(process),
                0x0FE17,
            ]));
            open_loop(
                run.clone(),
                mw.clone(),
                users,
                rate,
                params.max_inflight,
                rng,
            )
            .await;
            let deadline = Instant::now() + drain;
            while run.inflight.load(Ordering::Relaxed) > 0 && Instant::now() < deadline {
                time::sleep(Duration::from_millis(50)).await;
            }
            if run.inflight.load(Ordering::Relaxed) > 0 {
                eprintln!(
                    "worker {machine}.{process}: drain period expired with requests outstanding"
                );
            }
        }
    }

    phases.abort();
    sampler.abort();
    let net = mw.net().stats();
    let summary = WorkerSummary {
        machine,
        process,
        users: params.users,
        mode: params.mode.as_str().to_owned(),
        incarnation,
        connected_at_start: connected,
        total: run.total.snapshot(),
        measure: run.measure.snapshot(),
        hist: run
            .hist
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .snapshot(),
        events_dropped: 0,
        send_dropped: net.send_dropped.load(Ordering::Relaxed),
        reconnects: net.reconnects.load(Ordering::Relaxed),
    };
    mw.shutdown();
    log.finish();
    let summary = WorkerSummary {
        events_dropped: log.dropped(),
        ..summary
    };
    let path = log
        .path()
        .with_file_name(format!("{machine_name}-{process}.summary.json"));
    std::fs::write(&path, serde_json::to_vec_pretty(&summary)?)?;
    Ok(summary)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn phase_bounds_depend_only_on_the_shared_start() {
        // Every worker computes the same boundaries, however late it started.
        assert_eq!(phase_bounds(1_000_000, 10, 60), (1_010_000, 1_070_000));
        assert_eq!(phase_bounds(1_000_000, 0, 0), (1_000_000, 1_000_000));
    }
}
