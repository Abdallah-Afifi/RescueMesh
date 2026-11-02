//! The planning worker pool (design log DL-017, DL-060): a bounded set of OS
//! threads that run the [`Planner`] on immutable snapshots and hand results
//! back through a callback. Planning threads never touch protocol state.
//!
//! A job reads the newest committed snapshot when a thread starts it, not
//! the one current when it was queued (DL-068): a plan is then as fresh as
//! possible, so fewer are rejected as stale, and at most one snapshot per
//! thread is kept alive however long the queue grows.
//!
//! Implements the core task's [`PlanPool`] rules (DL-052): `submit` never
//! refuses a job; `is_full` is the admission limit for new client requests;
//! a job past the re-plan cap is answered `Refused` without planning, so the
//! request still commits and its retries are answered.

use crate::task::{PlanJob, PlanPool};
use proto::config::Config;
use proto::{LoadSample, OpResult, PlanOutput, Planner, ReqId};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System};

/// Called on a planning thread with each finished plan. The runtime forwards
/// it to [`crate::NodeTask::on_planned`].
/// Arguments: the request, its plan, and the time spent planning it in
/// microseconds (0 when it was refused without planning).
pub type OnPlanned = Arc<dyn Fn(ReqId, PlanOutput, u32) + Send + Sync>;

/// Weight of the newest sample in the service-time average, as 1/N.
const EWMA_N: u32 = 8;

/// After this long with nothing to plan, the service-time average decays by
/// 1/[`EWMA_N`] at each load sample. Otherwise a node that was slow once,
/// for example under the load knob, would keep its old average while idle
/// and might never be ranked first again to measure a new one.
const IDLE_DECAY_AFTER: Duration = Duration::from_secs(1);

#[derive(Default)]
struct Jobs {
    queue: VecDeque<PlanJob>,
    running: usize,
    stop: bool,
}

struct Shared {
    jobs: Mutex<Jobs>,
    ready: Condvar,
    service_ewma_us: AtomicU32,
    /// When the last plan finished.
    last_done: Mutex<Instant>,
}

pub struct WorkerPool {
    shared: Arc<Shared>,
    threads: Vec<JoinHandle<()>>,
    capacity: usize,
    cpu: Mutex<CpuSampler>,
}

impl WorkerPool {
    /// `planner.worker_threads` threads (default: cores − 1, at least one),
    /// admission limit `planner.queue_capacity`, re-plan cap
    /// `timeouts.max_replans`.
    pub fn new(cfg: &Config, planner: Arc<dyn Planner>, on_planned: OnPlanned) -> WorkerPool {
        let threads = cfg.planner.worker_threads.unwrap_or_else(|| {
            // Never 0: a pool with no threads would strand every claimed request.
            std::thread::available_parallelism().map_or(1, |n| n.get().saturating_sub(1).max(1))
        });
        WorkerPool::with_parts(
            threads,
            cfg.planner.queue_capacity,
            cfg.timeouts.max_replans,
            planner,
            on_planned,
        )
    }

    pub fn with_parts(
        threads: usize,
        capacity: usize,
        max_replans: u32,
        planner: Arc<dyn Planner>,
        on_planned: OnPlanned,
    ) -> WorkerPool {
        let shared = Arc::new(Shared {
            jobs: Mutex::new(Jobs::default()),
            ready: Condvar::new(),
            service_ewma_us: AtomicU32::new(0),
            last_done: Mutex::new(Instant::now()),
        });
        let threads = (0..threads.max(1))
            .map(|_| {
                let (shared, planner, done) = (shared.clone(), planner.clone(), on_planned.clone());
                std::thread::spawn(move || work(&shared, &*planner, &*done, max_replans))
            })
            .collect();
        WorkerPool {
            shared,
            threads,
            capacity,
            cpu: Mutex::new(CpuSampler::new()),
        }
    }

    fn jobs(&self) -> std::sync::MutexGuard<'_, Jobs> {
        self.shared.jobs.lock().unwrap_or_else(|e| e.into_inner())
    }
}

fn work(
    shared: &Shared,
    planner: &dyn Planner,
    done: &(dyn Fn(ReqId, PlanOutput, u32) + Send + Sync),
    max_replans: u32,
) {
    loop {
        let job = {
            let mut j = shared.jobs.lock().unwrap_or_else(|e| e.into_inner());
            while j.queue.is_empty() && !j.stop {
                j = shared.ready.wait(j).unwrap_or_else(|e| e.into_inner());
            }
            if j.stop {
                return;
            }
            j.running += 1;
            j.queue.pop_front().expect("queue is not empty")
        };
        let (out, took) = if job.attempt > max_replans {
            // Re-planning gave up: commit a refusal so retries get an answer.
            let refused = PlanOutput {
                read_set: vec![],
                writes: vec![],
                result: OpResult::Refused("resources kept changing; please retry".into()),
            };
            (refused, 0)
        } else {
            let started = Instant::now();
            // A planner panic must not kill the worker and strand a claimed
            // request (rule: never drop an Execute): it becomes a refusal.
            let out = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                planner.plan(&job.snapshot.get(), &job.req.req)
            }))
            .unwrap_or_else(|_| PlanOutput {
                read_set: vec![],
                writes: vec![],
                result: OpResult::Refused("planning failed".into()),
            });
            let took = u32::try_from(started.elapsed().as_micros()).unwrap_or(u32::MAX);
            let _ =
                shared
                    .service_ewma_us
                    .try_update(Ordering::Relaxed, Ordering::Relaxed, |old| {
                        Some(if old == 0 {
                            took
                        } else {
                            old - old / EWMA_N + took / EWMA_N
                        })
                    });
            (out, took)
        };
        shared
            .jobs
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .running -= 1;
        *shared.last_done.lock().unwrap_or_else(|e| e.into_inner()) = Instant::now();
        done(job.req.req.id, out, took);
    }
}

impl PlanPool for WorkerPool {
    fn submit(&mut self, job: PlanJob) {
        self.jobs().queue.push_back(job);
        self.shared.ready.notify_one();
    }

    fn cancel(&mut self, req: ReqId) {
        self.jobs().queue.retain(|j| j.req.req.id != req);
    }

    fn is_full(&self) -> bool {
        let j = self.jobs();
        j.queue.len() + j.running >= self.capacity
    }

    fn load(&self) -> LoadSample {
        let queue = {
            let j = self.jobs();
            j.queue.len() + j.running
        };
        let idle_for = self
            .shared
            .last_done
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .elapsed();
        if queue == 0 && idle_for >= IDLE_DECAY_AFTER {
            let _ = self.shared.service_ewma_us.try_update(
                Ordering::Relaxed,
                Ordering::Relaxed,
                |old| Some(old - old / EWMA_N),
            );
        }
        LoadSample {
            queue_len: u32::try_from(queue).unwrap_or(u32::MAX),
            cpu_permille: self
                .cpu
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .permille(),
            service_ewma_us: self.shared.service_ewma_us.load(Ordering::Relaxed),
        }
    }
}

impl Drop for WorkerPool {
    fn drop(&mut self) {
        self.jobs().stop = true;
        self.shared.ready.notify_all();
        for t in self.threads.drain(..) {
            let _ = t.join();
        }
    }
}

/// Shortest interval CPU use is measured over. Linux counts process CPU time
/// in 10 ms ticks, so shorter windows are mostly rounding.
const CPU_WINDOW: Duration = Duration::from_millis(250);

/// This process's CPU use as a share of the whole machine, in thousandths.
/// Measures the whole process, so the load knob's burner threads count too
/// (selection's `knob` relies on this).
///
/// Computed from the process's accumulated CPU time over our own wall-clock
/// window, not from `sysinfo`'s `cpu_usage`: on Apple Silicon that divides by
/// a wrong interval when refreshed less than 200 ms apart, and about one
/// reading in thirty came out near zero.
pub(crate) struct CpuSampler {
    sys: System,
    pid: Pid,
    cores: u64,
    last: Instant,
    last_cpu_ms: u64,
    value: u16,
}

impl CpuSampler {
    pub(crate) fn new() -> CpuSampler {
        let mut s = CpuSampler {
            sys: System::new(),
            pid: Pid::from_u32(std::process::id()),
            cores: std::thread::available_parallelism().map_or(1, |n| n.get() as u64),
            last: Instant::now(),
            last_cpu_ms: 0,
            value: 0,
        };
        s.last_cpu_ms = s.cpu_ms(); // the first reading is only a baseline
        s
    }

    /// CPU-milliseconds this process has used since it started.
    fn cpu_ms(&mut self) -> u64 {
        self.sys.refresh_processes_specifics(
            ProcessesToUpdate::Some(&[self.pid]),
            true,
            ProcessRefreshKind::nothing().with_cpu(),
        );
        self.sys
            .process(self.pid)
            .map_or(self.last_cpu_ms, |p| p.accumulated_cpu_time())
    }

    /// Re-measured at most once per [`CPU_WINDOW`]; heartbeats come more
    /// often than that, so in between the last value is reused.
    pub(crate) fn permille(&mut self) -> u16 {
        let wall = self.last.elapsed();
        if wall >= CPU_WINDOW {
            let cpu = self.cpu_ms();
            let used = cpu.saturating_sub(self.last_cpu_ms);
            let capacity = (wall.as_millis() as u64).max(1) * self.cores;
            self.value = (used * 1000 / capacity).min(1000) as u16;
            self.last = Instant::now();
            self.last_cpu_ms = cpu;
        }
        self.value
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proto::{ClientId, ClientReq, Op, PROTOCOL_VERSION, Query, Request, Snapshot};
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    use std::sync::mpsc;
    use std::time::Duration;

    /// Plans by echoing the request, after waiting for `open`.
    struct Gate {
        open: AtomicBool,
        planned: AtomicUsize,
        work: Duration,
    }

    impl Planner for Gate {
        fn plan(&self, _: &Snapshot, _: &Request) -> PlanOutput {
            while !self.open.load(Ordering::SeqCst) {
                std::thread::sleep(Duration::from_millis(1));
            }
            std::thread::sleep(self.work);
            self.planned.fetch_add(1, Ordering::SeqCst);
            PlanOutput {
                read_set: vec![],
                writes: vec![],
                result: OpResult::Accepted,
            }
        }
    }

    fn gate(open: bool, work_ms: u64) -> Arc<Gate> {
        Arc::new(Gate {
            open: AtomicBool::new(open),
            planned: AtomicUsize::new(0),
            work: Duration::from_millis(work_ms),
        })
    }

    fn job(seq: u64, attempt: u32) -> PlanJob {
        let id = ReqId {
            client: ClientId {
                machine: 1,
                process: 1,
                user: 1,
            },
            incarnation: 1,
            seq,
        };
        PlanJob {
            req: ClientReq {
                version: PROTOCOL_VERSION,
                req: Request {
                    id,
                    sent_at_ms: 0,
                    op: Op::Query(Query::Resource(id)),
                },
                rtt_us: [None; 3],
                acked_up_to: 0,
            },
            snapshot: crate::task::SnapshotSlot::new(Arc::new(Snapshot::default())),
            attempt,
        }
    }

    fn pool(
        threads: usize,
        capacity: usize,
        planner: Arc<Gate>,
    ) -> (WorkerPool, mpsc::Receiver<(ReqId, PlanOutput)>) {
        let (tx, rx) = mpsc::channel();
        let tx = Mutex::new(tx);
        let done: OnPlanned = Arc::new(move |r, o, _| {
            let _ = tx.lock().unwrap().send((r, o));
        });
        (
            WorkerPool::with_parts(threads, capacity, 3, planner, done),
            rx,
        )
    }

    fn recv(rx: &mpsc::Receiver<(ReqId, PlanOutput)>) -> (ReqId, PlanOutput) {
        rx.recv_timeout(Duration::from_secs(5)).expect("a plan")
    }

    #[test]
    fn every_job_is_planned_once_even_past_the_admission_limit() {
        let g = gate(false, 0);
        let (mut p, rx) = pool(2, 4, g.clone());
        for s in 0..10 {
            p.submit(job(s, 0)); // submit never refuses
        }
        assert!(p.is_full());
        assert_eq!(p.load().queue_len, 10);
        g.open.store(true, Ordering::SeqCst);
        let mut got: Vec<u64> = (0..10).map(|_| recv(&rx).0.seq).collect();
        got.sort_unstable();
        assert_eq!(got, (0..10).collect::<Vec<_>>());
        assert!(rx.recv_timeout(Duration::from_millis(50)).is_err());
        assert_eq!(g.planned.load(Ordering::SeqCst), 10);
        assert!(!p.is_full());
        assert_eq!(p.load().queue_len, 0);
    }

    #[test]
    fn a_job_past_the_replan_cap_is_refused_without_planning() {
        let g = gate(true, 0);
        let (mut p, rx) = pool(1, 4, g.clone());
        p.submit(job(1, 4));
        assert!(matches!(recv(&rx).1.result, OpResult::Refused(_)));
        p.submit(job(2, 3));
        assert!(matches!(recv(&rx).1.result, OpResult::Accepted));
        assert_eq!(g.planned.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn a_cancelled_job_that_has_not_started_is_never_planned() {
        let g = gate(false, 0);
        let (mut p, rx) = pool(1, 8, g.clone());
        for s in 0..3 {
            p.submit(job(s, 0));
        }
        std::thread::sleep(Duration::from_millis(20)); // job 0 is now running
        p.cancel(job(2, 0).req.req.id);
        g.open.store(true, Ordering::SeqCst);
        let got: Vec<u64> = (0..2).map(|_| recv(&rx).0.seq).collect();
        assert_eq!(got, vec![0, 1]);
        assert!(rx.recv_timeout(Duration::from_millis(50)).is_err());
    }

    #[test]
    fn service_time_is_reported_as_a_moving_average() {
        let g = gate(true, 20);
        let (mut p, rx) = pool(1, 8, g);
        assert_eq!(p.load().service_ewma_us, 0);
        p.submit(job(1, 0));
        recv(&rx);
        let us = p.load().service_ewma_us;
        assert!((20_000..200_000).contains(&us), "{us}");
        assert!(p.load().cpu_permille <= 1000);
    }

    /// Waits for `open`, then records the log position of its snapshot.
    struct Seen {
        open: AtomicBool,
        seen: Mutex<Vec<u64>>,
    }

    impl Planner for Seen {
        fn plan(&self, snapshot: &Snapshot, _: &Request) -> PlanOutput {
            while !self.open.load(Ordering::SeqCst) {
                std::thread::sleep(Duration::from_millis(1));
            }
            self.seen.lock().unwrap().push(snapshot.applied.0);
            PlanOutput {
                read_set: vec![],
                writes: vec![],
                result: OpResult::Accepted,
            }
        }
    }

    #[test]
    fn a_job_reads_the_snapshot_current_when_it_starts_not_when_queued() {
        let seen = Arc::new(Seen {
            open: AtomicBool::new(false),
            seen: Mutex::new(Vec::new()),
        });
        let (tx, rx) = mpsc::channel();
        let tx = Mutex::new(tx);
        let done: OnPlanned = Arc::new(move |r, _, _| {
            let _ = tx.lock().unwrap().send(r);
        });
        let mut p = WorkerPool::with_parts(1, 8, 3, seen.clone(), done);
        p.submit(job(1, 0)); // takes the only thread and waits at the gate
        std::thread::sleep(Duration::from_millis(20));
        let queued = job(2, 0);
        let slot = queued.snapshot.clone();
        p.submit(queued);
        // Committed state moves on while the job waits in the queue.
        slot.set(Arc::new(Snapshot {
            applied: proto::Seq(42),
            ..Snapshot::default()
        }));
        seen.open.store(true, Ordering::SeqCst);
        for _ in 0..2 {
            rx.recv_timeout(Duration::from_secs(5)).unwrap();
        }
        assert_eq!(*seen.seen.lock().unwrap(), vec![0, 42]);
    }

    #[test]
    fn cpu_use_of_a_busy_thread_is_measured() {
        let mut c = CpuSampler::new();
        let t = Instant::now();
        let mut x = 1u64;
        while t.elapsed() < CPU_WINDOW + Duration::from_millis(100) {
            x = std::hint::black_box(x.rotate_left(3) ^ 7);
        }
        let v = c.permille();
        // One busy thread out of `cores`; allow wide margins for CI noise.
        let one_core = 1000 / c.cores;
        assert!(
            v >= (one_core / 3) as u16 && v <= 1000,
            "{v} permille, one core is {one_core}"
        );
    }

    #[test]
    fn the_service_time_average_decays_while_idle() {
        let g = gate(true, 20);
        let (mut p, rx) = pool(1, 8, g);
        p.submit(job(1, 0));
        recv(&rx);
        let busy = p.load().service_ewma_us;
        assert!(busy >= 20_000, "{busy}");
        std::thread::sleep(IDLE_DECAY_AFTER + Duration::from_millis(50));
        let mut last = busy;
        for _ in 0..10 {
            last = p.load().service_ewma_us;
        }
        assert!(last < busy / 2, "{busy} -> {last}");
    }

    #[test]
    fn dropping_the_pool_stops_its_threads() {
        let (mut p, _rx) = pool(3, 8, gate(true, 0));
        p.submit(job(1, 0));
        drop(p);
    }
}
