//! The planning worker pool (design log DL-017, DL-060): a bounded set of OS
//! threads that run the [`Planner`] on immutable snapshots and hand results
//! back through a callback. Planning threads never touch protocol state.
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
use std::time::Instant;
use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System};

/// Called on a planning thread with each finished plan. The runtime forwards
/// it to [`crate::NodeTask::on_planned`].
/// Arguments: the request, its plan, and the time spent planning it in
/// microseconds (0 when it was refused without planning).
pub type OnPlanned = Arc<dyn Fn(ReqId, PlanOutput, u32) + Send + Sync>;

/// Weight of the newest sample in the service-time average, as 1/N.
const EWMA_N: u32 = 8;

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
                planner.plan(&job.snapshot, &job.req.req)
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

/// This process's CPU use as a share of the whole machine, in thousandths.
/// Measures the whole process, so the load knob's burner threads count too
/// (selection's `knob` relies on this).
struct CpuSampler {
    sys: System,
    pid: Pid,
    cores: u32,
    last: Instant,
    value: u16,
}

impl CpuSampler {
    fn new() -> CpuSampler {
        let mut s = CpuSampler {
            sys: System::new(),
            pid: Pid::from_u32(std::process::id()),
            cores: std::thread::available_parallelism().map_or(1, |n| n.get() as u32),
            last: Instant::now(),
            value: 0,
        };
        s.refresh(); // the first reading is only a baseline
        s
    }

    fn refresh(&mut self) {
        self.sys.refresh_processes_specifics(
            ProcessesToUpdate::Some(&[self.pid]),
            true,
            ProcessRefreshKind::nothing().with_cpu(),
        );
        self.last = Instant::now();
    }

    /// Re-measured at most once per `sysinfo` minimum interval; heartbeats
    /// come more often than that, so in between the last value is reused.
    fn permille(&mut self) -> u16 {
        if self.last.elapsed() >= sysinfo::MINIMUM_CPU_UPDATE_INTERVAL {
            self.refresh();
            let percent_of_one_core = self.sys.process(self.pid).map_or(0.0, |p| p.cpu_usage());
            let of_machine = (percent_of_one_core * 10.0).max(0.0) as u32 / self.cores;
            self.value = of_machine.min(1000) as u16;
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
            snapshot: Arc::new(Snapshot::default()),
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

    #[test]
    fn dropping_the_pool_stops_its_threads() {
        let (mut p, _rx) = pool(3, 8, gate(true, 0));
        p.submit(job(1, 0));
        drop(p);
    }
}
