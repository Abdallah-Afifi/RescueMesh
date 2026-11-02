//! **Placeholder** planning pool, used by the runtime's localhost tests. It
//! burns a chosen amount of CPU per job (`spin_us`), which is how the
//! false-suspicion test saturates a node's cores. `rm-node` itself runs
//! workstream D's real planner and pool (design log DL-065).
//!
//! The planner here is trivial (it stores what it is given and plans no
//! assignments) but it follows the contract: deterministic, pure, records
//! the version of every resource it reads, and returns `Refused` after
//! `timeouts.max_replans` re-plans. `spin_us` burns CPU per job, which is
//! how the false-suspicion test saturates a node's cores.
//!
//! Never report numbers measured with this planner.

use crate::runtime::{PlanSink, PoolFactory};
use crate::task::{PlanJob, PlanPool};
use proto::{
    Incident, IncidentStatus, LoadSample, Op, OpResult, Plan, PlanOutput, ReqId, ResourceOffer,
    Snapshot, Write,
};
use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

// ---------------------------------------------------------------- state machine

/// Since workstream D was merged, the placeholder pool runs on D's real state
/// machine; only the planning stays a placeholder.
pub type DevState = incident::State;

/// The placeholder planner: a pure function of the snapshot and the job.
pub fn plan(snap: &Snapshot, job: &PlanJob, max_replans: u32) -> PlanOutput {
    let req = &job.req.req;
    let refused = |why: &str| PlanOutput {
        read_set: vec![],
        writes: vec![],
        result: OpResult::Refused(why.into()),
    };
    if job.attempt > max_replans {
        return refused("resources kept changing");
    }
    match &req.op {
        Op::ReportIncident {
            category,
            zone,
            severity,
            ttl_ms,
            attachment,
        } => PlanOutput {
            read_set: vec![],
            writes: vec![Write::PutIncident(Incident {
                id: req.id,
                owner: req.id.client,
                category: *category,
                zone: *zone,
                severity: *severity,
                reported_at_ms: req.sent_at_ms,
                status: IncidentStatus::Open,
                ttl_ms: *ttl_ms,
                attachment: attachment.clone(),
            })],
            result: OpResult::Planned(Plan {
                incident: req.id,
                priority: i64::from(severity.0),
                assignments: vec![],
                route: vec![*zone],
                responders: vec![],
            }),
        },
        Op::OfferResource {
            kind,
            capacity,
            zone,
            lease_ms,
        } => PlanOutput {
            read_set: vec![],
            writes: vec![Write::PutResource(ResourceOffer {
                id: req.id,
                owner: req.id.client,
                kind: *kind,
                capacity: *capacity,
                available: *capacity,
                zone: *zone,
                expires_at_ms: req.sent_at_ms + lease_ms,
            })],
            result: OpResult::Accepted,
        },
        Op::UpdateResource {
            resource,
            available,
            extend_lease_ms,
        } => match snap.resources.get(resource) {
            None => refused("unknown resource"),
            Some((r, v)) => {
                let mut r = r.clone();
                r.available = (*available).min(r.capacity);
                if let Some(ms) = extend_lease_ms {
                    r.expires_at_ms = r.expires_at_ms.max(req.sent_at_ms + ms);
                }
                PlanOutput {
                    read_set: vec![(*resource, *v)],
                    writes: vec![Write::PutResource(r)],
                    result: OpResult::Accepted,
                }
            }
        },
        Op::Query(_) => refused("queries are not planned"),
    }
}

// ---------------------------------------------------------------- pool

#[derive(Default)]
struct Queue {
    jobs: VecDeque<PlanJob>,
    running: usize,
    ewma_us: u32,
}

pub struct DevPool {
    q: Arc<(Mutex<Queue>, Condvar)>,
    capacity: usize,
}

impl DevPool {
    /// `threads` workers; `spin_us` of busy CPU per job.
    pub fn factory(threads: usize, capacity: usize, max_replans: u32, spin_us: u64) -> PoolFactory {
        Box::new(move |sink: PlanSink| {
            let q: Arc<(Mutex<Queue>, Condvar)> = Arc::default();
            for i in 0..threads.max(1) {
                let (q, sink) = (q.clone(), sink.clone());
                thread::Builder::new()
                    .name(format!("plan-{i}"))
                    .spawn(move || worker(&q, &sink, max_replans, spin_us))
                    .expect("spawn planning thread");
            }
            Box::new(DevPool { q, capacity }) as Box<dyn PlanPool>
        })
    }
}

fn worker(q: &(Mutex<Queue>, Condvar), sink: &PlanSink, max_replans: u32, spin_us: u64) {
    loop {
        let job = {
            let mut g = q.0.lock().unwrap_or_else(|e| e.into_inner());
            loop {
                if let Some(j) = g.jobs.pop_front() {
                    g.running += 1;
                    break j;
                }
                g = q.1.wait(g).unwrap_or_else(|e| e.into_inner());
            }
        };
        let t = Instant::now();
        let out = plan(&job.snapshot, &job, max_replans);
        let spin = Duration::from_micros(spin_us);
        while t.elapsed() < spin {
            std::hint::spin_loop();
        }
        let us = t.elapsed().as_micros().min(u128::from(u32::MAX)) as u32;
        {
            let mut g = q.0.lock().unwrap_or_else(|e| e.into_inner());
            g.running -= 1;
            g.ewma_us = if g.ewma_us == 0 {
                us
            } else {
                ((u64::from(g.ewma_us) * 7 + u64::from(us)) / 8) as u32
            };
        }
        sink.done(job.req.req.id, out, us);
    }
}

impl PlanPool for DevPool {
    fn submit(&mut self, job: PlanJob) {
        let mut g = self.q.0.lock().unwrap_or_else(|e| e.into_inner());
        g.jobs.push_back(job);
        self.q.1.notify_one();
    }
    fn cancel(&mut self, req: ReqId) {
        let mut g = self.q.0.lock().unwrap_or_else(|e| e.into_inner());
        g.jobs.retain(|j| j.req.req.id != req);
    }
    fn is_full(&self) -> bool {
        let g = self.q.0.lock().unwrap_or_else(|e| e.into_inner());
        g.jobs.len() + g.running >= self.capacity
    }
    fn load(&self) -> LoadSample {
        let g = self.q.0.lock().unwrap_or_else(|e| e.into_inner());
        LoadSample {
            queue_len: (g.jobs.len() + g.running) as u32,
            cpu_permille: 0, // workstream D measures CPU
            service_ewma_us: g.ewma_us,
        }
    }
}
