//! Deterministic simulator of three full cloud nodes: each runs a
//! [`NodeTask`] (replication core + selector) with a fake state machine and a
//! fake planning pool, over a simulated network with delay, loss, heartbeats
//! and a timeout failure detector. Clients send every request to all three
//! nodes and retry it until they get an `Ok` answer.

#![allow(dead_code)]

use node::{NodeTask, Out, PlanJob, PlanPool};
use proto::config::{RankerKind, ScoreCfg, Timeouts};
use proto::{
    Category, ClientId, ClientReq, Entry, EntryKind, LoadSample, NodeId, NodeMsg, Op, OpResult,
    PROTOCOL_VERSION, Plan, PlanOutput, ReplyBody, ReqId, Request, ResourceId, ResourceKind,
    ResourceOffer, Seq, Severity, Snapshot, StateMachine, Version, Write, ZoneId,
};
use replication::rng::SplitMix64;
use replication::{CoreCfg, InjectSchedule, Role};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, Mutex};

pub const INITIAL: u32 = 1_000_000;
pub const MAX_REPLANS: u32 = 3;

// ---------------------------------------------------------------- state machine

pub struct FakeState {
    snap: Arc<Snapshot>,
}

impl FakeState {
    pub fn new() -> FakeState {
        FakeState {
            snap: Arc::new(Snapshot::default()),
        }
    }
}

impl StateMachine for FakeState {
    fn apply(&mut self, entry: &Entry) {
        let s = Arc::make_mut(&mut self.snap);
        s.applied = entry.seq;
        if let EntryKind::Result { writes, .. } = &entry.kind {
            for w in writes {
                if let Write::PutResource(r) = w {
                    s.resources.insert(r.id, (r.clone(), Version(entry.seq.0)));
                }
            }
        }
    }
    fn snapshot(&self) -> Arc<Snapshot> {
        self.snap.clone()
    }
    fn version_of(&self, r: ResourceId) -> Option<Version> {
        self.snap.resources.get(&r).map(|(_, v)| *v)
    }
    fn applied(&self) -> Seq {
        self.snap.applied
    }
    fn query(&self, _req: &Request) -> OpResult {
        OpResult::Accepted
    }
}

// ---------------------------------------------------------------- planning pool

#[derive(Default)]
pub struct PoolState {
    pub queue: VecDeque<PlanJob>,
    pub running: Vec<(u64, PlanJob)>,
    /// Admission limit for new client requests.
    pub capacity: usize,
    /// While true, queued jobs do not start (a stalled worker pool).
    pub paused: bool,
    pub submitted: u64,
    pub cancelled: BTreeSet<ReqId>,
    pub max_queue_seen: usize,
    pub attempts_seen: BTreeMap<u32, u64>,
}

#[derive(Clone)]
pub struct FakePool(pub Arc<Mutex<PoolState>>);

impl PlanPool for FakePool {
    fn submit(&mut self, job: PlanJob) {
        let mut p = self.0.lock().unwrap();
        p.submitted += 1;
        *p.attempts_seen.entry(job.attempt).or_default() += 1;
        p.cancelled.remove(&job.req.req.id);
        p.queue.push_back(job);
        p.max_queue_seen = p.max_queue_seen.max(p.queue.len());
    }
    fn cancel(&mut self, req: ReqId) {
        let mut p = self.0.lock().unwrap();
        p.queue.retain(|j| j.req.req.id != req);
        p.cancelled.insert(req);
    }
    fn is_full(&self) -> bool {
        let p = self.0.lock().unwrap();
        p.queue.len() + p.running.len() >= p.capacity
    }
    fn load(&self) -> LoadSample {
        let p = self.0.lock().unwrap();
        LoadSample {
            queue_len: (p.queue.len() + p.running.len()) as u32,
            cpu_permille: 0,
            service_ewma_us: 3_000,
        }
    }
}

pub fn resource_of(req: &ReqId, resources: u32) -> ResourceId {
    let r = (proto::hash::request(req) % u64::from(resources)) as u32;
    ReqId {
        client: ClientId {
            machine: 0,
            process: 0,
            user: r,
        },
        incarnation: 0,
        seq: 0,
    }
}

/// The fake planner: decrements one resource, or refuses after the re-plan
/// cap (workstream D's rule).
pub fn plan(job: &PlanJob, executor: NodeId, resources: u32) -> PlanOutput {
    let id = job.req.req.id;
    if job.attempt > MAX_REPLANS {
        return PlanOutput {
            read_set: vec![],
            writes: vec![],
            result: OpResult::Refused("resources kept changing".into()),
        };
    }
    let r = resource_of(&id, resources);
    let (avail, ver) = job
        .snapshot
        .resources
        .get(&r)
        .map(|(o, v)| (o.available, *v))
        .unwrap_or((INITIAL, Version(0)));
    PlanOutput {
        read_set: vec![(r, ver)],
        writes: vec![Write::PutResource(ResourceOffer {
            id: r,
            owner: r.client,
            kind: ResourceKind::Medic,
            capacity: INITIAL,
            available: avail - 1,
            zone: ZoneId(0),
            expires_at_ms: 0,
        })],
        result: OpResult::Planned(Plan {
            incident: id,
            priority: i64::from(avail),
            assignments: vec![],
            route: vec![ZoneId(u16::from(executor.0))],
            responders: vec![],
        }),
    }
}

/// One offer per four requests, otherwise a report, varied by `seq`, so the
/// real planner sees competing incidents and scarce responders.
fn mixed_op(seq: u64) -> Op {
    const KINDS: [ResourceKind; 4] = [
        ResourceKind::Medic,
        ResourceKind::Technician,
        ResourceKind::Volunteer,
        ResourceKind::Vehicle,
    ];
    const CATEGORIES: [Category; 4] = [
        Category::MedicalAssistance,
        Category::NetworkOutage,
        Category::Flooding,
        Category::BlockedRoad,
    ];
    if seq.is_multiple_of(4) {
        Op::OfferResource {
            kind: KINDS[(seq / 4 % 4) as usize],
            capacity: 1 + (seq % 3) as u32,
            zone: ZoneId((seq * 7 % 30) as u16),
            lease_ms: 300_000,
        }
    } else {
        Op::ReportIncident {
            category: CATEGORIES[(seq % 4) as usize],
            zone: ZoneId((seq * 11 % 30) as u16),
            severity: Severity(1 + (seq % 5) as u8),
            ttl_ms: 60_000,
            attachment: None,
        }
    }
}

// ---------------------------------------------------------------- simulator

#[derive(Debug, Clone)]
pub struct Params {
    pub seed: u64,
    pub drop: f64,
    pub delay_max: u64,
    pub request_every_ms: u64,
    pub resources: u32,
    pub pool_capacity: usize,
    pub workers: usize,
    pub service_ms: (u64, u64),
    pub client_retry_ms: u64,
    pub inject: Option<InjectSchedule>,
}

impl Params {
    pub fn calm(seed: u64) -> Params {
        Params {
            seed,
            drop: 0.0,
            delay_max: 3,
            request_every_ms: 10,
            resources: 8,
            pool_capacity: 64,
            workers: 2,
            service_ms: (2, 6),
            client_retry_ms: 600,
            inject: None,
        }
    }
}

enum Packet {
    Peer {
        from: NodeId,
        to: NodeId,
        msg: NodeMsg,
    },
    Heartbeat {
        to: NodeId,
        hb: proto::Heartbeat,
    },
    Client {
        to: NodeId,
        req: ClientReq,
    },
    Reply {
        from: NodeId,
        req: ReqId,
        body: ReplyBody,
    },
}

pub struct SimNode {
    pub task: NodeTask,
    pub pool: FakePool,
    pub silenced: bool,
    last_hb: [u64; 3],
    suspected: [bool; 3],
}

pub struct Sim {
    pub now: u64,
    pub params: Params,
    pub nodes: Vec<SimNode>,
    pub isolated: [bool; 3],
    net: BTreeMap<(u64, u64), Packet>,
    packet_no: u64,
    rng: SplitMix64,
    pub workload: bool,
    next_seq: u64,
    /// Requests the clients still wait for, with the time of the last send.
    pub pending: BTreeMap<ReqId, (ClientReq, u64)>,
    pub replies: BTreeMap<ReqId, Vec<(NodeId, ReplyBody)>>,
    pub submitted: BTreeSet<ReqId>,
    pub events: Vec<(u64, NodeId, proto::event::EventKind, Option<ReqId>)>,
    /// When set, nodes run workstream D's real planner and state machine
    /// instead of the fakes, and clients send a mix of offers and reports.
    pub planner: Option<incident::IncidentPlanner>,
}

pub fn core_cfg(p: &Params) -> CoreCfg {
    CoreCfg {
        heartbeat_ms: 100,
        commit_flush_ms: 10,
        coordinator_silence_ms: 1000,
        candidacy_base_ms: 150,
        candidacy_jitter_ms: 150,
        syncing_candidacy_extra_ms: 2000,
        election_backoff_min_ms: 150,
        election_backoff_max_ms: 300,
        lease_ms: 250,
        propose_retry_ms: 200,
        append_batch: 64,
        inject_margin_ms: 2000,
        inject: p.inject.clone(),
        seed: p.seed,
    }
}

impl Sim {
    pub fn new(params: Params) -> Sim {
        Sim::build(params, None)
    }

    /// Nodes run the real incident planner and state machine.
    pub fn with_real_planner(params: Params, planner: incident::IncidentPlanner) -> Sim {
        Sim::build(params, Some(planner))
    }

    fn build(params: Params, planner: Option<incident::IncidentPlanner>) -> Sim {
        let nodes = NodeId::ALL
            .iter()
            .map(|&id| {
                let pool = FakePool(Arc::new(Mutex::new(PoolState {
                    capacity: params.pool_capacity,
                    ..PoolState::default()
                })));
                let task = NodeTask::with_parts(
                    id,
                    core_cfg(&params),
                    RankerKind::MwbeCached,
                    ScoreCfg::default(),
                    &Timeouts::default(),
                    7,
                    if planner.is_some() {
                        Box::new(incident::State::new())
                    } else {
                        Box::new(FakeState::new())
                    },
                    Box::new(pool.clone()),
                );
                SimNode {
                    task,
                    pool,
                    silenced: false,
                    last_hb: [0; 3],
                    suspected: [false; 3],
                }
            })
            .collect();
        Sim {
            now: 0,
            rng: SplitMix64::new(params.seed ^ 0x6e6f6465),
            params,
            nodes,
            isolated: [false; 3],
            net: BTreeMap::new(),
            packet_no: 0,
            workload: false,
            next_seq: 1,
            pending: BTreeMap::new(),
            replies: BTreeMap::new(),
            submitted: BTreeSet::new(),
            events: Vec::new(),
            planner,
        }
    }

    pub fn task(&self, n: NodeId) -> &NodeTask {
        &self.nodes[n.index()].task
    }

    pub fn pool(&self, n: NodeId) -> std::sync::MutexGuard<'_, PoolState> {
        self.nodes[n.index()].pool.0.lock().unwrap()
    }

    fn blocked(&self, n: NodeId) -> bool {
        self.nodes[n.index()].silenced || self.isolated[n.index()]
    }

    pub fn coordinator(&self) -> Option<NodeId> {
        let c: Vec<NodeId> = NodeId::ALL
            .into_iter()
            .filter(|&n| self.task(n).core().role() == Role::Coordinator && !self.blocked(n))
            .collect();
        if c.len() == 1 { Some(c[0]) } else { None }
    }

    pub fn count(&self, n: NodeId, kind: proto::event::EventKind) -> usize {
        self.events
            .iter()
            .filter(|(_, m, k, _)| *m == n && *k == kind)
            .count()
    }

    fn transmit(&mut self, p: Packet) {
        if self.rng.chance(self.params.drop) {
            return;
        }
        let delay = self.rng.range(1, self.params.delay_max);
        self.packet_no += 1;
        self.net.insert((self.now + delay, self.packet_no), p);
    }

    fn process(&mut self, n: NodeId, outs: Vec<Out>) {
        for o in outs {
            match o {
                Out::Send { to, msg } => {
                    if !self.blocked(n) {
                        self.transmit(Packet::Peer { from: n, to, msg });
                    }
                }
                Out::Reply { req, body } => {
                    if !self.blocked(n) {
                        self.transmit(Packet::Reply { from: n, req, body });
                    }
                }
                Out::SetSilenced(on) => self.nodes[n.index()].silenced = on,
                Out::Event { kind, req, .. } => self.events.push((self.now, n, kind, req)),
            }
        }
    }

    pub fn client_request(&mut self, seq: u64) -> ClientReq {
        ClientReq {
            version: PROTOCOL_VERSION,
            req: Request {
                id: ReqId {
                    client: ClientId {
                        machine: 1,
                        process: 1,
                        user: (seq % 50) as u32,
                    },
                    incarnation: 3,
                    seq,
                },
                sent_at_ms: self.now,
                op: if self.planner.is_some() {
                    mixed_op(seq)
                } else {
                    Op::ReportIncident {
                        category: Category::Flooding,
                        zone: ZoneId(1),
                        severity: Severity(3),
                        ttl_ms: 60_000,
                        attachment: None,
                    }
                },
            },
            rtt_us: [Some(1_000); 3],
            acked_up_to: 0,
        }
    }

    /// The client sends `req` to all three nodes.
    pub fn send_to_all(&mut self, req: ClientReq) {
        let id = req.req.id;
        self.submitted.insert(id);
        self.pending.insert(id, (req.clone(), self.now));
        for to in NodeId::ALL {
            self.transmit(Packet::Client {
                to,
                req: req.clone(),
            });
        }
    }

    pub fn new_request(&mut self) -> ReqId {
        let r = self.client_request(self.next_seq);
        self.next_seq += 1;
        let id = r.req.id;
        self.send_to_all(r);
        id
    }

    pub fn run_for(&mut self, ms: u64) {
        for _ in 0..ms {
            self.step();
        }
    }

    pub fn run_until(&mut self, max_ms: u64, what: &str, cond: impl Fn(&Sim) -> bool) {
        for _ in 0..max_ms {
            if cond(self) {
                return;
            }
            self.step();
        }
        assert!(
            cond(self),
            "timed out after {max_ms} ms waiting for: {what}"
        );
    }

    pub fn step(&mut self) {
        self.now += 1;
        let now = self.now;

        while let Some((&key, _)) = self.net.first_key_value() {
            if key.0 > now {
                break;
            }
            match self.net.remove(&key).unwrap() {
                Packet::Peer { from, to, msg } => {
                    if !self.blocked(from) && !self.blocked(to) {
                        let o = self.nodes[to.index()].task.on_peer(now, from, msg);
                        self.process(to, o);
                    }
                }
                Packet::Heartbeat { to, hb } => {
                    if !self.blocked(hb.from) && !self.blocked(to) {
                        let node = &mut self.nodes[to.index()];
                        node.last_hb[hb.from.index()] = now;
                        node.task.on_heartbeat(&hb);
                        if node.suspected[hb.from.index()] {
                            node.suspected[hb.from.index()] = false;
                            let o = node.task.set_suspected(now, hb.from, false);
                            self.process(to, o);
                        }
                    }
                }
                Packet::Client { to, req } => {
                    if !self.blocked(to) {
                        let o = self.nodes[to.index()].task.on_client(now, req);
                        self.process(to, o);
                    }
                }
                Packet::Reply { from, req, body } => {
                    if matches!(body, ReplyBody::Ok(_)) {
                        self.pending.remove(&req);
                    }
                    self.replies.entry(req).or_default().push((from, body));
                }
            }
        }

        if now.is_multiple_of(100) {
            for from in NodeId::ALL {
                if self.blocked(from) {
                    continue;
                }
                let hb = self.nodes[from.index()].task.heartbeat();
                for to in NodeId::ALL.into_iter().filter(|&t| t != from) {
                    self.transmit(Packet::Heartbeat { to, hb: hb.clone() });
                }
            }
        }

        // Failure detectors, planning pools, ticks.
        for n in NodeId::ALL {
            for p in NodeId::ALL.into_iter().filter(|&p| p != n) {
                let node = &mut self.nodes[n.index()];
                if !node.suspected[p.index()] && now.saturating_sub(node.last_hb[p.index()]) > 300 {
                    node.suspected[p.index()] = true;
                    let o = node.task.set_suspected(now, p, true);
                    self.process(n, o);
                }
            }
            self.run_pool(n);
            let o = self.nodes[n.index()].task.tick(now);
            self.process(n, o);
        }

        if self.workload && now.is_multiple_of(self.params.request_every_ms) {
            self.new_request();
        }
        // Client retries.
        let due: Vec<ClientReq> = self
            .pending
            .values()
            .filter(|(_, t)| now >= t + self.params.client_retry_ms)
            .map(|(r, _)| r.clone())
            .collect();
        for r in due {
            self.send_to_all(r);
        }
    }

    fn run_pool(&mut self, n: NodeId) {
        if self.nodes[n.index()].silenced {
            return; // a silenced process is paused as a whole
        }
        let now = self.now;
        let (lo, hi) = self.params.service_ms;
        let finished: Vec<PlanJob> = {
            let mut p = self.nodes[n.index()].pool.0.lock().unwrap();
            while !p.paused && p.running.len() < self.params.workers {
                let Some(job) = p.queue.pop_front() else {
                    break;
                };
                let done_at = now + lo + (proto::hash::request(&job.req.req.id) % (hi - lo + 1));
                p.running.push((done_at, job));
            }
            let (done, still): (Vec<_>, Vec<_>) = p.running.drain(..).partition(|(t, _)| *t <= now);
            p.running = still;
            done.into_iter().map(|(_, j)| j).collect()
        };
        for job in finished {
            let cancelled = self.nodes[n.index()]
                .pool
                .0
                .lock()
                .unwrap()
                .cancelled
                .contains(&job.req.req.id);
            if cancelled {
                continue;
            }
            let out = match &self.planner {
                // The real pool refuses past the re-plan cap without planning.
                Some(_) if job.attempt > MAX_REPLANS => PlanOutput {
                    read_set: vec![],
                    writes: vec![],
                    result: OpResult::Refused("resources kept changing".into()),
                },
                Some(p) => proto::Planner::plan(p, &job.snapshot, &job.req.req),
                None => plan(&job, n, self.params.resources),
            };
            let o = self.nodes[n.index()]
                .task
                .on_planned(now, job.req.req.id, out);
            self.process(n, o);
        }
    }

    // ---------------------------------------------------------------- checks

    /// The committed log as node `n` holds it.
    pub fn committed(&self, n: NodeId) -> Vec<Entry> {
        let core = self.task(n).core();
        (1..=core.commit_seq().0)
            .map(|s| core.entry(Seq(s)).unwrap().clone())
            .collect()
    }

    /// Every client got the committed result and only it; every request
    /// committed at most once.
    pub fn check_answers(&self) {
        let longest = NodeId::ALL
            .into_iter()
            .max_by_key(|&n| self.task(n).core().commit_seq())
            .unwrap();
        let log = self.committed(longest);
        let mut result_of: BTreeMap<ReqId, OpResult> = BTreeMap::new();
        for e in &log {
            if let EntryKind::Result { req, result, .. } = &e.kind {
                assert!(
                    result_of.insert(*req, result.clone()).is_none(),
                    "{req} committed twice"
                );
            }
        }
        for n in NodeId::ALL {
            let mine = self.committed(n);
            assert_eq!(&log[..mine.len()], &mine[..], "{n} disagrees with the log");
        }
        for (req, replies) in &self.replies {
            for (from, body) in replies {
                if let ReplyBody::Ok(r) = body {
                    let committed = result_of
                        .get(req)
                        .unwrap_or_else(|| panic!("{from} answered {req}, which never committed"));
                    assert_eq!(
                        r, committed,
                        "{from} answered {req} with a different result"
                    );
                }
            }
        }
    }

    /// After load stops: every request answered and no node still executing.
    pub fn settle(&mut self) {
        self.workload = false;
        self.isolated = [false; 3];
        for n in &mut self.nodes {
            n.task.pause_injection(true);
        }
        self.run_until(60_000, "all requests answered", |s| {
            s.pending.is_empty()
                && NodeId::ALL.iter().all(|&n| {
                    let t = s.task(n);
                    t.executing() == 0 && t.core().outstanding() == 0
                })
        });
        self.check_answers();
    }
}
