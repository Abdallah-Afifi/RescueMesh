//! Deterministic simulator: three [`Core`]s over a simulated network.
//!
//! Virtual time advances 1 ms per step. Messages are delayed, dropped and
//! duplicated at random. A node is unreachable while it is silenced by fault
//! injection or isolated by the test. Each node has a timeout failure
//! detector fed by simulated heartbeats, and a tiny state machine with
//! resources whose availability every request decrements. A workload submits
//! requests to random ACTIVE nodes, sometimes to two at once.
//!
//! [`Sim::check_safety`] and [`Sim::check_converged`] verify the protocol's
//! invariants against everything that happened.

#![allow(dead_code)]

use proto::event::EventKind;
use proto::{
    ClientId, Entry, EntryKind, Epoch, NodeId, NodeMsg, OpResult, Plan, Proposal, ProposeOutcome,
    ReqId, ResourceId, ResourceKind, ResourceOffer, Seq, SilenceTarget, Version, View, ViewNo,
    Write, ZoneId,
};
use replication::rng::SplitMix64;
use replication::{Action, Core, CoreCfg, InjectSchedule, Role, Status};
use std::collections::{BTreeMap, BTreeSet};

pub const INITIAL_AVAILABLE: u32 = 1_000_000;

pub type ReadSet = Vec<(ResourceId, Version)>;

#[derive(Debug, Clone)]
pub struct Params {
    pub seed: u64,
    pub drop: f64,
    pub dup: f64,
    pub delay_min: u64,
    pub delay_max: u64,
    pub heartbeat_ms: u64,
    pub fd_timeout_ms: u64,
    /// Submit a request every this many ms while the workload is on.
    pub request_every_ms: u64,
    /// Probability that a second node also executes the same request.
    pub competing: f64,
    pub resources: u32,
    pub inject: Option<InjectSchedule>,
}

impl Params {
    pub fn calm(seed: u64) -> Params {
        Params {
            seed,
            drop: 0.0,
            dup: 0.0,
            delay_min: 1,
            delay_max: 3,
            heartbeat_ms: 100,
            fd_timeout_ms: 300,
            request_every_ms: 10,
            competing: 0.0,
            resources: 4,
            inject: None,
        }
    }
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

pub fn resource_id(r: u32) -> ResourceId {
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

pub fn request_id(n: u64) -> ReqId {
    ReqId {
        client: ClientId {
            machine: 9,
            process: 1,
            user: 1,
        },
        incarnation: 1,
        seq: n,
    }
}

#[derive(Debug)]
enum Packet {
    Msg {
        from: NodeId,
        to: NodeId,
        msg: NodeMsg,
    },
    Heartbeat {
        from: NodeId,
        to: NodeId,
    },
}

pub struct SimNode {
    pub core: Core,
    pub silenced: bool,
    pub applied: Vec<Entry>,
    /// resource -> (available, version)
    pub state: BTreeMap<ResourceId, (u32, Version)>,
    last_heartbeat: [u64; 3],
    suspected: [bool; 3],
    attempts: BTreeMap<ReqId, u32>,
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
    next_req: u64,
    /// Committed log as first observed; every node's committed prefix must match it.
    pub committed: Vec<Entry>,
    checked: [u64; 3],
    pub coordinator_of_epoch: BTreeMap<Epoch, NodeId>,
    pub replies: BTreeMap<ReqId, Vec<(NodeId, ProposeOutcome)>>,
    pub read_sets: BTreeMap<(ReqId, NodeId), Vec<ReadSet>>,
    pub submitted: BTreeSet<ReqId>,
    pub gave_up: BTreeSet<ReqId>,
    pub events: Vec<(u64, NodeId, EventKind)>,
    pub max_silenced_at_once: usize,
}

impl Sim {
    pub fn new(params: Params) -> Sim {
        let cfg = core_cfg(&params);
        let nodes = NodeId::ALL
            .iter()
            .map(|&id| SimNode {
                core: Core::new(id, cfg.clone()),
                silenced: false,
                applied: Vec::new(),
                state: BTreeMap::new(),
                last_heartbeat: [0; 3],
                suspected: [false; 3],
                attempts: BTreeMap::new(),
            })
            .collect();
        Sim {
            now: 0,
            rng: SplitMix64::new(params.seed.wrapping_mul(0x9e37_79b9) ^ 0xabcdef),
            params,
            nodes,
            isolated: [false; 3],
            net: BTreeMap::new(),
            packet_no: 0,
            workload: false,
            next_req: 1,
            committed: Vec::new(),
            checked: [0; 3],
            coordinator_of_epoch: BTreeMap::new(),
            replies: BTreeMap::new(),
            read_sets: BTreeMap::new(),
            submitted: BTreeSet::new(),
            gave_up: BTreeSet::new(),
            events: Vec::new(),
            max_silenced_at_once: 0,
        }
    }

    pub fn node(&self, n: NodeId) -> &SimNode {
        &self.nodes[n.index()]
    }

    pub fn core(&self, n: NodeId) -> &Core {
        &self.nodes[n.index()].core
    }

    fn blocked(&self, n: NodeId) -> bool {
        self.nodes[n.index()].silenced || self.isolated[n.index()]
    }

    pub fn coordinator(&self) -> Option<NodeId> {
        let c: Vec<NodeId> = NodeId::ALL
            .into_iter()
            .filter(|&n| self.core(n).role() == Role::Coordinator && !self.blocked(n))
            .collect();
        if c.len() == 1 { Some(c[0]) } else { None }
    }

    pub fn count_events(&self, kind: EventKind) -> usize {
        self.events.iter().filter(|(_, _, k)| *k == kind).count()
    }

    // -------------------------------------------------------------- stepping

    pub fn run_for(&mut self, ms: u64) {
        for _ in 0..ms {
            self.step();
        }
    }

    /// Steps until `cond` holds; panics after `max_ms`.
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
            let packet = self.net.remove(&key).expect("present");
            match packet {
                Packet::Msg { from, to, msg } => {
                    if !self.blocked(from) && !self.blocked(to) {
                        let actions = self.nodes[to.index()].core.on_message(now, from, msg);
                        self.process(to, actions);
                    }
                }
                Packet::Heartbeat { from, to } => {
                    if !self.blocked(from) && !self.blocked(to) {
                        let node = &mut self.nodes[to.index()];
                        node.last_heartbeat[from.index()] = now;
                        if node.suspected[from.index()] {
                            node.suspected[from.index()] = false;
                            let actions = node.core.set_suspected(now, from, false);
                            self.process(to, actions);
                        }
                    }
                }
            }
        }

        if now.is_multiple_of(self.params.heartbeat_ms) {
            for from in NodeId::ALL {
                if self.blocked(from) {
                    continue;
                }
                for to in NodeId::ALL.into_iter().filter(|&t| t != from) {
                    self.transmit(Packet::Heartbeat { from, to });
                }
            }
        }

        for n in NodeId::ALL {
            for p in NodeId::ALL.into_iter().filter(|&p| p != n) {
                let node = &mut self.nodes[n.index()];
                if !node.suspected[p.index()]
                    && now.saturating_sub(node.last_heartbeat[p.index()])
                        > self.params.fd_timeout_ms
                {
                    node.suspected[p.index()] = true;
                    let actions = node.core.set_suspected(now, p, true);
                    self.process(n, actions);
                }
            }
            let actions = self.nodes[n.index()].core.tick(now);
            self.process(n, actions);
        }

        if self.workload && now.is_multiple_of(self.params.request_every_ms) {
            self.submit_request();
        }

        self.check_online();
    }

    fn transmit(&mut self, packet: Packet) {
        if self.rng.chance(self.params.drop) {
            return;
        }
        let copies = if self.rng.chance(self.params.dup) {
            2
        } else {
            1
        };
        for _ in 0..copies {
            let delay = self.rng.range(self.params.delay_min, self.params.delay_max);
            self.packet_no += 1;
            self.net
                .insert((self.now + delay, self.packet_no), packet_clone(&packet));
        }
    }

    fn process(&mut self, n: NodeId, actions: Vec<Action>) {
        for a in actions {
            match a {
                Action::Send { to, msg } => {
                    if !self.blocked(n) {
                        self.transmit(Packet::Msg { from: n, to, msg });
                    }
                }
                Action::Apply(e) => {
                    let node = &mut self.nodes[n.index()];
                    assert_eq!(
                        e.seq.0,
                        node.applied.len() as u64 + 1,
                        "{n}: entries applied out of order"
                    );
                    if let EntryKind::Result { writes, .. } = &e.kind {
                        for w in writes {
                            if let Write::PutResource(r) = w {
                                node.state.insert(r.id, (r.available, Version(e.seq.0)));
                            }
                        }
                    }
                    node.applied.push(e);
                }
                Action::ProposeDone { req, outcome } => self.on_done(n, req, outcome),
                Action::SetSilenced(s) => {
                    self.nodes[n.index()].silenced = s;
                    let silent = self.nodes.iter().filter(|x| x.silenced).count();
                    self.max_silenced_at_once = self.max_silenced_at_once.max(silent);
                }
                Action::Event { kind, .. } => self.events.push((self.now, n, kind)),
            }
        }
    }

    // -------------------------------------------------------------- workload

    fn submit_request(&mut self) {
        let candidates: Vec<NodeId> = NodeId::ALL
            .into_iter()
            .filter(|&n| self.core(n).is_active() && !self.blocked(n))
            .collect();
        if candidates.is_empty() {
            return;
        }
        let req = request_id(self.next_req);
        self.next_req += 1;
        self.submitted.insert(req);
        let first = candidates[self.rng.range(0, candidates.len() as u64 - 1) as usize];
        self.execute(first, req);
        if candidates.len() > 1 && self.rng.chance(self.params.competing) {
            let others: Vec<NodeId> = candidates.into_iter().filter(|&c| c != first).collect();
            let second = others[self.rng.range(0, others.len() as u64 - 1) as usize];
            self.execute(second, req);
        }
    }

    /// `n` plans `req` from its applied state and proposes it.
    pub fn execute(&mut self, n: NodeId, req: ReqId) {
        let p = self.plan(n, req);
        self.propose_raw(n, p);
    }

    /// `n` proposes exactly `p`, which tests may have altered.
    pub fn propose_raw(&mut self, n: NodeId, p: Proposal) {
        let req = p.req;
        self.read_sets
            .entry((req, p.executor))
            .or_default()
            .push(p.read_set.clone());
        *self.nodes[n.index()].attempts.entry(req).or_default() += 1;
        let actions = self.nodes[n.index()].core.propose(self.now, p);
        self.process(n, actions);
    }

    pub fn plan(&self, n: NodeId, req: ReqId) -> Proposal {
        let node = self.node(n);
        let r = resource_id((proto::hash::request(&req) % u64::from(self.params.resources)) as u32);
        let (available, version) = node
            .state
            .get(&r)
            .copied()
            .unwrap_or((INITIAL_AVAILABLE, Version(0)));
        let offer = ResourceOffer {
            id: r,
            owner: r.client,
            kind: ResourceKind::Medic,
            capacity: INITIAL_AVAILABLE,
            available: available - 1,
            zone: ZoneId(0),
            expires_at_ms: 0,
        };
        Proposal {
            req,
            epoch: node.core.epoch(),
            view: node.core.view().no,
            executor: n,
            read_set: vec![(r, version)],
            writes: vec![Write::PutResource(offer)],
            // Differs per executor, so a wrong duplicate answer would be visible.
            result: OpResult::Planned(Plan {
                incident: req,
                priority: i64::from(available),
                assignments: vec![],
                route: vec![ZoneId(u16::from(n.0))],
                responders: vec![],
            }),
        }
    }

    fn on_done(&mut self, n: NodeId, req: ReqId, outcome: ProposeOutcome) {
        self.replies
            .entry(req)
            .or_default()
            .push((n, outcome.clone()));
        match outcome {
            ProposeOutcome::Stale { .. } => {
                let attempts = self.nodes[n.index()]
                    .attempts
                    .get(&req)
                    .copied()
                    .unwrap_or(0);
                if attempts <= 3 && self.core(n).is_active() && !self.blocked(n) {
                    self.execute(n, req);
                } else {
                    self.gave_up.insert(req);
                }
            }
            ProposeOutcome::NotInView => {
                self.gave_up.insert(req);
            }
            ProposeOutcome::NotCoordinator { .. } => panic!("NotCoordinator must not be final"),
            ProposeOutcome::Committed(_) | ProposeOutcome::Duplicate(_) => {}
        }
    }

    // -------------------------------------------------------------- operator and faults

    pub fn operator_silence(&mut self, at: NodeId, target: SilenceTarget, duration_ms: u64) {
        let actions = self.nodes[at.index()]
            .core
            .silence(self.now, target, duration_ms);
        self.process(at, actions);
    }

    pub fn pause_injection(&mut self) {
        for n in &mut self.nodes {
            n.core.pause_injection(true);
        }
    }

    // -------------------------------------------------------------- checks

    /// Checked after every step.
    fn check_online(&mut self) {
        for n in NodeId::ALL {
            let core = &self.nodes[n.index()].core;
            if core.role() == Role::Coordinator {
                let prev = self.coordinator_of_epoch.insert(core.epoch(), n);
                assert!(
                    prev.is_none_or(|p| p == n),
                    "two coordinators in epoch {}: {p:?} and {n}",
                    core.epoch(),
                    p = prev
                );
            }
            // Committed prefixes agree with the first observation.
            let commit = core.commit_seq().0;
            for s in self.checked[n.index()] + 1..=commit {
                let e = core.entry(Seq(s)).expect("committed entry present").clone();
                match self.committed.get((s - 1) as usize) {
                    Some(c) => assert_eq!(c, &e, "{n} disagrees with the committed log at seq {s}"),
                    None => {
                        assert_eq!(self.committed.len() as u64, s - 1);
                        self.committed.push(e);
                    }
                }
            }
            self.checked[n.index()] = self.checked[n.index()].max(commit);
            assert!(core.applied_seq() <= core.commit_seq());
        }
        let silent = self.nodes.iter().filter(|x| x.silenced).count();
        assert!(silent <= 1, "two nodes silenced by injection at once");
    }

    /// Invariants over the whole committed history.
    pub fn check_safety(&self) {
        let mut seen = BTreeSet::new();
        let mut state: BTreeMap<ResourceId, (u32, Version)> = BTreeMap::new();
        let mut view = View::new(ViewNo(0), NodeId::ALL.to_vec());
        let mut result_of: BTreeMap<ReqId, (NodeId, OpResult, u64)> = BTreeMap::new();
        for e in &self.committed {
            match &e.kind {
                EntryKind::View(v) => view = v.clone(),
                EntryKind::Result {
                    req,
                    executor,
                    writes,
                    result,
                } => {
                    // I1
                    assert!(seen.insert(*req), "request {req} committed twice");
                    // I5
                    assert!(
                        view.contains(*executor),
                        "{executor} executed {req} outside view {view:?}"
                    );
                    // I3: the plan read the latest versions, so no decrement is lost.
                    for w in writes {
                        if let Write::PutResource(r) = w {
                            let (avail, ver) = state
                                .get(&r.id)
                                .copied()
                                .unwrap_or((INITIAL_AVAILABLE, Version(0)));
                            assert_eq!(
                                r.available,
                                avail - 1,
                                "lost update to resource at seq {}",
                                e.seq
                            );
                            let read = self
                                .read_sets
                                .get(&(*req, *executor))
                                .expect("proposal recorded");
                            assert!(
                                read.iter().any(|rs| rs.contains(&(r.id, ver))),
                                "seq {} committed a plan that read a stale version",
                                e.seq
                            );
                            state.insert(r.id, (r.available, Version(e.seq.0)));
                        }
                    }
                    result_of.insert(*req, (*executor, result.clone(), e.seq.0));
                }
                _ => {}
            }
        }
        // Every answer an executor received matches the single committed result.
        for (req, replies) in &self.replies {
            for (node, outcome) in replies {
                match outcome {
                    ProposeOutcome::Committed(seq) => {
                        let (exec, _, s) = result_of
                            .get(req)
                            .unwrap_or_else(|| panic!("{req} acknowledged but not committed"));
                        assert_eq!(
                            (exec, s),
                            (node, &seq.0),
                            "{req}: Committed points at the wrong entry"
                        );
                    }
                    ProposeOutcome::Duplicate(r) => {
                        let (exec, res, _) = result_of
                            .get(req)
                            .unwrap_or_else(|| panic!("{req} duplicate of nothing"));
                        assert_ne!(
                            exec, node,
                            "{req}: executor told it is a duplicate of itself"
                        );
                        assert_eq!(
                            res, r,
                            "{req}: duplicate answer differs from the committed result"
                        );
                    }
                    _ => {}
                }
            }
        }
        // Each node applied exactly the committed prefix, in order.
        for n in &self.nodes {
            assert_eq!(&n.applied[..], &self.committed[..n.applied.len()]);
        }
    }

    /// After faults stop: one coordinator, everyone ACTIVE and identical,
    /// and every proposal answered.
    pub fn check_converged(&self) {
        let c = self
            .coordinator()
            .expect("exactly one reachable coordinator");
        let epoch = self.core(c).epoch();
        for n in NodeId::ALL {
            let core = self.core(n);
            assert_eq!(
                core.coordinator(),
                Some(c),
                "{n} follows a different coordinator"
            );
            assert_eq!(core.epoch(), epoch, "{n} is in a different epoch");
            assert_eq!(core.status(), Status::Active, "{n} is not ACTIVE");
            assert!(core.view().contains(n), "{n} is not in the view");
            assert_eq!(
                core.commit_seq().0,
                self.committed.len() as u64,
                "{n} commit position differs"
            );
            assert_eq!(
                core.applied_seq(),
                core.commit_seq(),
                "{n} has not applied everything"
            );
            assert_eq!(core.outstanding(), 0, "{n} has unanswered proposals");
            assert!(!core.is_silenced());
        }
    }

    /// Stop faults and load, let the cluster settle, then check everything.
    pub fn settle_and_check(&mut self) {
        self.isolated = [false; 3];
        self.pause_injection();
        self.workload = false;
        // Longest silence plus rejoin.
        self.run_until(40_000, "convergence", |s| {
            s.coordinator().is_some()
                && NodeId::ALL.iter().all(|&n| {
                    let c = s.core(n);
                    c.is_active()
                        && c.outstanding() == 0
                        && c.applied_seq().0 == s.committed.len() as u64
                })
                && s.committed.len() as u64 == s.core(s.coordinator().unwrap()).last_seq().0
        });
        self.check_safety();
        self.check_converged();
    }
}

fn packet_clone(p: &Packet) -> Packet {
    match p {
        Packet::Msg { from, to, msg } => Packet::Msg {
            from: *from,
            to: *to,
            msg: msg.clone(),
        },
        Packet::Heartbeat { from, to } => Packet::Heartbeat {
            from: *from,
            to: *to,
        },
    }
}
