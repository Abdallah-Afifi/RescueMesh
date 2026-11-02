//! The core task of a cloud node (design log DL-017): one owner for all
//! protocol state, driving the replication core and the selector and carrying
//! out what they ask for.
//!
//! Like the modules it drives, the task performs no I/O and reads no clock.
//! The runtime calls it with the current monotonic time in milliseconds and
//! carries out the returned [`Out`]s. Planning goes to a [`PlanPool`], and
//! committed state lives in a [`StateMachine`]; both belong to workstream D.
//!
//! # Runtime rules enforced here
//!
//! The selector never takes a claimed request back because time has passed,
//! so the task must not strand one (`crates/selection` docs):
//!
//! - **Never drop an `Execute`.** Every `Execute` the selector emits, from
//!   any input, goes to [`PlanPool::submit`], which cannot refuse it. A
//!   request already being planned is not submitted twice.
//! - **Reply `Busy` before selection.** A new write request that arrives
//!   while [`PlanPool::is_full`] answers true is answered `Busy` at once and
//!   never reaches the selector, so this node takes no part in it: its peers'
//!   claim timeout moves the request on and the client retries. Retries of a
//!   request the selector already holds (claimed or committed) still go to
//!   the selector, which answers or ignores them without new planning work.
//!
//! The third rule, committing `OpResult::Refused` once re-planning gives up,
//! belongs to the planner: each [`PlanJob`] carries its attempt number.

use proto::config::{Config, RankerKind, ScoreCfg, Timeouts};
use proto::event::EventKind;
use proto::{
    ClientReq, EntryKind, Heartbeat, LoadSample, NodeId, NodeMsg, OpResult, PROTOCOL_VERSION,
    PlanOutput, Proposal, ProposeOutcome, ReplyBody, ReqId, SilenceTarget, Snapshot, StateMachine,
};
use replication::{Action as CoreAction, Core, CoreCfg};
use selection::{Action as SelAction, RowState, Selector};
use serde_json::{Map, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

/// One request to plan, from a fresh snapshot of committed state.
#[derive(Debug, Clone)]
pub struct PlanJob {
    pub req: ClientReq,
    pub snapshot: Arc<Snapshot>,
    /// 0 for the first plan; one more after each `Stale` rejection. The
    /// planner returns `OpResult::Refused`, with no writes and an empty read
    /// set, once this exceeds `timeouts.max_replans`, so the request commits
    /// and its retries are answered.
    pub attempt: u32,
}

/// The planning worker pool as the core task sees it (workstream D).
///
/// The pool calls [`NodeTask::on_planned`] (through the runtime) with each
/// finished plan.
pub trait PlanPool: Send {
    /// Queues a job. Infallible: an `Execute` must never be dropped, so the
    /// queue may grow past the admission limit for jobs the selector assigns.
    fn submit(&mut self, job: PlanJob);
    /// Drops a job that has not started. A job already running finishes; the
    /// task ignores its result.
    fn cancel(&mut self, req: ReqId);
    /// Admission control for new client requests: true when the queue is at
    /// its limit (`planner.queue_capacity`).
    fn is_full(&self) -> bool;
    /// Queue length, CPU and service time for heartbeats and ranking.
    fn load(&self) -> LoadSample;
}

/// What the runtime must do after a call.
#[derive(Debug, Clone, PartialEq)]
pub enum Out {
    /// Send to a peer node.
    Send { to: NodeId, msg: NodeMsg },
    /// Answer the client that sent `req`.
    Reply { req: ReqId, body: ReplyBody },
    /// Fault injection: drop all traffic, or resume.
    SetSilenced(bool),
    /// Write to the event log.
    Event {
        kind: EventKind,
        req: Option<ReqId>,
        fields: Map<String, Value>,
    },
}

/// A request this node is executing.
struct Executing {
    req: ClientReq,
    attempt: u32,
    /// The result sent to the coordinator, once planned.
    proposed: Option<OpResult>,
}

/// The core task of one cloud node.
pub struct NodeTask {
    id: NodeId,
    core: Core,
    selector: Selector,
    state: Box<dyn StateMachine>,
    pool: Box<dyn PlanPool>,
    executing: BTreeMap<ReqId, Executing>,
    /// Requests committed with this node as executor whose entry is not yet
    /// applied here; the client is answered when it is (DL-058).
    reply_when_applied: BTreeSet<ReqId>,
    incarnation: u32,
    heartbeat_n: u64,
    /// Last heartbeat accepted from each peer: (incarnation, number).
    last_heartbeat: BTreeMap<NodeId, (u32, u64)>,
    out: Vec<Out>,
}

fn fields(pairs: &[(&str, Value)]) -> Map<String, Value> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_owned(), v.clone()))
        .collect()
}

impl NodeTask {
    /// A node configured from the run configuration. `incarnation` is drawn
    /// at random when the process starts.
    pub fn new(
        id: NodeId,
        cfg: &Config,
        incarnation: u32,
        state: Box<dyn StateMachine>,
        pool: Box<dyn PlanPool>,
    ) -> NodeTask {
        NodeTask::with_parts(
            id,
            CoreCfg::from_config(cfg),
            cfg.policy.ranker,
            cfg.score.clone(),
            &cfg.timeouts,
            incarnation,
            state,
            pool,
        )
    }

    /// A node from explicit parts, for tests with faster timings.
    #[allow(clippy::too_many_arguments)]
    pub fn with_parts(
        id: NodeId,
        core_cfg: CoreCfg,
        policy: RankerKind,
        score: ScoreCfg,
        timeouts: &Timeouts,
        incarnation: u32,
        state: Box<dyn StateMachine>,
        pool: Box<dyn PlanPool>,
    ) -> NodeTask {
        let core = Core::new(id, core_cfg);
        let mut selector = Selector::new(id, policy, score, timeouts);
        selector.set_view(0, core.view().clone(), core.is_active());
        NodeTask {
            id,
            core,
            selector,
            state,
            pool,
            executing: BTreeMap::new(),
            reply_when_applied: BTreeSet::new(),
            incarnation,
            heartbeat_n: 0,
            last_heartbeat: BTreeMap::new(),
            out: Vec::new(),
        }
    }

    // ------------------------------------------------------------ accessors

    pub fn id(&self) -> NodeId {
        self.id
    }
    pub fn core(&self) -> &Core {
        &self.core
    }
    pub fn selector(&self) -> &Selector {
        &self.selector
    }
    /// Requests this node is planning or has proposed and not yet settled.
    pub fn executing(&self) -> usize {
        self.executing.len()
    }

    /// Stop or resume scheduled fault injection (operator control).
    pub fn pause_injection(&mut self, paused: bool) {
        self.core.pause_injection(paused);
    }

    // ------------------------------------------------------------ inputs

    /// Call every few milliseconds.
    pub fn tick(&mut self, now: u64) -> Vec<Out> {
        let a = self.core.tick(now);
        self.dispatch_core(now, a);
        let s = self.selector.tick(now);
        self.dispatch_selection(s);
        self.take()
    }

    /// A client request arrived.
    pub fn on_client(&mut self, now: u64, req: ClientReq) -> Vec<Out> {
        let id = req.req.id;
        if req.version != PROTOCOL_VERSION {
            let body = ReplyBody::Error(format!("protocol version {} not supported", req.version));
            self.out.push(Out::Reply { req: id, body });
            return self.take();
        }
        if !req.req.op.is_write() {
            // Reads come from applied state and never enter selection (DL-019).
            let result = self.state.query(&req.req);
            self.out.push(Out::Reply {
                req: id,
                body: ReplyBody::Ok(result),
            });
            return self.take();
        }
        // Rule: reply Busy before selection. Only requests that could make
        // new planning work are refused; a retry of one the selector already
        // holds is answered or ignored there.
        let new_work = matches!(self.selector.row_state(&id), None | Some(RowState::Seen));
        if new_work && self.pool.is_full() {
            let queue = self.pool.load().queue_len;
            self.out.push(Out::Reply {
                req: id,
                body: ReplyBody::Busy,
            });
            self.event(EventKind::Busy, Some(id), &[("queue_len", queue.into())]);
            return self.take();
        }
        let s = self.selector.on_request(now, req);
        self.dispatch_selection(s);
        self.take()
    }

    /// A message from a peer node.
    pub fn on_peer(&mut self, now: u64, from: NodeId, msg: NodeMsg) -> Vec<Out> {
        match msg {
            NodeMsg::Claim { req, view, key } => {
                if !self.core.is_silenced() {
                    let s = self.selector.on_claim(from, req, view, key);
                    self.dispatch_selection(s);
                }
            }
            other => {
                let a = self.core.on_message(now, from, other);
                self.dispatch_core(now, a);
            }
        }
        self.take()
    }

    /// The pool finished planning `req`.
    pub fn on_planned(&mut self, now: u64, req: ReqId, plan: PlanOutput) -> Vec<Out> {
        let Some(ex) = self.executing.get_mut(&req) else {
            return self.take(); // aborted while it was being planned
        };
        ex.proposed = Some(plan.result.clone());
        let proposal = Proposal {
            req,
            epoch: self.core.epoch(),
            view: self.core.view().no,
            executor: self.id,
            read_set: plan.read_set,
            writes: plan.writes,
            result: plan.result,
        };
        let a = self.core.propose(now, proposal);
        self.dispatch_core(now, a);
        self.take()
    }

    /// The failure detector's opinion of a peer changed.
    pub fn set_suspected(&mut self, now: u64, peer: NodeId, suspected: bool) -> Vec<Out> {
        let a = self.core.set_suspected(now, peer, suspected);
        self.dispatch_core(now, a);
        self.take()
    }

    /// The operator asked this node to silence a node.
    pub fn silence(&mut self, now: u64, target: SilenceTarget, duration_ms: u64) -> Vec<Out> {
        let a = self.core.silence(now, target, duration_ms);
        self.dispatch_core(now, a);
        self.take()
    }

    /// A peer's heartbeat arrived. One older than the last accepted from the
    /// same process is dropped, so a late UDP datagram cannot restore an old
    /// load.
    pub fn on_heartbeat(&mut self, hb: &Heartbeat) {
        if hb.version != PROTOCOL_VERSION || hb.from == self.id || !hb.from.is_valid() {
            return;
        }
        if let Some(&(inc, n)) = self.last_heartbeat.get(&hb.from)
            && inc == hb.incarnation
            && hb.n <= n
        {
            return;
        }
        self.last_heartbeat.insert(hb.from, (hb.incarnation, hb.n));
        self.selector.on_heartbeat(hb.from, hb.load);
    }

    /// The heartbeat to send now; also feeds this node's own load to the
    /// selector.
    pub fn heartbeat(&mut self) -> Heartbeat {
        self.heartbeat_n += 1;
        let load = self.pool.load();
        self.selector.on_heartbeat(self.id, load);
        Heartbeat {
            version: PROTOCOL_VERSION,
            from: self.id,
            incarnation: self.incarnation,
            n: self.heartbeat_n,
            epoch: self.core.epoch(),
            coordinator: self.core.coordinator(),
            view: self.core.view().no,
            commit: self.core.commit_seq(),
            applied: self.core.applied_seq(),
            load,
        }
    }

    // ------------------------------------------------------------ dispatch

    fn take(&mut self) -> Vec<Out> {
        std::mem::take(&mut self.out)
    }

    fn event(&mut self, kind: EventKind, req: Option<ReqId>, pairs: &[(&str, Value)]) {
        self.out.push(Out::Event {
            kind,
            req,
            fields: fields(pairs),
        });
    }

    fn dispatch_core(&mut self, now: u64, actions: Vec<CoreAction>) {
        for a in actions {
            match a {
                CoreAction::Send { to, msg } => self.out.push(Out::Send { to, msg }),
                CoreAction::Apply(entry) => {
                    self.state.apply(&entry);
                    if let EntryKind::Result { req, result, .. } = &entry.kind {
                        if self.reply_when_applied.remove(req) {
                            self.out.push(Out::Reply {
                                req: *req,
                                body: ReplyBody::Ok(result.clone()),
                            });
                        }
                        self.selector.on_committed(*req, entry.seq, result.clone());
                    }
                }
                CoreAction::ProposeDone { req, outcome } => self.proposal_done(req, outcome),
                CoreAction::SetSilenced(on) => self.out.push(Out::SetSilenced(on)),
                CoreAction::Event { kind, req, fields } => {
                    self.out.push(Out::Event { kind, req, fields })
                }
            }
        }
        // The view or this node's status may have changed.
        let s = self
            .selector
            .set_view(now, self.core.view().clone(), self.core.is_active());
        self.dispatch_selection(s);
        self.core.set_rank1_hint(self.selector.rank1_hint());
    }

    fn dispatch_selection(&mut self, actions: Vec<SelAction>) {
        for a in actions {
            match a {
                SelAction::Ranked { req, rank, policy } => {
                    let rank: Vec<u8> = rank.iter().map(|n| n.0).collect();
                    self.event(
                        EventKind::Ranked,
                        Some(req),
                        &[("rank", rank.into()), ("policy", policy.into())],
                    );
                }
                SelAction::Claim { req, view, key } => {
                    for to in NodeId::ALL.into_iter().filter(|&n| n != self.id) {
                        self.out.push(Out::Send {
                            to,
                            msg: NodeMsg::Claim { req, view, key },
                        });
                    }
                    self.event(
                        EventKind::Claim,
                        Some(req),
                        &[
                            ("score_step", key.score_step.into()),
                            ("tie", key.tie.into()),
                        ],
                    );
                }
                SelAction::Execute(req) => {
                    // Rule: never drop an Execute.
                    let id = req.req.id;
                    if !self.executing.contains_key(&id) {
                        let job = PlanJob {
                            req: req.clone(),
                            snapshot: self.state.snapshot(),
                            attempt: 0,
                        };
                        self.executing.insert(
                            id,
                            Executing {
                                req,
                                attempt: 0,
                                proposed: None,
                            },
                        );
                        self.pool.submit(job);
                    }
                }
                SelAction::Abort(id) => {
                    // A proposal already with the coordinator cannot be
                    // recalled; its outcome still arrives and is answered.
                    if self
                        .executing
                        .get(&id)
                        .is_some_and(|e| e.proposed.is_none())
                    {
                        self.executing.remove(&id);
                        self.pool.cancel(id);
                    }
                }
                SelAction::Reply { req, result } => self.out.push(Out::Reply {
                    req,
                    body: ReplyBody::Ok(result),
                }),
            }
        }
    }

    fn proposal_done(&mut self, req: ReqId, outcome: ProposeOutcome) {
        match outcome {
            // Answer with the committed entry, never this node's own copy: a
            // late `Stale` for an earlier send can have replaced that copy with
            // a newer plan while the older one committed (DL-058).
            ProposeOutcome::Committed(seq) => {
                if self.executing.remove(&req).is_some() {
                    let applied = seq <= self.core.applied_seq();
                    match self.core.entry(seq).map(|e| &e.kind) {
                        Some(EntryKind::Result { result, .. }) if applied => {
                            let body = ReplyBody::Ok(result.clone());
                            self.out.push(Out::Reply { req, body });
                        }
                        _ => {
                            self.reply_when_applied.insert(req);
                        }
                    }
                }
            }
            ProposeOutcome::Duplicate(result) => {
                self.executing.remove(&req);
                self.out.push(Out::Reply {
                    req,
                    body: ReplyBody::Ok(result),
                });
            }
            ProposeOutcome::Stale { current } => {
                if let Some(ex) = self.executing.get_mut(&req) {
                    ex.attempt += 1;
                    ex.proposed = None;
                    let job = PlanJob {
                        req: ex.req.clone(),
                        snapshot: self.state.snapshot(),
                        attempt: ex.attempt,
                    };
                    let attempt = ex.attempt;
                    self.pool.submit(job);
                    self.event(
                        EventKind::Replan,
                        Some(req),
                        &[("current", current.0.into()), ("attempt", attempt.into())],
                    );
                }
            }
            // Not in the view: the view change that caused it reaches the
            // selector, which hands the request on.
            ProposeOutcome::NotInView | ProposeOutcome::NotCoordinator { .. } => {
                self.executing.remove(&req);
            }
        }
    }
}
