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
use std::sync::{Arc, Mutex, Weak};

/// The newest committed snapshot. The core task publishes it after applying
/// entries; a planning thread takes it when it starts a job (DL-068), so a
/// plan reads the freshest state available and a long queue pins no old
/// copies.
///
/// The core publishes a weak reference: holding a strong one here would make
/// the state copy its whole snapshot at the first write of every batch, even
/// with no planning thread using it. A planning thread upgrades it; if the
/// state has just moved its snapshot (a write while nobody held it), the
/// upgrade fails until the core republishes at the end of that batch, and
/// the thread waits briefly.
#[derive(Debug)]
pub struct SnapshotSlot(Mutex<Published>);

#[derive(Debug)]
enum Published {
    /// Owned here; for tests and fixed snapshots.
    Strong(Arc<Snapshot>),
    /// The state's own snapshot, published by the core task.
    Weak(Weak<Snapshot>),
}

impl SnapshotSlot {
    pub fn new(snapshot: Arc<Snapshot>) -> Arc<SnapshotSlot> {
        Arc::new(SnapshotSlot(Mutex::new(Published::Strong(snapshot))))
    }

    /// The newest snapshot; cheap (one `Arc` clone) except in the brief
    /// moment while the core applies a batch.
    pub fn get(&self) -> Arc<Snapshot> {
        loop {
            {
                let p = self.0.lock().unwrap_or_else(|e| e.into_inner());
                match &*p {
                    Published::Strong(s) => return Arc::clone(s),
                    Published::Weak(w) => {
                        if let Some(s) = w.upgrade() {
                            return s;
                        }
                    }
                }
            }
            std::thread::sleep(std::time::Duration::from_micros(50));
        }
    }

    /// Replaces the snapshot with one owned here.
    pub fn set(&self, snapshot: Arc<Snapshot>) {
        *self.0.lock().unwrap_or_else(|e| e.into_inner()) = Published::Strong(snapshot);
    }

    /// Publishes the state's snapshot without keeping it alive.
    fn publish(&self, snapshot: &Arc<Snapshot>) {
        *self.0.lock().unwrap_or_else(|e| e.into_inner()) =
            Published::Weak(Arc::downgrade(snapshot));
    }
}

/// One request to plan. The snapshot is read when planning starts.
#[derive(Debug, Clone)]
pub struct PlanJob {
    pub req: ClientReq,
    pub snapshot: Arc<SnapshotSlot>,
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
    /// After a `Stale` rejection: plan again once this node has applied the
    /// log up to the coordinator's position at rejection, or at the deadline
    /// (ms), whichever comes first.
    replan_at: Option<(proto::Seq, u64)>,
}

/// Longest wait for the log to catch up before re-planning anyway, in case
/// the entries that made the plan stale are never committed.
const REPLAN_WAIT_MS: u64 = 500;

/// Heartbeats the missed-heartbeat average spans (weight 1/N per heartbeat).
const MISSED_WINDOW: u32 = 32;

/// The core task of one cloud node.
pub struct NodeTask {
    id: NodeId,
    core: Core,
    selector: Selector,
    state: Box<dyn StateMachine>,
    snapshot: Arc<SnapshotSlot>,
    pool: Box<dyn PlanPool>,
    executing: BTreeMap<ReqId, Executing>,
    /// Requests committed with this node as executor whose entry is not yet
    /// applied here; the client is answered when it is (DL-061).
    reply_when_applied: BTreeSet<ReqId>,
    incarnation: u32,
    heartbeat_n: u64,
    /// Last heartbeat accepted from each peer: (incarnation, number).
    last_heartbeat: BTreeMap<NodeId, (u32, u64)>,
    /// Share of each peer's recent heartbeats that never arrived, in parts
    /// per million: a moving average over about the last [`MISSED_WINDOW`]
    /// heartbeats, from gaps in their numbers. Kept finer than the
    /// thousandths it is reported in, so it decays all the way to 0.
    missed: BTreeMap<NodeId, u32>,
    /// Peers whose next heartbeat starts a new gap count: this node was
    /// silenced, and the heartbeats it dropped meanwhile were not the
    /// peers' fault.
    rebase_missed: BTreeSet<NodeId>,
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
        let snapshot = Arc::new(SnapshotSlot(Mutex::new(Published::Weak(Arc::downgrade(
            &state.snapshot(),
        )))));
        NodeTask {
            id,
            core,
            selector,
            snapshot,
            state,
            pool,
            executing: BTreeMap::new(),
            reply_when_applied: BTreeSet::new(),
            incarnation,
            heartbeat_n: 0,
            last_heartbeat: BTreeMap::new(),
            missed: BTreeMap::new(),
            rebase_missed: BTreeSet::new(),
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

    /// This process's incarnation (DL-021), for `rm-ctl status`.
    pub fn incarnation(&self) -> u32 {
        self.incarnation
    }

    /// The transport last heard from `peer` at `at` (ms); see
    /// `Core::heard_from`.
    pub fn heard_from(&mut self, peer: NodeId, at: u64) {
        self.core.heard_from(peer, at);
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
            // A node that is not ACTIVE (catching up after a silence, or out
            // of the view) leaves them to its peers, which got the same
            // request: its state may be behind (DL-077).
            if !self.core.is_active() {
                return self.take();
            }
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

    /// The pool finished planning `req`, spending `service_us` on it. Logs
    /// the `plan_end` event the analysis computes per-node busy time and
    /// load distribution from (protocol Section 10), whether or not the
    /// request was aborted meanwhile: the work was done either way.
    pub fn on_planned(
        &mut self,
        now: u64,
        req: ReqId,
        plan: PlanOutput,
        service_us: u32,
    ) -> Vec<Out> {
        self.event(
            EventKind::PlanEnd,
            Some(req),
            &[
                ("service_us", service_us.into()),
                ("read_set_len", plan.read_set.len().into()),
            ],
        );
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
        let gap = match self.last_heartbeat.get(&hb.from) {
            Some(&(inc, n)) if inc == hb.incarnation => {
                if hb.n <= n {
                    return;
                }
                hb.n - n - 1
            }
            _ => 0,
        };
        let gap = if self.rebase_missed.remove(&hb.from) {
            0
        } else {
            gap
        };
        self.last_heartbeat.insert(hb.from, (hb.incarnation, hb.n));
        // The reliability term (Section 5.1): each missed heartbeat pulls the
        // average towards 1000, each received one towards 0. It is measured
        // but weighted 0 by default, because each node sees its own losses
        // and rankings would disagree (DL-047).
        const PPM: u32 = 1_000_000;
        let m = self.missed.entry(hb.from).or_insert(0);
        for _ in 0..gap.min(u64::from(MISSED_WINDOW)) {
            *m = *m - *m / MISSED_WINDOW + PPM / MISSED_WINDOW;
        }
        *m -= m.div_ceil(MISSED_WINDOW);
        let missed = u16::try_from(*m / 1000).unwrap_or(1000);
        self.selector.set_missed(hb.from, missed);
        self.selector.on_heartbeat(hb.from, hb.load);
    }

    /// The heartbeat thread sent `hb` to the peers. This node ranks itself
    /// from the load it carried from now on, the moment its peers start to
    /// (DL-069). Refreshing its own view when the content was built instead,
    /// up to a heartbeat interval before the datagram left, made it count
    /// its claims from a different point than its peers did, and a quarter
    /// of requests were then claimed twice or waited out a claim timeout.
    pub fn heartbeat_sent(&mut self, hb: &Heartbeat) {
        if hb.from == self.id {
            self.selector.on_heartbeat(self.id, hb.load);
        }
    }

    /// The measured round trip between cloud nodes, in microseconds; sets
    /// the claim timeout (DL-024, DL-069).
    pub fn set_peer_rtt_us(&mut self, rtt_us: u64) {
        self.selector.set_peer_rtt_us(rtt_us);
    }

    /// Missed-heartbeat share of `peer`, in thousandths, for the logs.
    pub fn missed_permille(&self, peer: NodeId) -> u32 {
        self.missed.get(&peer).copied().unwrap_or(0) / 1000
    }

    /// The heartbeat content to hand to the heartbeat thread. The thread
    /// numbers it, adds round-trip echoes and reports it back once sent
    /// ([`NodeTask::heartbeat_sent`]).
    pub fn heartbeat(&mut self) -> Heartbeat {
        self.heartbeat_n += 1;
        let load = self.pool.load();
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
            echo: Vec::new(),
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
        let mut applied = false;
        let mut views = Vec::new();
        for a in actions {
            match a {
                CoreAction::Send { to, msg } => self.out.push(Out::Send { to, msg }),
                CoreAction::Apply(entry) => {
                    applied = true;
                    if let EntryKind::View(v) = &entry.kind {
                        views.push(v.clone());
                    }
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
                CoreAction::ProposeDone { req, outcome } => self.proposal_done(now, req, outcome),
                CoreAction::SetSilenced(on) => {
                    if !on {
                        self.rebase_missed =
                            NodeId::ALL.into_iter().filter(|&n| n != self.id).collect();
                    }
                    self.out.push(Out::SetSilenced(on));
                }
                CoreAction::Event { kind, req, fields } => {
                    self.out.push(Out::Event { kind, req, fields })
                }
            }
        }
        // Every view, not only the batch's last: a node that left and
        // rejoined within one batch must count as a new member, or its old
        // claims stay valid (DL-046). After the batch's own events, so the
        // claims a re-admitted node makes are logged after its `active`
        // (DL-077).
        for v in views {
            let s = self.selector.set_view(now, v, self.core.is_active());
            self.dispatch_selection(s);
        }
        if applied {
            // Publish once per batch. The slot holds it weakly, so the state
            // copies its snapshot at a write only while a planning thread
            // actually holds the old one.
            self.snapshot.publish(&self.state.snapshot());
        }
        self.release_replans(now);
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
                            snapshot: self.snapshot.clone(),
                            attempt: 0,
                        };
                        self.executing.insert(
                            id,
                            Executing {
                                req,
                                attempt: 0,
                                proposed: None,
                                replan_at: None,
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

    /// Sends re-plans whose wait is over to the pool. A deferred re-plan is
    /// delayed, never dropped (rule: never drop an `Execute`).
    fn release_replans(&mut self, now: u64) {
        let applied = self.core.applied_seq();
        let ready: Vec<ReqId> = self
            .executing
            .iter()
            .filter(|(_, e)| {
                e.replan_at
                    .is_some_and(|(seq, at)| applied >= seq || now >= at)
            })
            .map(|(id, _)| *id)
            .collect();
        if ready.is_empty() {
            return;
        }
        for id in ready {
            if let Some(ex) = self.executing.get_mut(&id) {
                ex.replan_at = None;
                let job = PlanJob {
                    req: ex.req.clone(),
                    snapshot: self.snapshot.clone(),
                    attempt: ex.attempt,
                };
                self.pool.submit(job);
            }
        }
    }

    fn proposal_done(&mut self, now: u64, req: ReqId, outcome: ProposeOutcome) {
        match outcome {
            // Answer with the committed entry, never this node's own copy: a
            // late `Stale` for an earlier send can have replaced that copy with
            // a newer plan while the older one committed (DL-061).
            ProposeOutcome::Committed(seq) => {
                if self.executing.remove(&req).is_some() {
                    let applied = seq <= self.core.applied_seq();
                    match self.core.entry(seq).map(|e| &e.kind) {
                        Some(EntryKind::Result { req: r, result, .. }) if applied => {
                            debug_assert_eq!(
                                *r, req,
                                "Committed must point at this request's entry"
                            );
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
                    if ex.proposed.is_none() {
                        return; // a late copy for a send already re-planned
                    }
                    // Re-planning at once would read the same stale snapshot
                    // and be rejected again; wait until this node has applied
                    // what the coordinator had when it rejected (DL-063).
                    ex.attempt += 1;
                    ex.proposed = None;
                    ex.replan_at = Some((current, now + REPLAN_WAIT_MS));
                    let attempt = ex.attempt;
                    self.event(
                        EventKind::Replan,
                        Some(req),
                        &[("current", current.0.into()), ("attempt", attempt.into())],
                    );
                }
            }
            // Not in the coordinator's view. If this node's own applied view
            // no longer holds it either, the selector has handed the request
            // on, so drop it. If it does, the view change that excluded this
            // node may still be uncommitted and be discarded by a new
            // coordinator, leaving the request claimed by this node: dropping
            // it would strand it, so plan it again after the usual wait.
            ProposeOutcome::NotInView => {
                if !self.core.view().contains(self.id) {
                    self.executing.remove(&req);
                } else if let Some(ex) = self.executing.get_mut(&req) {
                    ex.proposed = None;
                    ex.replan_at = Some((proto::Seq(u64::MAX), now + REPLAN_WAIT_MS));
                }
            }
            // Never final: the core retries at the hinted coordinator.
            ProposeOutcome::NotCoordinator { .. } => {}
        }
    }
}
