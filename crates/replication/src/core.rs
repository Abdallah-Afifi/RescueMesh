use crate::rng::SplitMix64;
use proto::config::Config;
use proto::event::EventKind;
use proto::hash;
use proto::{
    Entry, EntryKind, Epoch, NodeId, NodeMsg, Proposal, ProposeOutcome, ReqId, ResourceId, Seq,
    SilenceTarget, Version, View, ViewNo, Write,
};
use serde_json::{Map, Value};
use std::collections::{BTreeMap, BTreeSet};

/// Longest injected silence the project description allows (p. 3).
const MAX_SILENCE_MS: u64 = 20_000;

/// Timing parameters the core needs. Built from the run configuration with
/// [`CoreCfg::from_config`]; tests build it directly with faster values.
#[derive(Debug, Clone, PartialEq)]
pub struct CoreCfg {
    /// Coordinator sends an `Append` (possibly empty) to each peer this often.
    pub heartbeat_ms: u64,
    /// A newly advanced commit position reaches followers within this time,
    /// even when no new entries are being sent.
    pub commit_flush_ms: u64,
    /// A coordinator not heard from for this long counts as lost even if the
    /// failure detector has not reported it.
    pub coordinator_silence_ms: u64,
    pub candidacy_base_ms: u64,
    pub candidacy_jitter_ms: u64,
    /// Extra delay before a SYNCING node stands, so an ACTIVE node is preferred.
    pub syncing_candidacy_extra_ms: u64,
    pub election_backoff_min_ms: u64,
    pub election_backoff_max_ms: u64,
    pub lease_ms: u64,
    pub propose_retry_ms: u64,
    /// Most entries carried by one `Append`.
    pub append_batch: usize,
    /// After an injection, no new one until its duration plus this margin.
    pub inject_margin_ms: u64,
    pub inject: Option<InjectSchedule>,
    /// Seeds this node's jitter generator.
    pub seed: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InjectSchedule {
    pub seed: u64,
    pub interval_min_ms: u64,
    pub interval_max_ms: u64,
    pub duration_min_ms: u64,
    pub duration_max_ms: u64,
}

impl CoreCfg {
    pub fn from_config(cfg: &Config) -> CoreCfg {
        let t = &cfg.timeouts;
        let i = &cfg.inject;
        CoreCfg {
            heartbeat_ms: t.heartbeat_ms,
            commit_flush_ms: (t.heartbeat_ms / 10).max(1),
            coordinator_silence_ms: t.fd_max_ms,
            candidacy_base_ms: t.candidacy_base_ms,
            candidacy_jitter_ms: t.candidacy_jitter_ms,
            syncing_candidacy_extra_ms: 2 * t.fd_max_ms,
            election_backoff_min_ms: t.election_backoff_min_ms,
            election_backoff_max_ms: t.election_backoff_max_ms,
            lease_ms: t.lease_ms,
            propose_retry_ms: t.propose_retry_ms,
            append_batch: 256,
            inject_margin_ms: 5_000,
            inject: i.enabled.then_some(InjectSchedule {
                seed: cfg.run.seed,
                interval_min_ms: i.interval_min_ms,
                interval_max_ms: i.interval_max_ms,
                duration_min_ms: i.duration_min_ms,
                duration_max_ms: i.duration_max_ms,
            }),
            seed: cfg.run.seed,
        }
    }
}

/// What the node runtime must do after each call into the core.
#[derive(Debug, Clone, PartialEq)]
pub enum Action {
    /// Send to a peer through the transport.
    Send { to: NodeId, msg: NodeMsg },
    /// A newly committed entry, in `seq` order, exactly once. `Result`
    /// entries go to the state machine; `View` entries also go to selection.
    Apply(Entry),
    /// Final answer to a proposal this node made as executor.
    ProposeDone { req: ReqId, outcome: ProposeOutcome },
    /// Fault injection: tell the transport to drop all traffic, or to resume.
    SetSilenced(bool),
    /// Write to the event log.
    Event {
        kind: EventKind,
        req: Option<ReqId>,
        fields: Map<String, Value>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Follower,
    PreCandidate,
    Candidate,
    Coordinator,
}

/// Whether this node may claim requests (Section 6).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    Active,
    Syncing,
}

/// The log. Index `i` holds the entry with `seq = i + 1`.
#[derive(Debug, Default)]
struct Log {
    entries: Vec<Entry>,
}

impl Log {
    fn last_seq(&self) -> u64 {
        self.entries.len() as u64
    }

    fn last_epoch(&self) -> Epoch {
        self.entries.last().map_or(Epoch(0), |e| e.epoch)
    }

    fn get(&self, seq: u64) -> Option<&Entry> {
        if seq == 0 {
            None
        } else {
            self.entries.get((seq - 1) as usize)
        }
    }

    /// Epoch of the entry at `seq`; `seq` 0 matches epoch 0.
    fn epoch_at(&self, seq: u64) -> Option<Epoch> {
        if seq == 0 {
            Some(Epoch(0))
        } else {
            self.get(seq).map(|e| e.epoch)
        }
    }

    fn up_to_date(&self, last_epoch: Epoch, last_seq: Seq) -> bool {
        (last_epoch, last_seq.0) >= (self.last_epoch(), self.last_seq())
    }
}

#[derive(Debug, Clone)]
struct Waiter {
    node: NodeId,
    req: ReqId,
    executor: NodeId,
}

/// State that exists only while this node is coordinator. Rebuilt from the
/// log at every election, never carried over.
#[derive(Debug)]
struct Leader {
    next: [u64; 3],
    matched: [u64; 3],
    sent_up_to: [u64; 3],
    last_ack: [u64; 3],
    last_send: [u64; 3],
    /// Commit position last sent to each peer.
    commit_sent: [u64; 3],
    /// Every request that has an entry in this log, committed or not.
    req_index: BTreeMap<ReqId, u64>,
    /// Version of every resource after the last entry in this log.
    versions: BTreeMap<ResourceId, Version>,
    waiters: BTreeMap<u64, Vec<Waiter>>,
    /// The latest view in the log, committed or not.
    log_view: View,
    log_view_seq: u64,
    joins: BTreeSet<NodeId>,
    inject_k: u64,
    next_inject_at: u64,
    inject_busy_until: u64,
    inject_paused: bool,
}

#[derive(Debug, Clone)]
struct Outstanding {
    proposal: Proposal,
    last_sent: Option<u64>,
}

/// Replication, election, membership and fault injection for one node.
/// See the crate documentation.
#[derive(Debug)]
pub struct Core {
    id: NodeId,
    cfg: CoreCfg,
    rng: SplitMix64,

    epoch: Epoch,
    voted_for: Option<NodeId>,
    log: Log,
    commit: u64,
    applied: u64,

    role: Role,
    coordinator: Option<NodeId>,
    last_coordinator_contact: u64,
    candidacy_at: Option<u64>,
    round_deadline: u64,
    votes: BTreeSet<NodeId>,
    pre_vote_epoch: Epoch,

    /// The latest applied view.
    view: View,
    status: Status,
    /// Commit position, learned from the coordinator after waking, that must
    /// be applied before this node is current again. `None` while unknown.
    catch_up_to: Option<u64>,
    last_join_sent: Option<u64>,

    suspected: [bool; 3],
    silenced_until: Option<u64>,
    /// Injection counter of the current silence, for the `inject_end` event.
    silence_k: Option<u64>,
    rank1_hint: Option<NodeId>,
    inject_paused: bool,

    outstanding: BTreeMap<ReqId, Outstanding>,
    leader: Option<Leader>,
    out: Vec<Action>,
}

/// Objects whose version the read-set check compares (invariant I3):
/// resources, and incidents, so two plans cannot both assign one waiting
/// incident (design log DL-057). Both are keyed by the creating `ReqId`.
fn versioned_ids(writes: &[Write]) -> impl Iterator<Item = ReqId> + '_ {
    writes.iter().filter_map(|w| match w {
        Write::PutResource(r) => Some(r.id),
        Write::PutIncident(i) => Some(i.id),
        Write::PutAssignment(_) => None,
    })
}

fn peers_of(id: NodeId) -> impl Iterator<Item = NodeId> {
    NodeId::ALL.into_iter().filter(move |&n| n != id)
}

fn fields(pairs: &[(&str, Value)]) -> Map<String, Value> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_owned(), v.clone()))
        .collect()
}

fn nodes_json(nodes: &[NodeId]) -> Value {
    Value::from(nodes.iter().map(|n| n.0).collect::<Vec<_>>())
}

impl Core {
    /// A node at start-up: empty log, all three nodes in view 0, ACTIVE.
    pub fn new(id: NodeId, cfg: CoreCfg) -> Core {
        assert!(id.is_valid(), "node id must be 1, 2 or 3");
        let rng = SplitMix64::new(cfg.seed ^ (u64::from(id.0) << 56) ^ 0x5eed);
        Core {
            id,
            cfg,
            rng,
            epoch: Epoch(0),
            voted_for: None,
            log: Log::default(),
            commit: 0,
            applied: 0,
            role: Role::Follower,
            coordinator: None,
            last_coordinator_contact: 0,
            candidacy_at: None,
            round_deadline: 0,
            votes: BTreeSet::new(),
            pre_vote_epoch: Epoch(0),
            view: View::new(ViewNo(0), NodeId::ALL.to_vec()),
            status: Status::Active,
            catch_up_to: Some(0),
            last_join_sent: None,
            suspected: [false; 3],
            silenced_until: None,
            silence_k: None,
            rank1_hint: None,
            inject_paused: false,
            outstanding: BTreeMap::new(),
            leader: None,
            out: Vec::new(),
        }
    }

    // ------------------------------------------------------------ accessors

    pub fn id(&self) -> NodeId {
        self.id
    }
    pub fn epoch(&self) -> Epoch {
        self.epoch
    }
    pub fn role(&self) -> Role {
        self.role
    }
    pub fn coordinator(&self) -> Option<NodeId> {
        self.coordinator
    }
    pub fn status(&self) -> Status {
        self.status
    }
    /// May this node claim requests? (Section 6)
    pub fn is_active(&self) -> bool {
        self.status == Status::Active && self.silenced_until.is_none()
    }
    pub fn is_silenced(&self) -> bool {
        self.silenced_until.is_some()
    }
    pub fn view(&self) -> &View {
        &self.view
    }
    pub fn commit_seq(&self) -> Seq {
        Seq(self.commit)
    }
    pub fn applied_seq(&self) -> Seq {
        Seq(self.applied)
    }
    pub fn last_seq(&self) -> Seq {
        Seq(self.log.last_seq())
    }
    pub fn entry(&self, seq: Seq) -> Option<&Entry> {
        self.log.get(seq.0)
    }
    /// Proposals made by this node that have no outcome yet.
    pub fn outstanding(&self) -> usize {
        self.outstanding.len()
    }

    /// The node currently ranked first, maintained by selection, so that an
    /// operator can target it with `SilenceTarget::CurrentRank1`.
    pub fn set_rank1_hint(&mut self, node: Option<NodeId>) {
        self.rank1_hint = node;
    }

    /// Stop or resume scheduled injections (operator control and tests).
    pub fn pause_injection(&mut self, paused: bool) {
        self.inject_paused = paused;
        if let Some(l) = self.leader.as_mut() {
            l.inject_paused = paused;
        }
    }

    // ------------------------------------------------------------ inputs

    /// Call regularly, at least every few milliseconds.
    pub fn tick(&mut self, now: u64) -> Vec<Action> {
        if let Some(until) = self.silenced_until {
            if now < until {
                return Vec::new();
            }
            self.wake(now);
        }
        if self.role == Role::Coordinator {
            self.leader_tick(now);
        } else {
            self.follower_tick(now);
        }
        self.resend_proposals(now);
        std::mem::take(&mut self.out)
    }

    /// The failure detector's opinion of a peer changed.
    pub fn set_suspected(&mut self, now: u64, peer: NodeId, suspected: bool) -> Vec<Action> {
        if peer == self.id || !peer.is_valid() {
            return Vec::new();
        }
        self.suspected[peer.index()] = suspected;
        if self.silenced_until.is_some() {
            return Vec::new();
        }
        if suspected {
            self.event(EventKind::Suspect, None, &[("peer", peer.0.into())]);
            if self.role == Role::Coordinator {
                self.maintain_view(now);
            }
        }
        std::mem::take(&mut self.out)
    }

    /// This node, as executor, proposes a result for the coordinator to commit.
    /// The answer arrives later as [`Action::ProposeDone`].
    pub fn propose(&mut self, now: u64, proposal: Proposal) -> Vec<Action> {
        let req = proposal.req;
        self.outstanding.insert(
            req,
            Outstanding {
                proposal,
                last_sent: None,
            },
        );
        if self.silenced_until.is_none() {
            self.send_proposal(now, req);
        }
        std::mem::take(&mut self.out)
    }

    /// An operator asked this node to silence a node (Section 8).
    pub fn silence(&mut self, now: u64, target: SilenceTarget, duration_ms: u64) -> Vec<Action> {
        if self.silenced_until.is_none() {
            self.handle_silence(now, target, duration_ms);
        }
        std::mem::take(&mut self.out)
    }

    pub fn on_message(&mut self, now: u64, from: NodeId, msg: NodeMsg) -> Vec<Action> {
        if self.silenced_until.is_some() || from == self.id || !from.is_valid() {
            return Vec::new();
        }
        match msg {
            NodeMsg::Ack | NodeMsg::Claim { .. } => {}
            NodeMsg::Propose(p) => self.handle_propose(now, from, p),
            NodeMsg::ProposeReply { req, outcome } => self.handle_propose_reply(now, req, outcome),
            NodeMsg::Append {
                epoch,
                prev_seq,
                prev_epoch,
                entries,
                commit,
            } => self.handle_append(now, from, epoch, prev_seq.0, prev_epoch, entries, commit.0),
            NodeMsg::AppendAck {
                epoch,
                ok,
                match_seq,
            } => self.handle_append_ack(now, from, epoch, ok, match_seq.0),
            NodeMsg::PreVote {
                next_epoch,
                last_epoch,
                last_seq,
            } => {
                let granted = self.role != Role::Coordinator
                    && next_epoch > self.epoch
                    && self.log.up_to_date(last_epoch, last_seq)
                    && self.coordinator_lost(now);
                self.send(
                    from,
                    NodeMsg::PreVoteReply {
                        next_epoch,
                        granted,
                    },
                );
            }
            NodeMsg::PreVoteReply {
                next_epoch,
                granted,
            } => {
                if self.role == Role::PreCandidate && next_epoch == self.pre_vote_epoch && granted {
                    self.votes.insert(from);
                    if self.votes.len() >= 2 {
                        self.start_election(now);
                    }
                }
            }
            NodeMsg::Election {
                epoch,
                last_epoch,
                last_seq,
            } => self.handle_election(now, from, epoch, last_epoch, last_seq),
            NodeMsg::Vote { epoch, granted } => {
                if epoch > self.epoch {
                    self.adopt_epoch(now, epoch, "higher epoch in vote");
                } else if self.role == Role::Candidate && epoch == self.epoch && granted {
                    self.votes.insert(from);
                    if self.votes.len() >= 2 {
                        self.become_coordinator(now);
                    }
                }
            }
            NodeMsg::Coordinator { epoch } => {
                if epoch > self.epoch || (epoch == self.epoch && self.role != Role::Coordinator) {
                    self.follow(now, epoch, from);
                }
            }
            NodeMsg::Join { last_seq, .. } => {
                if let Some(l) = self.leader.as_mut() {
                    l.joins.insert(from);
                    let i = from.index();
                    if last_seq.0 + 1 < l.next[i] {
                        l.next[i] = (last_seq.0 + 1).max(l.matched[i] + 1);
                        l.sent_up_to[i] = l.next[i] - 1;
                    }
                    self.maintain_view(now);
                }
            }
            NodeMsg::Silence {
                target,
                duration_ms,
            } => self.handle_silence(now, target, duration_ms),
        }
        std::mem::take(&mut self.out)
    }

    // ------------------------------------------------------------ helpers

    fn send(&mut self, to: NodeId, msg: NodeMsg) {
        self.out.push(Action::Send { to, msg });
    }

    fn event(&mut self, kind: EventKind, req: Option<ReqId>, pairs: &[(&str, Value)]) {
        self.out.push(Action::Event {
            kind,
            req,
            fields: fields(pairs),
        });
    }

    fn coordinator_lost(&self, now: u64) -> bool {
        match self.coordinator {
            None => true,
            Some(c) if c == self.id => self.role != Role::Coordinator,
            Some(c) => {
                self.suspected[c.index()]
                    || now.saturating_sub(self.last_coordinator_contact)
                        > self.cfg.coordinator_silence_ms
            }
        }
    }

    // ------------------------------------------------------------ election (Section 7)

    fn follower_tick(&mut self, now: u64) {
        let lost = self.coordinator_lost(now);
        match self.role {
            Role::Follower => {
                if !lost {
                    self.candidacy_at = None;
                } else if let Some(at) = self.candidacy_at {
                    if now >= at {
                        self.start_pre_vote(now);
                    }
                } else {
                    let mut delay = self.cfg.candidacy_base_ms * u64::from(3 - self.id.0)
                        + self.rng.range(0, self.cfg.candidacy_jitter_ms);
                    if self.status == Status::Syncing {
                        delay += self.cfg.syncing_candidacy_extra_ms;
                    }
                    self.candidacy_at = Some(now + delay);
                }
            }
            Role::PreCandidate | Role::Candidate => {
                if now >= self.round_deadline {
                    // The round failed: back off and try again.
                    self.role = Role::Follower;
                    self.votes.clear();
                    let backoff = self.rng.range(
                        self.cfg.election_backoff_min_ms,
                        self.cfg.election_backoff_max_ms,
                    );
                    self.candidacy_at = Some(now + backoff);
                }
            }
            Role::Coordinator => unreachable!(),
        }
        // A node outside the view asks the coordinator to re-admit it.
        let join_due = self
            .last_join_sent
            .is_none_or(|t| now >= t + self.cfg.heartbeat_ms);
        if self.status == Status::Syncing
            && !self.view.contains(self.id)
            && !lost
            && join_due
            && let Some(c) = self.coordinator
        {
            self.last_join_sent = Some(now);
            let msg = NodeMsg::Join {
                last_epoch: self.log.last_epoch(),
                last_seq: Seq(self.log.last_seq()),
            };
            self.send(c, msg);
        }
    }

    fn round_timeout(&mut self, now: u64) -> u64 {
        now + self.rng.range(
            self.cfg.election_backoff_min_ms,
            self.cfg.election_backoff_max_ms,
        )
    }

    fn start_pre_vote(&mut self, now: u64) {
        self.role = Role::PreCandidate;
        self.candidacy_at = None;
        self.pre_vote_epoch = self.epoch.next();
        self.votes = BTreeSet::from([self.id]);
        self.round_deadline = self.round_timeout(now);
        let msg = NodeMsg::PreVote {
            next_epoch: self.pre_vote_epoch,
            last_epoch: self.log.last_epoch(),
            last_seq: Seq(self.log.last_seq()),
        };
        for p in peers_of(self.id) {
            self.send(p, msg.clone());
        }
        self.event(
            EventKind::ElectionStart,
            None,
            &[
                ("epoch", self.pre_vote_epoch.0.into()),
                ("phase", "pre_vote".into()),
            ],
        );
    }

    fn start_election(&mut self, now: u64) {
        self.epoch = self.epoch.next();
        self.voted_for = Some(self.id);
        self.coordinator = None;
        self.role = Role::Candidate;
        self.votes = BTreeSet::from([self.id]);
        self.round_deadline = self.round_timeout(now);
        let msg = NodeMsg::Election {
            epoch: self.epoch,
            last_epoch: self.log.last_epoch(),
            last_seq: Seq(self.log.last_seq()),
        };
        for p in peers_of(self.id) {
            self.send(p, msg.clone());
        }
        self.event(
            EventKind::ElectionStart,
            None,
            &[("epoch", self.epoch.0.into()), ("phase", "vote".into())],
        );
    }

    fn handle_election(
        &mut self,
        now: u64,
        from: NodeId,
        epoch: Epoch,
        last_epoch: Epoch,
        last_seq: Seq,
    ) {
        if epoch < self.epoch {
            self.send(
                from,
                NodeMsg::Vote {
                    epoch: self.epoch,
                    granted: false,
                },
            );
            return;
        }
        // A node in contact with a live coordinator ignores candidates, so a
        // returning node cannot disrupt it (pre-vote normally stops it earlier).
        if !self.coordinator_lost(now) && self.coordinator != Some(from) {
            self.send(
                from,
                NodeMsg::Vote {
                    epoch,
                    granted: false,
                },
            );
            return;
        }
        if epoch > self.epoch {
            self.adopt_epoch(now, epoch, "higher epoch in election");
        }
        let granted =
            self.voted_for.is_none_or(|v| v == from) && self.log.up_to_date(last_epoch, last_seq);
        if granted {
            self.voted_for = Some(from);
            self.candidacy_at = None;
        }
        self.send(
            from,
            NodeMsg::Vote {
                epoch: self.epoch,
                granted,
            },
        );
    }

    /// Move to a newer epoch, giving up any role held in the old one.
    fn adopt_epoch(&mut self, now: u64, epoch: Epoch, reason: &str) {
        debug_assert!(epoch > self.epoch);
        self.epoch = epoch;
        self.voted_for = None;
        if self.role == Role::Coordinator {
            self.step_down(now, reason);
        }
        self.role = Role::Follower;
        self.coordinator = None;
        self.votes.clear();
    }

    /// Accept `from` as coordinator of `epoch`.
    fn follow(&mut self, now: u64, epoch: Epoch, from: NodeId) {
        if epoch > self.epoch {
            self.epoch = epoch;
            self.voted_for = None;
        }
        if self.role == Role::Coordinator {
            self.step_down(now, "another coordinator");
        }
        self.role = Role::Follower;
        self.votes.clear();
        self.coordinator = Some(from);
        self.last_coordinator_contact = now;
        self.candidacy_at = None;
    }

    fn step_down(&mut self, _now: u64, reason: &str) {
        self.role = Role::Follower;
        self.leader = None;
        self.coordinator = None;
        self.candidacy_at = None;
        self.event(
            EventKind::StepDown,
            None,
            &[("epoch", self.epoch.0.into()), ("reason", reason.into())],
        );
    }

    fn become_coordinator(&mut self, now: u64) {
        self.role = Role::Coordinator;
        self.coordinator = Some(self.id);
        self.candidacy_at = None;
        self.votes.clear();

        let mut req_index = BTreeMap::new();
        let mut versions = BTreeMap::new();
        let mut inject_k = 0;
        for e in &self.log.entries {
            match &e.kind {
                EntryKind::Result { req, writes, .. } => {
                    req_index.insert(*req, e.seq.0);
                    for id in versioned_ids(writes) {
                        versions.insert(id, Version(e.seq.0));
                    }
                }
                EntryKind::Inject { .. } => inject_k += 1,
                _ => {}
            }
        }
        let last = self.log.last_seq();
        let (log_view, log_view_seq) = self
            .log
            .entries
            .iter()
            .rev()
            .find_map(|e| match &e.kind {
                EntryKind::View(v) => Some((v.clone(), e.seq.0)),
                _ => None,
            })
            .unwrap_or_else(|| (self.view.clone(), 0));
        let first_interval = self.inject_interval(inject_k);
        self.leader = Some(Leader {
            next: [last + 1; 3],
            matched: [0; 3],
            sent_up_to: [last; 3],
            last_ack: [now; 3],
            last_send: [0; 3],
            commit_sent: [0; 3],
            req_index,
            versions,
            waiters: BTreeMap::new(),
            log_view: log_view.clone(),
            log_view_seq,
            joins: BTreeSet::new(),
            inject_k,
            next_inject_at: now + first_interval,
            inject_busy_until: now,
            inject_paused: self.inject_paused,
        });
        self.event(
            EventKind::ElectionWon,
            None,
            &[("epoch", self.epoch.0.into()), ("last_seq", last.into())],
        );
        for p in peers_of(self.id) {
            self.send(p, NodeMsg::Coordinator { epoch: self.epoch });
        }
        // First entry of the epoch: the view without suspected peers, with
        // this node in it. Committing it also commits every inherited entry.
        let mut members: Vec<NodeId> = log_view
            .active()
            .iter()
            .copied()
            .filter(|n| !self.suspected[n.index()])
            .collect();
        members.push(self.id);
        self.catch_up_to = Some(self.commit);
        self.append(now, EntryKind::View(View::new(log_view.no.next(), members)));
    }

    // ------------------------------------------------------------ coordinator side

    fn leader_tick(&mut self, now: u64) {
        let Some(l) = self.leader.as_ref() else {
            return;
        };
        let freshest = peers_of(self.id)
            .map(|p| l.last_ack[p.index()])
            .max()
            .unwrap_or(now);
        if now.saturating_sub(freshest) > self.cfg.lease_ms {
            self.step_down(now, "lease expired");
            return;
        }
        for p in peers_of(self.id) {
            let commit = self.commit;
            let due = self.leader.as_ref().is_some_and(|l| {
                let i = p.index();
                now >= l.last_send[i] + self.cfg.heartbeat_ms
                    || (l.commit_sent[i] < commit
                        && now >= l.last_send[i] + self.cfg.commit_flush_ms)
            });
            if due {
                // Also retransmits anything not yet acknowledged.
                let l = self.leader.as_mut().expect("coordinator");
                l.sent_up_to[p.index()] = l.next[p.index()] - 1;
                self.send_append(now, p);
            }
        }
        self.maintain_view(now);
        self.maybe_inject(now);
    }

    fn view_change_pending(&self) -> bool {
        self.leader
            .as_ref()
            .is_some_and(|l| l.log_view_seq > self.commit)
    }

    /// Remove suspected members; re-admit nodes that asked and have caught up.
    fn maintain_view(&mut self, now: u64) {
        if self.role != Role::Coordinator || self.view_change_pending() {
            return;
        }
        let Some(l) = self.leader.as_ref() else {
            return;
        };
        let lv = l.log_view.clone();
        if let Some(p) = peers_of(self.id).find(|p| self.suspected[p.index()] && lv.contains(*p)) {
            self.append(now, EntryKind::View(lv.without(p)));
            return;
        }
        let commit = self.commit;
        let joins: Vec<NodeId> = l.joins.iter().copied().collect();
        for j in joins {
            let l = self.leader.as_mut().expect("coordinator");
            if lv.contains(j) {
                l.joins.remove(&j);
            } else if !self.suspected[j.index()] && l.matched[j.index()] >= commit {
                l.joins.remove(&j);
                self.append(now, EntryKind::View(lv.with(j)));
                return;
            }
        }
    }

    /// Append to this coordinator's log and start replicating.
    fn append(&mut self, now: u64, kind: EntryKind) -> u64 {
        let seq = self.log.last_seq() + 1;
        let entry = Entry {
            seq: Seq(seq),
            epoch: self.epoch,
            kind,
        };
        let l = self.leader.as_mut().expect("only the coordinator appends");
        match &entry.kind {
            EntryKind::Result { req, writes, .. } => {
                l.req_index.insert(*req, seq);
                for id in versioned_ids(writes) {
                    l.versions.insert(id, Version(seq));
                }
            }
            EntryKind::View(v) => {
                l.log_view = v.clone();
                l.log_view_seq = seq;
            }
            EntryKind::Inject { .. } => l.inject_k += 1,
            EntryKind::Noop => {}
        }
        self.log.entries.push(entry);
        for p in peers_of(self.id) {
            let l = self.leader.as_ref().expect("coordinator");
            if l.sent_up_to[p.index()] + 1 == seq {
                self.send_append(now, p);
            }
        }
        self.advance_commit(now);
        seq
    }

    /// Send `p` the entries after what it has been sent so far.
    fn send_append(&mut self, now: u64, p: NodeId) {
        let i = p.index();
        let l = self.leader.as_mut().expect("coordinator");
        let from = l.sent_up_to[i] + 1;
        let to = self
            .log
            .last_seq()
            .min(from + self.cfg.append_batch as u64 - 1);
        let prev_seq = from - 1;
        let prev_epoch = self
            .log
            .epoch_at(prev_seq)
            .expect("prev entry exists in coordinator log");
        let entries: Vec<Entry> = if to >= from {
            self.log.entries[(from - 1) as usize..to as usize].to_vec()
        } else {
            Vec::new()
        };
        l.sent_up_to[i] = prev_seq + entries.len() as u64;
        l.last_send[i] = now;
        l.commit_sent[i] = self.commit;
        let msg = NodeMsg::Append {
            epoch: self.epoch,
            prev_seq: Seq(prev_seq),
            prev_epoch,
            entries,
            commit: Seq(self.commit),
        };
        self.send(p, msg);
    }

    fn handle_append_ack(
        &mut self,
        now: u64,
        from: NodeId,
        epoch: Epoch,
        ok: bool,
        match_seq: u64,
    ) {
        if epoch > self.epoch {
            self.adopt_epoch(now, epoch, "higher epoch in append ack");
            return;
        }
        if self.role != Role::Coordinator || epoch < self.epoch {
            return;
        }
        let last = self.log.last_seq();
        let i = from.index();
        let l = self.leader.as_mut().expect("coordinator");
        l.last_ack[i] = now;
        if ok {
            let m = match_seq.min(last);
            if m > l.matched[i] {
                l.matched[i] = m;
            }
            l.next[i] = l.matched[i] + 1;
            if l.sent_up_to[i] < l.matched[i] {
                l.sent_up_to[i] = l.matched[i];
            }
            let behind = l.sent_up_to[i] < last && l.sent_up_to[i] == l.matched[i];
            self.advance_commit(now);
            if behind {
                self.send_append(now, from);
            }
            self.maintain_view(now);
        } else {
            l.next[i] = (match_seq + 1)
                .max(l.matched[i] + 1)
                .min(l.next[i].max(l.matched[i] + 1));
            l.sent_up_to[i] = l.next[i] - 1;
            self.send_append(now, from);
        }
    }

    fn advance_commit(&mut self, now: u64) {
        let Some(l) = self.leader.as_ref() else {
            return;
        };
        let last = self.log.last_seq();
        let mut new_commit = self.commit;
        for n in (self.commit + 1..=last).rev() {
            let acks = 1 + peers_of(self.id)
                .filter(|p| l.matched[p.index()] >= n)
                .count();
            // Only entries of the current epoch are committed by counting;
            // earlier ones commit with them (Section 7, step 4).
            if acks >= 2 && self.log.epoch_at(n) == Some(self.epoch) {
                new_commit = n;
                break;
            }
        }
        if new_commit > self.commit {
            self.commit = new_commit;
            self.apply_committed(now);
            self.answer_waiters();
        }
    }

    fn outcome_for(&self, seq: u64, executor: NodeId) -> ProposeOutcome {
        match &self.log.get(seq).expect("indexed entry exists").kind {
            EntryKind::Result {
                executor: e,
                result,
                ..
            } => {
                if *e == executor {
                    ProposeOutcome::Committed(Seq(seq))
                } else {
                    ProposeOutcome::Duplicate(result.clone())
                }
            }
            _ => unreachable!("request index points at a Result entry"),
        }
    }

    fn answer_waiters(&mut self) {
        let commit = self.commit;
        let Some(l) = self.leader.as_mut() else {
            return;
        };
        let ready: Vec<u64> = l.waiters.range(..=commit).map(|(s, _)| *s).collect();
        let mut answers = Vec::new();
        for s in ready {
            for w in l.waiters.remove(&s).unwrap_or_default() {
                answers.push((s, w));
            }
        }
        for (s, w) in answers {
            let outcome = self.outcome_for(s, w.executor);
            self.reply_proposal(w.node, w.req, outcome);
        }
    }

    fn reply_proposal(&mut self, to: NodeId, req: ReqId, outcome: ProposeOutcome) {
        if to == self.id {
            self.finish_proposal(req, outcome);
        } else {
            self.send(to, NodeMsg::ProposeReply { req, outcome });
        }
    }

    fn handle_propose(&mut self, now: u64, from: NodeId, p: Proposal) {
        if self.role != Role::Coordinator {
            let hint = self.coordinator.filter(|&c| c != self.id);
            self.reply_proposal(from, p.req, ProposeOutcome::NotCoordinator { hint });
            return;
        }
        // Invariant I1: one entry per request.
        let existing = self
            .leader
            .as_ref()
            .expect("coordinator")
            .req_index
            .get(&p.req)
            .copied();
        if let Some(seq) = existing {
            if seq <= self.commit {
                let outcome = self.outcome_for(seq, p.executor);
                if matches!(outcome, ProposeOutcome::Duplicate(_)) {
                    self.event(
                        EventKind::DuplicateSuppressed,
                        Some(p.req),
                        &[("seq", seq.into())],
                    );
                }
                self.reply_proposal(from, p.req, outcome);
            } else {
                let w = Waiter {
                    node: from,
                    req: p.req,
                    executor: p.executor,
                };
                self.leader
                    .as_mut()
                    .expect("coordinator")
                    .waiters
                    .entry(seq)
                    .or_default()
                    .push(w);
            }
            return;
        }
        let l = self.leader.as_ref().expect("coordinator");
        // Invariant I5: only view members execute.
        if !l.log_view.contains(p.executor) {
            self.reply_proposal(from, p.req, ProposeOutcome::NotInView);
            return;
        }
        // Invariant I3: the plan must have read the current versions.
        let stale = p
            .read_set
            .iter()
            .any(|(r, v)| l.versions.get(r).copied().unwrap_or(Version(0)) != *v);
        if stale {
            let current = Seq(self.log.last_seq());
            self.reply_proposal(from, p.req, ProposeOutcome::Stale { current });
            return;
        }
        let waiter = Waiter {
            node: from,
            req: p.req,
            executor: p.executor,
        };
        let seq = self.log.last_seq() + 1;
        self.leader
            .as_mut()
            .expect("coordinator")
            .waiters
            .entry(seq)
            .or_default()
            .push(waiter);
        let req = p.req;
        let appended = self.append(
            now,
            EntryKind::Result {
                req,
                executor: p.executor,
                writes: p.writes,
                result: p.result,
            },
        );
        debug_assert_eq!(appended, seq);
        self.event(
            EventKind::Propose,
            Some(req),
            &[("seq", seq.into()), ("executor", p.executor.0.into())],
        );
    }

    // ------------------------------------------------------------ follower side

    #[allow(clippy::too_many_arguments)]
    fn handle_append(
        &mut self,
        now: u64,
        from: NodeId,
        epoch: Epoch,
        prev_seq: u64,
        prev_epoch: Epoch,
        entries: Vec<Entry>,
        leader_commit: u64,
    ) {
        if epoch < self.epoch {
            // Fencing: a coordinator from an older epoch learns it is stale.
            self.send(
                from,
                NodeMsg::AppendAck {
                    epoch: self.epoch,
                    ok: false,
                    match_seq: Seq(0),
                },
            );
            return;
        }
        if epoch > self.epoch || self.role != Role::Follower || self.coordinator != Some(from) {
            self.follow(now, epoch, from);
        }
        self.last_coordinator_contact = now;
        if self.catch_up_to.is_none() {
            self.catch_up_to = Some(leader_commit);
        }

        let last = self.log.last_seq();
        if prev_seq > last || self.log.epoch_at(prev_seq) != Some(prev_epoch) {
            let hint = if prev_seq > last { last } else { prev_seq - 1 };
            self.send(
                from,
                NodeMsg::AppendAck {
                    epoch: self.epoch,
                    ok: false,
                    match_seq: Seq(hint),
                },
            );
            return;
        }
        let n = entries.len() as u64;
        for e in entries {
            let s = e.seq.0;
            match self.log.epoch_at(s) {
                Some(ep) if s <= self.log.last_seq() => {
                    if ep != e.epoch {
                        assert!(
                            s > self.commit,
                            "a committed entry would be overwritten at seq {s}"
                        );
                        self.log.entries.truncate((s - 1) as usize);
                        self.log.entries.push(e);
                    }
                }
                _ => {
                    assert_eq!(s, self.log.last_seq() + 1, "append must be contiguous");
                    self.log.entries.push(e);
                }
            }
        }
        let matched = prev_seq + n;
        let new_commit = leader_commit.min(matched);
        if new_commit > self.commit {
            self.commit = new_commit;
            self.apply_committed(now);
        }
        self.send(
            from,
            NodeMsg::AppendAck {
                epoch: self.epoch,
                ok: true,
                match_seq: Seq(matched),
            },
        );
    }

    // ------------------------------------------------------------ applying and status

    fn apply_committed(&mut self, now: u64) {
        while self.applied < self.commit {
            self.applied += 1;
            let entry = self
                .log
                .get(self.applied)
                .expect("committed entry exists")
                .clone();
            match &entry.kind {
                EntryKind::View(v) => {
                    self.view = v.clone();
                    self.event(
                        EventKind::ViewChange,
                        None,
                        &[
                            ("view", v.no.0.into()),
                            ("active", nodes_json(v.active())),
                            ("seq", entry.seq.0.into()),
                        ],
                    );
                }
                EntryKind::Inject {
                    k,
                    victim,
                    duration_ms,
                    by_operator,
                } => {
                    if *victim == self.id && self.silenced_until.is_none() {
                        self.silenced_until = Some(now + duration_ms);
                        self.silence_k = Some(*k);
                        self.event(
                            EventKind::InjectStart,
                            None,
                            &[
                                ("k", (*k).into()),
                                ("victim", victim.0.into()),
                                ("duration_ms", (*duration_ms).into()),
                                ("by_operator", (*by_operator).into()),
                            ],
                        );
                        self.out.push(Action::SetSilenced(true));
                    }
                }
                EntryKind::Result { req, .. } => {
                    if self.role == Role::Coordinator {
                        self.event(
                            EventKind::Commit,
                            Some(*req),
                            &[("seq", entry.seq.0.into())],
                        );
                    }
                }
                EntryKind::Noop => {}
            }
            self.out.push(Action::Apply(entry));
        }
        self.update_status();
    }

    fn update_status(&mut self) {
        let current = self.catch_up_to.is_some_and(|w| self.applied >= w);
        let status = if self.view.contains(self.id) && current {
            Status::Active
        } else {
            Status::Syncing
        };
        if status != self.status {
            self.status = status;
            match status {
                Status::Active => self.event(
                    EventKind::Active,
                    None,
                    &[
                        ("applied", self.applied.into()),
                        ("view", self.view.no.0.into()),
                    ],
                ),
                Status::Syncing => self.event(
                    EventKind::SyncStart,
                    None,
                    &[
                        ("last_seq", self.log.last_seq().into()),
                        ("commit", self.commit.into()),
                    ],
                ),
            }
        }
    }

    // ------------------------------------------------------------ executor side

    fn send_proposal(&mut self, now: u64, req: ReqId) {
        let Some(o) = self.outstanding.get_mut(&req) else {
            return;
        };
        match self.coordinator {
            Some(c) if c == self.id && self.role == Role::Coordinator => {
                o.last_sent = Some(now);
                let p = o.proposal.clone();
                self.handle_propose(now, self.id, p);
            }
            Some(c) if c != self.id => {
                o.last_sent = Some(now);
                let p = o.proposal.clone();
                self.send(c, NodeMsg::Propose(p));
            }
            _ => {}
        }
    }

    fn resend_proposals(&mut self, now: u64) {
        let due: Vec<ReqId> = self
            .outstanding
            .iter()
            .filter(|(_, o)| {
                o.last_sent
                    .is_none_or(|t| now >= t + self.cfg.propose_retry_ms)
            })
            .map(|(r, _)| *r)
            .collect();
        for req in due {
            self.send_proposal(now, req);
        }
    }

    fn handle_propose_reply(&mut self, now: u64, req: ReqId, outcome: ProposeOutcome) {
        if !self.outstanding.contains_key(&req) {
            return;
        }
        if let ProposeOutcome::NotCoordinator { hint } = outcome {
            // Not final: retry, at the hinted node if there is one.
            if let (Some(h), Some(o)) = (hint, self.outstanding.get_mut(&req))
                && h != self.id
            {
                o.last_sent = Some(now);
                let p = o.proposal.clone();
                self.send(h, NodeMsg::Propose(p));
            }
            return;
        }
        self.finish_proposal(req, outcome);
    }

    fn finish_proposal(&mut self, req: ReqId, outcome: ProposeOutcome) {
        if self.outstanding.remove(&req).is_some() {
            self.out.push(Action::ProposeDone { req, outcome });
        }
    }

    // ------------------------------------------------------------ fault injection (Section 8)

    fn wake(&mut self, now: u64) {
        self.silenced_until = None;
        self.out.push(Action::SetSilenced(false));
        // `k` matches the event to its `inject_start` (event schema, DL-028).
        let k = self.silence_k.take();
        self.event(EventKind::InjectEnd, None, &[("k", k.into())]);
        if self.role == Role::Coordinator {
            self.step_down(now, "woke from silence");
        }
        self.role = Role::Follower;
        self.coordinator = None;
        self.votes.clear();
        self.candidacy_at = None;
        self.last_coordinator_contact = now;
        // Stay SYNCING until the commit position learned from the current
        // coordinator has been applied.
        self.catch_up_to = None;
        self.update_status();
    }

    fn inject_interval(&self, k: u64) -> u64 {
        self.cfg.inject.as_ref().map_or(u64::MAX / 4, |s| {
            hash::injection_interval_ms(s.seed, k, s.interval_min_ms, s.interval_max_ms)
        })
    }

    /// Single-failure precondition: everyone in view, nobody suspected, no
    /// view change in flight, previous injection over.
    fn injection_refusal(&self, now: u64) -> Option<&'static str> {
        let l = self.leader.as_ref()?;
        if l.log_view.active().len() != 3 {
            Some("not all nodes in view")
        } else if self.suspected.iter().any(|&s| s) {
            Some("a node is suspected")
        } else if self.view_change_pending() {
            Some("view change in progress")
        } else if now < l.inject_busy_until {
            Some("previous injection not over")
        } else {
            None
        }
    }

    fn maybe_inject(&mut self, now: u64) {
        let Some(sched) = self.cfg.inject.clone() else {
            return;
        };
        let Some(l) = self.leader.as_ref() else {
            return;
        };
        if l.inject_paused || now < l.next_inject_at || self.injection_refusal(now).is_some() {
            return;
        }
        let k = l.inject_k;
        let victim = hash::injection_victim(sched.seed, k);
        let duration = hash::injection_duration_ms(
            sched.seed,
            k,
            sched.duration_min_ms,
            sched.duration_max_ms,
        );
        self.inject(now, k, victim, duration, false);
    }

    fn inject(&mut self, now: u64, k: u64, victim: NodeId, duration_ms: u64, by_operator: bool) {
        let next_interval = self.inject_interval(k + 1);
        let l = self.leader.as_mut().expect("coordinator");
        l.inject_busy_until = now + duration_ms + self.cfg.inject_margin_ms;
        l.next_inject_at = now + duration_ms + next_interval;
        self.append(
            now,
            EntryKind::Inject {
                k,
                victim,
                duration_ms,
                by_operator,
            },
        );
    }

    fn handle_silence(&mut self, now: u64, target: SilenceTarget, duration_ms: u64) {
        if self.role != Role::Coordinator {
            match self.coordinator.filter(|&c| c != self.id) {
                Some(c) => self.send(
                    c,
                    NodeMsg::Silence {
                        target,
                        duration_ms,
                    },
                ),
                None => self.event(
                    EventKind::InjectRefused,
                    None,
                    &[("reason", "no coordinator".into())],
                ),
            }
            return;
        }
        let victim = match target {
            SilenceTarget::Node(n) if n.is_valid() => Some(n),
            SilenceTarget::Node(_) => None,
            SilenceTarget::Coordinator => Some(self.id),
            SilenceTarget::CurrentRank1 => self.rank1_hint,
        };
        let refusal = if victim.is_none() {
            Some("unknown target")
        } else if duration_ms == 0 || duration_ms > MAX_SILENCE_MS {
            Some("duration must be 1..=20000 ms")
        } else {
            self.injection_refusal(now)
        };
        match (victim, refusal) {
            (Some(v), None) => {
                let k = self.leader.as_ref().expect("coordinator").inject_k;
                self.inject(now, k, v, duration_ms, true);
            }
            (_, reason) => {
                let reason = reason.unwrap_or("unknown target");
                self.event(EventKind::InjectRefused, None, &[("reason", reason.into())]);
            }
        }
    }
}
