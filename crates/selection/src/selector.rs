//! The request table and the claim logic (protocol Sections 5.2 to 5.5).
//!
//! For every request ID a node has seen, the table holds one row:
//!
//! | State | Meaning | On a client retry |
//! |---|---|---|
//! | SEEN | received, no valid claim yet | rank again |
//! | CLAIMED(by, view) | a view member is executing it | do nothing |
//! | COMMITTED(seq) | its result is in the applied log | return the stored result |
//!
//! The flow for one request:
//!
//! 1. Every ACTIVE node ranks the view members ([`Selector::on_request`]).
//! 2. The node that ranks itself first claims at once: it broadcasts `CLAIM`
//!    and starts planning.
//! 3. Every other node waits for a claim. If none arrives from the node it is
//!    waiting on within the claim timeout `T_c`, it moves to the next node in
//!    its ranking, and claims when that is itself ([`Selector::tick`]).
//! 4. When two claims for one request meet, the lower claim key wins
//!    everywhere; a node whose own claim loses aborts its execution
//!    ([`Selector::on_claim`]). Every node keeps every valid claim it has
//!    received, so all nodes agree on the winner once all claims are
//!    delivered, whatever order they arrive in.
//! 5. A CLAIMED request moves to another node only when its claimer leaves
//!    the view, never because time has passed (Section 5.4,
//!    [`Selector::set_view`]). It moves to the lowest remaining claim if
//!    there is one, so survivors agree on the successor without new
//!    messages; only when no claim is left is the request ranked again.
//!    A slow executor keeps its request.
//!
//! A claim is valid if its sender has been in the view continuously since
//! the view number the claim carries (design log DL-046). A claim made just
//! before a view change by a node that stays in the view is therefore still
//! honoured, while a claim from a node that left the view, even if it was
//! later re-admitted, is not. The coordinator's view check at commit
//! (invariant I5, DL-035) remains the final guard.
//!
//! The coordinator's duplicate check (invariant I1) makes any execution that
//! slips through harmless: only one result per request is ever committed.

use crate::rankers::make_ranker;
use crate::score::{self, idle};
use proto::config::{RankerKind, ScoreCfg, Timeouts};
use proto::{
    ClaimKey, ClientId, ClientReq, LoadSample, NodeId, OpResult, PeerScore, Ranker, ReqId, Seq,
    View, ViewNo,
};
use std::collections::BTreeMap;

/// What the runtime must do after a call. See the table in the crate docs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    /// A ranking was computed; log it as the `ranked` event.
    Ranked {
        req: ReqId,
        rank: Vec<NodeId>,
        policy: &'static str,
    },
    /// This node claims the request: broadcast `NodeMsg::Claim` to both peers.
    Claim {
        req: ReqId,
        view: ViewNo,
        key: ClaimKey,
    },
    /// Plan this request on the worker pool and propose the result.
    Execute(ClientReq),
    /// This node's claim was beaten, or this node left the view: stop
    /// planning the request if it has not been proposed yet.
    Abort(ReqId),
    /// A retry of a committed request: send the stored result to the client.
    Reply { req: ReqId, result: OpResult },
}

/// A row's state, for tests, logs and the operator tool.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowState {
    Seen,
    Claimed(NodeId),
    Committed(Seq),
}

/// Counters for the measurements this workstream reports (protocol Section 10).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Stats {
    /// Rankings computed.
    pub ranked: u64,
    /// Claims this node made.
    pub claims_made: u64,
    /// Claims this node made because the nodes ranked before it sent none
    /// within the claim timeout.
    pub timeout_claims: u64,
    /// Claims this node made that a lower key then beat.
    pub claims_lost: u64,
    /// Times two different claims for one request met at this node.
    pub competing_claims: u64,
    /// Claimed requests returned to SEEN because the claimer left the view.
    pub takeovers: u64,
    /// Claims ignored because their sender was not continuously in the view
    /// since the view the claim carries.
    pub stale_claims: u64,
    /// Client retries answered from a committed result.
    pub retries_answered: u64,
}

enum State {
    /// Waiting for a claim from `order[waiting]` until `deadline`. An empty
    /// `order` means this node is not ranking (not ACTIVE, or never received
    /// the request itself).
    Seen {
        order: Vec<NodeId>,
        waiting: usize,
        deadline: u64,
    },
    Claimed {
        by: NodeId,
    },
    Committed {
        seq: Seq,
        result: OpResult,
    },
}

const NOT_RANKED: State = State::Seen {
    order: Vec::new(),
    waiting: 0,
    deadline: u64::MAX,
};

struct Row {
    /// The request itself, needed to rank it again. `None` if this node has
    /// only seen a peer's claim for it, or after it committed.
    req: Option<ClientReq>,
    state: State,
    /// Every valid claim received for this request, by claimer: the view it
    /// was made in and its key. The lowest key is the executor.
    claims: BTreeMap<NodeId, (ViewNo, ClaimKey)>,
}

impl Row {
    fn new(req: Option<ClientReq>) -> Row {
        Row {
            req,
            state: NOT_RANKED,
            claims: BTreeMap::new(),
        }
    }

    fn winner(&self) -> Option<(NodeId, ClaimKey)> {
        self.claims
            .iter()
            .map(|(&n, &(_, k))| (n, k))
            .min_by_key(|&(_, k)| k)
    }
}

/// Executor selection for one cloud node. Performs no I/O and reads no clock.
pub struct Selector {
    me: NodeId,
    cfg: ScoreCfg,
    ranker: Box<dyn Ranker>,
    claim_timeout_min_ms: u64,
    claim_timeout_rtt_multiple: u64,
    claim_timeout_ms: u64,
    view: View,
    active: bool,
    peers: BTreeMap<NodeId, PeerScore>,
    rows: BTreeMap<ReqId, Row>,
    /// For each view member, the view number from which it has been a
    /// member without interruption.
    member_since: BTreeMap<NodeId, ViewNo>,
    /// Highest `acked_up_to` seen per client incarnation, so a late duplicate
    /// of an acknowledged request is not executed again after its row is gone.
    acked: BTreeMap<(ClientId, u32), u64>,
    stats: Stats,
}

impl Selector {
    /// A selector for node `me` using the configured policy. It starts with
    /// an empty view and inactive; the runtime calls [`Selector::set_view`]
    /// with the core's view and status before passing any request.
    pub fn new(me: NodeId, policy: RankerKind, score: ScoreCfg, timeouts: &Timeouts) -> Self {
        let min = timeouts.claim_timeout_min_ms;
        Selector {
            me,
            ranker: make_ranker(policy, score.clone()),
            cfg: score,
            claim_timeout_min_ms: min,
            claim_timeout_rtt_multiple: u64::from(timeouts.claim_timeout_rtt_multiple),
            claim_timeout_ms: min,
            view: View::new(ViewNo(0), Vec::new()),
            active: false,
            peers: BTreeMap::new(),
            rows: BTreeMap::new(),
            member_since: BTreeMap::new(),
            acked: BTreeMap::new(),
            stats: Stats::default(),
        }
    }

    // ------------------------------------------------------------ inputs

    /// A heartbeat arrived from `node`, or this node sent its own (pass
    /// `me`). The claims counted since that node's previous heartbeat are now
    /// included in its reported queue, so the local correction restarts.
    pub fn on_heartbeat(&mut self, node: NodeId, load: LoadSample) {
        let p = self.peers.entry(node).or_insert_with(|| idle(node));
        p.load = load;
        p.claims_since_heartbeat = 0;
    }

    /// The failure detector's share of `node`'s recent heartbeats that were
    /// missed, in thousandths: the reliability term of the score.
    pub fn set_missed(&mut self, node: NodeId, missed_permille: u16) {
        self.peers
            .entry(node)
            .or_insert_with(|| idle(node))
            .missed_heartbeats_permille = missed_permille.min(score::FULL);
    }

    /// The measured round trip between cloud nodes. `T_c` is this times
    /// `timeouts.claim_timeout_rtt_multiple`, and at least
    /// `timeouts.claim_timeout_min_ms` (design log DL-024).
    pub fn set_peer_rtt_ms(&mut self, rtt_ms: u64) {
        self.claim_timeout_ms = self
            .claim_timeout_min_ms
            .max(rtt_ms.saturating_mul(self.claim_timeout_rtt_multiple));
    }

    /// The core applied a new view, or its ACTIVE status changed
    /// (`Core::is_active`). Claims by nodes that left the view are dropped:
    /// the lowest remaining claim becomes the executor, and a request with no
    /// claim left is ranked again over the new view (Section 5.4). Requests
    /// still waiting for a claim are ranked again too. Calling it with an
    /// unchanged view and status does nothing.
    pub fn set_view(&mut self, now: u64, view: View, active: bool) -> Vec<Action> {
        if view == self.view && active == self.active {
            return Vec::new();
        }
        for &n in view.active() {
            if !self.view.contains(n) || !self.member_since.contains_key(&n) {
                self.member_since.insert(n, view.no);
            }
        }
        self.member_since.retain(|n, _| view.contains(*n));
        self.view = view;
        self.active = active;

        let mut out = Vec::new();
        let ids: Vec<ReqId> = self.rows.keys().copied().collect();
        for id in ids {
            let Some(row) = self.rows.get_mut(&id) else {
                continue;
            };
            let held = match &row.state {
                State::Committed { .. } => continue,
                State::Claimed { by, .. } => Some(*by),
                State::Seen { .. } => None,
            };
            let (view, member_since) = (&self.view, &self.member_since);
            row.claims
                .retain(|&n, &mut (v, _)| claim_valid(view, member_since, n, v));
            match (held, row.winner()) {
                (None, _) => out.extend(self.rank(now, id)),
                (Some(by), Some((w, _))) if w != by => {
                    // The executor left; the lowest remaining claim takes over.
                    self.stats.takeovers += 1;
                    row.state = State::Claimed { by: w };
                    if by == self.me {
                        out.push(Action::Abort(id));
                    }
                    if w == self.me
                        && let Some(req) = row.req.clone()
                    {
                        out.push(Action::Execute(req));
                    }
                }
                (Some(_), Some(_)) => {}
                (Some(by), None) => {
                    self.stats.takeovers += 1;
                    if by == self.me {
                        out.push(Action::Abort(id));
                    }
                    out.extend(self.rank(now, id));
                }
            }
        }
        out
    }

    /// A client request arrived (Section 5.3, step 1, and Section 5.5).
    ///
    /// Only state-changing operations come here. Queries are answered by the
    /// runtime from applied state (`StateMachine::query`, design log DL-019).
    pub fn on_request(&mut self, now: u64, req: ClientReq) -> Vec<Action> {
        let id = req.req.id;
        let acked = self.acked.entry((id.client, id.incarnation)).or_insert(0);
        if req.acked_up_to > *acked {
            *acked = req.acked_up_to;
            self.forget_acked(id.client, id.incarnation, req.acked_up_to);
        }
        if id.seq <= self.acked[&(id.client, id.incarnation)] {
            // The client already holds the reply to this one.
            return Vec::new();
        }
        match self.rows.get_mut(&id) {
            Some(row) => match &row.state {
                State::Committed { result, .. } => {
                    self.stats.retries_answered += 1;
                    return vec![Action::Reply {
                        req: id,
                        result: result.clone(),
                    }];
                }
                State::Claimed { .. } => {
                    // Someone in the view is executing it. Keep the request
                    // in case that node leaves the view and we take over.
                    row.req.get_or_insert(req);
                    return Vec::new();
                }
                State::Seen { .. } => row.req = Some(req),
            },
            None => {
                self.rows.insert(id, Row::new(Some(req)));
            }
        }
        self.rank(now, id)
    }

    /// A peer's `CLAIM` arrived (Section 5.3, steps 3 and 5).
    pub fn on_claim(
        &mut self,
        from: NodeId,
        id: ReqId,
        view: ViewNo,
        key: ClaimKey,
    ) -> Vec<Action> {
        if !claim_valid(&self.view, &self.member_since, from, view) {
            self.stats.stale_claims += 1;
            return Vec::new();
        }
        let row = self.rows.entry(id).or_insert_with(|| Row::new(None));
        if matches!(row.state, State::Committed { .. }) {
            return Vec::new();
        }
        if row.claims.get(&from) == Some(&(view, key)) {
            return Vec::new(); // the same claim delivered twice
        }
        let first_from_sender = row.claims.insert(from, (view, key)).is_none();
        if row.claims.len() > 1 && first_from_sender {
            self.stats.competing_claims += 1;
        }
        let held = match row.state {
            State::Claimed { by, .. } => Some(by),
            _ => None,
        };
        let (w, _) = row.winner().expect("a claim was just stored");
        row.state = State::Claimed { by: w };
        if first_from_sender {
            // Local correction: the claimer is busier than its last heartbeat said.
            self.bump_claims(from);
        }
        if held == Some(self.me) && w != self.me {
            self.stats.claims_lost += 1;
            vec![Action::Abort(id)]
        } else {
            Vec::new()
        }
    }

    /// The core applied a `Result` entry. Retries of this request are now
    /// answered with the stored result, and nobody executes it again.
    pub fn on_committed(&mut self, id: ReqId, seq: Seq, result: OpResult) {
        let row = self.rows.entry(id).or_insert_with(|| Row::new(None));
        row.req = None;
        row.claims.clear();
        row.state = State::Committed { seq, result };
    }

    /// Advances claim timeouts (Section 5.3, step 4). Call every few
    /// milliseconds.
    pub fn tick(&mut self, now: u64) -> Vec<Action> {
        let due: Vec<ReqId> = self
            .rows
            .iter()
            .filter(|(_, r)| matches!(r.state, State::Seen { deadline, .. } if deadline <= now))
            .map(|(id, _)| *id)
            .collect();
        let mut out = Vec::new();
        for id in due {
            let Some(row) = self.rows.get_mut(&id) else {
                continue;
            };
            let State::Seen {
                order,
                waiting,
                deadline,
            } = &mut row.state
            else {
                continue;
            };
            *waiting += 1;
            match order.get(*waiting) {
                Some(&n) if n == self.me => {
                    self.stats.timeout_claims += 1;
                    out.extend(self.claim(id));
                }
                Some(_) => *deadline = now.saturating_add(self.claim_timeout_ms),
                None => *deadline = u64::MAX,
            }
        }
        out
    }

    // ------------------------------------------------------------ queries

    /// The node an idle request would go to right now: the view member with
    /// the lowest load score, ignoring client RTT, ties to the lower ID. The
    /// runtime passes it to `Core::set_rank1_hint` so the operator can
    /// silence "the current rank-1 server" in the failure demo.
    pub fn rank1_hint(&self) -> Option<NodeId> {
        self.view.active().iter().copied().min_by_key(|&n| {
            let s = score::combine(&self.cfg, score::terms(&self.cfg, &self.peer(n), None));
            (score::quantize(&self.cfg, s), n)
        })
    }

    pub fn row_state(&self, id: &ReqId) -> Option<RowState> {
        self.rows.get(id).map(|r| match &r.state {
            State::Seen { .. } => RowState::Seen,
            State::Claimed { by, .. } => RowState::Claimed(*by),
            State::Committed { seq, .. } => RowState::Committed(*seq),
        })
    }

    pub fn stats(&self) -> Stats {
        self.stats
    }

    pub fn policy(&self) -> &'static str {
        self.ranker.name()
    }

    pub fn claim_timeout_ms(&self) -> u64 {
        self.claim_timeout_ms
    }

    /// Rows currently held; for the memory and queue traces.
    pub fn table_len(&self) -> usize {
        self.rows.len()
    }

    // ------------------------------------------------------------ internals

    fn peer(&self, n: NodeId) -> PeerScore {
        self.peers.get(&n).copied().unwrap_or_else(|| idle(n))
    }

    fn scores(&self) -> Vec<PeerScore> {
        self.view.active().iter().map(|&n| self.peer(n)).collect()
    }

    fn bump_claims(&mut self, n: NodeId) {
        let p = self.peers.entry(n).or_insert_with(|| idle(n));
        p.claims_since_heartbeat = p.claims_since_heartbeat.saturating_add(1);
    }

    /// Ranks row `id` and claims it if this node comes first. Leaves the row
    /// unranked if this node may not execute or never received the request.
    fn rank(&mut self, now: u64, id: ReqId) -> Vec<Action> {
        let may_execute = self.active && self.view.contains(self.me);
        let req = self.rows.get(&id).and_then(|r| r.req.clone());
        let (true, Some(req)) = (may_execute, req) else {
            if let Some(row) = self.rows.get_mut(&id) {
                row.state = NOT_RANKED;
            }
            return Vec::new();
        };
        let order = self.ranker.rank(&req, &self.view, &self.scores());
        self.stats.ranked += 1;
        let mut out = vec![Action::Ranked {
            req: id,
            rank: order.clone(),
            policy: self.ranker.name(),
        }];
        if order.first() == Some(&self.me) {
            out.extend(self.claim(id));
        } else if let Some(row) = self.rows.get_mut(&id) {
            row.state = State::Seen {
                order,
                waiting: 0,
                deadline: now.saturating_add(self.claim_timeout_ms),
            };
        }
        out
    }

    fn claim(&mut self, id: ReqId) -> Vec<Action> {
        let Some(req) = self.rows.get(&id).and_then(|r| r.req.clone()) else {
            return Vec::new();
        };
        let key = score::claim_key(&self.cfg, &req, &self.peer(self.me));
        let view = self.view.no;
        if let Some(row) = self.rows.get_mut(&id) {
            row.claims.insert(self.me, (view, key));
            row.state = State::Claimed { by: self.me };
        }
        self.bump_claims(self.me);
        self.stats.claims_made += 1;
        vec![Action::Claim { req: id, view, key }, Action::Execute(req)]
    }

    /// Drops the rows of one client incarnation at or below `acked_up_to`
    /// (Section 5.2): the client has their replies.
    fn forget_acked(&mut self, client: ClientId, incarnation: u32, acked_up_to: u64) {
        if acked_up_to == 0 {
            return;
        }
        let lo = ReqId {
            client,
            incarnation,
            seq: 0,
        };
        let hi = ReqId {
            client,
            incarnation,
            seq: acked_up_to,
        };
        let done: Vec<ReqId> = self.rows.range(lo..=hi).map(|(id, _)| *id).collect();
        for id in done {
            self.rows.remove(&id);
        }
    }
}

/// A claim from `from` made in view `claimed_in` is valid if `from` has been
/// a member continuously since that view (design log DL-046). A claim from a
/// view this node has not applied yet is kept; it is checked again when that
/// view is applied.
fn claim_valid(
    view: &View,
    member_since: &BTreeMap<NodeId, ViewNo>,
    from: NodeId,
    claimed_in: ViewNo,
) -> bool {
    if claimed_in > view.no {
        return true;
    }
    view.contains(from)
        && member_since
            .get(&from)
            .is_some_and(|&since| claimed_in >= since)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use proto::{Category, Op, PROTOCOL_VERSION, Request, Severity, ZoneId};

    // ---------------------------------------------------------- helpers

    pub(crate) fn load(queue: u32, cpu: u16, service_ms: u32) -> LoadSample {
        LoadSample {
            queue_len: queue,
            cpu_permille: cpu,
            service_ewma_us: service_ms * 1000,
        }
    }

    pub(crate) fn req_with_rtt(seq: u64, rtt_us: [Option<u32>; 3]) -> ClientReq {
        ClientReq {
            version: PROTOCOL_VERSION,
            req: Request {
                id: ReqId {
                    client: ClientId {
                        machine: 1,
                        process: 1,
                        user: 1,
                    },
                    incarnation: 7,
                    seq,
                },
                sent_at_ms: 0,
                op: Op::ReportIncident {
                    category: Category::Flooding,
                    zone: ZoneId(1),
                    severity: Severity(3),
                    ttl_ms: 60_000,
                    attachment: None,
                },
            },
            rtt_us,
            acked_up_to: 0,
        }
    }

    /// A request whose client measured the same RTT to every node, so RTT
    /// does not affect the ranking.
    pub(crate) fn req(seq: u64) -> ClientReq {
        req_with_rtt(seq, [Some(1_000); 3])
    }

    pub(crate) fn req_from(user: u32, seq: u64) -> ClientReq {
        let mut r = req(seq);
        r.req.id.client.user = user;
        r
    }

    fn full_view() -> View {
        View::new(ViewNo(1), NodeId::ALL.to_vec())
    }

    fn node(me: u8, policy: RankerKind) -> Selector {
        let mut s = Selector::new(
            NodeId(me),
            policy,
            ScoreCfg::default(),
            &Timeouts::default(),
        );
        s.set_view(0, full_view(), true);
        s
    }

    fn cluster() -> Vec<Selector> {
        (1..=3).map(|n| node(n, RankerKind::MwbeCached)).collect()
    }

    fn claim_of(actions: &[Action]) -> Option<(ReqId, ViewNo, ClaimKey)> {
        actions.iter().find_map(|a| match a {
            Action::Claim { req, view, key } => Some((*req, *view, *key)),
            _ => None,
        })
    }

    fn ranking(actions: &[Action]) -> Vec<NodeId> {
        actions
            .iter()
            .find_map(|a| match a {
                Action::Ranked { rank, .. } => Some(rank.clone()),
                _ => None,
            })
            .expect("ranked")
    }

    // ---------------------------------------------------------- claiming

    #[test]
    fn only_the_first_ranked_node_claims() {
        let mut c = cluster();
        let r = req(1);
        let outs: Vec<Vec<Action>> = c.iter_mut().map(|s| s.on_request(0, r.clone())).collect();
        let order = ranking(&outs[0]);
        // All three nodes compute the same ranking.
        for o in &outs {
            assert_eq!(ranking(o), order);
        }
        for (i, o) in outs.iter().enumerate() {
            let me = NodeId(u8::try_from(i).unwrap() + 1);
            let claimed = claim_of(o).is_some();
            assert_eq!(claimed, me == order[0], "node {me}");
            assert_eq!(o.iter().any(|a| matches!(a, Action::Execute(_))), claimed);
        }
    }

    #[test]
    fn the_next_node_claims_after_the_claim_timeout() {
        let mut c = cluster();
        let r = req(1);
        let order = ranking(&c[0].on_request(0, r.clone()));
        let second = order[1];
        let third = order[2];
        let s2 = &mut c[second.index()];
        s2.on_request(0, r.clone());
        let tc = s2.claim_timeout_ms();
        assert!(s2.tick(tc - 1).is_empty(), "too early");
        assert_eq!(claim_of(&s2.tick(tc)).map(|c| c.0), Some(r.req.id));
        assert_eq!(s2.stats().timeout_claims, 1);

        // The third node waits one more timeout for the second.
        let s3 = &mut c[third.index()];
        s3.on_request(0, r.clone());
        assert!(claim_of(&s3.tick(tc)).is_none());
        assert!(claim_of(&s3.tick(2 * tc - 1)).is_none());
        assert!(claim_of(&s3.tick(2 * tc)).is_some());
    }

    #[test]
    fn a_claim_stops_the_waiting() {
        let mut c = cluster();
        let r = req(1);
        let outs: Vec<Vec<Action>> = c.iter_mut().map(|s| s.on_request(0, r.clone())).collect();
        let winner = ranking(&outs[0])[0];
        let (id, view, key) = claim_of(&outs[winner.index()]).unwrap();
        for s in c.iter_mut().filter(|s| s.me != winner) {
            s.on_claim(winner, id, view, key);
            assert_eq!(s.row_state(&id), Some(RowState::Claimed(winner)));
            // Time alone never moves a claimed request (Section 5.4).
            assert!(s.tick(1_000_000).is_empty());
        }
    }

    #[test]
    fn competing_claims_resolve_to_the_lower_key() {
        let mut c = cluster();
        let r = req(1);
        let outs: Vec<Vec<Action>> = c.iter_mut().map(|s| s.on_request(0, r.clone())).collect();
        let order = ranking(&outs[0]);
        let (first, second) = (order[0], order[1]);
        let first_claim = claim_of(&outs[first.index()]).unwrap();

        // The second node times out before the first node's claim reaches it.
        let tc = c[second.index()].claim_timeout_ms();
        let second_claim = claim_of(&c[second.index()].tick(tc)).unwrap();
        assert!(first_claim.2 < second_claim.2, "rank order follows the key");

        // Each sees the other's claim: the second aborts, the first carries on.
        let a = c[second.index()].on_claim(first, first_claim.0, first_claim.1, first_claim.2);
        assert_eq!(a, vec![Action::Abort(r.req.id)]);
        let b = c[first.index()].on_claim(second, second_claim.0, second_claim.1, second_claim.2);
        assert!(b.is_empty());
        for s in &c {
            if s.me != order[2] {
                assert_eq!(s.row_state(&r.req.id), Some(RowState::Claimed(first)));
            }
        }
        assert_eq!(c[second.index()].stats().claims_lost, 1);
    }

    fn key_of(n: u8) -> ClaimKey {
        ClaimKey {
            score_step: 0,
            tie: 0,
            node: NodeId(n),
        }
    }

    #[test]
    fn a_claim_from_a_member_that_never_left_survives_a_view_change() {
        // View 1 to view 2 removes node 3; node 2 stays a member throughout.
        // Its claim made in view 1, arriving after view 2 is applied, counts.
        let mut s = node(1, RankerKind::MwbeCached);
        s.set_view(0, full_view().without(NodeId(3)), true);
        let id = req(1).req.id;
        assert!(s.on_claim(NodeId(2), id, ViewNo(1), key_of(2)).is_empty());
        assert_eq!(s.row_state(&id), Some(RowState::Claimed(NodeId(2))));
        assert_eq!(s.stats().stale_claims, 0);
    }

    #[test]
    fn claims_from_before_a_node_left_the_view_are_ignored() {
        // Node 2 is removed in view 2 and re-admitted in view 3: its claims
        // from views 1 and 2 are stale, its claims from view 3 count.
        let mut s = node(1, RankerKind::MwbeCached);
        s.set_view(0, full_view().without(NodeId(2)), true);
        let id = req(1).req.id;
        assert!(s.on_claim(NodeId(2), id, ViewNo(2), key_of(2)).is_empty());
        s.set_view(0, View::new(ViewNo(3), NodeId::ALL.to_vec()), true);
        assert!(s.on_claim(NodeId(2), id, ViewNo(1), key_of(2)).is_empty());
        assert!(s.on_claim(NodeId(2), id, ViewNo(2), key_of(2)).is_empty());
        assert_eq!(s.row_state(&id), None);
        assert_eq!(s.stats().stale_claims, 3);
        s.on_claim(NodeId(2), id, ViewNo(3), key_of(2));
        assert_eq!(s.row_state(&id), Some(RowState::Claimed(NodeId(2))));
    }

    #[test]
    fn a_claim_from_a_view_not_yet_applied_is_checked_when_it_is() {
        // Node 3 claims in view 2, which this node has not applied yet.
        let mut s = node(1, RankerKind::MwbeCached);
        let id = req(1).req.id;
        s.on_claim(NodeId(3), id, ViewNo(2), key_of(3));
        assert_eq!(s.row_state(&id), Some(RowState::Claimed(NodeId(3))));
        // View 2 turns out not to contain node 3: the claim is dropped and
        // the request ranked again (here: never received, so left unranked).
        s.set_view(0, View::new(ViewNo(2), vec![NodeId(1), NodeId(2)]), true);
        assert_eq!(s.row_state(&id), Some(RowState::Seen));
    }

    // ---------------------------------------------------------- view changes

    #[test]
    fn a_claim_is_taken_over_when_the_claimer_leaves_the_view() {
        let mut c = cluster();
        let r = req(1);
        let outs: Vec<Vec<Action>> = c.iter_mut().map(|s| s.on_request(0, r.clone())).collect();
        let winner = ranking(&outs[0])[0];
        let (id, view, key) = claim_of(&outs[winner.index()]).unwrap();
        for s in c.iter_mut() {
            if s.me != winner {
                s.on_claim(winner, id, view, key);
            }
        }

        // The coordinator removes the claimer from the view.
        let new_view = full_view().without(winner);
        let mut claimers = Vec::new();
        for s in c.iter_mut().filter(|s| s.me != winner) {
            let out = s.set_view(10, new_view.clone(), true);
            if claim_of(&out).is_some() {
                claimers.push(s.me);
            }
            assert_eq!(s.stats().takeovers, 1);
        }
        assert_eq!(claimers.len(), 1, "exactly one survivor takes over");

        // The removed node, if it hears about its removal, aborts.
        let out = c[winner.index()].set_view(10, new_view, false);
        assert!(out.contains(&Action::Abort(id)));
    }

    #[test]
    fn survivors_agree_on_the_successor_whatever_order_they_learn_things() {
        // X claims with the lowest key; Y and Z hold it. X is removed. Y
        // applies the new view first, takes over and claims; Z still holds
        // X's lower key when Y's claim arrives, and only then applies the
        // view. Both must end with Y as the single executor.
        let mut c = cluster();
        let r = req(1);
        let outs: Vec<Vec<Action>> = c.iter_mut().map(|s| s.on_request(0, r.clone())).collect();
        let order = ranking(&outs[0]);
        let (x, y, z) = (order[0], order[1], order[2]);
        let (id, view, kx) = claim_of(&outs[x.index()]).unwrap();
        c[y.index()].on_claim(x, id, view, kx);
        c[z.index()].on_claim(x, id, view, kx);

        let v2 = full_view().without(x);
        let out_y = c[y.index()].set_view(10, v2.clone(), true);
        let (_, vy, ky) = claim_of(&out_y).expect("y ranks first among survivors and claims");
        assert!(out_y.iter().any(|a| matches!(a, Action::Execute(_))));
        assert!(c[z.index()].on_claim(y, id, vy, ky).is_empty());
        let out_z = c[z.index()].set_view(10, v2, true);
        assert!(claim_of(&out_z).is_none(), "z must not claim: {out_z:?}");
        assert!(!out_z.iter().any(|a| matches!(a, Action::Execute(_))));
        assert_eq!(c[z.index()].row_state(&id), Some(RowState::Claimed(y)));
        assert_eq!(c[y.index()].row_state(&id), Some(RowState::Claimed(y)));
        // No timeout ever makes z claim it either.
        assert!(c[z.index()].tick(1_000_000).is_empty());
    }

    #[test]
    fn a_node_whose_losing_claim_is_left_resumes_executing() {
        // Y's claim lost to X's lower key, so Y aborted. When X leaves the
        // view, Y's stored claim is the lowest remaining: Y executes again
        // without sending anything, and Z, holding the same two claims,
        // agrees.
        let mut c = cluster();
        let r = req(1);
        let outs: Vec<Vec<Action>> = c.iter_mut().map(|s| s.on_request(0, r.clone())).collect();
        let order = ranking(&outs[0]);
        let (x, y, z) = (order[0], order[1], order[2]);
        let (id, view, kx) = claim_of(&outs[x.index()]).unwrap();
        let tc = c[y.index()].claim_timeout_ms();
        let (_, vy, ky) = claim_of(&c[y.index()].tick(tc)).unwrap();
        assert_eq!(
            c[y.index()].on_claim(x, id, view, kx),
            vec![Action::Abort(id)]
        );
        c[z.index()].on_claim(x, id, view, kx);
        c[z.index()].on_claim(y, id, vy, ky);

        let v2 = full_view().without(x);
        let out_y = c[y.index()].set_view(20, v2.clone(), true);
        assert!(
            out_y.iter().any(|a| matches!(a, Action::Execute(_))),
            "{out_y:?}"
        );
        assert!(claim_of(&out_y).is_none(), "its claim is already known");
        let out_z = c[z.index()].set_view(20, v2, true);
        assert!(out_z.is_empty(), "{out_z:?}");
        assert_eq!(c[z.index()].row_state(&id), Some(RowState::Claimed(y)));
    }

    #[test]
    fn by_default_missed_heartbeats_seen_by_one_node_do_not_change_its_ranking() {
        // Only node 1 sees node 3 miss heartbeats. With the default weights
        // every node must still rank every request identically (DL-047).
        let mut c = cluster();
        for (i, s) in c.iter_mut().enumerate() {
            for n in NodeId::ALL {
                let busy = u32::from(n.0) * 5;
                s.on_heartbeat(n, load(busy, 300 + u16::from(n.0) * 40, busy));
            }
            if i == 0 {
                s.set_missed(NodeId(3), 1000);
            }
        }
        for seq in 1..=500 {
            let r = req_with_rtt(seq, [Some(4_000), Some(9_000), Some(2_000)]);
            let rankings: Vec<Vec<NodeId>> = c
                .iter()
                .map(|s| s.ranker.rank(&r, &s.view, &s.scores()))
                .collect();
            assert_eq!(rankings[0], rankings[1], "request {seq}");
            assert_eq!(rankings[1], rankings[2], "request {seq}");
        }
    }

    #[test]
    fn a_duplicate_claim_is_counted_once_in_the_local_correction() {
        let mut s = node(1, RankerKind::MwbeCached);
        let id = req(1).req.id;
        let before = s.peer(NodeId(2)).claims_since_heartbeat;
        s.on_claim(NodeId(2), id, ViewNo(1), key_of(2));
        s.on_claim(NodeId(2), id, ViewNo(1), key_of(2));
        assert_eq!(s.peer(NodeId(2)).claims_since_heartbeat, before + 1);
        assert_eq!(s.stats().competing_claims, 0);
    }

    #[test]
    fn a_late_duplicate_of_an_acknowledged_request_is_not_executed() {
        let mut s = node(3, RankerKind::HighestId);
        assert!(claim_of(&s.on_request(0, req(1))).is_some());
        let mut next = req(2);
        next.acked_up_to = 1; // the client has the reply to seq 1
        s.on_request(1, next);
        assert_eq!(s.row_state(&req(1).req.id), None, "row forgotten");
        // A delayed copy of seq 1, carrying the old acked_up_to of 0, arrives.
        assert!(s.on_request(2, req(1)).is_empty());
    }

    #[test]
    fn an_inactive_node_never_claims_until_it_is_active() {
        let mut s = Selector::new(
            NodeId(3),
            RankerKind::HighestId,
            ScoreCfg::default(),
            &Timeouts::default(),
        );
        s.set_view(0, full_view(), false); // SYNCING
        assert!(s.on_request(0, req(1)).is_empty());
        assert!(s.tick(1_000).is_empty());
        let out = s.set_view(1_000, full_view(), true);
        assert!(
            claim_of(&out).is_some(),
            "ranks pending requests once active"
        );
    }

    // ---------------------------------------------------------- retries

    #[test]
    fn retries_follow_the_request_table() {
        let mut s = node(3, RankerKind::HighestId);
        let r = req(1);
        assert!(claim_of(&s.on_request(0, r.clone())).is_some());
        // CLAIMED: a retry does nothing.
        assert!(s.on_request(5, r.clone()).is_empty());
        // COMMITTED: a retry gets the stored result.
        s.on_committed(r.req.id, Seq(4), OpResult::Accepted);
        assert_eq!(
            s.on_request(10, r.clone()),
            vec![Action::Reply {
                req: r.req.id,
                result: OpResult::Accepted
            }]
        );
        assert_eq!(s.stats().retries_answered, 1);
    }

    #[test]
    fn a_retry_of_a_seen_request_is_ranked_again() {
        let mut s = node(1, RankerKind::HighestId); // node 3 is always first
        assert!(claim_of(&s.on_request(0, req(1))).is_none());
        let out = s.on_request(5, req(1));
        assert_eq!(ranking(&out)[0], NodeId(3));
    }

    #[test]
    fn acknowledged_rows_are_forgotten() {
        let mut s = node(3, RankerKind::HighestId);
        for seq in 1..=5 {
            s.on_request(0, req(seq));
            s.on_committed(req(seq).req.id, Seq(seq), OpResult::Accepted);
        }
        let mut next = req(6);
        next.acked_up_to = 4;
        s.on_request(0, next);
        assert_eq!(s.row_state(&req(4).req.id), None);
        assert!(s.row_state(&req(5).req.id).is_some());
        assert_eq!(s.table_len(), 2);
        // A stray retry of an acknowledged request is ignored.
        let mut old = req(3);
        old.acked_up_to = 4;
        assert!(s.on_request(0, old).is_empty());
    }

    // ---------------------------------------------------------- load

    /// Between two heartbeats, a node that keeps winning looks busier with
    /// every claim, so the work does not all go to it (design log DL-008).
    #[test]
    fn local_correction_spreads_work_between_heartbeats() {
        let mut c = cluster();
        let reports = [load(0, 0, 0), load(0, 250, 0), load(0, 250, 0)];
        for s in c.iter_mut() {
            for (i, l) in reports.iter().enumerate() {
                s.on_heartbeat(NodeId::ALL[i], *l);
            }
        }
        let mut won = [0u32; 3];
        for seq in 1..=60 {
            let r = req(seq);
            let outs: Vec<Vec<Action>> = c.iter_mut().map(|s| s.on_request(0, r.clone())).collect();
            let winner = ranking(&outs[0])[0];
            won[winner.index()] += 1;
            let (id, view, key) = claim_of(&outs[winner.index()]).unwrap();
            for s in c.iter_mut().filter(|s| s.me != winner) {
                s.on_claim(winner, id, view, key);
            }
        }
        assert!(won[0] >= 16, "the idle node still gets most: {won:?}");
        assert!(won[1] > 0 && won[2] > 0, "but not all of it: {won:?}");
    }

    #[test]
    fn rank1_hint_is_the_least_loaded_member() {
        let mut s = node(1, RankerKind::MwbeCached);
        s.on_heartbeat(NodeId(1), load(30, 900, 200));
        s.on_heartbeat(NodeId(2), load(0, 100, 10));
        s.on_heartbeat(NodeId(3), load(10, 500, 100));
        assert_eq!(s.rank1_hint(), Some(NodeId(2)));
        s.set_view(0, full_view().without(NodeId(2)), true);
        assert_eq!(s.rank1_hint(), Some(NodeId(3)));
    }

    #[test]
    fn claim_timeout_follows_the_peer_rtt() {
        let mut s = node(1, RankerKind::MwbeCached);
        assert_eq!(s.claim_timeout_ms(), 20);
        s.set_peer_rtt_ms(4);
        assert_eq!(s.claim_timeout_ms(), 20, "never below the minimum");
        s.set_peer_rtt_ms(15);
        assert_eq!(s.claim_timeout_ms(), 45);
    }

    // ---------------------------------------------------------- simulation

    /// A claim in flight: (deliver at, to, from, claim).
    type InFlight = (u64, usize, NodeId, (ReqId, ViewNo, ClaimKey));

    /// Small deterministic generator for the simulation (no external crates).
    struct Lcg(u64);
    impl Lcg {
        fn below(&mut self, n: u64) -> u64 {
            self.0 = self
                .0
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            (self.0 >> 33) % n
        }
    }

    /// Three nodes, requests arriving at slightly different times, claims
    /// delayed by up to twice the claim timeout so that timeout claims and
    /// competing claims happen. Once everything is delivered, all three nodes
    /// must agree on one executor per request, and that executor must be the
    /// only node that did not abort.
    #[test]
    fn three_nodes_agree_on_one_executor_per_request() {
        for policy in [
            RankerKind::MwbeCached,
            RankerKind::TwoChoices,
            RankerKind::RoundRobin,
            RankerKind::HighestId,
        ] {
            for seed in 1..=20u64 {
                let mut rng = Lcg(seed);
                let mut c: Vec<Selector> = (1..=3).map(|n| node(n, policy)).collect();
                // (deliver_at, to, from, claim)
                let mut wire: Vec<InFlight> = Vec::new();
                let mut executing: BTreeMap<ReqId, Vec<NodeId>> = BTreeMap::new();
                let mut arrivals: Vec<(u64, usize, ClientReq)> = Vec::new();
                for seq in 1..=200u64 {
                    let t = seq * 2;
                    let r = req_from(u32::try_from(rng.below(5)).unwrap(), seq);
                    for to in 0..3 {
                        arrivals.push((t + rng.below(4), to, r.clone()));
                    }
                }
                let mut handle = |now: u64,
                                  me: usize,
                                  out: Vec<Action>,
                                  wire: &mut Vec<_>,
                                  rng: &mut Lcg| {
                    for a in out {
                        match a {
                            Action::Claim { req, view, key } => {
                                for to in (0..3).filter(|&t| t != me) {
                                    let delay = 1 + rng.below(40);
                                    wire.push((now + delay, to, NodeId::ALL[me], (req, view, key)));
                                }
                            }
                            Action::Execute(r) => {
                                executing.entry(r.req.id).or_default().push(NodeId::ALL[me]);
                            }
                            Action::Abort(id) => {
                                executing
                                    .get_mut(&id)
                                    .expect("aborting a running request")
                                    .retain(|&n| n != NodeId::ALL[me]);
                            }
                            _ => {}
                        }
                    }
                };
                for now in 0..=1_000u64 {
                    for (_, to, r) in arrivals.iter().filter(|(t, _, _)| *t == now) {
                        let out = c[*to].on_request(now, r.clone());
                        handle(now, *to, out, &mut wire, &mut rng);
                    }
                    let due: Vec<_> = wire.iter().filter(|m| m.0 == now).cloned().collect();
                    wire.retain(|m| m.0 != now);
                    for (_, to, from, (id, view, key)) in due {
                        let out = c[to].on_claim(from, id, view, key);
                        handle(now, to, out, &mut wire, &mut rng);
                    }
                    for (me, s) in c.iter_mut().enumerate() {
                        let out = s.tick(now);
                        handle(now, me, out, &mut wire, &mut rng);
                    }
                }
                assert!(wire.is_empty());
                for (id, running) in &executing {
                    assert_eq!(
                        running.len(),
                        1,
                        "{policy:?} seed {seed}: {id} ran on {running:?}"
                    );
                    for s in &c {
                        assert_eq!(s.row_state(id), Some(RowState::Claimed(running[0])));
                    }
                }
                assert_eq!(executing.len(), 200);
            }
        }
    }
}
