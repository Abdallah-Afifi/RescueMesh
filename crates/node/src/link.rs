//! Application-level acknowledgment and retransmission for one peer
//! (`proto::msg` module docs, design log DL-014, DL-054).
//!
//! TCP success proves nothing here: a silenced node accepts TCP data and
//! drops it inside the process. So every node-to-node message carries a
//! per-peer number `msg_id.n` (1, 2, 3, …) and is kept until the peer's
//! cumulative `ack_up_to` covers it.
//!
//! - **Sending.** New messages go out at once. The oldest unacknowledged
//!   messages are resent after `retransmit_ms`, at most [`LinkCfg::burst`] at
//!   a time, and the interval doubles (up to [`LinkCfg::max_rto_ms`]) while
//!   the peer acknowledges nothing, so a silent peer costs a trickle, not a
//!   flood. Any acknowledgment progress resets the interval.
//! - **Receiving.** A message is delivered once. Numbers already seen are
//!   duplicates and are dropped (and acknowledged again, because the sender
//!   evidently missed the acknowledgment). Messages may be delivered out of
//!   order: the protocol tolerates reordering, and waiting for a gap would
//!   stall live traffic behind a retransmission.
//! - **Acknowledging.** `ack_up_to` is the highest `n` such that every
//!   message up to it has been received. It rides on every outgoing message;
//!   if nothing else is sent within `ack_delay_ms`, a bare `Ack` with
//!   `n = 0` carries it. Bare acks are never numbered, acknowledged or
//!   retransmitted.
//! - **Streams.** Each direction is a stream numbered from 1 and identified
//!   by `msg_id.incarnation`. A receiver that sees a new incarnation starts
//!   counting again. If the peer's process restarts (outside the failure
//!   model; seen as a new heartbeat incarnation), [`Link::peer_restarted`]
//!   starts this side's stream again too.
//! - **Giving up on content.** When the peer leaves the view, or more than
//!   [`LinkCfg::max_bodies`] messages are waiting, the oldest bodies are
//!   replaced by `Ack` fillers that keep their numbers. The peer still
//!   receives every number, so its cumulative acknowledgment can advance,
//!   but stale content (old votes, appends the core will resend anyway) is
//!   not replayed. Fillers are never delivered.
//!
//! Like the protocol modules, this performs no I/O and reads no clock; the
//! transport threads pass the monotonic time in milliseconds.

use proto::{Envelope, MsgId, NodeId, NodeMsg, PROTOCOL_VERSION};
use std::collections::{BTreeSet, VecDeque};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinkCfg {
    pub retransmit_ms: u64,
    pub ack_delay_ms: u64,
    /// Most messages resent in one retransmission round.
    pub burst: usize,
    /// Ceiling of the backed-off retransmission interval.
    pub max_rto_ms: u64,
    /// Bodies kept for retransmission before the oldest become fillers.
    pub max_bodies: usize,
}

impl LinkCfg {
    pub fn from_timeouts(t: &proto::config::Timeouts) -> LinkCfg {
        LinkCfg {
            retransmit_ms: t.retransmit_ms,
            ack_delay_ms: t.ack_delay_ms,
            burst: 256,
            max_rto_ms: (t.retransmit_ms * 16).max(1_000),
            max_bodies: 4_096,
        }
    }
}

/// Counters for the message-overhead measurements.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LinkStats {
    pub sent_new: u64,
    pub retransmitted: u64,
    pub bare_acks: u64,
    pub delivered: u64,
    pub duplicates: u64,
    pub fillers_made: u64,
}

#[derive(Debug, Clone)]
struct Pending {
    n: u64,
    /// `NodeMsg::Ack` for a filler.
    body: NodeMsg,
    sent_at: Option<u64>,
}

/// The reliable link from this node to one peer, and the receive state for
/// messages from it.
#[derive(Debug)]
pub struct Link {
    me: NodeId,
    incarnation: u32,
    cfg: LinkCfg,

    // ---- sending
    next_n: u64,
    /// Unacknowledged messages, ascending `n`. Fillers always form a prefix:
    /// bodies are only ever turned into fillers oldest first.
    unacked: VecDeque<Pending>,
    fillers: usize,
    /// Messages from this `n` on have never been sent.
    unsent_from: u64,
    rto: u64,
    force_retransmit: bool,

    // ---- receiving
    peer_incarnation: Option<u32>,
    recv_up_to: u64,
    seen_above: BTreeSet<u64>,
    ack_owed_since: Option<u64>,

    stats: LinkStats,
}

impl Link {
    pub fn new(me: NodeId, incarnation: u32, cfg: LinkCfg) -> Link {
        Link {
            me,
            incarnation,
            rto: cfg.retransmit_ms,
            cfg,
            next_n: 1,
            unacked: VecDeque::new(),
            fillers: 0,
            unsent_from: 1,
            force_retransmit: false,
            peer_incarnation: None,
            recv_up_to: 0,
            seen_above: BTreeSet::new(),
            ack_owed_since: None,
            stats: LinkStats::default(),
        }
    }

    pub fn stats(&self) -> LinkStats {
        self.stats
    }

    /// Messages waiting for acknowledgment, fillers included.
    pub fn unacked(&self) -> usize {
        self.unacked.len()
    }

    /// Unacknowledged messages that still carry content.
    pub fn bodies(&self) -> usize {
        self.unacked.len() - self.fillers
    }

    /// The highest `n` received in order from the peer.
    pub fn recv_up_to(&self) -> u64 {
        self.recv_up_to
    }

    // ------------------------------------------------------------ sending

    /// Queues a new message; [`Link::poll`] sends it.
    pub fn push(&mut self, body: NodeMsg) {
        let n = self.next_n;
        self.next_n += 1;
        self.unacked.push_back(Pending {
            n,
            body,
            sent_at: None,
        });
        while self.bodies() > self.cfg.max_bodies {
            self.make_filler();
        }
    }

    /// The peer left the view: stop replaying content, keep the numbers.
    pub fn drop_content(&mut self) {
        while self.bodies() > 0 {
            self.make_filler();
        }
    }

    fn make_filler(&mut self) {
        if let Some(p) = self.unacked.get_mut(self.fillers) {
            p.body = NodeMsg::Ack;
            self.fillers += 1;
            self.stats.fillers_made += 1;
        }
    }

    /// A new connection, or this node woke from silence: resend the oldest
    /// unacknowledged messages on the next poll instead of waiting for the
    /// backed-off timer.
    pub fn kick(&mut self) {
        self.force_retransmit = true;
        self.rto = self.cfg.retransmit_ms;
    }

    fn envelope(&self, n: u64, body: NodeMsg) -> Envelope {
        Envelope {
            version: PROTOCOL_VERSION,
            from: self.me,
            msg_id: MsgId {
                incarnation: self.incarnation,
                n,
            },
            ack_up_to: self.recv_up_to,
            body,
        }
    }

    /// Everything that should go out now, in order: retransmissions (oldest
    /// first), then new messages, then a bare ack if one is owed and nothing
    /// else carried it.
    pub fn poll(&mut self, now: u64) -> Vec<Envelope> {
        let mut out = Vec::new();
        let sent_count = self.unacked.len() - (self.next_n - self.unsent_from) as usize;

        let oldest_due = self
            .unacked
            .front()
            .and_then(|p| p.sent_at)
            .is_some_and(|t| now >= t + self.rto);
        if sent_count > 0 && (self.force_retransmit || oldest_due) {
            for i in 0..sent_count.min(self.cfg.burst) {
                let p = &self.unacked[i];
                let env = self.envelope(p.n, p.body.clone());
                out.push(env);
                self.unacked[i].sent_at = Some(now);
                self.stats.retransmitted += 1;
            }
            if !self.force_retransmit {
                self.rto = (self.rto * 2).min(self.cfg.max_rto_ms);
            }
        }
        self.force_retransmit = false;

        for i in sent_count..self.unacked.len() {
            let p = &self.unacked[i];
            let env = self.envelope(p.n, p.body.clone());
            out.push(env);
            self.unacked[i].sent_at = Some(now);
            self.stats.sent_new += 1;
        }
        self.unsent_from = self.next_n;

        if out.is_empty()
            && self
                .ack_owed_since
                .is_some_and(|t| now >= t + self.cfg.ack_delay_ms)
        {
            out.push(self.envelope(0, NodeMsg::Ack));
            self.stats.bare_acks += 1;
        }
        if !out.is_empty() {
            self.ack_owed_since = None;
        }
        out
    }

    /// When [`Link::poll`] next has something to do, if anything.
    pub fn next_due(&self) -> Option<u64> {
        if self.force_retransmit || self.unsent_from < self.next_n {
            return Some(0);
        }
        let retransmit = self
            .unacked
            .front()
            .and_then(|p| p.sent_at)
            .map(|t| t + self.rto);
        let ack = self.ack_owed_since.map(|t| t + self.cfg.ack_delay_ms);
        match (retransmit, ack) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        }
    }

    fn on_ack(&mut self, up_to: u64) {
        // An acknowledgment for numbers never sent refers to an earlier
        // incarnation of this process; ignore it.
        if up_to >= self.unsent_from {
            return;
        }
        let mut progressed = false;
        while self.unacked.front().is_some_and(|p| p.n <= up_to) {
            self.unacked.pop_front();
            self.fillers = self.fillers.saturating_sub(1);
            progressed = true;
        }
        if progressed {
            self.rto = self.cfg.retransmit_ms;
        }
    }

    /// The peer's process restarted (its heartbeat incarnation changed): it
    /// has no record of this stream, so its acknowledgment could never pass
    /// the gap. Start the stream again under a new incarnation; the old
    /// content is stale. Driven by heartbeats, not by stream incarnations,
    /// so two links can never keep restarting each other.
    pub fn peer_restarted(&mut self) {
        self.incarnation = self.incarnation.wrapping_add(1);
        self.unacked.clear();
        self.fillers = 0;
        self.next_n = 1;
        self.unsent_from = 1;
        self.rto = self.cfg.retransmit_ms;
    }

    // ------------------------------------------------------------ receiving

    /// An envelope from the peer. Returns the body to deliver to the core
    /// task, or `None` for acknowledgments, fillers and duplicates.
    pub fn on_receive(&mut self, env: Envelope, now: u64) -> Option<NodeMsg> {
        self.on_ack(env.ack_up_to);
        let n = env.msg_id.n;
        if n == 0 {
            return None; // bare ack
        }
        if self.peer_incarnation != Some(env.msg_id.incarnation) {
            // A new stream from the peer: first contact, or its process
            // restarted. Every stream is numbered from 1.
            self.peer_incarnation = Some(env.msg_id.incarnation);
            self.recv_up_to = 0;
            self.seen_above.clear();
        }
        self.ack_owed_since.get_or_insert(now);
        if n <= self.recv_up_to || self.seen_above.contains(&n) {
            self.stats.duplicates += 1;
            return None;
        }
        if n == self.recv_up_to + 1 {
            self.recv_up_to = n;
            while self.seen_above.remove(&(self.recv_up_to + 1)) {
                self.recv_up_to += 1;
            }
        } else {
            self.seen_above.insert(n);
        }
        if env.body == NodeMsg::Ack {
            return None; // filler
        }
        self.stats.delivered += 1;
        Some(env.body)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proto::{Epoch, Seq};

    fn cfg() -> LinkCfg {
        LinkCfg {
            retransmit_ms: 100,
            ack_delay_ms: 20,
            burst: 4,
            max_rto_ms: 800,
            max_bodies: 1_000,
        }
    }

    fn msg(i: u64) -> NodeMsg {
        NodeMsg::Coordinator { epoch: Epoch(i) }
    }

    fn pair() -> (Link, Link) {
        (
            Link::new(NodeId(1), 11, cfg()),
            Link::new(NodeId(2), 22, cfg()),
        )
    }

    /// Delivers `envs` from one side to the other, returning delivered bodies.
    fn deliver(to: &mut Link, envs: &[Envelope], now: u64) -> Vec<NodeMsg> {
        envs.iter()
            .filter_map(|e| to.on_receive(e.clone(), now))
            .collect()
    }

    #[test]
    fn messages_are_numbered_delivered_once_and_acknowledged_by_piggyback() {
        let (mut a, mut b) = pair();
        for i in 1..=3 {
            a.push(msg(i));
        }
        let envs = a.poll(0);
        assert_eq!(
            envs.iter().map(|e| e.msg_id.n).collect::<Vec<_>>(),
            [1, 2, 3]
        );
        assert_eq!(deliver(&mut b, &envs, 1), [msg(1), msg(2), msg(3)]);
        // Delivered again (a retransmission crossing the ack): dropped.
        assert!(deliver(&mut b, &envs, 2).is_empty());
        assert_eq!(b.stats().duplicates, 3);
        // b's next message carries the acknowledgment.
        b.push(msg(9));
        let back = b.poll(3);
        assert_eq!(back[0].ack_up_to, 3);
        assert_eq!(deliver(&mut a, &back, 4), [msg(9)]);
        assert_eq!(a.unacked(), 0);
    }

    #[test]
    fn a_bare_ack_goes_out_after_the_delay_only_when_nothing_else_does() {
        let (mut a, mut b) = pair();
        a.push(msg(1));
        let envs = a.poll(0);
        deliver(&mut b, &envs, 10);
        assert!(b.poll(29).is_empty());
        assert_eq!(b.next_due(), Some(30));
        let ack = b.poll(30);
        assert_eq!(ack.len(), 1);
        assert_eq!(ack[0].msg_id.n, 0);
        assert_eq!(ack[0].body, NodeMsg::Ack);
        assert_eq!(ack[0].ack_up_to, 1);
        assert!(deliver(&mut a, &ack, 31).is_empty());
        assert_eq!(a.unacked(), 0);
        assert_eq!(b.next_due(), None);
        // A bare ack is not numbered, so it is never acknowledged itself.
        assert_eq!(a.recv_up_to(), 0);
    }

    #[test]
    fn unacknowledged_messages_are_resent_with_backoff_and_a_burst_limit() {
        let (mut a, _) = pair();
        for i in 1..=10 {
            a.push(msg(i));
        }
        assert_eq!(a.poll(0).len(), 10); // all new, no limit
        assert!(a.poll(99).is_empty());
        let r1 = a.poll(100);
        assert_eq!(
            r1.iter().map(|e| e.msg_id.n).collect::<Vec<_>>(),
            [1, 2, 3, 4]
        );
        // No acknowledgment: next round after 200 ms, then 400, capped at 800.
        assert!(a.poll(299).is_empty());
        assert_eq!(a.poll(300).len(), 4);
        assert!(a.poll(699).is_empty());
        assert_eq!(a.poll(700).len(), 4);
        assert_eq!(a.poll(1_500).len(), 4);
        assert!(a.poll(2_299).is_empty());
        assert_eq!(a.poll(2_300).len(), 4);
        // A kick (reconnect, wake) resends at once and resets the interval.
        a.kick();
        assert_eq!(a.poll(2_301).len(), 4);
        assert_eq!(a.poll(2_401).len(), 4);
    }

    #[test]
    fn acknowledgment_progress_resets_the_interval_and_stale_acks_are_ignored() {
        let (mut a, mut b) = pair();
        for i in 1..=6 {
            a.push(msg(i));
        }
        let envs = a.poll(0);
        a.poll(100);
        a.poll(300); // rto now 400
        // b got only the first two.
        deliver(&mut b, &envs[..2], 310);
        let ack = b.poll(400);
        deliver(&mut a, &ack, 401);
        assert_eq!(a.unacked(), 4);
        assert_eq!(a.poll(500).len(), 4, "interval back to 100 ms");
        // An acknowledgment beyond anything sent (an earlier incarnation).
        let mut bogus = ack[0].clone();
        bogus.ack_up_to = 1_000;
        a.on_receive(bogus, 600);
        assert_eq!(a.unacked(), 4);
    }

    #[test]
    fn out_of_order_messages_are_delivered_and_the_ack_waits_for_the_gap() {
        let (mut a, mut b) = pair();
        for i in 1..=4 {
            a.push(msg(i));
        }
        let envs = a.poll(0);
        // Message 2 is lost (b was silenced when it arrived).
        let got = deliver(
            &mut b,
            &[envs[0].clone(), envs[2].clone(), envs[3].clone()],
            1,
        );
        assert_eq!(got, [msg(1), msg(3), msg(4)]);
        assert_eq!(b.recv_up_to(), 1);
        assert_eq!(deliver(&mut b, &[envs[1].clone()], 2), [msg(2)]);
        assert_eq!(b.recv_up_to(), 4);
    }

    #[test]
    fn leaving_the_view_turns_content_into_fillers_that_still_advance_acks() {
        let (mut a, mut b) = pair();
        a.push(msg(1));
        a.push(msg(2));
        a.poll(0); // both lost
        a.drop_content();
        a.push(msg(3)); // a new message after the view change keeps its body
        assert_eq!(a.bodies(), 1);
        let envs = a.poll(0);
        let r = a.poll(100);
        assert_eq!(r.len(), 3);
        assert!(
            r[..2]
                .iter()
                .all(|e| e.body == NodeMsg::Ack && e.msg_id.n > 0)
        );
        assert_eq!(r[2].body, msg(3));
        let mut got = deliver(&mut b, &envs, 101);
        got.extend(deliver(&mut b, &r, 102));
        assert_eq!(got, [msg(3)], "fillers are not delivered");
        assert_eq!(b.recv_up_to(), 3);
        let ack = b.poll(200);
        deliver(&mut a, &ack, 201);
        assert_eq!(a.unacked(), 0);
    }

    #[test]
    fn the_body_cap_makes_the_oldest_into_fillers() {
        let mut a = Link::new(
            NodeId(1),
            1,
            LinkCfg {
                max_bodies: 3,
                ..cfg()
            },
        );
        for i in 1..=5 {
            a.push(msg(i));
        }
        assert_eq!(a.unacked(), 5);
        assert_eq!(a.bodies(), 3);
        let envs = a.poll(0);
        assert_eq!(envs[0].body, NodeMsg::Ack);
        assert_eq!(envs[1].body, NodeMsg::Ack);
        assert_eq!(envs[2].body, msg(3));
        assert_eq!(a.stats().fillers_made, 2);
    }

    #[test]
    fn a_restarted_peer_starts_a_new_stream() {
        let (mut a, mut b) = pair();
        for i in 1..=3 {
            a.push(msg(i));
        }
        let envs = a.poll(0);
        deliver(&mut b, &envs, 1);
        // a restarts with a new incarnation and numbers from 1 again.
        let mut a2 = Link::new(NodeId(1), 12, cfg());
        a2.push(msg(7));
        let envs = a2.poll(10);
        assert_eq!(deliver(&mut b, &envs, 11), [msg(7)]);
        assert_eq!(b.recv_up_to(), 1);
    }

    #[test]
    fn a_lost_first_message_is_still_recovered() {
        let (mut a, mut b) = pair();
        for i in 1..=3 {
            a.push(msg(i));
        }
        let envs = a.poll(0);
        // Message 1 is lost; b's first contact is message 2.
        assert_eq!(deliver(&mut b, &envs[1..], 1), [msg(2), msg(3)]);
        assert_eq!(b.recv_up_to(), 0);
        let r = a.poll(100);
        assert_eq!(deliver(&mut b, &r, 101), [msg(1)]);
        assert_eq!(b.recv_up_to(), 3);
    }

    #[test]
    fn when_the_peer_restarts_this_side_restarts_its_stream() {
        let (mut a, mut b) = pair();
        b.push(msg(1));
        deliver(&mut a, &b.poll(0), 1);
        for i in 1..=3 {
            a.push(msg(i));
        }
        a.poll(2); // lost: b is restarting
        // b comes back as a new process and talks first.
        let mut b2 = Link::new(NodeId(2), 23, cfg());
        b2.push(msg(5));
        assert_eq!(deliver(&mut a, &b2.poll(10), 11), [msg(5)]);
        assert_eq!(a.unacked(), 3, "a stream change alone restarts nothing");
        a.peer_restarted(); // the runtime saw b's new heartbeat incarnation
        assert_eq!(a.unacked(), 0, "stale stream dropped");
        a.push(msg(6));
        let envs = a.poll(12);
        assert_eq!(envs[0].msg_id.n, 1);
        assert_eq!(deliver(&mut b2, &envs, 13), [msg(6)]);
        assert_eq!(b2.recv_up_to(), 1);
    }

    #[test]
    fn many_messages_over_a_lossy_link_all_arrive_exactly_once() {
        let c = LinkCfg { burst: 64, ..cfg() };
        let (mut a, mut b) = (
            Link::new(NodeId(1), 11, c.clone()),
            Link::new(NodeId(2), 22, c),
        );
        let mut x: u64 = 0x9e37_79b9_7f4a_7c15;
        let mut lossy = |envs: Vec<Envelope>| -> Vec<Envelope> {
            envs.into_iter()
                .filter(|_| {
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    x % 10 >= 3 // 30% loss
                })
                .collect()
        };
        let mut got = Vec::new();
        for now in 0..20_000u64 {
            if now < 5_000 && now % 5 == 0 {
                a.push(NodeMsg::Coordinator {
                    epoch: Epoch(now / 5),
                });
            }
            let to_b = lossy(a.poll(now));
            got.extend(deliver(&mut b, &to_b, now));
            let to_a = lossy(b.poll(now));
            deliver(&mut a, &to_a, now);
        }
        assert_eq!(got.len(), 1_000);
        let mut epochs: Vec<u64> = got
            .iter()
            .map(|m| match m {
                NodeMsg::Coordinator { epoch } => epoch.0,
                _ => unreachable!(),
            })
            .collect();
        epochs.sort_unstable();
        epochs.dedup();
        assert_eq!(epochs.len(), 1_000, "no duplicates delivered");
        assert_eq!(a.unacked(), 0);
        let _ = Seq(0);
    }
}
