//! Ranking policies (protocol Sections 5.1 and 10).
//!
//! Every policy implements [`proto::Ranker`] and must be a pure function of
//! its arguments, so all nodes compute the same ranking from the same inputs.
//! Policies that look random draw from [`proto::hash`] of the request, never
//! from a random-number generator, and round-robin derives its position from
//! the request ID, not from a counter (design log DL-023).
//!
//! | Policy | Ranks by |
//! |---|---|
//! | `mwbe-cached` | MWBE key from heartbeat scores plus the local correction (primary) |
//! | `mwbe-bids` | the same key; only the source of the scores differs, see [`make_ranker`] |
//! | `random` | a per-request random permutation |
//! | `hash` | a fixed node per request ID, the others in ring order after it |
//! | `round-robin` | each client cycles through the nodes, each from its own offset |
//! | `two-choices` | the less loaded of two random nodes, then the other, then the rest |
//! | `highest-id` | always the highest node ID: no balancing (plain Bully) |

use crate::score::{self, idle};
use proto::config::{RankerKind, ScoreCfg};
use proto::{ClaimKey, ClientId, ClientReq, NodeId, PeerScore, Ranker, ReqId, View, hash};

/// Builds the ranker selected in the configuration (`policy.ranker`).
///
/// `MwbeBids` ranks with the same key as `MwbeCached`. The difference is in
/// where the scores come from: for fresh bids the runtime collects a bid from
/// every view member before calling [`Ranker::rank`], instead of using the
/// last heartbeat. That collection round is optional (protocol Section 10).
pub fn make_ranker(kind: RankerKind, cfg: ScoreCfg) -> Box<dyn Ranker> {
    match kind {
        RankerKind::MwbeCached => Box::new(Mwbe {
            cfg,
            name: "mwbe-cached",
        }),
        RankerKind::MwbeBids => Box::new(Mwbe {
            cfg,
            name: "mwbe-bids",
        }),
        RankerKind::Random => Box::new(Random),
        RankerKind::Hash => Box::new(HashRanker),
        RankerKind::RoundRobin => Box::new(RoundRobin),
        RankerKind::TwoChoices => Box::new(TwoChoices { cfg }),
        RankerKind::HighestId => Box::new(HighestId),
    }
}

/// The score entry for `node`, or an idle entry if no heartbeat has arrived.
fn peer_of(scores: &[PeerScore], node: NodeId) -> PeerScore {
    scores
        .iter()
        .copied()
        .find(|p| p.node == node)
        .unwrap_or_else(|| idle(node))
}

/// The view's members in ring order starting at position `start`.
fn rotate(view: &View, start: u64) -> Vec<NodeId> {
    let members = view.active();
    let n = members.len();
    if n == 0 {
        return Vec::new();
    }
    let start = usize::try_from(start % n as u64).expect("below n");
    members[start..]
        .iter()
        .chain(&members[..start])
        .copied()
        .collect()
}

/// The view's members in a per-request random order.
fn shuffled(req: &ReqId, view: &View) -> Vec<NodeId> {
    let mut order = view.active().to_vec();
    order.sort_by_key(|&n| (hash::tie_break(req, n), n));
    order
}

/// The MWBE key of every view member, best first.
fn by_key(cfg: &ScoreCfg, req: &ClientReq, view: &View, scores: &[PeerScore]) -> Vec<NodeId> {
    let mut keys: Vec<ClaimKey> = view
        .active()
        .iter()
        .map(|&n| score::claim_key(cfg, req, &peer_of(scores, n)))
        .collect();
    keys.sort();
    keys.into_iter().map(|k| k.node).collect()
}

/// Metric-Weighted Bid Election: lowest quantized score first, ties broken by
/// `hash(reqID, nodeID)` so that an idle cluster spreads requests instead of
/// sending them all to one node (design log DL-008).
pub struct Mwbe {
    pub cfg: ScoreCfg,
    pub name: &'static str,
}

impl Ranker for Mwbe {
    fn name(&self) -> &'static str {
        self.name
    }

    fn rank(&self, req: &ClientReq, view: &View, scores: &[PeerScore]) -> Vec<NodeId> {
        by_key(&self.cfg, req, view, scores)
    }
}

/// A random permutation per request, drawn from the request hash.
pub struct Random;

impl Ranker for Random {
    fn name(&self) -> &'static str {
        "random"
    }

    fn rank(&self, req: &ClientReq, view: &View, _: &[PeerScore]) -> Vec<NodeId> {
        shuffled(&req.req.id, view)
    }
}

/// `hash(reqID) mod n` picks the first node; the rest follow in ring order.
pub struct HashRanker;

impl Ranker for HashRanker {
    fn name(&self) -> &'static str {
        "hash"
    }

    fn rank(&self, req: &ClientReq, view: &View, _: &[PeerScore]) -> Vec<NodeId> {
        rotate(view, hash::request(&req.req.id))
    }
}

/// Each client incarnation cycles through the nodes in turn. Its starting
/// offset is a hash of the client, so different clients do not all start on
/// the same node. The position comes from the request ID, not a counter, so
/// every node computes the same answer.
pub struct RoundRobin;

impl Ranker for RoundRobin {
    fn name(&self) -> &'static str {
        "round-robin"
    }

    fn rank(&self, req: &ClientReq, view: &View, _: &[PeerScore]) -> Vec<NodeId> {
        let id = req.req.id;
        let offset = client_offset(id.client, id.incarnation);
        rotate(view, offset.wrapping_add(id.seq))
    }
}

fn client_offset(client: ClientId, incarnation: u32) -> u64 {
    hash::request(&ReqId {
        client,
        incarnation,
        seq: 0,
    })
}

/// "Power of two choices": two nodes drawn at random per request, the one with
/// the lower MWBE key first, then the other, then the remaining node(s).
pub struct TwoChoices {
    pub cfg: ScoreCfg,
}

impl Ranker for TwoChoices {
    fn name(&self) -> &'static str {
        "two-choices"
    }

    fn rank(&self, req: &ClientReq, view: &View, scores: &[PeerScore]) -> Vec<NodeId> {
        let mut order = shuffled(&req.req.id, view);
        if order.len() >= 2 {
            let key = |n: NodeId| score::claim_key(&self.cfg, req, &peer_of(scores, n));
            if key(order[1]) < key(order[0]) {
                order.swap(0, 1);
            }
        }
        order
    }
}

/// Always the highest node ID: the "no balancing" reference, which is what
/// plain Bully election would do.
pub struct HighestId;

impl Ranker for HighestId {
    fn name(&self) -> &'static str {
        "highest-id"
    }

    fn rank(&self, _: &ClientReq, view: &View, _: &[PeerScore]) -> Vec<NodeId> {
        view.active().iter().rev().copied().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::selector::tests::{load, req, req_from};
    use proto::ViewNo;

    const ALL_KINDS: [RankerKind; 7] = [
        RankerKind::MwbeCached,
        RankerKind::MwbeBids,
        RankerKind::Random,
        RankerKind::Hash,
        RankerKind::RoundRobin,
        RankerKind::TwoChoices,
        RankerKind::HighestId,
    ];

    fn view(nodes: &[u8]) -> View {
        View::new(ViewNo(1), nodes.iter().map(|&n| NodeId(n)).collect())
    }

    fn idle_scores() -> Vec<PeerScore> {
        NodeId::ALL.iter().map(|&n| idle(n)).collect()
    }

    /// Every policy must return each view member exactly once, and nothing else.
    #[test]
    fn every_policy_returns_a_permutation_of_the_view() {
        let views = [view(&[1, 2, 3]), view(&[1, 3]), view(&[2]), view(&[])];
        for kind in ALL_KINDS {
            let r = make_ranker(kind, ScoreCfg::default());
            for v in &views {
                for seq in 0..200 {
                    let mut got = r.rank(&req(seq), v, &idle_scores());
                    got.sort();
                    assert_eq!(got, v.active(), "{} on {:?}", r.name(), v.active());
                }
            }
        }
    }

    /// The same inputs always give the same ranking, on any node.
    #[test]
    fn rankings_are_deterministic() {
        let scores = vec![
            PeerScore {
                node: NodeId(1),
                load: load(5, 300, 40),
                claims_since_heartbeat: 2,
                missed_heartbeats_permille: 0,
            },
            PeerScore {
                node: NodeId(2),
                load: load(9, 700, 80),
                claims_since_heartbeat: 0,
                missed_heartbeats_permille: 100,
            },
        ];
        for kind in ALL_KINDS {
            let a = make_ranker(kind, ScoreCfg::default());
            let b = make_ranker(kind, ScoreCfg::default());
            for seq in 0..200 {
                assert_eq!(
                    a.rank(&req(seq), &view(&[1, 2, 3]), &scores),
                    b.rank(&req(seq), &view(&[1, 2, 3]), &scores),
                );
            }
        }
    }

    /// With every node idle, the hash tie-break spreads first choices across
    /// all three nodes instead of always picking one (design log DL-008).
    #[test]
    fn idle_cluster_spreads_requests() {
        for kind in [
            RankerKind::MwbeCached,
            RankerKind::Random,
            RankerKind::Hash,
            RankerKind::RoundRobin,
            RankerKind::TwoChoices,
        ] {
            let r = make_ranker(kind, ScoreCfg::default());
            let mut count = [0u32; 3];
            for seq in 0..3000 {
                let first = r.rank(&req(seq), &view(&[1, 2, 3]), &idle_scores())[0];
                count[first.index()] += 1;
            }
            for c in count {
                assert!((750..=1250).contains(&c), "{}: {count:?}", r.name());
            }
        }
    }

    #[test]
    fn highest_id_never_balances() {
        let r = make_ranker(RankerKind::HighestId, ScoreCfg::default());
        for seq in 0..100 {
            assert_eq!(r.rank(&req(seq), &view(&[1, 2, 3]), &[])[0], NodeId(3));
        }
        assert_eq!(r.rank(&req(0), &view(&[1, 2]), &[])[0], NodeId(2));
    }

    #[test]
    fn mwbe_prefers_the_least_loaded_node() {
        let r = make_ranker(RankerKind::MwbeCached, ScoreCfg::default());
        let scores = vec![
            PeerScore {
                node: NodeId(1),
                load: load(40, 900, 300),
                claims_since_heartbeat: 0,
                missed_heartbeats_permille: 0,
            },
            PeerScore {
                node: NodeId(2),
                load: load(2, 100, 20),
                claims_since_heartbeat: 0,
                missed_heartbeats_permille: 0,
            },
            PeerScore {
                node: NodeId(3),
                load: load(20, 500, 150),
                claims_since_heartbeat: 0,
                missed_heartbeats_permille: 0,
            },
        ];
        for seq in 0..100 {
            assert_eq!(
                r.rank(&req(seq), &view(&[1, 2, 3]), &scores),
                vec![NodeId(2), NodeId(3), NodeId(1)]
            );
        }
    }

    /// One client's consecutive requests visit every node in turn.
    #[test]
    fn round_robin_cycles_per_client() {
        let r = make_ranker(RankerKind::RoundRobin, ScoreCfg::default());
        for user in 0..20 {
            let firsts: Vec<NodeId> = (0..3)
                .map(|seq| r.rank(&req_from(user, seq), &view(&[1, 2, 3]), &[])[0])
                .collect();
            let mut sorted = firsts.clone();
            sorted.sort();
            assert_eq!(sorted, NodeId::ALL, "user {user}: {firsts:?}");
        }
    }

    /// Two-choices never puts the more loaded of its two picks first.
    #[test]
    fn two_choices_takes_the_less_loaded_of_two() {
        let cfg = ScoreCfg::default();
        let r = make_ranker(RankerKind::TwoChoices, cfg.clone());
        let busy = PeerScore {
            node: NodeId(1),
            load: load(60, 1000, 500),
            claims_since_heartbeat: 0,
            missed_heartbeats_permille: 0,
        };
        let scores = vec![busy, idle(NodeId(2)), idle(NodeId(3))];
        for seq in 0..500 {
            assert_ne!(r.rank(&req(seq), &view(&[1, 2, 3]), &scores)[0], NodeId(1));
        }
    }
}
