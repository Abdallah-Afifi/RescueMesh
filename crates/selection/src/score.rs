//! The MWBE load score (protocol Section 5.1). Lower is better.
//!
//! ```text
//! S = w_q · min(q / Q_MAX, 1)        queue length, plus claims seen since the last heartbeat
//!   + w_c · c                        CPU utilisation
//!   + w_s · min(s / S_MAX, 1)        moving average of recent service time
//!   + w_f · f                        fraction of the node's heartbeats this node missed
//!   + w_r · min(rtt / R_MAX, 1)      client-measured round-trip time to the node
//! ```
//!
//! Every value is an integer in thousandths (0..=1000), and the weights are
//! thousandths that sum to 1000, so the score itself is in 0..=1000. Nothing
//! here uses floating point, so all three nodes compute the identical score
//! and claim key from the same inputs (design log DL-023).

use proto::config::ScoreCfg;
use proto::{ClaimKey, ClientReq, NodeId, PeerScore, hash};

/// One whole, in thousandths.
pub const FULL: u16 = 1000;

/// Value of the RTT term when the client has not yet measured its round trip
/// to a node. Treated as the cap: a node the client has never heard back from
/// is ranked as if it were the furthest away.
pub const UNKNOWN_RTT: u16 = FULL;

/// The five terms of the score, each capped and in thousandths.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Terms {
    pub queue: u16,
    pub cpu: u16,
    pub service: u16,
    pub missed: u16,
    pub rtt: u16,
}

/// `value / cap` in thousandths, capped at 1000. A zero cap counts as full.
fn ratio(value: u64, cap: u64) -> u16 {
    if cap == 0 {
        return FULL;
    }
    let r = value.saturating_mul(u64::from(FULL)) / cap;
    u16::try_from(r.min(u64::from(FULL))).expect("capped at 1000")
}

/// The capped terms for one node. `rtt_us` is the client's measurement of its
/// round trip to that node, taken from the request.
pub fn terms(cfg: &ScoreCfg, peer: &PeerScore, rtt_us: Option<u32>) -> Terms {
    // Local correction (Section 5.1): claims this node has seen from the peer
    // since the peer's last heartbeat count as extra queue length.
    let queue = u64::from(peer.load.queue_len) + u64::from(peer.claims_since_heartbeat);
    Terms {
        queue: ratio(queue, u64::from(cfg.queue_cap)),
        cpu: peer.load.cpu_permille.min(FULL),
        service: ratio(
            u64::from(peer.load.service_ewma_us),
            u64::from(cfg.service_cap_ms) * 1000,
        ),
        missed: peer.missed_heartbeats_permille.min(FULL),
        rtt: match rtt_us {
            Some(us) => ratio(u64::from(us), u64::from(cfg.rtt_cap_ms) * 1000),
            None => UNKNOWN_RTT,
        },
    }
}

/// Weighted sum of the terms, in thousandths (0..=1000).
pub fn combine(cfg: &ScoreCfg, t: Terms) -> u16 {
    let sum = u64::from(cfg.w_queue) * u64::from(t.queue)
        + u64::from(cfg.w_cpu) * u64::from(t.cpu)
        + u64::from(cfg.w_service) * u64::from(t.service)
        + u64::from(cfg.w_missed) * u64::from(t.missed)
        + u64::from(cfg.w_rtt) * u64::from(t.rtt);
    // The weights sum to 1000 (checked by `Config::validate`), so this is at
    // most 1000; the `min` only guards against an unvalidated config.
    u16::try_from((sum / u64::from(FULL)).min(u64::from(FULL))).expect("capped at 1000")
}

/// Rounds a score down to whole quantization steps. With the default quantum
/// of 50 thousandths, scores 0.200 to 0.249 are all step 4, so small
/// measurement noise does not reorder nodes that are effectively equal.
pub fn quantize(cfg: &ScoreCfg, score: u16) -> u16 {
    score / cfg.quantum_permille.max(1)
}

/// The score of `peer` for request `req`, in thousandths.
pub fn score(cfg: &ScoreCfg, req: &ClientReq, peer: &PeerScore) -> u16 {
    let rtt = req.rtt_us.get(peer.node.index()).copied().flatten();
    combine(cfg, terms(cfg, peer, rtt))
}

/// The ranking key of `peer` for `req`: quantized score, then the hash
/// tie-break, then the node id. Lower wins. A claim carries this key, and two
/// competing claims are resolved by comparing their keys (Section 5.3, step 5).
pub fn claim_key(cfg: &ScoreCfg, req: &ClientReq, peer: &PeerScore) -> ClaimKey {
    ClaimKey {
        score_step: quantize(cfg, score(cfg, req, peer)),
        tie: hash::tie_break(&req.req.id, peer.node),
        node: peer.node,
    }
}

/// What a node is assumed to report before its first heartbeat arrives: idle.
pub fn idle(node: NodeId) -> PeerScore {
    PeerScore {
        node,
        load: Default::default(),
        claims_since_heartbeat: 0,
        missed_heartbeats_permille: 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::selector::tests::{load, req_with_rtt};

    fn peer(node: u8, queue: u32, cpu: u16, service_ms: u32) -> PeerScore {
        PeerScore {
            node: NodeId(node),
            load: load(queue, cpu, service_ms),
            claims_since_heartbeat: 0,
            missed_heartbeats_permille: 0,
        }
    }

    /// The worked example used in the team's explanation of MWBE.
    #[test]
    fn worked_example() {
        // Equal weights of 200 on all five terms, as when the example was worked.
        let cfg = ScoreCfg {
            w_queue: 200,
            w_cpu: 200,
            w_service: 200,
            w_missed: 200,
            w_rtt: 200,
            // The cap the example was worked with (the default is now 50 ms,
            // DL-070).
            service_cap_ms: 500,
            ..ScoreCfg::default()
        };
        let req = req_with_rtt(1, [Some(25_000), Some(10_000), Some(25_000)]);
        let n1 = peer(1, 16, 500, 250);
        let n2 = peer(2, 32, 800, 250);
        let n3 = peer(3, 0, 250, 125);
        assert_eq!(score(&cfg, &req, &n1), 350);
        assert_eq!(score(&cfg, &req, &n2), 400);
        assert_eq!(score(&cfg, &req, &n3), 200);
        assert_eq!(claim_key(&cfg, &req, &n1).score_step, 7);
        assert_eq!(claim_key(&cfg, &req, &n2).score_step, 8);
        assert_eq!(claim_key(&cfg, &req, &n3).score_step, 4);
    }

    #[test]
    fn every_term_is_capped() {
        let cfg = ScoreCfg::default();
        let mut p = peer(1, 10_000, 5_000, 100_000);
        p.missed_heartbeats_permille = 9_999;
        let t = terms(&cfg, &p, Some(u32::MAX));
        assert_eq!(
            t,
            Terms {
                queue: 1000,
                cpu: 1000,
                service: 1000,
                missed: 1000,
                rtt: 1000
            }
        );
        assert_eq!(combine(&cfg, t), 1000);
    }

    #[test]
    fn claims_since_heartbeat_count_as_queue() {
        let cfg = ScoreCfg::default();
        let mut p = peer(1, 16, 0, 0);
        let before = terms(&cfg, &p, None).queue;
        p.claims_since_heartbeat = 16;
        let after = terms(&cfg, &p, None).queue;
        assert_eq!((before, after), (250, 500));
    }

    #[test]
    fn unknown_rtt_counts_as_the_cap() {
        let cfg = ScoreCfg::default();
        let p = peer(1, 0, 0, 0);
        assert_eq!(terms(&cfg, &p, None).rtt, 1000);
        assert_eq!(terms(&cfg, &p, Some(0)).rtt, 0);
    }

    #[test]
    fn quantization_groups_nearby_scores() {
        let cfg = ScoreCfg::default();
        assert_eq!(quantize(&cfg, 200), 4);
        assert_eq!(quantize(&cfg, 249), 4);
        assert_eq!(quantize(&cfg, 250), 5);
        assert_eq!(quantize(&cfg, 1000), 20);
    }
}
