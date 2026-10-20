//! Hashes that every node must compute identically.
//!
//! `std`'s `DefaultHasher` is not guaranteed to be stable across Rust
//! versions or platforms, so it must not be used for anything that nodes
//! compare. These functions use 64-bit FNV-1a over an explicit little-endian
//! encoding of their inputs, and are pinned by the tests below.

use crate::ids::{NodeId, ReqId};

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

pub fn fnv1a(bytes: &[u8]) -> u64 {
    let mut h = FNV_OFFSET;
    for &b in bytes {
        h ^= u64::from(b);
        h = h.wrapping_mul(FNV_PRIME);
    }
    h
}

/// FNV-1a followed by a 64-bit finalizer (from SplitMix64) so that inputs
/// differing in one field spread over the whole range.
fn mix(bytes: &[u8]) -> u64 {
    let mut z = fnv1a(bytes);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

fn req_bytes(req: &ReqId, out: &mut Vec<u8>) {
    out.extend_from_slice(&req.client.machine.to_le_bytes());
    out.extend_from_slice(&req.client.process.to_le_bytes());
    out.extend_from_slice(&req.client.user.to_le_bytes());
    out.extend_from_slice(&req.incarnation.to_le_bytes());
    out.extend_from_slice(&req.seq.to_le_bytes());
}

/// Hash of a request alone. Used by the hash, random and two-choices
/// baselines as their source of randomness.
pub fn request(req: &ReqId) -> u64 {
    let mut b = Vec::with_capacity(20);
    req_bytes(req, &mut b);
    mix(&b)
}

/// Tie-break between nodes with equal quantized scores (Section 5.1).
/// Lower wins.
pub fn tie_break(req: &ReqId, node: NodeId) -> u64 {
    let mut b = Vec::with_capacity(21);
    req_bytes(req, &mut b);
    b.push(node.0);
    mix(&b)
}

/// Draw number `salt` for injection `k` of the run with `seed`.
fn injection_draw(seed: u64, k: u64, salt: u8) -> u64 {
    let mut b = Vec::with_capacity(17);
    b.extend_from_slice(&seed.to_le_bytes());
    b.extend_from_slice(&k.to_le_bytes());
    b.push(salt);
    mix(&b)
}

/// Victim of injection `k` (Section 8).
pub fn injection_victim(seed: u64, k: u64) -> NodeId {
    let i = injection_draw(seed, k, 0) % 3;
    NodeId(u8::try_from(i).expect("i < 3") + 1)
}

/// Duration of injection `k`, uniform in `min_ms..=max_ms`.
pub fn injection_duration_ms(seed: u64, k: u64, min_ms: u64, max_ms: u64) -> u64 {
    assert!(min_ms <= max_ms);
    min_ms + injection_draw(seed, k, 1) % (max_ms - min_ms + 1)
}

/// Delay before injection `k`, uniform in `min_ms..=max_ms`.
pub fn injection_interval_ms(seed: u64, k: u64, min_ms: u64, max_ms: u64) -> u64 {
    assert!(min_ms <= max_ms);
    min_ms + injection_draw(seed, k, 2) % (max_ms - min_ms + 1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::ClientId;

    fn rid(seq: u64) -> ReqId {
        ReqId {
            client: ClientId {
                machine: 1,
                process: 2,
                user: 3,
            },
            incarnation: 0xdead_beef,
            seq,
        }
    }

    #[test]
    fn fnv1a_reference_values() {
        // Published FNV-1a 64-bit test vectors.
        assert_eq!(fnv1a(b""), 0xcbf2_9ce4_8422_2325);
        assert_eq!(fnv1a(b"a"), 0xaf63_dc4c_8601_ec8c);
        assert_eq!(fnv1a(b"foobar"), 0x8594_4171_f739_67e8);
    }

    #[test]
    fn hashes_are_pinned() {
        // If these change, nodes built from different commits would disagree.
        // Update them only together with PROTOCOL_VERSION.
        let now = [
            tie_break(&rid(7), NodeId(1)),
            tie_break(&rid(7), NodeId(2)),
            request(&rid(7)),
        ];
        assert_eq!(
            now,
            [
                872_998_127_811_016_634,
                5_752_222_816_421_574_914,
                11_613_004_960_083_260_597
            ]
        );
    }

    #[test]
    fn tie_break_spreads_wins_evenly() {
        let mut wins = [0u32; 3];
        for s in 0..30_000 {
            let r = rid(s);
            let best = NodeId::ALL
                .iter()
                .min_by_key(|&&n| tie_break(&r, n))
                .unwrap();
            wins[best.index()] += 1;
        }
        for w in wins {
            assert!((9_000..=11_000).contains(&w), "uneven tie-break: {wins:?}");
        }
    }

    #[test]
    fn injection_schedule_is_in_range_and_covers_all_nodes() {
        let mut seen = [false; 3];
        for k in 0..100 {
            seen[injection_victim(42, k).index()] = true;
            let d = injection_duration_ms(42, k, 1_000, 20_000);
            assert!((1_000..=20_000).contains(&d));
        }
        assert_eq!(seen, [true; 3]);
        assert_eq!(injection_victim(42, 5), injection_victim(42, 5));
    }
}
