//! A small seeded random-number generator (SplitMix64).
//!
//! Workloads must be repeatable from a seed (spec p. 8), and the standard
//! library has no seedable generator, so the load generator and the retry
//! jitter use this one. It is not for anything cryptographic.

use proto::hash::fnv1a;

#[derive(Debug, Clone)]
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Rng {
        Rng(seed)
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// Uniform in `0..n`. Panics if `n` is zero.
    pub fn below(&mut self, n: u64) -> u64 {
        assert!(n > 0, "below(0)");
        ((u128::from(self.next_u64()) * u128::from(n)) >> 64) as u64
    }

    /// Uniform in `[0, 1)`.
    pub fn unit(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }

    /// Exponentially distributed with the given mean (Poisson inter-arrival times).
    pub fn exp(&mut self, mean: f64) -> f64 {
        -mean * (1.0 - self.unit()).ln()
    }
}

/// One seed from several integers, so that every simulated user gets its own
/// independent but reproducible stream.
pub fn seed_from(parts: &[u64]) -> u64 {
    let mut bytes = Vec::with_capacity(parts.len() * 8);
    for p in parts {
        bytes.extend_from_slice(&p.to_le_bytes());
    }
    Rng::new(fnv1a(&bytes)).next_u64()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_seed_same_sequence() {
        let mut a = Rng::new(7);
        let mut b = Rng::new(7);
        for _ in 0..100 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
        assert_ne!(Rng::new(7).next_u64(), Rng::new(8).next_u64());
    }

    #[test]
    fn below_and_unit_stay_in_range_and_are_roughly_uniform() {
        let mut r = Rng::new(1);
        let mut buckets = [0u32; 10];
        for _ in 0..100_000 {
            let u = r.unit();
            assert!((0.0..1.0).contains(&u));
            buckets[(u * 10.0) as usize] += 1;
            assert!(r.below(3) < 3);
        }
        for b in buckets {
            assert!((9_000..=11_000).contains(&b), "{buckets:?}");
        }
    }

    #[test]
    fn exponential_mean_is_close() {
        let mut r = Rng::new(3);
        let n = 200_000;
        let mean: f64 = (0..n).map(|_| r.exp(2.0)).sum::<f64>() / f64::from(n);
        assert!((mean - 2.0).abs() < 0.05, "{mean}");
    }

    #[test]
    fn seed_from_separates_inputs() {
        assert_ne!(seed_from(&[1, 2, 3]), seed_from(&[1, 2, 4]));
        assert_eq!(seed_from(&[1, 2, 3]), seed_from(&[1, 2, 3]));
    }
}
