//! A latency histogram with about 3% relative error and a fixed size, so each
//! client process can keep one, send it to the frontend as a small summary,
//! and have the frontend merge them. Values are microseconds.
//!
//! Buckets are 32 per power of two. Values below 32 µs get exact buckets.

use serde::{Deserialize, Serialize};

const SUB_BITS: u32 = 5;
const SUB: u64 = 1 << SUB_BITS;
/// Values at or above this (about 12.7 days) are clamped.
const MAX_VALUE: u64 = (1 << 40) - 1;
const BUCKETS: usize = 36 * SUB as usize;

#[derive(Debug, Clone)]
pub struct LatencyHist {
    counts: Vec<u64>,
    count: u64,
    sum_us: u128,
    min_us: u64,
    max_us: u64,
}

impl Default for LatencyHist {
    fn default() -> Self {
        LatencyHist::new()
    }
}

/// Compact form that goes into the summary JSON: only non-empty buckets.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistSnapshot {
    pub count: u64,
    pub sum_us: u128,
    pub min_us: u64,
    pub max_us: u64,
    pub buckets: Vec<(u32, u64)>,
}

fn index_of(v: u64) -> usize {
    let v = v.min(MAX_VALUE);
    if v < SUB {
        return v as usize;
    }
    let e = 63 - v.leading_zeros();
    let sub = (v >> (e - SUB_BITS)) & (SUB - 1);
    ((u64::from(e - SUB_BITS) + 1) * SUB + sub) as usize
}

/// Lower bound and width of a bucket.
fn bounds(idx: usize) -> (u64, u64) {
    let idx = idx as u64;
    if idx < SUB {
        return (idx, 1);
    }
    let group = idx / SUB;
    let sub = idx % SUB;
    let shift = group - 1;
    ((SUB + sub) << shift, 1 << shift)
}

impl LatencyHist {
    pub fn new() -> LatencyHist {
        LatencyHist {
            counts: vec![0; BUCKETS],
            count: 0,
            sum_us: 0,
            min_us: u64::MAX,
            max_us: 0,
        }
    }

    pub fn record(&mut self, us: u64) {
        self.counts[index_of(us)] += 1;
        self.count += 1;
        self.sum_us += u128::from(us);
        self.min_us = self.min_us.min(us);
        self.max_us = self.max_us.max(us);
    }

    pub fn count(&self) -> u64 {
        self.count
    }

    pub fn min_us(&self) -> Option<u64> {
        (self.count > 0).then_some(self.min_us)
    }

    pub fn max_us(&self) -> Option<u64> {
        (self.count > 0).then_some(self.max_us)
    }

    pub fn mean_us(&self) -> Option<f64> {
        (self.count > 0).then(|| self.sum_us as f64 / self.count as f64)
    }

    /// The value below which a fraction `q` (0..=1) of samples fall.
    pub fn quantile_us(&self, q: f64) -> Option<u64> {
        if self.count == 0 {
            return None;
        }
        let rank = ((q.clamp(0.0, 1.0) * self.count as f64).ceil() as u64).max(1);
        let mut seen = 0;
        for (i, &c) in self.counts.iter().enumerate() {
            seen += c;
            if seen >= rank {
                let (lo, width) = bounds(i);
                return Some((lo + width / 2).clamp(self.min_us, self.max_us));
            }
        }
        Some(self.max_us)
    }

    pub fn merge(&mut self, other: &LatencyHist) {
        for (a, b) in self.counts.iter_mut().zip(&other.counts) {
            *a += b;
        }
        self.count += other.count;
        self.sum_us += other.sum_us;
        self.min_us = self.min_us.min(other.min_us);
        self.max_us = self.max_us.max(other.max_us);
    }

    pub fn snapshot(&self) -> HistSnapshot {
        HistSnapshot {
            count: self.count,
            sum_us: self.sum_us,
            min_us: self.min_us,
            max_us: self.max_us,
            buckets: self
                .counts
                .iter()
                .enumerate()
                .filter(|(_, c)| **c > 0)
                .map(|(i, c)| (i as u32, *c))
                .collect(),
        }
    }

    pub fn from_snapshot(s: &HistSnapshot) -> LatencyHist {
        let mut h = LatencyHist::new();
        for &(i, c) in &s.buckets {
            if let Some(slot) = h.counts.get_mut(i as usize) {
                *slot = c;
            }
        }
        h.count = s.count;
        h.sum_us = s.sum_us;
        h.min_us = s.min_us;
        h.max_us = s.max_us;
        h
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buckets_are_monotonic_and_contain_their_values() {
        let mut last = 0;
        for v in (0..5_000u64).chain([1 << 20, (1 << 30) + 12345, MAX_VALUE]) {
            let i = index_of(v);
            assert!(i >= last || v >= 5_000, "index went backwards at {v}");
            let (lo, w) = bounds(i);
            assert!(
                lo <= v && v < lo + w,
                "{v} not in bucket {i} = [{lo}, {})",
                lo + w
            );
            if v < 5_000 {
                last = i;
            }
        }
        assert!(index_of(u64::MAX) < BUCKETS);
    }

    #[test]
    fn quantiles_match_a_sorted_list_within_error() {
        let mut h = LatencyHist::new();
        let values: Vec<u64> = (1..=100_000).map(|i| i * 13 % 50_000 + 100).collect();
        for &v in &values {
            h.record(v);
        }
        let mut sorted = values;
        sorted.sort_unstable();
        for q in [0.5, 0.95, 0.99, 0.999] {
            let exact =
                sorted[((q * sorted.len() as f64).ceil() as usize - 1).min(sorted.len() - 1)];
            let got = h.quantile_us(q).unwrap();
            let err = (got as f64 - exact as f64).abs() / exact as f64;
            assert!(err < 0.04, "q={q} exact={exact} got={got}");
        }
        assert_eq!(h.min_us(), Some(100));
        assert_eq!(h.count(), 100_000);
    }

    #[test]
    fn merge_and_snapshot_round_trip() {
        let mut a = LatencyHist::new();
        let mut b = LatencyHist::new();
        for v in 1..1000 {
            a.record(v);
            b.record(v * 1000);
        }
        let mut m = a.clone();
        m.merge(&b);
        assert_eq!(m.count(), 1998);
        let back = LatencyHist::from_snapshot(&m.snapshot());
        assert_eq!(back.snapshot(), m.snapshot());
        assert_eq!(back.quantile_us(0.99), m.quantile_us(0.99));
        assert!(LatencyHist::new().quantile_us(0.5).is_none());
    }
}
