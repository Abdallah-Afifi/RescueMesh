//! φ-accrual failure detector (Hayashibara et al., 2004), one per peer
//! (`docs/protocol-v2.md` Section 6, design log DL-024, DL-053).
//!
//! The detector keeps a sliding window of heartbeat inter-arrival times and
//! models them as a normal distribution with the window's mean μ and
//! standard deviation σ. After `t` ms without a heartbeat,
//!
//! ```text
//! φ(t) = −log10( P(next heartbeat arrives later than t) )
//!      = −log10( Q((t − μ) / σ) ),   Q = upper tail of the standard normal
//! ```
//!
//! and the peer is suspected once φ exceeds `timeouts.fd_phi_threshold`.
//! Because φ grows monotonically with `t`, the threshold is equivalent to a
//! timeout `T_d = μ + z·σ` with `Q(z) = 10^−threshold`. That timeout is
//! clamped to `[fd_min_ms, fd_max_ms]`: the floor keeps a very regular
//! network from producing a hair-trigger, and the ceiling bounds detection
//! time when the network is erratic.
//!
//! **Rejoin.** A gap longer than `fd_max_ms` is not an inter-arrival sample:
//! the peer was silent, not slow. When a suspected peer is heard again its
//! window is cleared and rebuilt from fresh samples, so a 20 s silence never
//! distorts the estimate (the plan's "window reset on rejoin").
//!
//! Like the protocol modules, this performs no I/O and reads no clock; the
//! heartbeat thread passes the current monotonic time in milliseconds.
//! Floating point is fine here: the detector's output is local to one node
//! and is never compared across nodes.

use proto::config::Timeouts;
use std::collections::VecDeque;

/// Inter-arrival samples kept per peer.
pub const WINDOW: usize = 100;

#[derive(Debug, Clone, PartialEq)]
pub struct FdCfg {
    pub heartbeat_ms: u64,
    pub phi_threshold: f64,
    pub min_ms: u64,
    pub max_ms: u64,
}

impl FdCfg {
    pub fn from_timeouts(t: &Timeouts) -> FdCfg {
        FdCfg {
            heartbeat_ms: t.heartbeat_ms,
            phi_threshold: t.fd_phi_threshold,
            min_ms: t.fd_min_ms,
            max_ms: t.fd_max_ms,
        }
    }
}

/// What the detector reports when its opinion of a peer changes.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Reading {
    pub suspected: bool,
    /// φ at the moment of the change (0 when cleared by a heartbeat).
    pub phi: f64,
    /// Time since the last heartbeat from the peer.
    pub silent_ms: u64,
    /// The clamped timeout `T_d` in force.
    pub timeout_ms: u64,
    /// Samples in the window when the decision was taken.
    pub samples: usize,
}

#[derive(Debug, Clone)]
pub struct PhiDetector {
    cfg: FdCfg,
    /// `z` with `Q(z) = 10^-threshold`, computed once.
    z: f64,
    window: VecDeque<u64>,
    /// Last heartbeat, or the time the detector started / was reset.
    last: u64,
    /// False until the first real heartbeat: the gap from start-up to the
    /// first heartbeat is not a sample.
    heard: bool,
    suspected: bool,
}

impl PhiDetector {
    /// A detector that starts its first timeout at `now`, so a peer that
    /// never comes up is suspected after `T_d`.
    pub fn new(cfg: FdCfg, now: u64) -> PhiDetector {
        let z = z_for_phi(cfg.phi_threshold);
        PhiDetector {
            cfg,
            z,
            window: VecDeque::with_capacity(WINDOW),
            last: now,
            heard: false,
            suspected: false,
        }
    }

    pub fn is_suspected(&self) -> bool {
        self.suspected
    }

    pub fn samples(&self) -> usize {
        self.window.len()
    }

    /// A heartbeat arrived. Returns a reading if the peer was suspected and
    /// is now cleared.
    pub fn heartbeat(&mut self, now: u64) -> Option<Reading> {
        let gap = now.saturating_sub(self.last);
        let was_suspected = self.suspected;
        if was_suspected {
            // Rejoin: start the estimate again from fresh samples.
            self.window.clear();
        } else if self.heard && gap <= self.cfg.max_ms {
            if self.window.len() == WINDOW {
                self.window.pop_front();
            }
            self.window.push_back(gap);
        }
        self.heard = true;
        self.last = now;
        self.suspected = false;
        was_suspected.then(|| Reading {
            suspected: false,
            phi: 0.0,
            silent_ms: gap,
            timeout_ms: self.timeout_ms(),
            samples: self.window.len(),
        })
    }

    /// Call often (every few ms). Returns a reading when the peer becomes
    /// suspected.
    pub fn check(&mut self, now: u64) -> Option<Reading> {
        if self.suspected {
            return None;
        }
        let silent = now.saturating_sub(self.last);
        let timeout = self.timeout_ms();
        if silent <= timeout {
            return None;
        }
        self.suspected = true;
        Some(Reading {
            suspected: true,
            phi: self.phi(now),
            silent_ms: silent,
            timeout_ms: timeout,
            samples: self.window.len(),
        })
    }

    /// This node itself was silenced and has just woken: the gap is ours,
    /// not the peer's, so restart the timeout from `now` without suspecting.
    pub fn restart(&mut self, now: u64) {
        self.last = now;
    }

    /// Mean and standard deviation of the window. Until two samples exist
    /// the heartbeat interval is assumed, with σ a quarter of it.
    fn stats(&self) -> (f64, f64) {
        let hb = self.cfg.heartbeat_ms.max(1) as f64;
        if self.window.len() < 2 {
            return (hb, hb / 4.0);
        }
        let n = self.window.len() as f64;
        let mean = self.window.iter().map(|&x| x as f64).sum::<f64>() / n;
        let var = self
            .window
            .iter()
            .map(|&x| {
                let d = x as f64 - mean;
                d * d
            })
            .sum::<f64>()
            / (n - 1.0);
        // A perfectly regular window would give σ = 0 and an infinite φ for
        // any delay; keep σ at least a tenth of the interval and 1 ms.
        (mean, var.sqrt().max(hb / 10.0).max(1.0))
    }

    /// The clamped timeout `T_d` in milliseconds.
    pub fn timeout_ms(&self) -> u64 {
        let (mean, sd) = self.stats();
        let t = (mean + self.z * sd).ceil();
        let t = if t.is_finite() {
            t as u64
        } else {
            self.cfg.max_ms
        };
        t.clamp(self.cfg.min_ms, self.cfg.max_ms)
    }

    /// φ after `now - last` ms of silence.
    pub fn phi(&self, now: u64) -> f64 {
        let (mean, sd) = self.stats();
        let t = now.saturating_sub(self.last) as f64;
        phi_of_tail(upper_tail((t - mean) / sd))
    }
}

fn phi_of_tail(q: f64) -> f64 {
    // Q underflows to 0 far out in the tail; report a large finite φ.
    if q <= 1e-300 { 300.0 } else { -q.log10() }
}

/// `Q(x) = P(Z > x)` for a standard normal `Z`.
pub fn upper_tail(x: f64) -> f64 {
    0.5 * erfc(x / std::f64::consts::SQRT_2)
}

/// Complementary error function, fractional error below 1.2e-7 everywhere
/// (Numerical Recipes, `erfcc`, Chebyshev fit).
pub fn erfc(x: f64) -> f64 {
    let z = x.abs();
    let t = 1.0 / (1.0 + 0.5 * z);
    let r = t
        * (-z * z - 1.265_512_23
            + t * (1.000_023_68
                + t * (0.374_091_96
                    + t * (0.096_784_18
                        + t * (-0.186_288_06
                            + t * (0.278_868_07
                                + t * (-1.135_203_98
                                    + t * (1.488_515_87
                                        + t * (-0.822_152_23 + t * 0.170_872_77)))))))))
            .exp();
    if x >= 0.0 { r } else { 2.0 - r }
}

/// The `z` at which φ reaches `phi`, by bisection on the monotone tail.
pub fn z_for_phi(phi: f64) -> f64 {
    let (mut lo, mut hi) = (-10.0_f64, 40.0_f64);
    for _ in 0..200 {
        let mid = 0.5 * (lo + hi);
        if phi_of_tail(upper_tail(mid)) < phi {
            lo = mid;
        } else {
            hi = mid;
        }
    }
    0.5 * (lo + hi)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> FdCfg {
        FdCfg {
            heartbeat_ms: 100,
            phi_threshold: 8.0,
            min_ms: 300,
            max_ms: 1000,
        }
    }

    #[test]
    fn tail_and_inverse_match_known_values() {
        assert!((upper_tail(0.0) - 0.5).abs() < 1e-7);
        assert!((upper_tail(1.959_964) - 0.025).abs() < 1e-6);
        // φ = 8 means a 1e-8 tail, about 5.61 standard deviations.
        let z = z_for_phi(8.0);
        assert!((z - 5.612).abs() < 0.01, "{z}");
        assert!((phi_of_tail(upper_tail(z)) - 8.0).abs() < 1e-6);
    }

    #[test]
    fn regular_heartbeats_are_never_suspected() {
        let mut d = PhiDetector::new(cfg(), 0);
        for i in 1..=500u64 {
            let now = i * 100 + (i * 7919) % 15; // up to 15 ms jitter
            assert_eq!(d.check(now), None, "at {now}");
            d.heartbeat(now);
        }
        assert_eq!(d.samples(), WINDOW);
        // A regular LAN gives a tiny σ, so the floor decides.
        assert_eq!(d.timeout_ms(), 300);
    }

    #[test]
    fn a_silent_peer_is_suspected_between_the_floor_and_the_ceiling() {
        let mut d = PhiDetector::new(cfg(), 0);
        let mut now = 0;
        for _ in 0..50 {
            now += 100;
            d.heartbeat(now);
        }
        let last = now;
        let mut suspected_at = None;
        while suspected_at.is_none() {
            now += 1;
            if let Some(r) = d.check(now) {
                assert!(r.suspected);
                assert!(r.phi >= 8.0, "{r:?}");
                suspected_at = Some(now);
            }
        }
        let after = suspected_at.unwrap() - last;
        assert!((300..=301).contains(&after), "{after}");
        // Reported once, not on every check.
        assert_eq!(d.check(now + 50), None);
        assert!(d.is_suspected());
    }

    #[test]
    fn erratic_arrivals_raise_the_timeout_up_to_the_ceiling() {
        let mut d = PhiDetector::new(cfg(), 0);
        let mut now = 0;
        for i in 0..100u64 {
            now += if i % 2 == 0 { 20 } else { 380 }; // mean 200, σ about 180
            d.heartbeat(now);
        }
        let t = d.timeout_ms();
        assert!(t > 300 && t <= 1000, "{t}");
        for i in 0..100u64 {
            now += if i % 2 == 0 { 10 } else { 900 };
            d.heartbeat(now);
        }
        assert_eq!(d.timeout_ms(), 1000);
    }

    #[test]
    fn a_long_silence_is_not_a_sample_and_rejoin_resets_the_window() {
        let mut d = PhiDetector::new(cfg(), 0);
        let mut now = 0;
        for _ in 0..50 {
            now += 100;
            d.heartbeat(now);
        }
        now += 20_000;
        assert!(d.check(now).is_some());
        let r = d.heartbeat(now).expect("cleared");
        assert!(!r.suspected);
        assert_eq!(r.silent_ms, 20_000);
        assert_eq!(d.samples(), 0, "window rebuilt from fresh samples");
        for _ in 0..3 {
            now += 100;
            d.heartbeat(now);
        }
        assert_eq!(d.timeout_ms(), 300, "estimate not distorted by the gap");
    }

    #[test]
    fn a_peer_that_never_starts_is_suspected_and_the_startup_gap_is_ignored() {
        let mut d = PhiDetector::new(cfg(), 1_000);
        assert_eq!(d.check(1_200), None);
        assert!(d.check(1_301).is_some());
        let mut d = PhiDetector::new(cfg(), 0);
        d.heartbeat(250);
        assert_eq!(d.samples(), 0);
        d.heartbeat(350);
        assert_eq!(d.samples(), 1);
    }

    #[test]
    fn restart_after_own_silence_does_not_suspect() {
        let mut d = PhiDetector::new(cfg(), 0);
        d.heartbeat(100);
        // We were silenced from 100 to 15_000 and checked nothing meanwhile.
        d.restart(15_000);
        assert_eq!(d.check(15_200), None);
        assert!(d.check(15_400).is_some());
    }
}
