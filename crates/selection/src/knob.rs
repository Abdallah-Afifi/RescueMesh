//! The artificial load knob for the load-imbalance demo (spec p. 5): burns a
//! chosen share of CPU on this node so that MWBE can be seen moving work away
//! from it.
//!
//! The burner runs on its own threads and only spends CPU; it does not touch
//! protocol state or the planning queue. Its effect reaches the score through
//! the measured CPU utilisation and, indirectly, through slower planning.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

/// Length of one busy/idle cycle.
const PERIOD: Duration = Duration::from_millis(10);

/// Highest duty cycle allowed, in thousandths. Leaves every burner core some
/// idle time, so the heartbeat thread and the core task still get scheduled.
pub const MAX_DUTY_PERMILLE: u16 = 900;

/// Most burner threads allowed: one core is always left free. Saturating
/// every core would delay heartbeats, get the node suspected and removed from
/// the view, and turn the load-imbalance demo into a failover demo.
pub fn max_threads() -> usize {
    std::thread::available_parallelism()
        .map_or(1, |n| n.get())
        .saturating_sub(1)
}

/// Burns `duty_permille` thousandths of each `threads` CPU cores until stopped
/// or dropped.
pub struct CpuBurner {
    stop: Arc<AtomicBool>,
    threads: Vec<JoinHandle<()>>,
}

impl CpuBurner {
    /// Starts `threads` burner threads (at most [`max_threads`]), each busy
    /// for `duty_permille` thousandths (at most [`MAX_DUTY_PERMILLE`]) of
    /// every 10 ms period.
    pub fn start(threads: usize, duty_permille: u16) -> CpuBurner {
        let stop = Arc::new(AtomicBool::new(false));
        let busy = PERIOD * u32::from(duty_permille.min(MAX_DUTY_PERMILLE)) / 1000;
        let threads = (0..threads.min(max_threads()))
            .map(|_| {
                let stop = Arc::clone(&stop);
                std::thread::spawn(move || burn(&stop, busy))
            })
            .collect();
        CpuBurner { stop, threads }
    }

    /// Stops the threads and waits for them to finish.
    pub fn stop(mut self) {
        self.halt();
    }

    fn halt(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        for t in self.threads.drain(..) {
            let _ = t.join();
        }
    }
}

impl Drop for CpuBurner {
    fn drop(&mut self) {
        self.halt();
    }
}

fn burn(stop: &AtomicBool, busy: Duration) {
    let mut x: u64 = 0x9e37_79b9_7f4a_7c15;
    while !stop.load(Ordering::Relaxed) {
        let start = Instant::now();
        while start.elapsed() < busy {
            // Cheap arithmetic the optimiser cannot remove.
            x = std::hint::black_box(x.rotate_left(5) ^ x.wrapping_mul(31));
        }
        if let Some(rest) = PERIOD.checked_sub(start.elapsed()) {
            std::thread::sleep(rest);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn burner_starts_and_stops() {
        let b = CpuBurner::start(2, 500);
        std::thread::sleep(Duration::from_millis(30));
        b.stop();
    }

    #[test]
    fn the_burner_always_leaves_a_core_free() {
        let b = CpuBurner::start(10_000, 1000);
        assert!(b.threads.len() <= max_threads());
        assert!(b.threads.len() < std::thread::available_parallelism().map_or(1, |n| n.get()));
        b.stop();
    }

    #[test]
    fn dropping_the_burner_stops_it() {
        let b = CpuBurner::start(1, 1000);
        drop(b);
    }
}
