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

/// Burns `duty_permille` thousandths of each `threads` CPU cores until stopped
/// or dropped.
pub struct CpuBurner {
    stop: Arc<AtomicBool>,
    threads: Vec<JoinHandle<()>>,
}

impl CpuBurner {
    /// Starts `threads` burner threads, each busy for `duty_permille`
    /// thousandths (capped at 1000) of every 10 ms period.
    pub fn start(threads: usize, duty_permille: u16) -> CpuBurner {
        let stop = Arc::new(AtomicBool::new(false));
        let busy = PERIOD * u32::from(duty_permille.min(1000)) / 1000;
        let threads = (0..threads)
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
    fn dropping_the_burner_stops_it() {
        let b = CpuBurner::start(1, 1000);
        drop(b);
    }
}
