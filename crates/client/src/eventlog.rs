//! Writer for the event log (`proto::event`, design log DL-028).
//!
//! One file per process: `<log_dir>/<run_id>/<machine>-<process>.jsonl`.
//! Events go through a bounded channel to a dedicated writer thread, so a
//! slow disk never delays a request. If the channel is ever full the event is
//! dropped and counted; the count is reported in the run summary, so lost
//! events are visible rather than silent.

use proto::NodeId;
use proto::event::{Event, EventKind};
use std::fs::{self, File};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError, SyncSender, TrySendError};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

const QUEUE: usize = 1 << 17;
const FLUSH_EVERY: Duration = Duration::from_millis(500);

enum Msg {
    Line(String),
    Stop,
}

pub struct EventLog {
    tx: SyncSender<Msg>,
    dropped: AtomicU64,
    run_id: String,
    machine: String,
    node: Option<NodeId>,
    start: Instant,
    path: PathBuf,
    writer: Mutex<Option<JoinHandle<()>>>,
}

impl EventLog {
    /// Creates `<dir>/<run_id>/<machine>-<process>.jsonl`. `node` is stamped on
    /// every event; clients pass `None`.
    pub fn create(
        dir: &Path,
        run_id: &str,
        machine: &str,
        process: &str,
        node: Option<NodeId>,
    ) -> io::Result<Arc<EventLog>> {
        let run_dir = dir.join(run_id);
        fs::create_dir_all(&run_dir)?;
        let path = run_dir.join(format!("{machine}-{process}.jsonl"));
        let mut out = BufWriter::with_capacity(1 << 16, File::create(&path)?);
        let (tx, rx) = mpsc::sync_channel::<Msg>(QUEUE);
        let writer = thread::Builder::new()
            .name("eventlog".into())
            .spawn(move || {
                let mut last_flush = Instant::now();
                loop {
                    match rx.recv_timeout(FLUSH_EVERY) {
                        Ok(Msg::Line(l)) => {
                            let _ = out.write_all(l.as_bytes());
                            let _ = out.write_all(b"\n");
                        }
                        Ok(Msg::Stop) | Err(RecvTimeoutError::Disconnected) => break,
                        Err(RecvTimeoutError::Timeout) => {}
                    }
                    if last_flush.elapsed() >= FLUSH_EVERY {
                        let _ = out.flush();
                        last_flush = Instant::now();
                    }
                }
                let _ = out.flush();
            })?;
        Ok(Arc::new(EventLog {
            tx,
            dropped: AtomicU64::new(0),
            run_id: run_id.to_owned(),
            machine: machine.to_owned(),
            node,
            start: Instant::now(),
            path,
            writer: Mutex::new(Some(writer)),
        }))
    }

    /// A new event stamped with this log's identity and the current time.
    pub fn event(&self, kind: EventKind) -> Event {
        Event::now(&self.run_id, &self.machine, self.node, kind, self.start)
    }

    pub fn emit(&self, ev: Event) {
        match self.tx.try_send(Msg::Line(ev.to_line())) {
            Ok(()) | Err(TrySendError::Disconnected(_)) => {}
            Err(TrySendError::Full(_)) => {
                self.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Events lost because the writer fell behind. Should be zero.
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Flushes everything queued and closes the file. Safe to call twice.
    pub fn finish(&self) {
        let handle = self.writer.lock().unwrap_or_else(|e| e.into_inner()).take();
        if let Some(h) = handle {
            let _ = self.tx.send(Msg::Stop);
            let _ = h.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn events_reach_the_file_in_order() {
        let dir = std::env::temp_dir().join(format!("rm-eventlog-{}", std::process::id()));
        let log = EventLog::create(&dir, "t1", "client1", "0", None).unwrap();
        for i in 0..1000 {
            log.emit(log.event(EventKind::ClientSend).field("attempt", i));
        }
        log.finish();
        log.finish();
        let text = fs::read_to_string(log.path()).unwrap();
        let lines: Vec<_> = text.lines().collect();
        assert_eq!(lines.len(), 1000);
        let last: serde_json::Value = serde_json::from_str(lines[999]).unwrap();
        assert_eq!(last["attempt"], 999);
        assert_eq!(last["kind"], "client_send");
        assert_eq!(log.dropped(), 0);
        let _ = fs::remove_dir_all(&dir);
    }
}
