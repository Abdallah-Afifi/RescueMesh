//! The node runtime (workstream B; design log DL-017, DL-053, DL-054): runs
//! the [`NodeTask`] on its own thread and connects it to the transport, the
//! heartbeat thread, the planning pool and the event log.
//!
//! ```text
//!   client conns ─┐                         ┌─> Net::send / Net::reply
//!   peer conns ───┤                         │
//!   heartbeats ───┼──> channel ──> core ────┼─> Net::set_silenced, set_view
//!   pool results ─┤     (Input)   thread    ├─> heartbeat slot (every interval)
//!   rm-ctl ───────┘               NodeTask  └─> event log
//! ```
//!
//! The core thread is the only owner of protocol state (DL-017, DL-029). It
//! blocks on one channel; every input is handled to completion, and the
//! task's outputs are carried out at once. It calls `NodeTask::tick` every
//! `tick_ms` (2 ms by default) and refreshes the heartbeat content every
//! heartbeat interval. Once a second it logs a `resource_sample` with the
//! node's CPU, memory, network bytes, queue length and message counters.

use crate::fd::Reading;
use crate::heartbeat::{self, HeartbeatSlot};
use crate::net::{Clock, Input, Net, OpCmd};
use crate::task::{NodeTask, Out, PlanPool};
use client::EventLog;
use loadgen::sampler::{ProcSampler, log_clock_offset, log_machine_info};
use proto::config::Config;
use proto::event::EventKind;
use proto::{NodeId, PlanOutput, ReplyBody, ReqId, StateMachine};
use serde_json::{Map, Value};
use std::io;
use std::sync::mpsc::{self, RecvTimeoutError, Sender};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

/// Handed to the planning pool so it can return finished plans to the core
/// task (the pool calls [`NodeTask::on_planned`] through this).
#[derive(Clone)]
pub struct PlanSink(Sender<Input>);

impl PlanSink {
    /// A finished plan for `req`. Results of cancelled jobs may still be
    /// delivered; the core task ignores them.
    pub fn done(&self, req: ReqId, plan: PlanOutput, service_us: u32) {
        let _ = self.0.send(Input::Planned {
            req,
            plan,
            service_us,
        });
    }
}

/// Builds the planning pool once the runtime can give it a [`PlanSink`].
pub type PoolFactory = Box<dyn FnOnce(PlanSink) -> Box<dyn PlanPool>>;

pub struct NodeOpts {
    pub id: NodeId,
    pub cfg: Config,
    /// Random per process start (DL-021).
    pub incarnation: u32,
    pub log: Option<Arc<EventLog>>,
    pub tick_ms: u64,
}

impl NodeOpts {
    pub fn new(id: NodeId, cfg: Config, incarnation: u32, log: Option<Arc<EventLog>>) -> NodeOpts {
        NodeOpts {
            id,
            cfg,
            incarnation,
            log,
            tick_ms: 2,
        }
    }
}

/// A running node.
pub struct Node {
    pub net: Arc<Net>,
    /// Inputs to the core task (tests and the binary use it for operator
    /// commands).
    pub input: Sender<Input>,
    core: JoinHandle<()>,
}

impl Node {
    /// Blocks until the core thread stops (it runs for the life of the
    /// process).
    pub fn join(self) {
        let _ = self.core.join();
    }

    /// Asks the core task for its status line (the same JSON `rm-ctl
    /// status` prints).
    pub fn status(&self) -> String {
        let (tx, rx) = mpsc::channel();
        let _ = self.input.send(Input::Operator(OpCmd::Status(tx)));
        rx.recv_timeout(Duration::from_secs(2))
            .unwrap_or_else(|_| "{}".into())
    }
}

/// Starts the transport, the heartbeat thread and the core thread.
pub fn start(opts: NodeOpts, state: Box<dyn StateMachine>, pool: PoolFactory) -> io::Result<Node> {
    let clock = Clock::new();
    let (tx, rx) = mpsc::channel::<Input>();
    let net = Net::start(&opts.cfg, opts.id, opts.incarnation, clock, tx.clone())?;
    let pool = pool(PlanSink(tx.clone()));
    let task = NodeTask::new(opts.id, &opts.cfg, opts.incarnation, state, pool);
    let slot: HeartbeatSlot = Arc::new(Mutex::new(None));
    heartbeat::spawn(
        &opts.cfg,
        opts.id,
        clock,
        net.clone(),
        slot.clone(),
        tx.clone(),
    )?;

    if let Some(log) = &opts.log {
        log_machine_info(log);
        log_clock_offset(log, "start");
        // Nodes are usually killed at the end of a run, so there is no
        // "end" sample; log the offset periodically instead, off the core
        // thread because it runs `chronyc`.
        let log = log.clone();
        thread::Builder::new()
            .name("clock-offset".into())
            .spawn(move || {
                loop {
                    thread::sleep(Duration::from_secs(30));
                    log_clock_offset(&log, "periodic");
                }
            })?;
    }
    let mut core = Core {
        id: opts.id,
        task,
        net: net.clone(),
        log: opts.log,
        clock,
        slot,
        view_no: None,
        sampler: ProcSampler::new(),
        queue_len: 0,
    };
    let tick_ms = opts.tick_ms.max(1);
    let hb_ms = opts.cfg.timeouts.heartbeat_ms;
    let handle = thread::Builder::new()
        .name(format!("core-{}", opts.id))
        .spawn(move || core.run(rx, tick_ms, hb_ms))?;
    Ok(Node {
        net,
        input: tx,
        core: handle,
    })
}

struct Core {
    id: NodeId,
    task: NodeTask,
    net: Arc<Net>,
    log: Option<Arc<EventLog>>,
    clock: Clock,
    slot: HeartbeatSlot,
    view_no: Option<u64>,
    sampler: ProcSampler,
    queue_len: u32,
}

impl Core {
    fn run(&mut self, rx: mpsc::Receiver<Input>, tick_ms: u64, hb_ms: u64) {
        let (mut next_tick, mut next_hb, mut next_sample) = (0u64, 0u64, 1_000u64);
        loop {
            let now = self.clock.now_ms();
            let wait = next_tick.min(next_hb).saturating_sub(now);
            match rx.recv_timeout(Duration::from_millis(wait)) {
                Ok(i) => {
                    self.handle(i);
                    // Drain a batch before the next tick, without starving it.
                    for _ in 0..256 {
                        match rx.try_recv() {
                            Ok(i) => self.handle(i),
                            Err(_) => break,
                        }
                    }
                }
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => return,
            }
            let now = self.clock.now_ms();
            if now >= next_tick {
                let outs = self.task.tick(now);
                self.exec(outs, None);
                next_tick = now + tick_ms;
            }
            if now >= next_hb {
                let hb = self.task.heartbeat();
                self.queue_len = hb.load.queue_len;
                *self.slot.lock().unwrap_or_else(|e| e.into_inner()) = Some(hb);
                next_hb = now + hb_ms;
            }
            if now >= next_sample {
                self.sample();
                next_sample = now + 1_000;
            }
        }
    }

    fn handle(&mut self, input: Input) {
        let now = self.clock.now_ms();
        let (outs, reading) = match input {
            Input::Peer { from, msg } => (self.task.on_peer(now, from, msg), None),
            Input::Client(req) => (self.task.on_client(now, req), None),
            Input::Heartbeat(hb) => {
                self.task.on_heartbeat(&hb);
                return;
            }
            Input::Suspect { peer, reading } => (
                self.task.set_suspected(now, peer, reading.suspected),
                Some(reading),
            ),
            Input::Planned {
                req,
                plan,
                service_us,
            } => (self.task.on_planned(now, req, plan, service_us), None),
            Input::Operator(OpCmd::Status(reply)) => {
                let _ = reply.send(self.status_line());
                return;
            }
            Input::Operator(OpCmd::Silence {
                target,
                duration_ms,
                reply,
            }) => {
                if self.task.core().is_silenced() {
                    let _ = reply.send("error: this node is silenced".into());
                    return;
                }
                let outs = self.task.silence(now, target, duration_ms);
                let _ = reply.send(format!(
                    "ok: {} asked to silence {target:?} for {duration_ms} ms",
                    self.id
                ));
                (outs, None)
            }
        };
        self.exec(outs, reading);
    }

    fn exec(&mut self, outs: Vec<Out>, reading: Option<Reading>) {
        for o in outs {
            match o {
                Out::Send { to, msg } => self.net.send(to, msg),
                Out::Reply { req, body } => {
                    let outcome = match &body {
                        ReplyBody::Ok(_) => "ok",
                        ReplyBody::Busy => "busy",
                        ReplyBody::Error(_) => "error",
                    };
                    self.net.reply(req, body);
                    if let Some(log) = &self.log {
                        log.emit(
                            log.event(EventKind::Reply)
                                .req(&req)
                                .field("outcome", outcome),
                        );
                    }
                }
                Out::SetSilenced(on) => self.net.set_silenced(on),
                Out::Event {
                    kind,
                    req,
                    mut fields,
                } => {
                    if kind == EventKind::Suspect
                        && let Some(r) = reading
                    {
                        // The detector's view of the decision, for the
                        // detection-time measurement.
                        fields.insert("phi".into(), round2(r.phi).into());
                        fields.insert("silent_ms".into(), r.silent_ms.into());
                        fields.insert("timeout_ms".into(), r.timeout_ms.into());
                        fields.insert("samples".into(), (r.samples as u64).into());
                    }
                    self.emit(kind, req, fields);
                }
            }
        }
        let view = self.task.core().view();
        if self.view_no != Some(view.no.0) {
            self.view_no = Some(view.no.0);
            self.net.set_view(view.active());
        }
    }

    fn emit(&self, kind: EventKind, req: Option<ReqId>, fields: Map<String, Value>) {
        let Some(log) = &self.log else { return };
        let mut ev = log.event(kind);
        if let Some(r) = req {
            ev = ev.req(&r);
        }
        ev.fields.extend(fields);
        log.emit(ev);
    }

    fn sample(&mut self) {
        let Some(log) = self.log.clone() else { return };
        let p = self.sampler.sample();
        let s = self.net.stats();
        let l = self.net.link_stats();
        let get = |c: &std::sync::atomic::AtomicU64| c.load(std::sync::atomic::Ordering::Relaxed);
        log.emit(
            log.event(EventKind::ResourceSample)
                .field("scope", "node")
                .field("cpu_permille", p.cpu_permille)
                .field("cpu_core_permille", p.cpu_core_permille)
                .field("rss_bytes", p.rss_bytes)
                .field("net_rx_bytes", get(&s.tcp_rx_bytes) + get(&s.udp_rx_bytes))
                .field("net_tx_bytes", get(&s.tcp_tx_bytes) + get(&s.udp_tx_bytes))
                .field("queue_len", self.queue_len)
                .field("udp_tx_bytes", get(&s.udp_tx_bytes))
                .field("udp_rx_bytes", get(&s.udp_rx_bytes))
                .field("heartbeats_tx", get(&s.heartbeats_tx))
                .field("heartbeats_rx", get(&s.heartbeats_rx))
                .field("peer_msgs_tx", l.sent_new)
                .field("peer_retransmits", l.retransmitted)
                .field("peer_bare_acks", l.bare_acks)
                .field("peer_msgs_rx", l.delivered)
                .field("peer_duplicates", l.duplicates)
                .field("client_reqs", get(&s.client_reqs))
                .field("client_replies", get(&s.client_replies))
                .field("client_reply_dropped", get(&s.client_reply_dropped))
                .field("dropped_silenced", get(&s.dropped_silenced))
                .field("silenced", self.net.is_silenced()),
        );
    }

    /// One line of JSON, written by hand so it needs no serde types.
    fn status_line(&self) -> String {
        let c = self.task.core();
        let active: Vec<String> = c.view().active().iter().map(|n| n.0.to_string()).collect();
        let coordinator = c
            .coordinator()
            .map_or_else(|| "null".to_owned(), |n| n.0.to_string());
        format!(
            concat!(
                "{{\"node\":{},\"role\":\"{:?}\",\"status\":\"{:?}\",\"silenced\":{},",
                "\"epoch\":{},\"coordinator\":{},\"view\":{},\"active\":[{}],",
                "\"commit\":{},\"applied\":{},\"executing\":{},",
                "\"unacked\":{{{}}}}}"
            ),
            self.id.0,
            c.role(),
            c.status(),
            c.is_silenced(),
            c.epoch().0,
            coordinator,
            c.view().no.0,
            active.join(","),
            c.commit_seq().0,
            c.applied_seq().0,
            self.task.executing(),
            NodeId::ALL
                .iter()
                .filter(|&&n| n != self.id)
                .map(|&n| format!("\"{}\":{}", n.0, self.net.unacked(n)))
                .collect::<Vec<_>>()
                .join(","),
        )
    }
}

fn round2(x: f64) -> f64 {
    (x * 100.0).round() / 100.0
}
