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
//! heartbeat interval. Before each tick it tells the core when the transport
//! last heard from each peer, which keeps the coordinator lease honest when
//! acknowledgments wait in the queue (DL-066). Once a second it logs a
//! `resource_sample` with the node's CPU, memory, network bytes, queue
//! length, message counters by protocol role and how late its loop ran.
//!
//! It also owns the artificial load knob of the load-imbalance demo
//! (`rm-ctl load`, DL-067): CPU burner threads inside this process, so the
//! node's measured CPU, which MWBE ranks by, rises.

use crate::fd::Reading;
use crate::heartbeat::{self, HeartbeatSlot};
use crate::net::{Clock, Input, Net, OpCmd};
use crate::task::{NodeTask, Out, PlanPool};
use client::EventLog;
use loadgen::sampler::{ProcSampler, log_clock_offset, log_machine_info};
use proto::config::Config;
use proto::event::EventKind;
use proto::{LoadSample, NodeId, PlanOutput, ReplyBody, ReqId, StateMachine};
use selection::knob::{self, CpuBurner};
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
    /// process). False if it panicked.
    pub fn join(self) -> bool {
        self.core.join().is_ok()
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
        load: LoadSample::default(),
        plans: 0,
        lag_ms_max: 0,
        knob: None,
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
    /// The load in the last heartbeat.
    load: LoadSample,
    /// Plans the pool finished, for `rm-ctl status`.
    plans: u64,
    /// How late the loop ran a tick, worst case since the last sample: an
    /// overload signal for the measurements.
    lag_ms_max: u64,
    knob: Option<Knob>,
}

/// The running load knob.
struct Knob {
    _burner: CpuBurner,
    threads: usize,
    duty_permille: u16,
    /// Stop at this time (ms), if it was started for a fixed time.
    until: Option<u64>,
}

impl Core {
    fn run(&mut self, rx: mpsc::Receiver<Input>, tick_ms: u64, hb_ms: u64) {
        // Start from now, so start-up time is not reported as loop lateness.
        let start = self.clock.now_ms();
        let (mut next_tick, mut next_hb, mut next_sample) = (start, start, start + 1_000);
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
                self.lag_ms_max = self.lag_ms_max.max(now - next_tick);
                for p in NodeId::ALL.into_iter().filter(|&p| p != self.id) {
                    self.task.heard_from(p, self.net.last_heard(p));
                }
                let outs = self.task.tick(now);
                self.exec(outs, None);
                next_tick = now + tick_ms;
                if self
                    .knob
                    .as_ref()
                    .and_then(|k| k.until)
                    .is_some_and(|t| now >= t)
                {
                    self.set_knob(0, 0, 0);
                }
            }
            if now >= next_hb {
                let rtt = self.peer_rtts().into_iter().flatten().max();
                if let Some(rtt) = rtt {
                    self.task.set_peer_rtt_us(rtt);
                }
                let hb = self.task.heartbeat();
                self.load = hb.load;
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
            Input::Client(req) => {
                self.net.client_dequeued();
                (self.task.on_client(now, req), None)
            }
            Input::Heartbeat(hb) => {
                self.task.on_heartbeat(&hb);
                return;
            }
            Input::HeartbeatSent(hb) => {
                self.task.heartbeat_sent(&hb);
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
            } => {
                self.plans += 1;
                (self.task.on_planned(now, req, plan, service_us), None)
            }
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
                // A coordinator that declines says why at once; a follower
                // forwards the request and cannot know the outcome.
                let refused = outs.iter().find_map(|o| match o {
                    Out::Event {
                        kind: EventKind::InjectRefused,
                        fields,
                        ..
                    } => Some(
                        fields
                            .get("reason")
                            .and_then(Value::as_str)
                            .unwrap_or("refused"),
                    ),
                    _ => None,
                });
                let _ = reply.send(match refused {
                    Some(r) => format!("error: injection refused: {r}"),
                    None => format!(
                        "ok: {} asked to silence {target:?} for {duration_ms} ms",
                        self.id
                    ),
                });
                (outs, None)
            }
            Input::Operator(OpCmd::Load {
                threads,
                duty_permille,
                duration_ms,
                reply,
            }) => {
                let _ = reply.send(self.set_knob(threads, duty_permille, duration_ms));
                return;
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
                    // Logged only if it went out: silenced, or the client's
                    // connection gone or backed up, it is counted instead
                    // (`client_reply_dropped`).
                    if self.net.reply(req, body)
                        && let Some(log) = &self.log
                    {
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

    /// Measured round trip to each node in µs (`None` for this node and for
    /// peers not yet measured), indexed by node index.
    fn peer_rtts(&self) -> [Option<u64>; 3] {
        let s = self.net.stats();
        NodeId::ALL.map(|n| {
            let v = s.peer_rtt_us[n.index()].load(std::sync::atomic::Ordering::Relaxed);
            (n != self.id && v > 0).then_some(v)
        })
    }

    /// Starts, changes or stops (`threads == 0`) the load knob, and answers
    /// the operator.
    fn set_knob(&mut self, threads: usize, duty_permille: u16, duration_ms: u64) -> String {
        // Stop the old burner first, so the threads never add up.
        self.knob = None;
        let threads = threads.min(knob::max_threads());
        let duty_permille = duty_permille.min(knob::MAX_DUTY_PERMILLE);
        let on = threads > 0 && duty_permille > 0;
        if on {
            self.knob = Some(Knob {
                _burner: CpuBurner::start(threads, duty_permille),
                threads,
                duty_permille,
                until: (duration_ms > 0).then(|| self.clock.now_ms() + duration_ms),
            });
        }
        let mut f = Map::new();
        f.insert("threads".into(), (if on { threads } else { 0 }).into());
        f.insert(
            "duty_permille".into(),
            (if on { duty_permille } else { 0 }).into(),
        );
        f.insert("duration_ms".into(), duration_ms.into());
        f.insert("on".into(), on.into());
        self.emit(EventKind::LoadKnob, None, f);
        if on {
            let until = if duration_ms > 0 {
                format!("for {duration_ms} ms")
            } else {
                "until stopped".to_owned()
            };
            format!(
                "ok: {} burning {threads} threads at {duty_permille} permille {until}",
                self.id
            )
        } else {
            format!("ok: {} load knob off", self.id)
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
        // Per peer: measured round trip and missed-heartbeat share.
        let mut per_peer = Map::new();
        for (n, rtt) in NodeId::ALL.into_iter().zip(self.peer_rtts()) {
            if n == self.id {
                continue;
            }
            if let Some(us) = rtt {
                per_peer.insert(format!("rtt_us_n{}", n.0), us.into());
            }
            per_peer.insert(
                format!("missed_permille_n{}", n.0),
                self.task.missed_permille(n).into(),
            );
        }
        let mut ev = log.event(EventKind::ResourceSample);
        ev.fields.extend(per_peer);
        log.emit(
            ev.field("scope", "node")
                .field("cpu_permille", p.cpu_permille)
                .field("cpu_core_permille", p.cpu_core_permille)
                .field("rss_bytes", p.rss_bytes)
                .field("net_rx_bytes", get(&s.tcp_rx_bytes) + get(&s.udp_rx_bytes))
                .field("net_tx_bytes", get(&s.tcp_tx_bytes) + get(&s.udp_tx_bytes))
                .field("queue_len", self.load.queue_len)
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
                .field("client_overload_dropped", get(&s.client_overload_dropped))
                .field("client_backlog", self.net.client_backlog())
                .field("decode_errors", get(&s.decode_errors))
                .field("version_errors", get(&s.version_errors))
                .field("msgs_tx_election", get(&s.msgs_tx_election))
                .field("msgs_tx_replication", get(&s.msgs_tx_replication))
                .field("msgs_tx_claim", get(&s.msgs_tx_claim))
                .field("msgs_tx_commit", get(&s.msgs_tx_commit))
                .field("msgs_tx_other", get(&s.msgs_tx_other))
                .field("core_lag_ms_max", std::mem::take(&mut self.lag_ms_max))
                .field("events_dropped", log.dropped())
                .field(
                    "load_knob_threads",
                    self.knob.as_ref().map_or(0, |k| k.threads),
                )
                .field("claim_timeout_ms", self.task.selector().claim_timeout_ms())
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
        let (knob_threads, knob_duty) = self
            .knob
            .as_ref()
            .map_or((0, 0), |k| (k.threads, k.duty_permille));
        format!(
            concat!(
                "{{\"node\":{},\"role\":\"{:?}\",\"status\":\"{:?}\",\"silenced\":{},",
                "\"epoch\":{},\"coordinator\":{},\"view\":{},\"active\":[{}],",
                "\"commit\":{},\"applied\":{},\"executing\":{},",
                "\"plans\":{},\"claims\":{},\"queue_len\":{},\"cpu_permille\":{},",
                "\"load_knob\":{{\"threads\":{},\"duty_permille\":{}}},",
                "\"incarnation\":{},\"suspected\":[{}],\"unacked\":{{{}}}}}"
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
            self.plans,
            self.task.selector().stats().claims_made,
            self.load.queue_len,
            self.load.cpu_permille,
            knob_threads,
            knob_duty,
            self.task.incarnation(),
            c.suspected_peers()
                .iter()
                .map(|n| n.0.to_string())
                .collect::<Vec<_>>()
                .join(","),
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
