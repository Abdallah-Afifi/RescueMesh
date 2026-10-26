//! The client middleware (`docs/protocol-v2.md` Sections 5.3 and 5.5).
//!
//! One [`Middleware`] per client process owns the network and the settings
//! shared by every simulated user in it; each user is a [`Client`]. A call to
//! [`Client::submit`]:
//!
//! 1. gives the operation a [`ReqId`] = (client, process incarnation, next
//!    sequence number), unchanged by retries, which is what makes retries
//!    idempotent;
//! 2. multicasts `REQ(reqID, op, rtt[1..3], ackedUpTo)` to all three nodes;
//! 3. waits for the first `Ok` reply from any node. A `Busy` or `Error`
//!    reply shortens the wait: the client backs off and resends;
//! 4. on timeout, resends the same request to all three nodes, with the
//!    timeout multiplied by `backoff_factor` each time and ±25% random
//!    jitter, at most `max_retries` times;
//! 5. reports `ackedUpTo`, the highest sequence number below which every
//!    reply has been received, so nodes can forget those request-table rows.
//!
//! The first timeout is `timeout_initial_ms` (2 s); once enough samples exist
//! it is `timeout_p99_multiple` × the observed p99 of single-attempt latency
//! (design log DL-024). Latency is always measured on this process's own
//! clock, from the first send to the first `Ok` reply.

use crate::eventlog::EventLog;
use crate::net::{ClientNet, NetConfig, Registration};
use crate::rng::{Rng, seed_from};
use proto::codec::{self, CodecError};
use proto::config::{ClientCfg, Config};
use proto::event::EventKind;
use proto::{
    ClientId, ClientMsg, ClientReq, NodeId, Op, OpResult, PROTOCOL_VERSION, ReplyBody, ReqId,
    Request,
};
use std::collections::{BTreeSet, VecDeque};
use std::fmt;
use std::sync::atomic::{AtomicU8, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::{self, Instant};

/// Run phase stamped on each request's events so the analysis can discard the
/// warm-up (spec p. 5, statistically meaningful results).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    Warmup,
    Measure,
    Cooldown,
}

impl Phase {
    pub fn as_str(self) -> &'static str {
        match self {
            Phase::Warmup => "warmup",
            Phase::Measure => "measure",
            Phase::Cooldown => "cooldown",
        }
    }

    fn from_u8(v: u8) -> Phase {
        match v {
            0 => Phase::Warmup,
            1 => Phase::Measure,
            _ => Phase::Cooldown,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RetryPolicy {
    pub timeout_initial: Duration,
    pub p99_multiple: u32,
    pub max_retries: u32,
    pub backoff_factor: f64,
    pub min_timeout: Duration,
    pub max_timeout: Duration,
    /// Base delay before resending after a `Busy` or `Error` reply.
    pub busy_base: Duration,
    /// Samples needed before the adaptive timeout replaces the initial one.
    pub min_samples: usize,
}

impl RetryPolicy {
    pub fn from_cfg(c: &ClientCfg) -> RetryPolicy {
        RetryPolicy {
            timeout_initial: Duration::from_millis(c.timeout_initial_ms),
            p99_multiple: c.timeout_p99_multiple,
            max_retries: c.max_retries,
            backoff_factor: c.backoff_factor,
            min_timeout: Duration::from_millis(100),
            max_timeout: Duration::from_secs(30),
            busy_base: Duration::from_millis(50),
            min_samples: 200,
        }
    }

    /// Timeout for attempt number `attempt` (0 is the first send).
    pub fn attempt_timeout(&self, base: Duration, attempt: u32, rng: &mut Rng) -> Duration {
        let growth = self.backoff_factor.powi(attempt as i32);
        let jitter = 0.75 + 0.5 * rng.unit();
        base.mul_f64(growth * jitter).min(self.max_timeout)
    }

    /// How long to wait after a `Busy` reply before resending.
    pub fn busy_delay(&self, attempt: u32, rng: &mut Rng) -> Duration {
        let growth = self.backoff_factor.powi(attempt as i32);
        let jitter = 0.5 + rng.unit();
        self.busy_base
            .mul_f64(growth * jitter)
            .min(self.max_timeout)
    }
}

/// Rolling window of recent single-attempt latencies, for the adaptive timeout.
struct LatencyWindow {
    samples: Mutex<VecDeque<u32>>,
    recorded: AtomicU64,
    p99_us: AtomicU64,
    min_samples: usize,
}

const WINDOW: usize = 4096;
const RECOMPUTE_EVERY: u64 = 128;

impl LatencyWindow {
    fn new(min_samples: usize) -> LatencyWindow {
        LatencyWindow {
            samples: Mutex::new(VecDeque::with_capacity(WINDOW)),
            recorded: AtomicU64::new(0),
            p99_us: AtomicU64::new(0),
            min_samples,
        }
    }

    fn record(&self, us: u64) {
        let mut s = self.samples.lock().unwrap_or_else(|e| e.into_inner());
        if s.len() == WINDOW {
            s.pop_front();
        }
        s.push_back(u32::try_from(us).unwrap_or(u32::MAX));
        let n = self.recorded.fetch_add(1, Ordering::Relaxed) + 1;
        if n.is_multiple_of(RECOMPUTE_EVERY) && s.len() >= self.min_samples {
            let mut v: Vec<u32> = s.iter().copied().collect();
            v.sort_unstable();
            let idx = (v.len() * 99).div_ceil(100) - 1;
            self.p99_us.store(u64::from(v[idx]), Ordering::Relaxed);
        }
    }

    fn p99(&self) -> Option<Duration> {
        match self.p99_us.load(Ordering::Relaxed) {
            0 => None,
            us => Some(Duration::from_micros(us)),
        }
    }
}

struct Inner {
    net: ClientNet,
    policy: RetryPolicy,
    incarnation: u32,
    log: Option<Arc<EventLog>>,
    window: LatencyWindow,
    phase: AtomicU8,
    submitted: AtomicU64,
    completed: AtomicU64,
    gave_up: AtomicU64,
    retries: AtomicU64,
    inflight: AtomicU32,
}

/// Counters for the sampler and the run summary.
#[derive(Debug, Clone, Copy, Default)]
pub struct MiddlewareStats {
    pub submitted: u64,
    pub completed: u64,
    pub gave_up: u64,
    /// Resends beyond the first send, summed over all requests.
    pub retries: u64,
    pub inflight: u32,
}

#[derive(Clone)]
pub struct Middleware(Arc<Inner>);

impl Middleware {
    /// Starts the network tasks; call inside a tokio runtime. `incarnation`
    /// must be random per process start (see [`fresh_incarnation`]).
    pub fn start(cfg: &Config, log: Option<Arc<EventLog>>, incarnation: u32) -> Middleware {
        Middleware::with_net(
            &NetConfig::from_config(cfg),
            RetryPolicy::from_cfg(&cfg.client),
            log,
            incarnation,
        )
    }

    pub fn with_net(
        net: &NetConfig,
        policy: RetryPolicy,
        log: Option<Arc<EventLog>>,
        incarnation: u32,
    ) -> Middleware {
        let window = LatencyWindow::new(policy.min_samples);
        Middleware(Arc::new(Inner {
            net: ClientNet::start(net),
            policy,
            incarnation,
            log,
            window,
            phase: AtomicU8::new(0),
            submitted: AtomicU64::new(0),
            completed: AtomicU64::new(0),
            gave_up: AtomicU64::new(0),
            retries: AtomicU64::new(0),
            inflight: AtomicU32::new(0),
        }))
    }

    /// A simulated user. Cheap to create; owns its own sequence numbers.
    pub fn client(&self, id: ClientId) -> Client {
        Client {
            mw: self.clone(),
            id,
            next_seq: AtomicU64::new(0),
            acks: Mutex::new(AckTracker::default()),
        }
    }

    pub fn set_phase(&self, p: Phase) {
        self.0.phase.store(p as u8, Ordering::Relaxed);
    }

    pub fn phase(&self) -> Phase {
        Phase::from_u8(self.0.phase.load(Ordering::Relaxed))
    }

    pub fn incarnation(&self) -> u32 {
        self.0.incarnation
    }

    pub fn net(&self) -> &ClientNet {
        &self.0.net
    }

    pub fn stats(&self) -> MiddlewareStats {
        let i = &self.0;
        MiddlewareStats {
            submitted: i.submitted.load(Ordering::Relaxed),
            completed: i.completed.load(Ordering::Relaxed),
            gave_up: i.gave_up.load(Ordering::Relaxed),
            retries: i.retries.load(Ordering::Relaxed),
            inflight: i.inflight.load(Ordering::Relaxed),
        }
    }

    /// The timeout the next first attempt will use, before jitter.
    pub fn base_timeout(&self) -> Duration {
        let p = &self.0.policy;
        match self.0.window.p99() {
            Some(p99) => (p99 * p.p99_multiple).clamp(p.min_timeout, p.max_timeout),
            None => p.timeout_initial,
        }
    }

    /// Waits until the connections to all three nodes are up, or the timeout
    /// passes. Requests sent earlier are not lost for good (they are retried)
    /// but would start with a needless timeout.
    pub async fn wait_connected(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            if self.0.net.stats().connected.load(Ordering::Relaxed) == 0b111 {
                return true;
            }
            if Instant::now() >= deadline {
                return false;
            }
            time::sleep(Duration::from_millis(10)).await;
        }
    }

    pub fn shutdown(&self) {
        self.0.net.shutdown();
    }
}

/// A random incarnation number for a starting process (DL-021). Mixes the
/// wall clock, the process ID and a caller-supplied salt.
pub fn fresh_incarnation(salt: u64) -> u32 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let mixed = seed_from(&[nanos, u64::from(std::process::id()), salt]);
    (mixed >> 32) as u32
}

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

pub fn op_name(op: &Op) -> &'static str {
    match op {
        Op::ReportIncident { .. } => "report_incident",
        Op::OfferResource { .. } => "offer_resource",
        Op::UpdateResource { .. } => "update_resource",
        Op::Query(_) => "query",
    }
}

/// Highest `n` such that replies for sequence numbers `1..=n` have all been
/// received. Requests complete out of order when several are outstanding, and
/// nodes may forget everything at or below this value, so it must have no
/// gaps. A request that was given up on leaves a gap on purpose: no reply was
/// received, so its row must stay.
#[derive(Debug, Default)]
pub struct AckTracker {
    watermark: u64,
    above: BTreeSet<u64>,
}

impl AckTracker {
    pub fn complete(&mut self, seq: u64) {
        if seq <= self.watermark {
            return;
        }
        self.above.insert(seq);
        while self.above.remove(&(self.watermark + 1)) {
            self.watermark += 1;
        }
    }

    pub fn watermark(&self) -> u64 {
        self.watermark
    }
}

/// A successful request.
#[derive(Debug, Clone)]
pub struct Completed {
    pub req: ReqId,
    /// The node whose reply arrived first.
    pub from: NodeId,
    pub result: OpResult,
    /// Sends made, including the first.
    pub attempts: u32,
    /// First send to first `Ok` reply, on this process's clock.
    pub latency: Duration,
}

#[derive(Debug)]
pub enum SubmitError {
    /// No `Ok` reply after every allowed attempt.
    GaveUp {
        req: ReqId,
        attempts: u32,
        latency: Duration,
        /// Text of the last `Error` reply, if any arrived.
        last_error: Option<String>,
    },
    Encode(CodecError),
}

impl fmt::Display for SubmitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SubmitError::GaveUp {
                req,
                attempts,
                latency,
                last_error,
            } => write!(
                f,
                "request {req} gave up after {attempts} attempts in {latency:?} ({})",
                last_error.as_deref().unwrap_or("no reply")
            ),
            SubmitError::Encode(e) => write!(f, "cannot encode request: {e}"),
        }
    }
}

impl std::error::Error for SubmitError {}

struct ReqCtx {
    req: Request,
    op: &'static str,
    phase: &'static str,
    started: Instant,
}

/// One simulated user. `submit` takes `&self`, so a user may have several
/// requests outstanding (the open-loop generator does this).
pub struct Client {
    mw: Middleware,
    id: ClientId,
    next_seq: AtomicU64,
    acks: Mutex<AckTracker>,
}

impl Client {
    pub fn id(&self) -> ClientId {
        self.id
    }

    pub fn acked_up_to(&self) -> u64 {
        self.acks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .watermark()
    }

    /// Sends `op` and waits for its result, retrying as described in the
    /// module documentation. Sequence numbers start at 1.
    pub async fn submit(&self, op: Op) -> Result<Completed, SubmitError> {
        let inner = &*self.mw.0;
        let seq = self.next_seq.fetch_add(1, Ordering::Relaxed) + 1;
        let id = ReqId {
            client: self.id,
            incarnation: inner.incarnation,
            seq,
        };
        let ctx = ReqCtx {
            op: op_name(&op),
            phase: self.mw.phase().as_str(),
            req: Request {
                id,
                sent_at_ms: now_ms(),
                op,
            },
            started: Instant::now(),
        };
        inner.submitted.fetch_add(1, Ordering::Relaxed);
        inner.inflight.fetch_add(1, Ordering::Relaxed);
        let mut reg = inner.net.register(id);
        let out = self.drive(&ctx, &mut reg).await;
        inner.inflight.fetch_sub(1, Ordering::Relaxed);
        match &out {
            Ok(c) => {
                inner.completed.fetch_add(1, Ordering::Relaxed);
                inner
                    .retries
                    .fetch_add(u64::from(c.attempts - 1), Ordering::Relaxed);
                self.acks
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .complete(seq);
            }
            Err(SubmitError::GaveUp { attempts, .. }) => {
                inner.gave_up.fetch_add(1, Ordering::Relaxed);
                inner
                    .retries
                    .fetch_add(u64::from(attempts - 1), Ordering::Relaxed);
            }
            Err(SubmitError::Encode(_)) => {}
        }
        out
    }

    async fn drive(&self, ctx: &ReqCtx, reg: &mut Registration) -> Result<Completed, SubmitError> {
        let inner = &*self.mw.0;
        let pol = &inner.policy;
        let id = ctx.req.id;
        let mut rng = Rng::new(seed_from(&[
            u64::from(id.client.machine),
            u64::from(id.client.process),
            u64::from(id.client.user),
            u64::from(id.incarnation),
            id.seq,
        ]));
        let base = self.mw.base_timeout();
        let mut last_error = None;

        for attempt in 0..=pol.max_retries {
            let frame = self.encode(&ctx.req)?;
            inner.net.send_all(&frame);
            self.log_send(ctx, attempt);
            let attempt_start = Instant::now();
            let timeout = pol.attempt_timeout(base, attempt, &mut rng);
            let mut deadline = attempt_start + timeout;
            loop {
                tokio::select! {
                    msg = reg.rx.recv() => {
                        let Some(reply) = msg else { break };
                        match reply.body {
                            ReplyBody::Ok(result) => {
                                let latency = ctx.started.elapsed();
                                let attempt_latency = attempt_start.elapsed();
                                if attempt == 0 {
                                    inner.window.record(attempt_latency.as_micros() as u64);
                                }
                                self.log_reply(ctx, attempt, Some(reply.from), "ok", latency, attempt_latency);
                                return Ok(Completed {
                                    req: id,
                                    from: reply.from,
                                    result,
                                    attempts: attempt + 1,
                                    latency,
                                });
                            }
                            ReplyBody::Busy => {
                                self.log_reply(ctx, attempt, Some(reply.from), "busy", ctx.started.elapsed(), attempt_start.elapsed());
                                deadline = deadline.min(Instant::now() + pol.busy_delay(attempt, &mut rng));
                            }
                            ReplyBody::Error(e) => {
                                self.log_reply(ctx, attempt, Some(reply.from), "error", ctx.started.elapsed(), attempt_start.elapsed());
                                last_error = Some(e);
                                deadline = deadline.min(Instant::now() + pol.busy_delay(attempt, &mut rng));
                            }
                        }
                    }
                    () = time::sleep_until(deadline) => break,
                }
            }
            if attempt < pol.max_retries {
                self.log_retry(ctx, attempt + 1, timeout);
            }
        }

        let latency = ctx.started.elapsed();
        self.log_reply(ctx, pol.max_retries, None, "gave_up", latency, latency);
        Err(SubmitError::GaveUp {
            req: id,
            attempts: pol.max_retries + 1,
            latency,
            last_error,
        })
    }

    fn encode(&self, req: &Request) -> Result<Arc<[u8]>, SubmitError> {
        let msg = ClientMsg::Req(ClientReq {
            version: PROTOCOL_VERSION,
            req: req.clone(),
            rtt_us: self.mw.0.net.rtt_us(),
            acked_up_to: self.acked_up_to(),
        });
        codec::encode_frame(self.mw.0.net.format(), &msg)
            .map(Arc::from)
            .map_err(SubmitError::Encode)
    }

    fn log_send(&self, ctx: &ReqCtx, attempt: u32) {
        if let Some(log) = &self.mw.0.log {
            log.emit(
                log.event(EventKind::ClientSend)
                    .req(&ctx.req.id)
                    .field("attempt", attempt)
                    .field("op", ctx.op)
                    .field("phase", ctx.phase),
            );
        }
    }

    /// `latency_us` is measured from the first send of the request, so the
    /// reply that completes it carries the latency the user experienced;
    /// `attempt_latency_us` is from the latest send. Outcomes: `ok`, `busy`,
    /// `error`, and `gave_up` (no `from`).
    fn log_reply(
        &self,
        ctx: &ReqCtx,
        attempt: u32,
        from: Option<NodeId>,
        outcome: &str,
        latency: Duration,
        attempt_latency: Duration,
    ) {
        if let Some(log) = &self.mw.0.log {
            let mut ev = log
                .event(EventKind::ClientReply)
                .req(&ctx.req.id)
                .field("attempt", attempt)
                .field("outcome", outcome)
                .field("latency_us", latency.as_micros() as u64)
                .field("attempt_latency_us", attempt_latency.as_micros() as u64)
                .field("op", ctx.op)
                .field("phase", ctx.phase);
            if let Some(n) = from {
                ev = ev.field("from", n.0);
            }
            log.emit(ev);
        }
    }

    fn log_retry(&self, ctx: &ReqCtx, attempt: u32, timeout: Duration) {
        if let Some(log) = &self.mw.0.log {
            log.emit(
                log.event(EventKind::ClientRetry)
                    .req(&ctx.req.id)
                    .field("attempt", attempt)
                    .field("timeout_ms", timeout.as_millis() as u64)
                    .field("phase", ctx.phase),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ack_watermark_has_no_gaps() {
        let mut a = AckTracker::default();
        a.complete(2);
        a.complete(3);
        assert_eq!(a.watermark(), 0, "1 is still outstanding");
        a.complete(1);
        assert_eq!(a.watermark(), 3);
        a.complete(5);
        assert_eq!(a.watermark(), 3, "4 is missing");
        a.complete(4);
        assert_eq!(a.watermark(), 5);
        a.complete(2);
        assert_eq!(a.watermark(), 5);
    }

    #[test]
    fn timeouts_grow_and_stay_in_bounds() {
        let p = RetryPolicy::from_cfg(&ClientCfg::default());
        let mut rng = Rng::new(1);
        let base = Duration::from_millis(100);
        for attempt in 0..4 {
            let nominal = 100.0 * 2f64.powi(attempt as i32);
            for _ in 0..200 {
                let t = p.attempt_timeout(base, attempt, &mut rng).as_secs_f64() * 1000.0;
                assert!(
                    t >= nominal * 0.75 - 1e-6 && t <= nominal * 1.25 + 1e-6,
                    "{t}"
                );
            }
        }
        let huge = p.attempt_timeout(Duration::from_secs(20), 4, &mut rng);
        assert_eq!(huge, p.max_timeout);
    }

    #[test]
    fn adaptive_timeout_needs_enough_samples() {
        let w = LatencyWindow::new(200);
        for i in 0..1000u64 {
            w.record(1000 + i % 100);
        }
        let p99 = w.p99().expect("enough samples");
        assert!(p99 >= Duration::from_micros(1090) && p99 <= Duration::from_micros(1100));
        assert!(LatencyWindow::new(200).p99().is_none());
    }
}
