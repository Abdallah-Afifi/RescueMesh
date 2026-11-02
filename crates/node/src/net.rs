//! TCP transport of a cloud node (workstream B; design log DL-014, DL-054).
//!
//! One listening port per node serves three kinds of connection, told apart
//! by their first four bytes:
//!
//! | First bytes | Who | Then |
//! |---|---|---|
//! | `RMP1` + node id | a peer node | length-prefixed [`Envelope`] frames |
//! | `RMO1` | the operator tool `rm-ctl` | one text command per line |
//! | anything else | a client process | length-prefixed [`ClientMsg`] frames |
//!
//! A client frame starts with a 4-byte big-endian length of at most 16 MiB,
//! so its first byte is 0x00 or 0x01 and can never be `R` (0x52); clients
//! need no preamble and `crates/client` is unchanged.
//!
//! **Peers.** Each node dials each peer for its own outgoing messages, so a
//! pair of nodes uses two connections, one per direction. A writer thread
//! per peer owns the outgoing connection, reconnects with back-off, and
//! sends what the peer's [`Link`] says is due: new messages, retransmissions
//! and bare acknowledgments. Inbound connections are read by one thread
//! each; envelopes go through the same [`Link`] (acknowledgments and
//! duplicate suppression) and the bodies to the core task.
//!
//! **Clients.** One reader and one writer thread per client connection.
//! Replies are routed by the request's `ClientId` to the connection that
//! last carried a request from that user (DL-039: all users of a client
//! process share one connection per node).
//!
//! **Silence switch.** While [`Net::set_silenced`] is on, this process
//! ignores all traffic: inbound peer envelopes and client requests are read
//! from the socket and dropped before they are acknowledged or delivered,
//! nothing is sent to peers or clients, and new outgoing messages are
//! discarded. TCP connections stay open, so to the other nodes and the
//! clients the process looks exactly like one that has stopped responding
//! (protocol Section 8). The heartbeat thread checks the same switch.

use crate::link::{Link, LinkCfg, LinkStats};
use proto::codec::{self, WireFormat};
use proto::config::Config;
use proto::{
    ClientId, ClientMsg, ClientReply, ClientReq, Envelope, Heartbeat, NodeId, NodeMsg,
    PROTOCOL_VERSION, PlanOutput, ReplyBody, ReqId, SilenceTarget,
};
use std::collections::{BTreeMap, HashMap};
use std::io::{self, BufRead, BufReader, BufWriter, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, Sender, SyncSender};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread;
use std::time::{Duration, Instant};

pub const PEER_PREAMBLE: &[u8; 4] = b"RMP1";
pub const OPERATOR_PREAMBLE: &[u8; 4] = b"RMO1";

/// Replies queued per client connection before new ones are dropped (the
/// client retries).
const CLIENT_QUEUE: usize = 8_192;

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// Monotonic milliseconds since the node started, shared by every thread so
/// all components agree on "now".
#[derive(Debug, Clone, Copy)]
pub struct Clock(Instant);

impl Clock {
    pub fn new() -> Clock {
        Clock(Instant::now())
    }
    pub fn now_ms(&self) -> u64 {
        self.0.elapsed().as_millis() as u64
    }
}

impl Default for Clock {
    fn default() -> Self {
        Clock::new()
    }
}

/// Everything that reaches the core task. Produced by the transport, the
/// heartbeat thread, the planning pool and the operator channel.
pub enum Input {
    Peer {
        from: NodeId,
        msg: NodeMsg,
    },
    Client(ClientReq),
    Heartbeat(Heartbeat),
    /// The failure detector changed its opinion of `peer`.
    Suspect {
        peer: NodeId,
        reading: crate::fd::Reading,
    },
    Planned {
        req: ReqId,
        plan: PlanOutput,
        /// Time spent planning, microseconds.
        service_us: u32,
    },
    Operator(OpCmd),
}

pub enum OpCmd {
    Status(Sender<String>),
    Silence {
        target: SilenceTarget,
        duration_ms: u64,
        reply: Sender<String>,
    },
}

/// Cumulative counters, for the overhead and network measurements.
#[derive(Debug, Default)]
pub struct NetStats {
    pub tcp_rx_bytes: AtomicU64,
    pub tcp_tx_bytes: AtomicU64,
    pub udp_rx_bytes: AtomicU64,
    pub udp_tx_bytes: AtomicU64,
    pub heartbeats_tx: AtomicU64,
    pub heartbeats_rx: AtomicU64,
    pub client_reqs: AtomicU64,
    pub client_replies: AtomicU64,
    pub client_reply_dropped: AtomicU64,
    pub client_conns: AtomicU64,
    pub peer_connects: AtomicU64,
    pub decode_errors: AtomicU64,
    pub version_errors: AtomicU64,
    /// Inbound frames or datagrams dropped because this node was silenced.
    pub dropped_silenced: AtomicU64,
}

fn add(c: &AtomicU64, n: u64) {
    c.fetch_add(n, Ordering::Relaxed);
}

struct PeerLink {
    id: NodeId,
    addr: String,
    link: Mutex<Link>,
    cv: Condvar,
}

struct Clients {
    next: AtomicU64,
    conns: Mutex<HashMap<u64, SyncSender<ClientReply>>>,
    route: Mutex<HashMap<ClientId, u64>>,
}

/// The node's transport. Cheap to share: every method takes `&self`.
pub struct Net {
    me: NodeId,
    format: WireFormat,
    clock: Clock,
    silenced: AtomicBool,
    stats: NetStats,
    peers: BTreeMap<NodeId, Arc<PeerLink>>,
    clients: Clients,
    core: Mutex<Sender<Input>>,
    local: SocketAddr,
}

impl Net {
    /// Binds the node's TCP port and starts the acceptor and one writer
    /// thread per peer.
    pub fn start(
        cfg: &Config,
        me: NodeId,
        incarnation: u32,
        clock: Clock,
        core: Sender<Input>,
    ) -> io::Result<Arc<Net>> {
        let my = cfg
            .node(me)
            .ok_or_else(|| io::Error::other(format!("{me} is not in the cluster config")))?;
        // Bind every interface: the configured host is how the others reach
        // this machine, which may be one of several addresses.
        let listener = TcpListener::bind(("0.0.0.0", my.port))?;
        let local = listener.local_addr()?;
        let lcfg = LinkCfg::from_timeouts(&cfg.timeouts);
        let peers = cfg
            .cluster
            .nodes
            .iter()
            .filter(|n| n.id != me)
            .map(|n| {
                let p = PeerLink {
                    id: n.id,
                    addr: format!("{}:{}", n.host, n.port),
                    link: Mutex::new(Link::new(me, incarnation, lcfg.clone())),
                    cv: Condvar::new(),
                };
                (n.id, Arc::new(p))
            })
            .collect();
        let net = Arc::new(Net {
            me,
            format: cfg.run.wire_format,
            clock,
            silenced: AtomicBool::new(false),
            stats: NetStats::default(),
            peers,
            clients: Clients {
                next: AtomicU64::new(1),
                conns: Mutex::new(HashMap::new()),
                route: Mutex::new(HashMap::new()),
            },
            core: Mutex::new(core),
            local,
        });
        {
            let net = net.clone();
            thread::Builder::new()
                .name(format!("accept-{me}"))
                .spawn(move || net.accept_loop(listener))?;
        }
        for p in net.peers.values() {
            let (net, p) = (net.clone(), p.clone());
            thread::Builder::new()
                .name(format!("to-{}", p.id))
                .spawn(move || net.writer_loop(&p))?;
        }
        Ok(net)
    }

    pub fn me(&self) -> NodeId {
        self.me
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local
    }

    pub fn stats(&self) -> &NetStats {
        &self.stats
    }

    /// Summed over all peer links.
    pub fn link_stats(&self) -> LinkStats {
        let mut s = LinkStats::default();
        for p in self.peers.values() {
            let l = lock(&p.link).stats();
            s.sent_new += l.sent_new;
            s.retransmitted += l.retransmitted;
            s.bare_acks += l.bare_acks;
            s.delivered += l.delivered;
            s.duplicates += l.duplicates;
            s.fillers_made += l.fillers_made;
        }
        s
    }

    /// Unacknowledged messages to `peer`, fillers included.
    pub fn unacked(&self, peer: NodeId) -> usize {
        self.peers.get(&peer).map_or(0, |p| lock(&p.link).unacked())
    }

    fn to_core(&self, i: Input) {
        let _ = lock(&self.core).send(i);
    }

    // ------------------------------------------------------------ outbound API

    /// Queues a message to a peer; acknowledgment and retransmission happen
    /// inside. Dropped while silenced.
    pub fn send(&self, to: NodeId, msg: NodeMsg) {
        if self.is_silenced() {
            return;
        }
        if let Some(p) = self.peers.get(&to) {
            lock(&p.link).push(msg);
            p.cv.notify_one();
        }
    }

    /// Answers the client that sent `req`. Dropped while silenced, or if the
    /// client's connection is gone or backed up (the client retries).
    pub fn reply(&self, req: ReqId, body: ReplyBody) {
        if self.is_silenced() {
            return;
        }
        let conn = lock(&self.clients.route).get(&req.client).copied();
        let tx = conn.and_then(|c| lock(&self.clients.conns).get(&c).cloned());
        let sent = tx.is_some_and(|tx| {
            tx.try_send(ClientReply {
                req,
                from: self.me,
                body,
            })
            .is_ok()
        });
        add(
            if sent {
                &self.stats.client_replies
            } else {
                &self.stats.client_reply_dropped
            },
            1,
        );
    }

    /// The fault-injection switch (protocol Section 8).
    pub fn set_silenced(&self, on: bool) {
        let was = self.silenced.swap(on, Ordering::SeqCst);
        if was && !on {
            // Resend what is still unacknowledged at once instead of waiting
            // for the backed-off retransmission timer.
            for p in self.peers.values() {
                lock(&p.link).kick();
                p.cv.notify_one();
            }
        }
    }

    pub fn is_silenced(&self) -> bool {
        self.silenced.load(Ordering::SeqCst)
    }

    /// The committed view changed: stop replaying content to peers that
    /// left it (DL-014: retransmit until acknowledged or the peer leaves).
    pub fn set_view(&self, active: &[NodeId]) {
        for p in self.peers.values() {
            if !active.contains(&p.id) {
                lock(&p.link).drop_content();
            }
        }
    }

    /// The peer's process restarted (new heartbeat incarnation).
    pub fn peer_restarted(&self, peer: NodeId) {
        if let Some(p) = self.peers.get(&peer) {
            lock(&p.link).peer_restarted();
            p.cv.notify_one();
        }
    }

    // ------------------------------------------------------------ peer writer

    fn writer_loop(&self, p: &PeerLink) {
        let mut backoff = Duration::from_millis(50);
        loop {
            let stream = match connect(&p.addr) {
                Ok(s) => s,
                Err(_) => {
                    thread::sleep(backoff);
                    backoff = (backoff * 2).min(Duration::from_secs(1));
                    continue;
                }
            };
            backoff = Duration::from_millis(50);
            add(&self.stats.peer_connects, 1);
            let _ = self.write_stream(p, stream);
            // The connection failed; reconnect.
        }
    }

    fn write_stream(&self, p: &PeerLink, stream: TcpStream) -> io::Result<()> {
        stream.set_nodelay(true)?;
        stream.set_write_timeout(Some(Duration::from_secs(2)))?;
        let mut w = BufWriter::with_capacity(1 << 16, stream);
        w.write_all(PEER_PREAMBLE)?;
        w.write_all(&[self.me.0])?;
        w.flush()?;
        lock(&p.link).kick();
        loop {
            let envs = {
                let mut g = lock(&p.link);
                loop {
                    let now = self.clock.now_ms();
                    if !self.is_silenced() {
                        let e = g.poll(now);
                        if !e.is_empty() {
                            break e;
                        }
                    }
                    let wait = if self.is_silenced() {
                        20
                    } else {
                        g.next_due()
                            .map_or(50, |t| t.saturating_sub(now).clamp(1, 50))
                    };
                    g =
                        p.cv.wait_timeout(g, Duration::from_millis(wait))
                            .unwrap_or_else(|e| e.into_inner())
                            .0;
                }
            };
            for env in &envs {
                match codec::encode_frame(self.format, env) {
                    Ok(frame) => {
                        w.write_all(&frame)?;
                        add(&self.stats.tcp_tx_bytes, frame.len() as u64);
                    }
                    Err(e) => eprintln!("{}: cannot encode message to {}: {e}", self.me, p.id),
                }
            }
            w.flush()?;
        }
    }

    // ------------------------------------------------------------ inbound

    fn accept_loop(self: Arc<Self>, listener: TcpListener) {
        for stream in listener.incoming() {
            let Ok(stream) = stream else { continue };
            let net = self.clone();
            let _ = thread::Builder::new()
                .name("conn".into())
                .spawn(move || net.serve(stream));
        }
    }

    fn serve(self: &Arc<Self>, stream: TcpStream) {
        let _ = stream.set_nodelay(true);
        let mut r = BufReader::with_capacity(1 << 16, Counting(&stream, &self.stats.tcp_rx_bytes));
        let mut first = [0u8; 4];
        if r.read_exact(&mut first).is_err() {
            return;
        }
        match &first {
            p if p == PEER_PREAMBLE => {
                let mut id = [0u8; 1];
                if r.read_exact(&mut id).is_ok() {
                    self.serve_peer(NodeId(id[0]), r);
                }
            }
            p if p == OPERATOR_PREAMBLE => self.serve_operator(&stream, r),
            _ => self.serve_client(&stream, first, r),
        }
        let _ = stream.shutdown(Shutdown::Both);
    }

    fn serve_peer<R: Read>(&self, from: NodeId, mut r: R) {
        let Some(p) = self.peers.get(&from).cloned() else {
            eprintln!(
                "{}: connection claims to be from unknown node {from}",
                self.me
            );
            return;
        };
        loop {
            let env: Envelope = match codec::read_frame(self.format, &mut r) {
                Ok(e) => e,
                Err(codec::CodecError::Decode(e)) => {
                    // The frame boundary is intact, so the stream can go on.
                    add(&self.stats.decode_errors, 1);
                    eprintln!("{}: bad frame from {from}: {e}", self.me);
                    continue;
                }
                Err(_) => return,
            };
            if self.is_silenced() {
                add(&self.stats.dropped_silenced, 1);
                continue;
            }
            if env.version != PROTOCOL_VERSION {
                add(&self.stats.version_errors, 1);
                continue;
            }
            if env.from != from {
                add(&self.stats.decode_errors, 1);
                continue;
            }
            let now = self.clock.now_ms();
            let body = lock(&p.link).on_receive(&env, now);
            p.cv.notify_one(); // an acknowledgment may now be owed
            if let Some(msg) = body {
                self.to_core(Input::Peer { from, msg });
            }
        }
    }

    fn serve_client<R: Read>(self: &Arc<Self>, stream: &TcpStream, prefix: [u8; 4], mut r: R) {
        let id = self.clients.next.fetch_add(1, Ordering::Relaxed);
        add(&self.stats.client_conns, 1);
        let (tx, rx) = mpsc::sync_channel::<ClientReply>(CLIENT_QUEUE);
        let Ok(ws) = stream.try_clone() else { return };
        let writer = {
            let net = self.clone();
            thread::Builder::new()
                .name(format!("client-{id}"))
                .spawn(move || client_writer(&net, ws, rx))
        };
        if writer.is_err() {
            return;
        }
        lock(&self.clients.conns).insert(id, tx);

        let mut next_prefix = Some(prefix);
        loop {
            let pre = match next_prefix.take() {
                Some(p) => p,
                None => {
                    let mut p = [0u8; 4];
                    if r.read_exact(&mut p).is_err() {
                        break;
                    }
                    p
                }
            };
            let Ok(len) = codec::frame_len(pre) else {
                break;
            };
            let mut body = vec![0u8; len];
            if r.read_exact(&mut body).is_err() {
                break;
            }
            if self.is_silenced() {
                add(&self.stats.dropped_silenced, 1);
                continue;
            }
            match codec::decode::<ClientMsg>(self.format, &body) {
                Ok(ClientMsg::Req(req)) => {
                    add(&self.stats.client_reqs, 1);
                    lock(&self.clients.route).insert(req.req.id.client, id);
                    self.to_core(Input::Client(req));
                }
                Ok(ClientMsg::Reply(_)) => {}
                Err(_) => add(&self.stats.decode_errors, 1),
            }
        }
        lock(&self.clients.conns).remove(&id);
    }

    /// Operator commands, one per line (`rm-ctl`):
    /// `status`, or `silence <1|2|3|coordinator|rank1> <duration_ms>`.
    /// Answered even while silenced, so the demo can watch a silenced node;
    /// a silenced node refuses `silence`.
    fn serve_operator<R: Read>(&self, stream: &TcpStream, r: R) {
        let Ok(mut w) = stream.try_clone() else {
            return;
        };
        for line in BufReader::new(r).lines() {
            let Ok(line) = line else { return };
            let answer = self.operator(line.trim());
            if writeln!(w, "{answer}").is_err() {
                return;
            }
        }
    }

    fn operator(&self, line: &str) -> String {
        let (tx, rx) = mpsc::channel();
        let words: Vec<&str> = line.split_whitespace().collect();
        let cmd = match words.as_slice() {
            ["status"] => OpCmd::Status(tx),
            ["silence", target, ms] => {
                let Some(target) = parse_target(target) else {
                    return format!("error: unknown target {target:?}");
                };
                let Ok(duration_ms) = ms.parse::<u64>() else {
                    return format!("error: bad duration {ms:?}");
                };
                if duration_ms == 0 || duration_ms > 20_000 {
                    return "error: duration must be 1..=20000 ms".into();
                }
                OpCmd::Silence {
                    target,
                    duration_ms,
                    reply: tx,
                }
            }
            _ => return format!("error: unknown command {line:?}"),
        };
        self.to_core(Input::Operator(cmd));
        answer_within(&rx, Duration::from_secs(2))
    }
}

fn answer_within(rx: &Receiver<String>, t: Duration) -> String {
    rx.recv_timeout(t)
        .unwrap_or_else(|_| "error: the node did not answer".into())
}

/// `1`, `2`, `3`, `coordinator` or `rank1`.
pub fn parse_target(s: &str) -> Option<SilenceTarget> {
    match s {
        "coordinator" => Some(SilenceTarget::Coordinator),
        "rank1" => Some(SilenceTarget::CurrentRank1),
        _ => s
            .parse::<u8>()
            .ok()
            .map(NodeId)
            .filter(|n| n.is_valid())
            .map(SilenceTarget::Node),
    }
}

fn client_writer(net: &Net, stream: TcpStream, rx: Receiver<ClientReply>) {
    let _ = stream.set_write_timeout(Some(Duration::from_secs(5)));
    write_replies(net, &stream, &rx);
    // On every exit, including a write error or timeout, close both
    // directions: the reader then sees the connection end and the client
    // reconnects. Otherwise the node would keep executing this client's
    // requests but never answer them again (DL-064).
    let _ = stream.shutdown(Shutdown::Both);
}

fn write_replies(net: &Net, stream: &TcpStream, rx: &Receiver<ClientReply>) {
    let mut w = BufWriter::with_capacity(1 << 16, stream);
    while let Ok(first) = rx.recv() {
        let mut next = Some(first);
        while let Some(reply) = next.take() {
            match codec::encode_frame(net.format, &ClientMsg::Reply(reply)) {
                Ok(f) => {
                    if w.write_all(&f).is_err() {
                        return;
                    }
                    add(&net.stats.tcp_tx_bytes, f.len() as u64);
                }
                Err(e) => eprintln!("{}: cannot encode reply: {e}", net.me),
            }
            next = rx.try_recv().ok();
        }
        if w.flush().is_err() {
            return;
        }
    }
}

fn connect(addr: &str) -> io::Result<TcpStream> {
    let mut last = io::Error::other(format!("{addr} did not resolve"));
    for a in addr.to_socket_addrs()? {
        match TcpStream::connect_timeout(&a, Duration::from_secs(1)) {
            Ok(s) => return Ok(s),
            Err(e) => last = e,
        }
    }
    Err(last)
}

/// Counts bytes read from a stream.
struct Counting<'a>(&'a TcpStream, &'a AtomicU64);

impl Read for Counting<'_> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = (&mut &*self.0).read(buf)?;
        add(self.1, n as u64);
        Ok(n)
    }
}

impl proto::Transport for Net {
    fn send(&self, to: NodeId, body: NodeMsg) {
        Net::send(self, to, body);
    }
    fn broadcast(&self, body: NodeMsg) {
        for to in NodeId::ALL.into_iter().filter(|&n| n != self.me) {
            Net::send(self, to, body.clone());
        }
    }
    fn set_silenced(&self, silenced: bool) {
        Net::set_silenced(self, silenced);
    }
    fn is_silenced(&self) -> bool {
        Net::is_silenced(self)
    }
}
