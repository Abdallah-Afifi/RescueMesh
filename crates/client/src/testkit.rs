//! A stand-in for the three cloud nodes, so the client middleware and the load
//! generator can be tested and developed before the real nodes exist.
//!
//! It speaks the real client protocol (length-prefixed `ClientMsg` frames) but
//! contains none of the real logic: the executor of a request is picked from
//! the request hash among the nodes it believes alive, planning is a sleep
//! behind a bounded worker pool, and a silenced node accepts TCP data and
//! drops it, as the real fault injection does (DL-014). It can also write
//! node-side events, so the analysis scripts can be tried end to end.
//!
//! Not a model of the real system: use it to check client behaviour, not to
//! report results.

use crate::eventlog::EventLog;
use proto::codec::{self, WireFormat};
use proto::event::EventKind;
use proto::{
    ClientMsg, ClientReply, ClientReq, NodeId, OpResult, PROTOCOL_VERSION, ReplyBody, ReqId,
};
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tokio::time::{self, Instant};

#[derive(Debug, Clone)]
pub struct StubOpts {
    /// Time one request occupies a worker.
    pub service: Duration,
    /// Requests a node serves at once.
    pub workers: usize,
    /// Waiting plus running requests above which a node replies `Busy`.
    pub queue_cap: usize,
    /// How long after a node goes silent the others stop assigning it work.
    pub detect_delay: Duration,
    /// How long after its silence ends a node is trusted again.
    pub rejoin_delay: Duration,
    pub format: WireFormat,
}

impl Default for StubOpts {
    fn default() -> Self {
        StubOpts {
            service: Duration::from_millis(5),
            workers: 4,
            queue_cap: 256,
            detect_delay: Duration::from_millis(500),
            rejoin_delay: Duration::from_millis(300),
            format: WireFormat::Postcard,
        }
    }
}

#[derive(Default)]
struct NodeState {
    /// `(start, end)` of the current or most recent silence.
    silence: Mutex<Option<(Instant, Instant)>>,
    received: AtomicU64,
    dropped_silent: AtomicU64,
    served: AtomicU64,
    queued: AtomicUsize,
}

struct State {
    opts: StubOpts,
    nodes: [NodeState; 3],
    permits: [Arc<Semaphore>; 3],
    logs: Option<[Arc<EventLog>; 3]>,
    commit_seq: AtomicU64,
    injections: AtomicU64,
    view_no: AtomicU64,
}

impl State {
    fn is_silent(&self, i: usize) -> bool {
        let w = *self.nodes[i]
            .silence
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        matches!(w, Some((s, e)) if Instant::now() >= s && Instant::now() < e)
    }

    /// The other nodes' view: silent long enough to be noticed, and not yet
    /// trusted again after it ends.
    fn excluded(&self, i: usize) -> bool {
        let w = *self.nodes[i]
            .silence
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let now = Instant::now();
        matches!(w, Some((s, e)) if now >= s + self.opts.detect_delay && now < e + self.opts.rejoin_delay)
    }

    fn executor(&self, req: &ReqId) -> usize {
        let h = (proto::hash::request(req) % 3) as usize;
        (0..3)
            .map(|k| (h + k) % 3)
            .find(|&i| !self.excluded(i))
            .unwrap_or(h)
    }

    fn log(&self, i: usize, kind: EventKind) -> Option<proto::event::Event> {
        self.logs.as_ref().map(|l| l[i].event(kind))
    }

    fn emit(&self, i: usize, ev: proto::event::Event) {
        if let Some(l) = &self.logs {
            l[i].emit(ev);
        }
    }
}

pub struct StubCluster {
    state: Arc<State>,
    addrs: [SocketAddr; 3],
    tasks: Vec<JoinHandle<()>>,
}

impl StubCluster {
    /// Three listeners on the given addresses (port 0 picks free ports).
    pub async fn start(
        bind: [SocketAddr; 3],
        opts: StubOpts,
        logs: Option<[Arc<EventLog>; 3]>,
    ) -> io::Result<StubCluster> {
        let workers = opts.workers.max(1);
        let state = Arc::new(State {
            opts,
            nodes: Default::default(),
            permits: [
                Arc::new(Semaphore::new(workers)),
                Arc::new(Semaphore::new(workers)),
                Arc::new(Semaphore::new(workers)),
            ],
            logs,
            commit_seq: AtomicU64::new(0),
            injections: AtomicU64::new(0),
            view_no: AtomicU64::new(1),
        });
        let mut tasks = Vec::new();
        let mut addrs = Vec::new();
        for (i, a) in bind.iter().enumerate() {
            let listener = TcpListener::bind(a).await?;
            addrs.push(listener.local_addr()?);
            tasks.push(tokio::spawn(accept_loop(i, listener, state.clone())));
        }
        Ok(StubCluster {
            state,
            addrs: addrs.try_into().expect("three addresses"),
            tasks,
        })
    }

    /// Three free ports on the loopback interface.
    pub async fn start_local(opts: StubOpts) -> io::Result<StubCluster> {
        let any: SocketAddr = "127.0.0.1:0".parse().expect("literal");
        StubCluster::start([any; 3], opts, None).await
    }

    pub fn addrs(&self) -> [SocketAddr; 3] {
        self.addrs
    }

    /// A configuration pointing at this cluster (heartbeat ports are unused).
    pub fn config_text(&self, extra: &str) -> String {
        let mut s = String::from("[run]\nrun_id = \"stub\"\n");
        s.push_str(extra);
        for (i, a) in self.addrs.iter().enumerate() {
            s.push_str(&format!(
                "[[cluster.nodes]]\nid = {}\nhost = \"{}\"\nport = {}\nheartbeat_port = 9\n",
                i + 1,
                a.ip(),
                a.port()
            ));
        }
        s
    }

    /// Makes a node ignore all traffic for `duration`, starting now.
    pub fn silence(&self, node: NodeId, duration: Duration) {
        let i = node.index();
        let now = Instant::now();
        *self.state.nodes[i]
            .silence
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = Some((now, now + duration));
        let st = self.state.clone();
        let k = st.injections.fetch_add(1, Ordering::Relaxed) + 1;
        tokio::spawn(async move {
            if let Some(ev) = st.log(i, EventKind::InjectStart) {
                st.emit(
                    i,
                    ev.field("k", k)
                        .field("victim", i as u64 + 1)
                        .field("duration_ms", duration.as_millis() as u64)
                        .field("by_operator", true),
                );
            }
            time::sleep(st.opts.detect_delay.min(duration)).await;
            let view = st.view_no.fetch_add(1, Ordering::Relaxed) + 1;
            let active: Vec<u64> = (1..=3u64).filter(|&n| n != i as u64 + 1).collect();
            if let Some(ev) = st.log((i + 1) % 3, EventKind::ViewChange) {
                st.emit((i + 1) % 3, ev.field("view", view).field("active", active));
            }
            time::sleep(duration.saturating_sub(st.opts.detect_delay)).await;
            if let Some(ev) = st.log(i, EventKind::InjectEnd) {
                st.emit(i, ev.field("k", k));
            }
            time::sleep(st.opts.rejoin_delay).await;
            let view = st.view_no.fetch_add(1, Ordering::Relaxed) + 1;
            if let Some(ev) = st.log(i, EventKind::Active) {
                st.emit(
                    i,
                    ev.field("applied", st.commit_seq.load(Ordering::Relaxed))
                        .field("view", view),
                );
            }
        });
    }

    /// Request frames a node has received while not silenced.
    pub fn received(&self, node: NodeId) -> u64 {
        self.state.nodes[node.index()]
            .received
            .load(Ordering::Relaxed)
    }

    /// Frames a node threw away while silenced.
    pub fn dropped_silent(&self, node: NodeId) -> u64 {
        self.state.nodes[node.index()]
            .dropped_silent
            .load(Ordering::Relaxed)
    }

    /// Requests a node has executed.
    pub fn served(&self, node: NodeId) -> u64 {
        self.state.nodes[node.index()]
            .served
            .load(Ordering::Relaxed)
    }

    pub fn shutdown(&self) {
        for t in &self.tasks {
            t.abort();
        }
    }
}

impl Drop for StubCluster {
    fn drop(&mut self) {
        self.shutdown();
    }
}

async fn accept_loop(i: usize, listener: TcpListener, st: Arc<State>) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            return;
        };
        let _ = stream.set_nodelay(true);
        tokio::spawn(serve_conn(i, stream, st.clone()));
    }
}

async fn serve_conn(i: usize, stream: TcpStream, st: Arc<State>) {
    let (rd, wr) = stream.into_split();
    let wr = Arc::new(tokio::sync::Mutex::new(wr));
    let mut rd: BufReader<OwnedReadHalf> = BufReader::new(rd);
    let mut prefix = [0u8; 4];
    loop {
        if rd.read_exact(&mut prefix).await.is_err() {
            return;
        }
        let Ok(len) = codec::frame_len(prefix) else {
            return;
        };
        let mut body = vec![0u8; len];
        if rd.read_exact(&mut body).await.is_err() {
            return;
        }
        let Ok(ClientMsg::Req(creq)) = codec::decode::<ClientMsg>(st.opts.format, &body) else {
            continue;
        };
        if st.is_silent(i) {
            st.nodes[i].dropped_silent.fetch_add(1, Ordering::Relaxed);
            continue;
        }
        st.nodes[i].received.fetch_add(1, Ordering::Relaxed);
        if creq.version != PROTOCOL_VERSION {
            continue;
        }
        // Like a real node, every node whose queue is full refuses at once,
        // before any election; the others take part as usual.
        if st.nodes[i].queued.load(Ordering::Relaxed) >= st.opts.queue_cap {
            tokio::spawn(answer(
                i,
                creq.req.id,
                ReplyBody::Busy,
                st.clone(),
                wr.clone(),
            ));
            continue;
        }
        if st.executor(&creq.req.id) != i {
            continue;
        }
        tokio::spawn(execute(i, creq, st.clone(), wr.clone()));
    }
}

async fn execute(
    i: usize,
    creq: ClientReq,
    st: Arc<State>,
    wr: Arc<tokio::sync::Mutex<OwnedWriteHalf>>,
) {
    let node = &st.nodes[i];
    let id = creq.req.id;
    let queued = node.queued.fetch_add(1, Ordering::Relaxed) + 1;
    let body = if queued > st.opts.queue_cap {
        node.queued.fetch_sub(1, Ordering::Relaxed);
        ReplyBody::Busy
    } else {
        let Ok(_permit) = st.permits[i].clone().acquire_owned().await else {
            return;
        };
        let t = Instant::now();
        time::sleep(st.opts.service).await;
        node.queued.fetch_sub(1, Ordering::Relaxed);
        node.served.fetch_add(1, Ordering::Relaxed);
        if let Some(ev) = st.log(i, EventKind::PlanEnd) {
            st.emit(
                i,
                ev.req(&id)
                    .field("service_us", t.elapsed().as_micros() as u64)
                    .field("queue_len", queued as u64),
            );
        }
        if creq.req.op.is_write() {
            let seq = st.commit_seq.fetch_add(1, Ordering::Relaxed) + 1;
            if let Some(ev) = st.log(i, EventKind::Commit) {
                st.emit(i, ev.req(&id).field("seq", seq));
            }
        }
        ReplyBody::Ok(OpResult::Accepted)
    };
    answer(i, id, body, st, wr).await;
}

async fn answer(
    i: usize,
    id: ReqId,
    body: ReplyBody,
    st: Arc<State>,
    wr: Arc<tokio::sync::Mutex<OwnedWriteHalf>>,
) {
    // A silenced node sends nothing, even for work it finished.
    if st.is_silent(i) {
        return;
    }
    let reply = ClientMsg::Reply(ClientReply {
        req: id,
        from: NodeId(i as u8 + 1),
        body,
    });
    if let Ok(frame) = codec::encode_frame(st.opts.format, &reply) {
        let mut w = wr.lock().await;
        let _ = w.write_all(&frame).await;
    }
}
