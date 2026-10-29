//! Client-side transport: one TCP connection per cloud node, shared by every
//! simulated user in the process, plus round-trip-time probing.
//!
//! "Multicast" is one send to each of the three addresses (design log DL-014;
//! IP multicast is not used). Replies are matched to waiting requests by
//! `ReqId`. A node that is silenced by fault injection keeps its TCP
//! connection open and simply never answers, so the client needs no special
//! handling for it: the other nodes answer and retries cover the rest.
//!
//! **RTT.** The protocol needs the client's round-trip time to each node
//! (`ClientReq::rtt_us`), but replies come from only one node, so reply times
//! cannot measure the other two, and they include planning time anyway. We
//! time a fresh TCP handshake to each node every `probe_interval` and keep an
//! exponentially weighted average. The handshake is completed by the node's
//! kernel, so it measures the network path and is not affected by the node's
//! CPU load, which is what the selection score's communication-delay term
//! wants (spec p. 3).

use crate::rng::Rng;
use proto::codec::{self, WireFormat};
use proto::config::Config;
use proto::{ClientMsg, ClientReply, NodeId, ReqId};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU8, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::net::tcp::OwnedReadHalf;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time;

#[derive(Debug, Clone)]
pub struct NetConfig {
    /// `host:port` per node, indexed by [`NodeId::index`].
    pub nodes: [String; 3],
    pub format: WireFormat,
    /// Zero disables RTT probing.
    pub probe_interval: Duration,
    pub connect_timeout: Duration,
    /// A write that takes longer than this drops the connection and
    /// reconnects (the peer has stopped reading).
    pub write_timeout: Duration,
    /// Frames queued per node while its connection is busy or down; beyond
    /// this, frames are dropped and counted. Retries make that harmless.
    pub queue_cap: usize,
}

impl NetConfig {
    pub fn from_config(cfg: &Config) -> NetConfig {
        let addr = |id: NodeId| {
            cfg.node(id)
                .map(|n| format!("{}:{}", n.host, n.port))
                .unwrap_or_default()
        };
        NetConfig {
            nodes: [addr(NodeId(1)), addr(NodeId(2)), addr(NodeId(3))],
            format: cfg.run.wire_format,
            probe_interval: Duration::from_secs(2),
            connect_timeout: Duration::from_secs(2),
            write_timeout: Duration::from_secs(5),
            queue_cap: 8192,
        }
    }
}

#[derive(Debug, Default)]
pub struct NetStats {
    pub tx_frames: AtomicU64,
    pub tx_bytes: AtomicU64,
    pub rx_frames: AtomicU64,
    pub rx_bytes: AtomicU64,
    pub reconnects: AtomicU64,
    /// Frames not sent because a node's queue was full.
    pub send_dropped: AtomicU64,
    pub decode_errors: AtomicU64,
    /// Bit `i` set while the connection to node `i + 1` is up.
    pub connected: AtomicU8,
}

struct Shared {
    format: WireFormat,
    waiters: Mutex<HashMap<ReqId, mpsc::UnboundedSender<ClientReply>>>,
    rtt_us: [AtomicU32; 3],
    stats: NetStats,
}

impl Shared {
    fn dispatch(&self, reply: ClientReply) {
        let tx = self
            .waiters
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&reply.req)
            .cloned();
        if let Some(tx) = tx {
            let _ = tx.send(reply);
        }
    }

    fn update_rtt(&self, node: usize, sample_us: u32) {
        let slot = &self.rtt_us[node];
        let old = slot.load(Ordering::Relaxed);
        let new = if old == 0 {
            sample_us
        } else {
            ((u64::from(old) * 3 + u64::from(sample_us)) / 4) as u32
        };
        slot.store(new.max(1), Ordering::Relaxed);
    }

    fn set_connected(&self, node: usize, up: bool) {
        let bit = 1u8 << node;
        if up {
            self.stats.connected.fetch_or(bit, Ordering::Relaxed);
        } else {
            self.stats.connected.fetch_and(!bit, Ordering::Relaxed);
        }
    }
}

/// A request's place in the reply dispatch table. Replies from any node for
/// this request arrive on `rx`. Dropping it removes the entry.
pub struct Registration {
    shared: Arc<Shared>,
    id: ReqId,
    pub rx: mpsc::UnboundedReceiver<ClientReply>,
}

impl Drop for Registration {
    fn drop(&mut self) {
        self.shared
            .waiters
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.id);
    }
}

pub struct ClientNet {
    shared: Arc<Shared>,
    out: [mpsc::Sender<Arc<[u8]>>; 3],
    tasks: Mutex<Vec<JoinHandle<()>>>,
}

impl ClientNet {
    /// Starts the connection and probe tasks. Must be called inside a tokio
    /// runtime. Connections are made in the background; requests sent before
    /// they are up are dropped and recovered by the retry logic.
    pub fn start(cfg: &NetConfig) -> ClientNet {
        let shared = Arc::new(Shared {
            format: cfg.format,
            waiters: Mutex::new(HashMap::new()),
            rtt_us: [AtomicU32::new(0), AtomicU32::new(0), AtomicU32::new(0)],
            stats: NetStats::default(),
        });
        let mut tasks = Vec::new();
        let mut senders = Vec::new();
        for i in 0..3 {
            let (tx, rx) = mpsc::channel::<Arc<[u8]>>(cfg.queue_cap);
            senders.push(tx);
            tasks.push(tokio::spawn(connection_loop(
                i,
                cfg.nodes[i].clone(),
                shared.clone(),
                rx,
                cfg.connect_timeout,
                cfg.write_timeout,
            )));
            if !cfg.probe_interval.is_zero() {
                tasks.push(tokio::spawn(probe_loop(
                    i,
                    cfg.nodes[i].clone(),
                    shared.clone(),
                    cfg.probe_interval,
                    cfg.connect_timeout,
                )));
            }
        }
        let out: [mpsc::Sender<Arc<[u8]>>; 3] = senders.try_into().expect("three senders");
        ClientNet {
            shared,
            out,
            tasks: Mutex::new(tasks),
        }
    }

    pub fn format(&self) -> WireFormat {
        self.shared.format
    }

    pub fn register(&self, id: ReqId) -> Registration {
        let (tx, rx) = mpsc::unbounded_channel();
        self.shared
            .waiters
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(id, tx);
        Registration {
            shared: self.shared.clone(),
            id,
            rx,
        }
    }

    /// Queues one frame to every node (the multicast). Returns how many nodes
    /// accepted it.
    pub fn send_all(&self, frame: &Arc<[u8]>) -> usize {
        let mut accepted = 0;
        for tx in &self.out {
            if tx.try_send(frame.clone()).is_ok() {
                accepted += 1;
            } else {
                self.shared
                    .stats
                    .send_dropped
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
        accepted
    }

    /// Latest RTT estimate per node in microseconds; `None` until measured.
    pub fn rtt_us(&self) -> [Option<u32>; 3] {
        let get = |i: usize| match self.shared.rtt_us[i].load(Ordering::Relaxed) {
            0 => None,
            v => Some(v),
        };
        [get(0), get(1), get(2)]
    }

    pub fn stats(&self) -> &NetStats {
        &self.shared.stats
    }

    /// Stops all background tasks.
    pub fn shutdown(&self) {
        for t in self
            .tasks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .drain(..)
        {
            t.abort();
        }
    }
}

impl Drop for ClientNet {
    fn drop(&mut self) {
        self.shutdown();
    }
}

async fn connection_loop(
    node: usize,
    addr: String,
    shared: Arc<Shared>,
    mut out_rx: mpsc::Receiver<Arc<[u8]>>,
    connect_timeout: Duration,
    write_timeout: Duration,
) {
    let initial = Duration::from_millis(50);
    let mut backoff = initial;
    let mut first = true;
    loop {
        let stream = match time::timeout(connect_timeout, TcpStream::connect(&addr)).await {
            Ok(Ok(s)) => s,
            _ => {
                time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(1));
                continue;
            }
        };
        let _ = stream.set_nodelay(true);
        backoff = initial;
        if !first {
            shared.stats.reconnects.fetch_add(1, Ordering::Relaxed);
        }
        first = false;
        // Frames queued while the connection was down are stale: their
        // requests have been retried since.
        while out_rx.try_recv().is_ok() {}
        shared.set_connected(node, true);
        let (rd, mut wr) = stream.into_split();
        let mut reader = tokio::spawn(read_loop(rd, shared.clone()));
        loop {
            tokio::select! {
                frame = out_rx.recv() => {
                    let Some(frame) = frame else {
                        reader.abort();
                        return;
                    };
                    match time::timeout(write_timeout, wr.write_all(&frame)).await {
                        Ok(Ok(())) => {
                            shared.stats.tx_frames.fetch_add(1, Ordering::Relaxed);
                            shared.stats.tx_bytes.fetch_add(frame.len() as u64, Ordering::Relaxed);
                        }
                        _ => break,
                    }
                }
                _ = &mut reader => break,
            }
        }
        reader.abort();
        shared.set_connected(node, false);
    }
}

async fn read_loop(rd: OwnedReadHalf, shared: Arc<Shared>) {
    let mut rd = BufReader::with_capacity(1 << 16, rd);
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
        shared.stats.rx_frames.fetch_add(1, Ordering::Relaxed);
        shared
            .stats
            .rx_bytes
            .fetch_add(4 + len as u64, Ordering::Relaxed);
        match codec::decode::<ClientMsg>(shared.format, &body) {
            Ok(ClientMsg::Reply(r)) => shared.dispatch(r),
            Ok(ClientMsg::Req(_)) => {}
            Err(_) => {
                shared.stats.decode_errors.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

async fn probe_loop(
    node: usize,
    addr: String,
    shared: Arc<Shared>,
    interval: Duration,
    connect_timeout: Duration,
) {
    let mut rng = Rng::new(0x5eed ^ node as u64 ^ u64::from(std::process::id()) << 8);
    // First probe soon, spread out so the three nodes are not probed together.
    time::sleep(Duration::from_millis(rng.below(200))).await;
    loop {
        let t = Instant::now();
        if let Ok(Ok(stream)) = time::timeout(connect_timeout, TcpStream::connect(&addr)).await {
            let us = u32::try_from(t.elapsed().as_micros()).unwrap_or(u32::MAX);
            drop(stream);
            shared.update_rtt(node, us);
        }
        time::sleep(interval.mul_f64(0.8 + 0.4 * rng.unit())).await;
    }
}
