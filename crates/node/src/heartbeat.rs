//! Heartbeats over UDP and the failure detector, on a dedicated OS thread
//! (protocol Section 6, design log DL-014, DL-017, DL-053).
//!
//! The thread sends this node's latest heartbeat to both peers every
//! `timeouts.heartbeat_ms`, receives theirs, feeds a [`PhiDetector`] per
//! peer, and tells the core task when a peer becomes suspected or is heard
//! again. It touches no protocol state and never waits for the core task or
//! the planning pool, so a CPU-saturating planning load cannot delay
//! heartbeats or cause false suspicion through this node's own scheduling.
//!
//! **Content.** The core task refreshes the heartbeat it wants sent (epoch,
//! coordinator, view, commit position, load) every interval through a
//! [`HeartbeatSlot`]; this thread stamps its own sequence number `n` on
//! each datagram. If the core task were ever slow, heartbeats keep going
//! with slightly older content, which is what the detector should see: the
//! process is alive.
//!
//! **Silence.** While the node is silenced the thread sends nothing and
//! drains and drops everything it receives. On waking it restarts every
//! detector's timeout, because the gap was its own, not the peers'.
//!
//! **Peer restarts.** A heartbeat with a new incarnation from a peer means
//! its process restarted; the transport then restarts its stream to that
//! peer ([`crate::link::Link::peer_restarted`]).

use crate::fd::{FdCfg, PhiDetector};
use crate::net::{Clock, Input, Net};
use proto::codec;
use proto::config::Config;
use proto::{Heartbeat, NodeId, PROTOCOL_VERSION};
use std::collections::BTreeMap;
use std::io;
use std::net::{SocketAddr, ToSocketAddrs, UdpSocket};
use std::sync::mpsc::Sender;
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

/// The heartbeat the core task currently wants sent.
pub type HeartbeatSlot = Arc<Mutex<Option<Heartbeat>>>;

/// How often the thread wakes to check detectors and receive.
const POLL: Duration = Duration::from_millis(5);

struct Peer {
    host: String,
    port: u16,
    addr: Option<SocketAddr>,
    fd: PhiDetector,
    incarnation: Option<u32>,
}

pub fn spawn(
    cfg: &Config,
    me: NodeId,
    clock: Clock,
    net: Arc<Net>,
    slot: HeartbeatSlot,
    core: Sender<Input>,
) -> io::Result<JoinHandle<()>> {
    let my = cfg
        .node(me)
        .ok_or_else(|| io::Error::other(format!("{me} is not in the cluster config")))?;
    let socket = UdpSocket::bind(("0.0.0.0", my.heartbeat_port))?;
    socket.set_read_timeout(Some(POLL))?;
    let fdcfg = FdCfg::from_timeouts(&cfg.timeouts);
    let now = clock.now_ms();
    let peers: BTreeMap<NodeId, Peer> = cfg
        .cluster
        .nodes
        .iter()
        .filter(|n| n.id != me)
        .map(|n| {
            (
                n.id,
                Peer {
                    host: n.host.clone(),
                    port: n.heartbeat_port,
                    addr: None,
                    fd: PhiDetector::new(fdcfg.clone(), now),
                    incarnation: None,
                },
            )
        })
        .collect();
    let interval = cfg.timeouts.heartbeat_ms;
    let format = cfg.run.wire_format;
    thread::Builder::new()
        .name(format!("heartbeat-{me}"))
        .spawn(move || run(me, clock, net, slot, core, socket, peers, interval, format))
}

#[allow(clippy::too_many_arguments)]
fn run(
    me: NodeId,
    clock: Clock,
    net: Arc<Net>,
    slot: HeartbeatSlot,
    core: Sender<Input>,
    socket: UdpSocket,
    mut peers: BTreeMap<NodeId, Peer>,
    interval: u64,
    format: codec::WireFormat,
) {
    let stats = net.stats();
    let mut n: u64 = 0;
    let mut next_send = clock.now_ms();
    let mut was_silenced = false;
    let mut buf = [0u8; 2 * codec::MAX_DATAGRAM];
    loop {
        let now = clock.now_ms();
        let silenced = net.is_silenced();
        if was_silenced && !silenced {
            for p in peers.values_mut() {
                p.fd.restart(now);
            }
        }
        was_silenced = silenced;

        // ---- send
        if now >= next_send {
            let hb = slot.lock().unwrap_or_else(|e| e.into_inner()).clone();
            if let (false, Some(mut hb)) = (silenced, hb) {
                n += 1;
                hb.n = n;
                match codec::encode_datagram(format, &hb) {
                    Ok(bytes) => {
                        for p in peers.values_mut() {
                            if p.addr.is_none() {
                                p.addr = (p.host.as_str(), p.port)
                                    .to_socket_addrs()
                                    .ok()
                                    .and_then(|mut a| a.next());
                            }
                            if let Some(a) = p.addr
                                && socket.send_to(&bytes, a).is_ok()
                            {
                                stats.udp_tx_bytes.fetch_add(bytes.len() as u64, ORD);
                                stats.heartbeats_tx.fetch_add(1, ORD);
                            }
                        }
                    }
                    Err(e) => eprintln!("{me}: cannot encode heartbeat: {e}"),
                }
            }
            next_send += interval;
            if next_send <= now {
                next_send = now + interval; // fell behind; do not burst
            }
        }

        // ---- receive (waits up to POLL)
        match socket.recv_from(&mut buf) {
            Ok((len, _)) => {
                stats.udp_rx_bytes.fetch_add(len as u64, ORD);
                if silenced {
                    stats.dropped_silenced.fetch_add(1, ORD);
                } else if let Ok(hb) = codec::decode::<Heartbeat>(format, &buf[..len]) {
                    if hb.version != PROTOCOL_VERSION {
                        stats.version_errors.fetch_add(1, ORD);
                    } else if let Some(p) = peers.get_mut(&hb.from) {
                        stats.heartbeats_rx.fetch_add(1, ORD);
                        let now = clock.now_ms();
                        if p.incarnation.is_some_and(|i| i != hb.incarnation) {
                            net.peer_restarted(hb.from);
                        }
                        p.incarnation = Some(hb.incarnation);
                        if let Some(reading) = p.fd.heartbeat(now) {
                            let peer = hb.from;
                            if core.send(Input::Suspect { peer, reading }).is_err() {
                                return;
                            }
                        }
                        if core.send(Input::Heartbeat(hb)).is_err() {
                            return;
                        }
                    }
                } else {
                    stats.decode_errors.fetch_add(1, ORD);
                }
            }
            Err(e)
                if matches!(
                    e.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) => {}
            // On some platforms an ICMP "port unreachable" from an earlier
            // send surfaces here; it says nothing about liveness.
            Err(_) => thread::sleep(Duration::from_millis(1)),
        }

        // ---- detect
        // Use the value read at the top of the loop, not a fresh read: if the
        // silence ended during the receive wait, the detectors have not been
        // restarted yet and would compare against heartbeats from before the
        // silence, suspecting every peer on wake (DL-064).
        if !silenced {
            let now = clock.now_ms();
            for (&peer, p) in peers.iter_mut() {
                if let Some(reading) = p.fd.check(now)
                    && core.send(Input::Suspect { peer, reading }).is_err()
                {
                    return;
                }
            }
        }
    }
}

const ORD: std::sync::atomic::Ordering = std::sync::atomic::Ordering::Relaxed;
