//! Workstream B end to end on one machine: three full nodes (transport,
//! heartbeats, failure detector, core task, placeholder planner) on
//! localhost, talking over real TCP and UDP sockets. A test client sends
//! requests exactly as `crates/client` does, and the operator channel is
//! driven exactly as `rm-ctl` does.
//!
//! These take a few seconds each because they run in real time.

use node::dev::{DevPool, DevState};
use node::runtime::{self, Node, NodeOpts};
use proto::codec::{self, WireFormat};
use proto::config::{ClusterCfg, Config, NodeAddr, RunCfg};
use proto::{
    Category, ClientId, ClientMsg, ClientReply, ClientReq, NodeId, Op, PROTOCOL_VERSION, ReplyBody,
    ReqId, Request, Severity, ZoneId,
};
use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::sync::mpsc::{self, Receiver};
use std::thread;
use std::time::{Duration, Instant};

fn config(test_no: u16) -> Config {
    // Distinct ports per test (tests run in parallel) and per process.
    let base = 21_000 + (std::process::id() % 300) as u16 * 40 + test_no * 8;
    Config {
        run: RunCfg {
            run_id: format!("rt{test_no}"),
            ..RunCfg::default()
        },
        cluster: ClusterCfg {
            nodes: (1..=3u8)
                .map(|i| NodeAddr {
                    id: NodeId(i),
                    host: "127.0.0.1".into(),
                    port: base + u16::from(i),
                    heartbeat_port: base + 4 + u16::from(i),
                })
                .collect(),
        },
        timeouts: Default::default(),
        score: Default::default(),
        policy: Default::default(),
        planner: Default::default(),
        inject: Default::default(),
        client: Default::default(),
    }
}

fn start(cfg: &Config, spin_us: u64, threads: usize) -> Vec<Node> {
    cfg.validate().expect("valid test config");
    NodeId::ALL
        .iter()
        .map(|&id| {
            let opts = NodeOpts::new(id, cfg.clone(), 0x1000 + u32::from(id.0), None);
            let pool = DevPool::factory(threads, cfg.planner.queue_capacity, 3, spin_us);
            runtime::start(opts, Box::new(DevState::new()), pool).expect("node starts")
        })
        .collect()
}

/// A value from the status line, e.g. `field(s, "view")`.
fn field(status: &str, key: &str) -> String {
    let pat = format!("\"{key}\":");
    let Some(i) = status.find(&pat) else {
        return String::new();
    };
    let rest = &status[i + pat.len()..];
    let end = if rest.starts_with('[') {
        rest.find(']').map_or(rest.len(), |e| e + 1)
    } else {
        rest.find([',', '}']).unwrap_or(rest.len())
    };
    rest[..end].trim_matches('"').to_owned()
}

fn wait_until(what: &str, max: Duration, mut cond: impl FnMut() -> bool) -> Duration {
    let t = Instant::now();
    while t.elapsed() < max {
        if cond() {
            return t.elapsed();
        }
        thread::sleep(Duration::from_millis(10));
    }
    panic!("timed out after {max:?} waiting for: {what}");
}

fn settled(nodes: &[Node], members: &str) -> bool {
    let s: Vec<String> = nodes.iter().map(Node::status).collect();
    let coord = field(&s[0], "coordinator");
    coord != "null"
        && s.iter().all(|x| {
            field(x, "coordinator") == coord
                && field(x, "view") != "0" // the coordinator's first view entry is applied
                && field(x, "status") == "Active"
                && field(x, "active") == members
        })
}

/// A client process: one connection per node, like `crates/client`.
struct TestClient {
    conns: Vec<TcpStream>,
    replies: Receiver<ClientReply>,
    seq: u64,
}

impl TestClient {
    fn connect(cfg: &Config) -> TestClient {
        let (tx, rx) = mpsc::channel();
        let conns = cfg
            .cluster
            .nodes
            .iter()
            .map(|n| {
                let s = TcpStream::connect((n.host.as_str(), n.port)).expect("connect");
                let mut r = s.try_clone().unwrap();
                let tx = tx.clone();
                thread::spawn(move || {
                    while let Ok(ClientMsg::Reply(rep)) =
                        codec::read_frame::<ClientMsg, _>(WireFormat::Postcard, &mut r)
                    {
                        if tx.send(rep).is_err() {
                            return;
                        }
                    }
                });
                s
            })
            .collect();
        TestClient {
            conns,
            replies: rx,
            seq: 0,
        }
    }

    fn request(&mut self, user: u32) -> ClientReq {
        self.seq += 1;
        ClientReq {
            version: PROTOCOL_VERSION,
            req: Request {
                id: ReqId {
                    client: ClientId {
                        machine: 9,
                        process: 1,
                        user,
                    },
                    incarnation: 77,
                    seq: self.seq,
                },
                sent_at_ms: 1_000 + self.seq,
                op: Op::ReportIncident {
                    category: Category::Flooding,
                    zone: ZoneId(3),
                    severity: Severity(4),
                    ttl_ms: 60_000,
                    attachment: None,
                },
            },
            rtt_us: [Some(200); 3],
            acked_up_to: 0,
        }
    }

    /// Multicast: one send to each node.
    fn send(&mut self, req: &ClientReq) {
        let frame =
            codec::encode_frame(WireFormat::Postcard, &ClientMsg::Req(req.clone())).unwrap();
        for c in &mut self.conns {
            let _ = c.write_all(&frame);
        }
    }

    /// Sends `req` to all nodes, retrying every `retry`, until an `Ok` reply.
    fn call(&mut self, req: &ClientReq, retry: Duration, max: Duration) -> (NodeId, Duration) {
        let t = Instant::now();
        while t.elapsed() < max {
            self.send(req);
            let round = Instant::now();
            while round.elapsed() < retry {
                if let Ok(r) = self.replies.recv_timeout(Duration::from_millis(10))
                    && r.req == req.req.id
                    && matches!(r.body, ReplyBody::Ok(_))
                {
                    return (r.from, t.elapsed());
                }
            }
        }
        panic!("no answer to {} within {max:?}", req.req.id);
    }
}

fn operator(cfg: &Config, node: NodeId, line: &str) -> String {
    let n = cfg.node(node).unwrap();
    let mut s = TcpStream::connect((n.host.as_str(), n.port)).unwrap();
    s.write_all(b"RMO1").unwrap();
    writeln!(s, "{line}").unwrap();
    let mut out = String::new();
    BufReader::new(s).read_line(&mut out).unwrap();
    out.trim().to_owned()
}

#[test]
fn three_nodes_elect_a_coordinator_and_answer_clients_over_real_sockets() {
    let cfg = config(0);
    let nodes = start(&cfg, 0, 2);
    wait_until(
        "one coordinator, three active nodes",
        Duration::from_secs(10),
        || settled(&nodes, "[1,2,3]"),
    );
    let mut c = TestClient::connect(&cfg);
    let mut executors = std::collections::BTreeSet::new();
    for i in 0..60 {
        let req = c.request(i);
        let (from, _) = c.call(&req, Duration::from_millis(500), Duration::from_secs(5));
        executors.insert(from);
    }
    // Every node has heard the others' heartbeats, and the links carried
    // and acknowledged messages.
    for n in &nodes {
        let s = n.net.stats();
        assert!(s.heartbeats_rx.load(std::sync::atomic::Ordering::Relaxed) > 10);
        let l = n.net.link_stats();
        assert!(l.sent_new > 0 && l.delivered > 0, "{l:?}");
    }
    let commit: u64 = field(&nodes[0].status(), "commit").parse().unwrap();
    assert!(commit >= 60, "commit {commit}");
    eprintln!("executors used: {executors:?}");
}

#[test]
fn an_operator_silence_is_detected_the_others_keep_serving_and_the_node_rejoins() {
    let cfg = config(1);
    let nodes = start(&cfg, 0, 2);
    wait_until("settled", Duration::from_secs(10), || {
        settled(&nodes, "[1,2,3]")
    });

    // Silence a node that is not the coordinator, through a third node,
    // exactly as `rm-ctl silence 2 4000` would.
    let coord: u8 = field(&nodes[0].status(), "coordinator").parse().unwrap();
    let victim = NodeId::ALL.into_iter().find(|n| n.0 != coord).unwrap();
    let via = NodeId::ALL
        .into_iter()
        .find(|n| *n != victim && n.0 != coord)
        .unwrap();
    let answer = operator(&cfg, via, &format!("silence {} 4000", victim.0));
    assert!(answer.starts_with("ok"), "{answer}");
    wait_until("victim silenced", Duration::from_secs(2), || {
        field(&nodes[victim.index()].status(), "silenced") == "true"
    });
    let t_silenced = Instant::now();

    // The coordinator suspects it and removes it from the view: detection
    // (300–1000 ms) plus one commit.
    let others: Vec<&Node> = NodeId::ALL
        .iter()
        .filter(|n| **n != victim)
        .map(|n| &nodes[n.index()])
        .collect();
    let without: String = format!(
        "[{}]",
        NodeId::ALL
            .iter()
            .filter(|n| **n != victim)
            .map(|n| n.0.to_string())
            .collect::<Vec<_>>()
            .join(",")
    );
    wait_until(
        "victim removed from the view",
        Duration::from_secs(3),
        || {
            others
                .iter()
                .all(|n| field(&n.status(), "active") == without)
        },
    );
    let removal = t_silenced.elapsed();
    assert!(
        removal < Duration::from_millis(1_800),
        "removal took {removal:?}"
    );

    // Clients are still served while it is silent.
    let mut c = TestClient::connect(&cfg);
    for i in 0..20 {
        let req = c.request(i);
        let (from, _) = c.call(&req, Duration::from_millis(500), Duration::from_secs(5));
        assert_ne!(from, victim, "a silenced node answered");
    }

    // After the silence it catches up and is re-admitted.
    wait_until("rejoined and converged", Duration::from_secs(15), || {
        settled(&nodes, "[1,2,3]") && field(&nodes[victim.index()].status(), "silenced") == "false"
    });
    let commits: Vec<String> = nodes.iter().map(|n| field(&n.status(), "commit")).collect();
    eprintln!("removal after {removal:?}; commits after rejoin {commits:?}");
}

#[test]
fn silencing_the_coordinator_elects_a_new_one_and_fences_the_old() {
    let cfg = config(2);
    let nodes = start(&cfg, 0, 2);
    wait_until("settled", Duration::from_secs(10), || {
        settled(&nodes, "[1,2,3]")
    });
    let old = field(&nodes[0].status(), "coordinator");
    let old_epoch: u64 = field(&nodes[0].status(), "epoch").parse().unwrap();
    let answer = operator(&cfg, NodeId(1), "silence coordinator 3000");
    assert!(answer.starts_with("ok"), "{answer}");
    let others: Vec<&Node> = nodes
        .iter()
        .filter(|n| n.net.me().0.to_string() != old)
        .collect();
    let took = wait_until("a new coordinator", Duration::from_secs(5), || {
        let s: Vec<String> = others.iter().map(|n| n.status()).collect();
        let c = field(&s[0], "coordinator");
        c != "null" && c != old && s.iter().all(|x| field(x, "coordinator") == c)
    });
    eprintln!("new coordinator after {took:?}");
    wait_until("old coordinator rejoined", Duration::from_secs(15), || {
        settled(&nodes, "[1,2,3]")
    });
    let epoch: u64 = field(&nodes[0].status(), "epoch").parse().unwrap();
    assert!(epoch > old_epoch);
}

#[test]
fn no_false_suspicion_while_planning_saturates_every_core() {
    let cfg = config(3);
    let cores = thread::available_parallelism().map_or(2, |n| n.get());
    // Each node gets as many spinning planning threads as the machine has
    // cores, so the three nodes together oversubscribe the CPU threefold.
    let nodes = start(&cfg, 20_000, cores);
    wait_until("settled", Duration::from_secs(10), || {
        settled(&nodes, "[1,2,3]")
    });
    let view_before = field(&nodes[0].status(), "view");
    let mut c = TestClient::connect(&cfg);
    let t = Instant::now();
    let mut sent = 0u32;
    while t.elapsed() < Duration::from_secs(6) {
        for _ in 0..20 {
            let req = c.request(sent);
            c.send(&req);
            sent += 1;
        }
        thread::sleep(Duration::from_millis(10));
    }
    // Every node was busy...
    let queued: Vec<String> = nodes
        .iter()
        .map(|n| field(&n.status(), "executing"))
        .collect();
    // ...yet no node was ever removed from the view.
    for n in &nodes {
        let st = n.status();
        assert_eq!(
            field(&st, "view"),
            view_before,
            "a view change happened: {st}"
        );
        assert_eq!(field(&n.status(), "active"), "[1,2,3]");
    }
    eprintln!("{sent} requests sent; executing per node {queued:?}");
}
