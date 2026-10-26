//! The client middleware against the stand-in cloud (`client::testkit`).

use client::testkit::{StubCluster, StubOpts};
use client::{EventLog, Middleware, NetConfig, RetryPolicy, SubmitError};
use proto::codec::WireFormat;
use proto::{ClientId, NodeId, Op, OpResult, Query, ReqId, ZoneId};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

fn policy(initial_ms: u64, retries: u32) -> RetryPolicy {
    RetryPolicy {
        timeout_initial: Duration::from_millis(initial_ms),
        p99_multiple: 3,
        max_retries: retries,
        backoff_factor: 2.0,
        min_timeout: Duration::from_millis(20),
        max_timeout: Duration::from_secs(10),
        busy_base: Duration::from_millis(20),
        min_samples: 200,
    }
}

fn net(stub: &StubCluster) -> NetConfig {
    let a = stub.addrs();
    NetConfig {
        nodes: [a[0].to_string(), a[1].to_string(), a[2].to_string()],
        format: WireFormat::Postcard,
        probe_interval: Duration::from_millis(100),
        connect_timeout: Duration::from_secs(1),
        write_timeout: Duration::from_secs(2),
        queue_cap: 1024,
    }
}

fn uid(n: u32) -> ClientId {
    ClientId {
        machine: 1,
        process: 1,
        user: n,
    }
}

fn query() -> Op {
    Op::Query(Query::IncidentsInZone(ZoneId(3)))
}

fn temp_dir(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("rm-client-{name}-{}", std::process::id()))
}

#[tokio::test]
async fn request_is_multicast_answered_and_rtt_is_measured() {
    let stub = StubCluster::start_local(StubOpts::default()).await.unwrap();
    let mw = Middleware::with_net(&net(&stub), policy(500, 3), None, 77);
    assert!(mw.wait_connected(Duration::from_secs(2)).await);
    let user = mw.client(uid(1));

    let done = user.submit(query()).await.unwrap();
    assert_eq!(done.attempts, 1);
    assert!(matches!(done.result, OpResult::Accepted));
    assert_eq!(
        done.req,
        ReqId {
            client: uid(1),
            incarnation: 77,
            seq: 1
        }
    );

    tokio::time::sleep(Duration::from_millis(100)).await;
    for n in NodeId::ALL {
        assert_eq!(
            stub.received(n),
            1,
            "{n} should have received the multicast"
        );
    }
    let total: u64 = NodeId::ALL.iter().map(|&n| stub.served(n)).sum();
    assert_eq!(total, 1, "exactly one node executes");
    // The first probe of each node runs within 200 ms of start-up.
    let mut rtt = mw.net().rtt_us();
    for _ in 0..100 {
        if rtt.iter().all(Option::is_some) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        rtt = mw.net().rtt_us();
    }
    assert!(rtt.iter().all(Option::is_some), "RTT probes ran: {rtt:?}");
}

#[tokio::test]
async fn sequence_numbers_and_ack_watermark_advance_with_concurrent_requests() {
    let stub = StubCluster::start_local(StubOpts::default()).await.unwrap();
    let mw = Middleware::with_net(&net(&stub), policy(500, 3), None, 1);
    assert!(mw.wait_connected(Duration::from_secs(2)).await);
    let user = Arc::new(mw.client(uid(1)));
    let mut tasks = Vec::new();
    for _ in 0..50 {
        let u = user.clone();
        tasks.push(tokio::spawn(async move {
            u.submit(query()).await.unwrap().req.seq
        }));
    }
    let mut seqs = Vec::new();
    for t in tasks {
        seqs.push(t.await.unwrap());
    }
    seqs.sort_unstable();
    assert_eq!(seqs, (1..=50).collect::<Vec<_>>());
    assert_eq!(user.acked_up_to(), 50);
    let s = mw.stats();
    assert_eq!(
        (s.submitted, s.completed, s.gave_up, s.inflight),
        (50, 50, 0, 0)
    );
}

#[tokio::test]
async fn silent_executor_is_survived_by_retrying_with_the_same_request_id() {
    let dir = temp_dir("retry");
    let stub = StubCluster::start_local(StubOpts {
        detect_delay: Duration::from_millis(300),
        ..StubOpts::default()
    })
    .await
    .unwrap();
    let log = EventLog::create(&dir, "r", "client1", "1", None).unwrap();
    let mw = Middleware::with_net(&net(&stub), policy(150, 5), Some(log.clone()), 5);
    assert!(mw.wait_connected(Duration::from_secs(2)).await);
    stub.silence(NodeId(1), Duration::from_secs(5));

    let mut tasks = Vec::new();
    for n in 0..60 {
        let user = mw.client(uid(n));
        tasks.push(tokio::spawn(async move { user.submit(query()).await }));
    }
    let mut retried = 0;
    for t in tasks {
        let done = t
            .await
            .unwrap()
            .expect("every request is eventually answered");
        assert_ne!(done.from, NodeId(1), "the silent node never replies");
        if done.attempts > 1 {
            retried += 1;
            assert!(
                done.latency >= Duration::from_millis(100),
                "latency is measured from the first send, not the last"
            );
        }
    }
    assert!(
        retried > 0,
        "some requests had node 1 as executor and had to be retried"
    );
    assert!(stub.dropped_silent(NodeId(1)) > 0);

    log.finish();
    let text = std::fs::read_to_string(log.path()).unwrap();
    let mut sends: BTreeMap<String, Vec<u64>> = BTreeMap::new();
    let mut retries = 0;
    for line in text.lines() {
        let v: serde_json::Value = serde_json::from_str(line).unwrap();
        match v["kind"].as_str().unwrap() {
            "client_send" => sends
                .entry(v["req"].as_str().unwrap().to_owned())
                .or_default()
                .push(v["attempt"].as_u64().unwrap()),
            "client_retry" => retries += 1,
            _ => {}
        }
    }
    assert_eq!(sends.len(), 60, "one request ID per operation");
    assert!(sends.values().any(|a| a.len() > 1), "retries reuse the ID");
    assert!(
        sends
            .values()
            .all(|a| a.iter().copied().eq(0..a.len() as u64))
    );
    assert!(retries > 0);
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn gives_up_when_nobody_answers_and_logs_it() {
    let dir = temp_dir("giveup");
    let stub = StubCluster::start_local(StubOpts::default()).await.unwrap();
    let log = EventLog::create(&dir, "g", "client1", "1", None).unwrap();
    let mw = Middleware::with_net(&net(&stub), policy(40, 2), Some(log.clone()), 5);
    assert!(mw.wait_connected(Duration::from_secs(2)).await);
    for n in NodeId::ALL {
        stub.silence(n, Duration::from_secs(10));
    }
    let user = mw.client(uid(1));
    let err = user.submit(query()).await.unwrap_err();
    match err {
        SubmitError::GaveUp { attempts, .. } => assert_eq!(attempts, 3),
        other => panic!("unexpected {other}"),
    }
    assert_eq!(
        user.acked_up_to(),
        0,
        "no reply was received, so nothing is acknowledged"
    );
    assert_eq!(mw.stats().gave_up, 1);
    log.finish();
    let text = std::fs::read_to_string(log.path()).unwrap();
    assert!(text.contains("\"outcome\":\"gave_up\""));
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn busy_replies_are_retried_and_eventually_reported() {
    let stub = StubCluster::start_local(StubOpts {
        queue_cap: 0,
        ..StubOpts::default()
    })
    .await
    .unwrap();
    let mw = Middleware::with_net(&net(&stub), policy(2000, 2), None, 5);
    assert!(mw.wait_connected(Duration::from_secs(2)).await);
    let t = std::time::Instant::now();
    let err = mw.client(uid(1)).submit(query()).await.unwrap_err();
    assert!(matches!(err, SubmitError::GaveUp { attempts: 3, .. }));
    assert!(
        t.elapsed() < Duration::from_secs(2),
        "Busy shortens the wait; it does not sit out the 2 s timeout"
    );
}

#[tokio::test]
async fn adaptive_timeout_follows_observed_latency() {
    let stub = StubCluster::start_local(StubOpts {
        service: Duration::from_millis(2),
        ..StubOpts::default()
    })
    .await
    .unwrap();
    let mw = Middleware::with_net(&net(&stub), policy(2000, 3), None, 5);
    assert!(mw.wait_connected(Duration::from_secs(2)).await);
    assert_eq!(mw.base_timeout(), Duration::from_secs(2));
    let user = Arc::new(mw.client(uid(1)));
    for _ in 0..10 {
        let mut batch = Vec::new();
        for _ in 0..40 {
            let u = user.clone();
            batch.push(tokio::spawn(
                async move { u.submit(query()).await.unwrap() },
            ));
        }
        for b in batch {
            b.await.unwrap();
        }
    }
    let t = mw.base_timeout();
    assert!(
        t < Duration::from_millis(500),
        "after 400 fast replies the timeout should drop well below the initial 2 s, got {t:?}"
    );
}
