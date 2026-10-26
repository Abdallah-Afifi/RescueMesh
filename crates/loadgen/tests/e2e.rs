//! A worker against the stand-in cloud: phases, counters, event log, summary.

use client::testkit::{StubCluster, StubOpts};
use loadgen::params::{LoadParams, Mode};
use loadgen::worker::{self, WorkerArgs};
use proto::config::Config;
use std::time::Duration;

fn params(mode: Mode) -> LoadParams {
    LoadParams {
        processes: 1,
        users: 10,
        mode,
        rate: 200.0,
        think_ms: 5,
        duration_secs: 2,
        warmup_secs: 1,
        drain_secs: 5,
        seed: 3,
        mix: "report=50,offer=30,update=15,query=5".into(),
        max_inflight: 1000,
    }
}

async fn run_once(mode: Mode, name: &str) -> (loadgen::summary::WorkerSummary, std::path::PathBuf) {
    let stub = StubCluster::start_local(StubOpts {
        service: Duration::from_millis(2),
        ..StubOpts::default()
    })
    .await
    .unwrap();
    let dir = std::env::temp_dir().join(format!("rm-e2e-{name}-{}", std::process::id()));
    let extra = format!("log_dir = {:?}\n", dir.to_string_lossy());
    let text = stub
        .config_text(&extra)
        .replace("run_id = \"stub\"", &format!("run_id = \"{name}\""));
    let cfg = Config::parse(&text).unwrap();
    cfg.validate().unwrap();
    let s = worker::run(WorkerArgs {
        cfg,
        machine: 2,
        process: 0,
        params: params(mode),
        start_at_ms: client::now_ms() + 300,
    })
    .await
    .unwrap();
    (s, dir.join(name))
}

fn events(dir: &std::path::Path) -> Vec<serde_json::Value> {
    std::fs::read_to_string(dir.join("client2-0.jsonl"))
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}

#[tokio::test]
async fn closed_loop_run_produces_counts_events_and_a_summary() {
    let (s, dir) = run_once(Mode::Closed, "closed").await;
    assert!(s.connected_at_start);
    assert!(s.measure.ok > 100, "{:?}", s.measure);
    assert_eq!(s.measure.gave_up, 0);
    assert!(s.total.started >= s.measure.started);
    assert_eq!(s.events_dropped, 0);
    assert_eq!(s.hist.count, s.measure.ok);

    let ev = events(&dir);
    let replies: Vec<_> = ev
        .iter()
        .filter(|e| e["kind"] == "client_reply" && e["outcome"] == "ok")
        .collect();
    assert_eq!(replies.len() as u64, s.total.ok);
    let measured = replies.iter().filter(|e| e["phase"] == "measure").count() as u64;
    assert_eq!(
        measured, s.measure.ok,
        "events and counters agree on the window"
    );
    assert!(replies.iter().any(|e| e["phase"] == "warmup"));
    assert!(
        ev.iter()
            .any(|e| e["kind"] == "resource_sample" && e["rss_bytes"].as_u64().unwrap() > 0)
    );
    assert!(dir.join("client2-0.summary.json").exists());
    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
}

#[tokio::test]
async fn open_loop_offers_roughly_the_requested_rate() {
    let (s, dir) = run_once(Mode::Open, "open").await;
    // 200/s for a 2 s window: Poisson, so allow a wide band.
    assert!((300..=520).contains(&s.measure.started), "{:?}", s.measure);
    assert_eq!(s.measure.dropped_client, 0);
    assert_eq!(s.measure.gave_up, 0);
    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
}
