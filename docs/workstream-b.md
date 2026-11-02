# Workstream B: node runtime, transport, heartbeats, failure detection

How the node's runtime is built and how to use it. Design decisions: design log DL-053 to DL-055, DL-065 (integration), and DL-066 to DL-073 from the pre-deployment reviews.

## Where the code is

| File | What it does |
|---|---|
| `crates/node/src/runtime.rs` | Runs the core task (`NodeTask`) on its own thread; carries out its outputs; event logging; per-second resource samples |
| `crates/node/src/net.rs` | TCP transport: peers, clients and the operator channel on one port; the silence switch |
| `crates/node/src/link.rs` | Application-level acknowledgment, retransmission and duplicate suppression for one peer |
| `crates/node/src/heartbeat.rs` | UDP heartbeats on a dedicated thread; drives the failure detectors |
| `crates/node/src/fd.rs` | φ-accrual failure detector |
| `crates/node/src/workers.rs` | Workstream D's planning pool, which `rm-node` runs (DL-060, DL-065, DL-068) |
| `crates/node/src/dev.rs` | Placeholder planner and pool for runtime stress tests, `rm-node --dev-spin-us` (DL-055) |
| `crates/node/src/main.rs` | `rm-node` |
| `crates/ctl/src/main.rs` | `rm-ctl status`, `rm-ctl silence` and `rm-ctl load` |
| `crates/node/tests/runtime_localhost.rs` | Three real nodes on localhost |

## Threads in one node

```
acceptor ──> reader per connection ──┐  (peer envelopes, client requests, operator lines)
heartbeat thread (UDP + detectors) ──┼──> channel ──> core thread (NodeTask) ──> writer per peer
planning threads (pool) ─────────────┘                     │                  ──> writer per client
                                                           └──> heartbeat slot, event log
```

Only the core thread touches protocol state (DL-017, DL-029). The transport owns connections and retransmission buffers; the heartbeat thread owns heartbeat history and the detectors.

## Try it on one machine

```sh
cargo build --release
# three terminals
target/release/rm-node --config config/local.toml --id 1
target/release/rm-node --config config/local.toml --id 2
target/release/rm-node --config config/local.toml --id 3
# a fourth
target/release/rm-ctl --config config/local.toml status
target/release/rm-loadgen run --config config/local.toml --machine 1 \
    --processes 2 --users 20 --mode open --rate 200 --warmup-secs 3 --duration-secs 20
target/release/rm-ctl --config config/local.toml silence coordinator --duration-ms 5000
target/release/rm-ctl --config config/local.toml status   # watch the new coordinator, then the rejoin
target/release/rm-ctl --config config/local.toml load 2 --threads 8 --duration-ms 20000   # load-imbalance demo
```

`rm-node` stops cleanly on SIGTERM or Ctrl-C, flushing its event log.

`rm-node --dev-spin-us 20000` makes every plan burn 20 ms of CPU (placeholder planner only), for the false-suspicion check.

## Done criteria (Phase 1 plan, workstream B)

| Criterion | Where it is shown |
|---|---|
| Three nodes exchange heartbeats | `three_nodes_elect_a_coordinator_and_answer_clients_over_real_sockets`; `heartbeats_rx` in `resource_sample` |
| A silenced node is suspected within the expected bound | `an_operator_silence_is_detected_…`: removed from the view in about 250–280 ms on localhost; `suspect` events carry `silent_ms` and `timeout_ms` |
| No false suspicion during a CPU-saturating run | `no_false_suspicion_while_planning_saturates_every_core` |
| On separate machines | See `docs/deployment.md` |

## What the logs contain for the B measurements

- **Detection time:** `suspect` events (`peer`, `phi`, `silent_ms`, `timeout_ms`, `samples`), compared with `inject_start` of the victim. `silent_ms` is the time since the peer's last heartbeat when the detector fired.
- **Heartbeat and protocol overhead per node:** cumulative counters in every node `resource_sample` (`scope = "node"`): `heartbeats_tx`, `heartbeats_rx`, `udp_tx_bytes`, `udp_rx_bytes`, `peer_msgs_tx`, `peer_msgs_rx`, `peer_retransmits`, `peer_bare_acks`, `peer_duplicates`, `client_reqs`, `client_replies`, `client_reply_dropped`, `dropped_silenced`, and new messages by protocol role, `msgs_tx_election`, `msgs_tx_replication`, `msgs_tx_claim`, `msgs_tx_commit`, `msgs_tx_other` (DL-067). Take differences between samples.
- **Round trips and reliability:** `rtt_us_n<i>` (from heartbeat echoes) and `missed_permille_n<i>` per peer, and the claim timeout in use, `claim_timeout_ms` (DL-069).
- **Overload signals:** `core_lag_ms_max` (how late the core loop ran in the last second), `client_backlog`, `client_overload_dropped`, `events_dropped`.
- **Network usage:** `net_rx_bytes` and `net_tx_bytes` (TCP + UDP, counted by the transport), which `analysis/analyze.py` already plots.
- **False-suspicion rate under load:** `suspect` events with no `inject_start` for that peer shortly before them.

## Running the tests

```sh
cargo test -p node                       # unit tests (fd, link) and the core-task simulator
cargo test -p node --test runtime_localhost -- --nocapture   # real sockets, about 8 s
```

The localhost tests pick ports from 21000 upwards based on the process id; if one is busy, rerun.
