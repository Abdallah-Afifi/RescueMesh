# Phase 1 report: source material

Material for the Phase 1 report that the other documents do not already hold, collected in one place. The report requirements (project description p. 7) not covered here are covered by:

| Report section | Source |
|---|---|
| System architecture, process/node model | `docs/protocol-v2.md` Sections 2–3, `docs/workstream-b.md` ("Threads in one node"), DL-017, DL-018, DL-039, DL-054 |
| Protocols and message formats, timeouts, retries, duplicates | `docs/protocol-v2.md` Sections 5–9, `crates/proto/src/msg.rs`, DL-014, DL-015, DL-024, DL-041, DL-054 |
| Election survey and rationale | `docs/phase0/104_survey_v2.docx`, `docs/protocol-v2.md` Section 7 |
| Fault model, detection, failover, recovery, convergence | `docs/protocol-v2.md` Sections 2, 6, 8, 9; DL-030 to DL-036, DL-053, DL-066, DL-073 |
| Experimental methodology, raw data, plots | `docs/workstream-e.md`, `experiments/sweeps/*.toml`, `analysis/analyze.py`, `results/summary/` once the sweeps have run |
| Design decisions that changed | `docs/design-log.md` (every entry says what it changes) |

## 1. What comes from libraries, and what we wrote

The project description (p. 8) asks the report to say which functions libraries provide and which distributed protocols, election logic, consistency mechanisms and recovery procedures the team designed and implemented.

**Libraries**

| Library | Used for | Where |
|---|---|---|
| `tokio` | Asynchronous sockets and timers in the client middleware and load generator; signal handling in `rm-node` and `rm-loadgen` | `crates/client`, `crates/loadgen`, `crates/node/src/main.rs` |
| `serde`, `serde_json`, `postcard` | Serialization of messages (postcard on the wire, JSON for logs and debugging) and of the event log | `crates/proto/src/codec.rs`, `crates/proto/src/event.rs` |
| `toml` | Reading the run configuration | `crates/proto/src/config.rs` |
| `clap` | Command-line parsing | the four binaries |
| `sysinfo` | Process and machine CPU time, memory, network counters | `crates/node/src/workers.rs`, `crates/loadgen/src/sampler.rs` |
| Rust standard library | TCP and UDP sockets, OS threads, channels, mutexes, `BTreeMap` | everywhere |
| `numpy`, `pandas`, `scipy`, `matplotlib` | Tables, Student-t confidence intervals, plots | `analysis/analyze.py` |

No distributed-systems framework, RPC library, consensus library or message broker is used.

**Designed and written by the team**

- **Transport reliability:**
  - framing;
  - per-peer application-level numbering, cumulative acknowledgment, retransmission with backoff, duplicate suppression and fillers (`crates/node/src/link.rs`, DL-054);
  - the silence switch (`crates/node/src/net.rs`).
- **Failure detection:** UDP heartbeats on a dedicated thread, and a φ-accrual detector (`crates/node/src/heartbeat.rs`, `fd.rs`, DL-053). Heartbeat echoes measure the round trip between nodes (DL-069).
- **Coordinator election:** Bully-ordered candidacy delays, pre-vote, epoch fencing, and a lease for stepping down (`crates/replication/src/core.rs`, protocol Section 7). The vote rule and the log repair follow Raft's published algorithm, but the code is ours.
- **Replicated log and membership:**
  - sequencing through the coordinator, majority commit, apply in order;
  - view entries for removal and re-admission;
  - rejoin and catch-up, including probes to silent peers and the post-wake sync window (DL-030 to DL-036, DL-066, DL-073).
- **Fault injection:** the replicated schedule (victim and duration from a hash of the seed and an injection counter), the single-failure precondition, and operator injection (protocol Section 8, DL-011, DL-025).
- **Executor election per request (MWBE):**
  - the load score;
  - local correction and quantization;
  - claims, claim keys and competing-claim resolution;
  - takeover on view change;
  - five baseline rankers (`crates/selection`, protocol Section 5, DL-038, DL-046 to DL-050, DL-069).
- **Consistency mechanisms:**
  - the request table and duplicate suppression (I1);
  - read-set version checks at the coordinator (I3, DL-057);
  - holds and expiry decided only inside version-checked plan writes (DL-072).
- **Incident service:**
  - the planner, with priority, assignment, responder list and route;
  - our own Hungarian algorithm, with the dual-price optimality check for bounded columns;
  - the replicated state machine (`crates/incident`, DL-020, DL-056 to DL-063, DL-071, DL-072).
- **Clients and measurement:** the client middleware (multicast to the three nodes, retries with backoff and jitter, RTT probes, `ackedUpTo`), the load generator, the experiment runner and the analysis (`crates/client`, `crates/loadgen`, `experiments/`, `analysis/`).

## 2. Consistency model (Phase 1 state)

Phase 1 replicates incidents, resources and response assignments. Presence, discovery and offline changes are Phase 2 (protocol Section 11).

- **Writes are totally ordered and durable at a majority.**
  - Every state change (report, offer, update) is one `Result` entry in the coordinator's log.
  - It is committed once two of the three nodes hold it, and applied in sequence order on every node.
  - A client is answered only after its entry is committed, and from the committed entry itself (DL-061).
  - All nodes therefore apply the same writes in the same order and hold identical state after the same prefix. Committed writes are linearizable: their order is the log order, and that order respects real time, because an acknowledged write is in the log before the reply.
- **Plans are validated optimistically.**
  - An executor plans from a snapshot. Its proposal lists every incident and resource it writes, with the version it read.
  - The coordinator rejects it if any of those changed (I3), and the executor plans again. Plans are therefore serializable with respect to the objects they write.
  - A change to an object a plan only considered can make it less than optimal, never invalid (DL-057).
- **Exactly one result per request.** At most one entry per request ID is committed (I1); retries and duplicate executions return that entry.
- **Reads are served locally and may be stale.**
  - Queries are answered by whichever node receives them, from its applied state, without going through the log (DL-019).
  - A node may lag the coordinator by the entries in flight, so a client can read its own write slightly late from another node. This is bounded by replication delay, and by the catch-up time for a node that just rejoined; a rejoining node is not ACTIVE until it has caught up.
- **Time-dependent state** (incident TTL, assignment and hold expiry, resource leases):
  - It is computed from committed data and the request's client timestamp, clamped to within 5 s of the committed clients' clock. That clock is the median timestamp of the last 31 entries.
  - Every node reaches the same decision, and a client with a wrong clock cannot release units still held (DL-072).
- **Under failure:** with one of three nodes silent, commits continue. During a coordinator election, commits pause for about `T_d` plus two round trips; planning continues. With two nodes down nothing commits, which is outside the single-failure model.
- **Limits:** state is held in memory; a real process crash (not an injected silence) loses that node's copy, and it rejoins as a fresh node. This is outside the failure model (protocol Section 12).

## 3. Load metrics for executor election: what is used and why

The project description (p. 3) lists candidate election criteria: CPU utilisation, request queue length, recent service time, I/O bandwidth, communication reliability with the originating client, communication delay, and others.

| Metric | In the score? | Why |
|---|---|---|
| Queue length `q` (queued plus running plans), plus claims seen since the node's last heartbeat | Yes, weight 250 | Direct measure of work waiting at the node. The local correction stops one heartbeat interval's requests herding onto one node (protocol 5.1, DL-069). |
| CPU utilisation `c` of the node process | Yes, weight 250 | Planning is CPU-bound, and capacity differs between machines. Measured from the process's CPU time, so the load knob's burner raises it (DL-060, DL-070). |
| Recent service time `s` (moving average) | Yes, weight 250, capped at 50 ms | Captures a slow machine or a contended CPU that queue length alone misses. The cap was lowered from 500 ms so the term moves the score (DL-070). It decays while idle so a slowed node is tried again. |
| Communication delay: client-measured RTT to each node | Yes, weight 250, capped at 50 ms | Per client, so it travels in the request, not on heartbeats. Every node sees the same value, so rankings agree (DL-040). |
| Reliability: share of the node's heartbeats missed | Measured, weight 0 | Each node measures it for its peers from its own vantage point, so a non-zero weight makes the three nodes rank from different inputs and disagree, causing competing claims (DL-047). It is logged per peer (`missed_permille_n<i>`) so the report can show it. |
| Reliability of communication with the originating client | No | It would need each node's history of each client, which differs between nodes, for the same reason as above. Client retries and multicast to all three nodes already route around an unresponsive node. |
| I/O bandwidth | No | State is in memory and planning does no disk I/O. Network use per request is small and similar on every node. Network load is measured (`net_tx_bytes`) but is not a bottleneck worth balancing. |

All terms are integers in thousandths, combined with integer weights and quantized to steps of 0.05, with a hash tie-break. All three nodes therefore compute the identical ranking from the same inputs (DL-023). The evidence that the policy works:
- MWBE against five baselines (`experiments/sweeps/latency.toml`);
- the imbalance experiment (`imbalance.toml`, `rm-ctl load`);
- Jain's index over busy time.

## 4. Test plan

| Requirement or use case | Automated test | Live scenario |
|---|---|---|
| One result per request; duplicates harmless (I1) | `replication/tests/scenarios.rs` `competing_executors_get_one_result`; `node/tests/core_task.rs` chaos tests | client retries in every sweep |
| No lost commits (I2); one coordinator per epoch (I4) | `replication/tests/simulation.rs` (random loss, delay, duplication, silences, many seeds); `rules.rs` | `failure.toml` |
| No double assignment (I3) | `two_plans_writing_one_incident_cannot_both_commit`; the real-planner chaos test's ledger audit; planner hold tests | every sweep's committed state |
| Only view members execute (I5) | `a_node_outside_the_view_cannot_commit_results`; selector view tests | — |
| Election and failover | `coordinator_failover_keeps_committing_and_the_old_coordinator_is_fenced`; `silencing_the_coordinator_elects_a_new_one_and_fences_the_old` (real sockets) | demo step 3; `failure.toml` |
| Periodic injection, one node at a time | `scheduled_injections_silence_one_node_at_a_time_and_it_recovers` | demo step 5; `autoinject.toml` |
| Rejoin and convergence before participating | `a_short_silence_needs_no_view_change`; `after_a_short_silence_on_an_idle_cluster_the_node_becomes_active`; `a_retransmitted_append_from_before_a_silence_does_not_end_the_catch_up`; `a_woken_old_coordinator_is_sent_only_what_it_missed` | demo step 4 |
| Failure detection without false suspicion | `fd.rs` unit tests; `no_false_suspicion_while_planning_saturates_every_core`; `a_node_waking_from_a_silence_does_not_suspect_its_peers` | `false_suspects` in every sweep's analysis |
| Executor election spreads load; responds to imbalance | selector tests (`local_correction_spreads_work_between_heartbeats`, competing claims); `the_load_knob_raises_one_nodes_cpu_and_stops_on_time` | demo steps 1–2; `imbalance.toml`, `latency.toml` |
| Planner: deterministic, combines the required factors, bounded cost | `incident/tests/planner.rs` (determinism, urgency, capability, holds, expiry, bounded columns against unbounded); ignored benches | `latency.toml` service times |
| Concurrency and backpressure | `workers.rs` pool tests; `a_full_pool_answers_busy_before_selection`; `an_execute_is_never_dropped_even_when_the_pool_is_full` | saturation points in `latency.toml`, `clients.toml` |
| Measurement pipeline | `analysis/test_analyze.py`, `experiments/test_run_sweep.py` | every sweep |

All of it runs in CI (`.github/workflows/ci.yml`): `cargo fmt --check`, `cargo clippy -D warnings`, `cargo test --workspace` and the Python tests.

## 5. Contributions (to be completed by each member)

The report needs a separate section on each member's role and concrete contribution (p. 6–7). Workstreams and owners follow `docs/phase1-plan.md` Section 2. The git history shows each workstream's first implementation under its author, and the integration commits name whose work they merge.

| Member | Workstream | Code | Commits |
|---|---|---|---|
| Abdallah Afifi | A: replication core, coordinator election, recovery, fault injection; integration; deployment; review fixes | `crates/replication`, `crates/proto`, `crates/node/src/task.rs`, integration of all crates, `docs/deployment.md` | `1cf8819`, `362678b`, the merge and fix commits |
| Hanna Mostafa | B: node runtime, transport, heartbeats, φ-accrual detector, `rm-ctl` | `crates/node/src/{runtime,net,link,heartbeat,fd}.rs`, `crates/ctl` | `2c1561a` |
| Joudy El Gayar | C: executor selection (MWBE), load score, baselines, load knob | `crates/selection` | `6611f2e` |
| Youssef Badawy | D: incident model, planner, planning pool | `crates/incident`, `crates/node/src/workers.rs` | `85a1b2c` |
| Sara Faheem | E: client middleware, load generator, experiment runner, analysis | `crates/client`, `crates/loadgen`, `experiments/`, `analysis/` | `1989da5` |

Each member should add two to four sentences: what they designed, what they measured, which report section they wrote, and which design-log entries are theirs.
