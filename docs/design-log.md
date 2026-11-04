# RescueMesh design log

One entry per decision. Never delete an entry; when a decision changes, add a new entry and mark the old one "superseded by". Each entry records the alternatives considered and the technical reason, because the final report must list decisions that changed and why.

Status values: proposed (one member), agreed (team), superseded.

---

## DL-001. Two election jobs: per-request executor and stable coordinator

- Date: 2026-10-04 (Phase 0 survey). Status: agreed.
- Decision: use a metric-keyed election (MWBE) to pick the executor of each request, and a separate quorum-based, epoch-fenced election for a long-lived coordinator.
- Alternatives: one algorithm for both jobs; full Raft with all requests through the leader; ring algorithms; plain Bully.
- Reason: the two jobs differ in frequency and in what matters (latency and load-awareness against safety). See survey Sections 6 and 7.5.

## DL-002. Survey design as submitted (baseline for later changes)

- Date: 2026-10-04. Status: superseded in part by DL-003 to DL-012.
- Decision, as submitted: any executor commits its result after one peer acknowledgment; "first committed result wins"; coordinator off the request path; static-ID tie-break; elapsed-time watchdog `T_w` for takeover; node states ACTIVE / SUSPECTED / SYNCING; donor named by the coordinator; coordinator picks the silenced node with a seeded random generator.
- Why it is being changed: a post-submission review on 2026-10-13 found the problems recorded in the entries below.

## DL-003. Commits are ordered by the coordinator in a replicated log

- Date: 2026-10-13. Status: agreed 2026-10-20 (proposed 2026-10-13).
- Supersedes: DL-002 "any executor commits after one peer ack" and "coordinator off the request path".
- Problem: the survey used a single "state version" for planning, recovery and the election key, but nothing ordered commits. Example: node 1 commits A through node 2 while node 2 commits B through node 3. Node 1 holds {A}, node 3 holds {B}; there is no version and no replay order.
- Decision: the executor sends its result to the coordinator, which assigns a sequence number, checks resource versions and replicates. The reply goes out after a majority holds the entry. The coordinator is on the commit path, not the compute path.
- Alternatives:
  - Version vectors with mergeable state. Keeps the coordinator off the path, but can only detect a double assignment after the fact. Kept for the Phase 2 offline layer.
  - Full Raft with the leader also executing. Rejected: concentrates compute on one node, which defeats the load-balancing experiments.
- Cost accepted: one extra message per request, commits pause during a coordinator election, and we now implement a small replicated log that the survey said it would avoid.
- Spec: `protocol-v2.md` Sections 3, 5.3, 7.

## DL-004. One result per request is enforced at the sequencer

- Date: 2026-10-13. Status: agreed 2026-10-20.
- Supersedes: DL-002 "first committed result wins".
- Problem: with two executors for one request, each could refuse the other's result (client never answered) or each could accept (two different plans). The determinism argument fails because the two may plan from different snapshots.
- Decision: the coordinator returns the existing result for any `reqID` already in its log. Stated as invariant I1.
- Alternative: resolve between peers by claim key on replication messages. Rejected: more rules, and still needs an order for cross-request conflicts.

## DL-005. Resource-version check at commit

- Date: 2026-10-13. Status: agreed 2026-10-20.
- Problem: two different requests planned from the same snapshot could both assign the same resource. Idempotency by request ID does not cover this.
- Decision: a plan carries the versions of the resources it read; the coordinator rejects it if any is out of date and the executor plans again. Stated as invariant I3.
- Alternative: lock resources during planning. Rejected: holds locks across a long computation and across a possible executor failure.
- Risk: high re-plan rate under contention. Measured and capped.

## DL-006. Agreed membership view; "suspected" is not a node state

- Date: 2026-10-13. Status: agreed 2026-10-20.
- Supersedes: DL-002 node states.
- Problem: each node ranked its own private idea of who was active. The epoch changed only on coordinator elections, so a returning non-coordinator still had a valid epoch and its claims were accepted. A silence shorter than the suspicion threshold never triggered a resync.
- Decision: the view `(viewNo, activeSet)` changes only through committed log entries. Claims carry `viewNo`. Heartbeats carry `viewNo` and `commitIndex`. An injected node always resyncs on wake-up.
- Alternative: gossip-style membership (SWIM). Rejected as unnecessary for three nodes that already share a log.

## DL-007. Takeover on view change, not on elapsed time

- Date: 2026-10-13. Status: agreed 2026-10-20.
- Supersedes: DL-002 watchdog `T_w`.
- Problem: under load a queued request looked like a dead executor, so the next node re-executed it and doubled the load. With cached scores, a silenced node's frozen score could keep it ranked first for every request until it was suspected.
- Decision: a short claim timeout `T_c` (a few round trips) covers "nobody claimed". A claimed request is taken over only when the claimer is removed from the view. The request table has states SEEN / CLAIMED / COMMITTED so a client retry cannot start a second execution.
- Alternative: keep `T_w` and tune it. Rejected: any value is wrong at some load.

## DL-008. Hash tie-break and local score correction

- Date: 2026-10-13. Status: agreed 2026-10-20.
- Supersedes: DL-002 static-ID tie-break.
- Problem: with quantised scores and a static-ID tie-break, one node receives every request when the cluster is idle. With scores cached for a heartbeat interval, all nodes agree on the same "best" node for the whole interval.
- Decision: tie-break by `hash(reqID, nodeID)`; add claims seen since a peer's last heartbeat to that peer's queue length.
- Alternative: fresh bids on every request. Kept as an optional comparison; costs six messages and a wait per request.

## DL-009. Score inputs made obtainable

- Date: 2026-10-13. Status: agreed 2026-10-20.
- Problem: client RTT is per client and cannot ride on server heartbeats; "normalised to [0, 1]" was undefined for queue length and service time; the failure-rate term was self-reported.
- Decision: the client measures RTT per server and sends it in the request; cluster-wide caps `Q_MAX`, `S_MAX`, `R_MAX` define normalisation; the reliability term is the heartbeat miss ratio observed by the ranking node.

## DL-010. Coordinator election details

- Date: 2026-10-13. Status: agreed 2026-10-20.
- Problem: with one node silenced, the two remaining nodes could both stand in the same epoch and refuse each other, with no retry rule. The survey also said both that a new leader must wait for the old lease and that failover takes detection plus two round trips.
- Decision: candidacy delay ordered by ID plus random jitter; random backoff and a new epoch on failure; vote key `(lastEntryEpoch, lastSeq)`. Safety comes from the majority rule and the epoch, because every coordinator action is a log entry; the lease only makes a cut-off coordinator stop sooner. No lease wait before the new coordinator acts.
- Alternative: lease-based safety. Rejected: adds a clock assumption we do not need.

## DL-011. Fault injection through the log

- Date: 2026-10-13. Status: agreed 2026-10-20. Open question for the instructor.
- Supersedes: DL-002 seeded generator held by the coordinator.
- Problem: repeatability broke when the coordinator changed; nothing prevented silencing a node while another was still syncing; no way to target a specific node for the required demo.
- Decision: victim and duration are derived from `hash(seed, k)`, with the counter `k` in replicated state; injection only when all three nodes are active; an operator `SILENCE` command for targeted injection.
- Open: the project description says the servers "elect" the machine to silence. Ask whether a deterministic choice committed by the elected coordinator satisfies this, or whether a bidding round is expected.

## DL-012. Recovery completes when the node applies its re-admission entry

- Date: 2026-10-13. Status: agreed 2026-10-20.
- Supersedes: DL-002 "matches the donor's committed version at the time of the check" and "donor named by the coordinator".
- Problem: under sustained load the donor's version keeps moving; rejoin blocked whenever a coordinator election was running.
- Decision: the joining node takes the missing suffix (or snapshot) from any active node while buffering live entries; the coordinator re-admits it with a `VIEW` entry; the node is active when it applies that entry.

## DL-013. Evaluation baselines and method

- Date: 2026-10-13. Status: agreed 2026-10-20.
- Supersedes: survey Section 7.8 baselines.
- Problem: plain Bully and a per-request ring are easy to beat and cannot show that a metric election is better than a zero-message policy.
- Decision: baselines are random, hash, round-robin, two-choices and always-highest-ID. Failover is measured as a latency excursion. Fairness uses busy time. Five or more seeds with confidence intervals. One failure detector (φ-accrual); the ring baseline and the second detector are dropped to fit the Phase 1 schedule.

---

Entries DL-014 to DL-029 settle the decisions left open by `protocol-v2.md` Section 13. Agreed by the team on 2026-10-20 with the instruction to prefer the robust option where there was a choice. The shared types, traits, config and event schema they describe are implemented in `crates/proto`.

## DL-014. Transport: TCP for messages, UDP for heartbeats, application-level acknowledgment

- Date: 2026-10-20. Status: agreed.
- Decision:
  - TCP between clients and nodes and between nodes, one connection per peer, frames prefixed with a 4-byte length (limit 16 MiB).
  - Heartbeats are UDP datagrams of at most 1200 bytes, sent from a dedicated thread.
  - Every node-to-node message has a per-peer sequence number. Every message carries a cumulative `ack_up_to`; unacknowledged messages are retransmitted every `retransmit_ms` until acknowledged or until the peer leaves the view. A bare `Ack` goes out after `ack_delay_ms` when there is nothing else to send.
  - Client "multicast" is one send to each of the three addresses; IP multicast is not used (see DL-026).
- Alternatives: everything over TCP; everything over UDP with our own reliability; one acknowledgment message per message.
- Reason:
  - Heartbeats on their own socket cannot queue behind large replication messages, and losing one is harmless.
  - A silenced node still accepts TCP data and drops it inside the process, so delivery must be confirmed by the application.
  - Cumulative acknowledgments piggybacked on normal traffic add no messages in steady state, so the per-request message counts in Section 6.2 of the survey still hold.

## DL-015. Wire format: postcard, with JSON for debugging

- Date: 2026-10-20. Status: agreed.
- Decision: message bodies are encoded with `postcard` 1.x. `run.wire_format = "json"` switches every process to JSON for debugging. Every frame and heartbeat carries `PROTOCOL_VERSION`; mismatched versions are dropped and logged.
- Alternatives: bincode; protobuf; JSON only.
- Reason: postcard's 1.x wire format is documented and declared stable, it is compact, and it works directly with `serde`. Protobuf would add a schema compiler for no benefit inside one Rust code base. JSON alone is several times larger and slower to parse at tens of thousands of requests.

## DL-016. Rust stack and toolchain

- Date: 2026-10-20. Status: agreed.
- Decision:
  - `tokio` for asynchronous I/O, `serde` for serialization, `clap` for command lines, `toml` for configuration.
  - The event log is written by our own code, not a logging framework, so its format is exactly the schema in DL-028.
  - The compiler is pinned to Rust 1.99.1 in `rust-toolchain.toml`, edition 2024, and `unsafe` code is forbidden.
  - CI runs formatting, clippy with warnings as errors, and all tests on every push.
- Reason: these libraries do I/O plumbing and parsing only, so none of them hides a mechanism being assessed (project description p. 8). A pinned compiler means all six machines build the same binary behaviour.

## DL-017. Cloud-node parallelization model

- Date: 2026-10-20. Status: agreed.
- Decision:
  - One asynchronous task per connection.
  - A single **core task** owns all protocol state: the log, commit position, view, epoch, request table and peer scores. Other tasks talk to it through channels, so the core needs no locks.
  - Planning runs on a bounded pool of (cores − 1) threads by default (`planner.worker_threads`). Planning threads receive an immutable snapshot and return a result to the core; they never touch shared state.
  - When the planning queue is full (`planner.queue_capacity`), the node replies `Busy` and the client backs off.
  - Heartbeats are sent and received on a dedicated OS thread.
- Alternatives: shared state behind locks; one thread per request; planning on the asynchronous runtime.
- Reason: a single owner makes the protocol state easy to reason about and test. A bounded pool gives backpressure and the queue length the score needs. Keeping heartbeats off the busy threads prevents false suspicion under load.

## DL-018. Client parallelization model

- Date: 2026-10-20. Status: agreed. Open question for the instructor.
- Decision: on each client machine the frontend starts K operating-system processes, each running M simulated users as asynchronous tasks (for example 20 × 50). Each user has its own `ClientId` and sequence numbers.
- Alternative: one operating-system process per simulated user.
- Reason: thousands of separate processes would exhaust client-machine resources before the servers saturate, and the measurements would then describe the clients. Open: confirm that a user as a task inside a process satisfies "simulated user processes".

## DL-019. Operations and read semantics

- Date: 2026-10-20. Status: agreed.
- Decision:
  - The operations are `ReportIncident` (runs the planner), `OfferResource`, `UpdateResource` and `Query`.
  - Every operation that changes state follows the same claim, propose and commit path.
  - Queries are answered by any node from its applied state and may be slightly behind the coordinator. This is documented as the read consistency of Phase 1.
- Alternative: route reads through the coordinator for up-to-date answers. Rejected for Phase 1: it puts the coordinator on every read and is not needed by any required scenario.

## DL-020. Planning algorithm

- Date: 2026-10-20. Status: agreed. Owner: Youssef.
- Decision:
  - Campus map: a synthetic weighted graph of 30 zones; shortest paths by Dijkstra.
  - Priority: an integer weighted sum of severity, age (relative to the request's own timestamp), distance, resource availability and responder capability.
  - Assignment: the Hungarian algorithm over the top-K pending incidents and the available resources. Its cost grows about as K³, so `planner.top_k` is the cost knob.
  - Output: priority, assignments and a route.
- Determinism rules: integer arithmetic only, ties broken by ID, `BTreeMap` for all collections that are iterated, no clock or random numbers.
- Alternatives: greedy assignment (too cheap to load the servers); a linear-programming solver (a library would hide the computation and is hard to make deterministic).

## DL-021. Identifiers

- Date: 2026-10-20. Status: agreed.
- Decision:
  - `NodeId` 1–3.
  - `ClientId` = (client machine, process, user).
  - `ReqId` = (`ClientId`, incarnation, sequence number), where the incarnation is random at process start.
  - An incident's or resource's ID is the `ReqId` of the request that created it.
  - Node-to-node messages carry `MsgId` = (sender incarnation, per-peer counter).
- Reason: this satisfies "every state-changing operation has an unambiguous identifier" without any central allocator, and a restarted client can never reuse an ID.

## DL-022. No snapshots in Phase 1

- Date: 2026-10-20. Status: agreed.
- Decision: the whole log stays in memory and a rejoining node catches up from the log alone. Snapshot transfer is a stretch goal.
- Reason: a run of tens of thousands of entries fits easily in memory, and this removes the riskiest part of rejoin from the critical path.

## DL-023. Determinism rules for anything nodes compare

- Date: 2026-10-20. Status: agreed.
- Decision:
  - Cross-node hashes use 64-bit FNV-1a over an explicit byte encoding, pinned by tests (`proto::hash`). Rust's `DefaultHasher` is never used for these, because it is not guaranteed stable across versions.
  - Load metrics, score weights and quantization are integers in thousandths, so every node computes the identical claim key.
  - `Ranker` and `Planner` implementations are pure functions of their inputs. Policies that look random draw from the request hash, never from a random-number generator; round-robin derives its position from the request ID.
  - The fault-injection victim, duration and interval come from `hash(seed, k)`.
- Reason: two nodes that disagree on a ranking produce competing claims, and two nodes that disagree on a plan break the determinism the project description asks for. These rules remove both sources of disagreement.

## DL-024. Starting values for timeouts and caps

- Date: 2026-10-20. Status: agreed. To be tuned by measurement; later changes get their own entries.
- Values (all in `proto::config` defaults):

| Parameter | Value |
|---|---|
| Heartbeat interval | 100 ms |
| Failure detector | φ threshold 8; timeout clamped to 300–1000 ms |
| Claim timeout | 3 × peer RTT, at least 20 ms |
| Candidacy delay | 150 ms × (3 − node ID) + 0–150 ms jitter |
| Election back-off | 150–300 ms |
| Coordinator lease | 250 ms |
| PROPOSE retry | 200 ms |
| Re-plans per request | at most 3 |
| Retransmit / bare-ack delay | 100 ms / 20 ms |
| Client timeout | 2 s initially, then 3 × observed p99; backoff × 2 with jitter; at most 4 retries |
| Score caps | queue 64, service time 500 ms, RTT 50 ms |
| Score weights | 200 each (thousandths) |
| Quantization | 50 thousandths (0.05) |
| Planner | 30 zones, top_k 100, queue capacity 256 |

- Change from the proposal: the lease is 250 ms, not 300 ms, so it is strictly shorter than the shortest detection timeout. A cut-off coordinator then always stops acting before a peer can suspect it. `Config::validate` enforces this and the other relations below.
- Enforced relations: detection floor ≥ 2 heartbeats; lease < detection floor; score weights sum to 1000; quantum divides 1000; injection duration ≤ 20 s; injection interval > longest injection.

## DL-025. Fault-injection schedule

- Date: 2026-10-20. Status: agreed.
- Decision: the next injection follows 45–75 s after the previous one; the duration is uniform in 1–20 s; both are drawn from `hash(seed, k)`. Injection is off by default and enabled per experiment. The interval must exceed the longest duration, so a node has time to rejoin before the next silence.
- Reason: several failures per run, without the system being degraded most of the time, and every fault sequence replayable from the seed.

## DL-026. Deployment on six machines: clocks and multicast

- Date: 2026-10-20. Status: agreed.
- Decision:
  - All six machines run chrony against the same source. Each process logs its measured clock offset at the start and end of a run (`clock_offset` event).
  - Each process logs its hardware once (`machine_info` event).
  - IP multicast is not used anywhere (DL-014).
- Reason: failover timelines combine events from several machines, so clock error must be measured to be corrected or bounded. Campus networks often block multicast.

## DL-027. One shared configuration file, validated at start-up

- Date: 2026-10-20. Status: agreed.
- Decision:
  - One TOML file is used unchanged on all six machines; each process learns its own identity from the command line.
  - Unknown fields are rejected, so a misspelled setting fails loudly instead of silently taking the default.
  - Every binary calls `Config::validate`, which reports all violated constraints at once.
  - Templates: `config/cluster.example.toml` for the lab and `config/local.toml` for one machine.

## DL-028. Event-log schema

- Date: 2026-10-20. Status: agreed. Owner: Sara (consumer), everyone (producers).
- Decision:
  - One JSON object per line, one file per process.
  - Common fields: wall-clock and monotonic timestamps, run ID, machine, node, event kind and request ID.
  - Kind-specific fields are flattened into the same object, and the event kinds are a fixed list in `proto::event::EventKind`. New kinds are added at the end; existing kinds are never renamed.
  - Every process writes a resource sample once per second.
- Reason: every number in the report must be computable from these files alone, so the schema is fixed before anyone writes code that emits events.

## DL-029. State ownership

- Date: 2026-10-20. Status: agreed.

| State | Owner | Changed by |
|---|---|---|
| Log, commit position, view, epoch | Replication core (Abdallah) | the core task only |
| Request table, peer scores | Selection (Joudy) | the core task, through the selection module |
| Incidents, resources, assignments | State machine (Youssef) | applying committed entries only |
| Connections, retransmission buffers, heartbeat history | Transport (Hanna) | transport tasks and the heartbeat thread |
| Client sequence numbers, RTT estimates | Client middleware (Sara) | the client process |

- Reason: the project description asks for state ownership to be defined explicitly (p. 6), and a single owner per piece of state is what makes the single-core-task model of DL-017 work.

---

Entries DL-030 to DL-037 record decisions made while implementing workstream A (`crates/replication`), 2026-10-20.

## DL-030. Rejoin catches up through the coordinator, not a donor

- Date: 2026-10-20. Status: agreed. Supersedes the donor part of DL-012.
- Decision: the coordinator replicates its log to all three nodes, members of the view or not, so a returning node receives what it missed through the normal `Append` stream and log repair. `Join` only asks to be re-admitted to the view; the `JoinData` message is removed.
- Alternative: a donor chosen among the active nodes sends the missing suffix (DL-012).
- Reason: one replication path instead of two, and it is the path every other test already exercises. The donor's only advantage was that catch-up could proceed during an election. Without a coordinator nothing can commit, so re-admission has to wait for one anyway.

## DL-031. A coordinator that loses its lease becomes a follower, not SYNCING

- Date: 2026-10-20. Status: agreed. Changes protocol Section 7, step 8.
- Decision: on a higher epoch or an expired lease, the coordinator steps down to follower. A node's status changes only when it applies a view that excludes it, or when it wakes from an injected silence.
- Reason: with SYNCING as a separate trigger, a coordinator that was briefly cut off but never removed from the view would need a re-admission it cannot get, because it is already a member.

## DL-032. When a node is ACTIVE

- Date: 2026-10-20. Status: agreed.
- Decision: a node is ACTIVE when it is in the latest view it has applied, and it has applied the commit position it learned from the coordinator after its last silence. At start-up that position is 0.
- Reason: this covers both rejoin cases with one rule. After a short silence the node never left the view and only needs to catch up. After a long one it was removed and the re-admission entry proves it caught up.

## DL-033. A SYNCING node may stand for coordinator after an extra delay

- Date: 2026-10-20. Status: agreed. Refines protocol Section 7.
- Decision: SYNCING nodes wait an extra `2 × fd_max_ms` before standing, instead of never standing.
- Reason: if they never stood, a cluster in which the only reachable nodes are SYNCING would never elect a coordinator. Safety does not depend on this rule, because the pre-vote and vote checks already refuse a candidate whose log is behind.

## DL-034. Coordinator keep-alives and prompt commit notices

- Date: 2026-10-20. Status: agreed.
- Decision:
  - The coordinator sends each peer an `Append` every `heartbeat_ms`, empty if there is nothing new. This doubles as retransmission of anything unacknowledged and renews the lease.
  - When the commit position advances and no `Append` has gone to a peer within `heartbeat_ms / 10`, one is sent.
  - The core does not read UDP heartbeats. It takes the failure detector's verdicts through `set_suspected`.
- Reason: without the prompt notice, followers learned of commits up to 100 ms late, which delayed reads and view changes. The first scenario test exposed this.

## DL-035. The coordinator enforces view membership at commit

- Date: 2026-10-20. Status: agreed.
- Decision: a proposal whose executor is not in the coordinator's latest view is refused with the new outcome `ProposeOutcome::NotInView`.
- Reason: this enforces invariant I5 at the single point every result passes through, rather than trusting each executor's own view.

## DL-036. The majority is always two of the three nodes

- Date: 2026-10-20. Status: agreed.
- Decision: commit and election count acknowledgments from any two of the three nodes, members of the view or not. The view decides only who may execute requests.
- Reason: changing the voting set as the view changes would require Raft's joint-consensus reconfiguration, and nothing in the project needs it. A SYNCING node holds real log entries, so its acknowledgment is as good as anyone's.

## DL-037. How workstream A is tested

- Date: 2026-10-20. Status: agreed.
- Decision: the core performs no I/O and reads no clock, so it is tested in three layers:
  - Rule tests (`tests/rules.rs`): one node driven with hand-written messages, checking the election, commit and fencing rules one at a time.
  - Scenario tests (`tests/scenarios.rs`): three nodes on a loss-free simulated network, one test per behaviour: failover, fencing, lease, rejoin, injection, duplicates, stale plans.
  - Randomized simulation (`tests/simulation.rs`): loss up to 5%, duplication, delays up to 15 ms, competing executors, scheduled injections and random isolations, including runs that lose the majority. Every run checks invariants I1–I5, agreement of committed logs, one coordinator per epoch, and that executors never receive two different results. After a quiet period it checks that the cluster has converged.
- Evidence on 2026-10-20:
  - All tests pass, including 2,000 randomized runs (`SIM_SEEDS=1000`).
  - Eleven deliberately broken versions of the core (no version check, no duplicate check, the Raft "Figure 8" commit rule, votes or pre-votes ignoring log freshness, no log matching, no view check, no lease, no catch-up wait) are each caught by at least one test.

## DL-038. Executor selection: details settled while implementing workstream C

- Date: 2026-10-21. Status: agreed (integration review, 2026-11-02) (C), for review by B.
- Context: `crates/selection` implements protocol Section 5 and the baselines of Section 10. The protocol leaves the following points open; each is decided here.
- Decisions:
  - **Unknown client RTT.** Until the client has measured its round trip to a node, the RTT term for that node is the cap (1000 thousandths). A node the client has never heard back from ranks as the furthest away. When no RTT is known for any node, the term is equal for all and has no effect.
  - **Quantization rounds down.** Scores 0.200 to 0.249 are all step 4 with the default quantum of 0.05.
  - **Fresh bids reuse the MWBE ranker.** `MwbeBids` ranks with the same key as `MwbeCached`; only the source of the scores differs. Collecting a bid per request is a runtime concern and stays optional (Section 10).
  - **Baselines.** `random` is a per-request permutation by `hash(reqID, nodeID)`; `hash` puts node `hash(reqID) mod n` first and the rest in ring order; `round-robin` cycles through the nodes per client incarnation from a hashed offset, so different clients do not start on the same node; `two-choices` takes the lower MWBE key of two nodes drawn from the request hash; `highest-id` always puts the highest ID first (plain Bully, no balancing).
  - **Competing claims use the MWBE key under every policy.** A claim always carries the MWBE key, so two competing claims are resolved the same way whatever the policy. Under MWBE the first-ranked node always holds the lowest key.
  - **A claim may arrive before the request.** The client sends to all three nodes, so a peer's claim can overtake the request. The row is created as CLAIMED and the request is stored when it arrives, in case the claimer later leaves the view.
  - **Own claims survive SYNCING.** A node that is SYNCING but still in the view keeps its claims, because the other nodes still see it as the claimer and the coordinator accepts proposals from view members. It aborts its claims only when it applies a view that excludes it.
  - **Acknowledged requests are ignored.** A request with `seq ≤ ackedUpTo` is a stray retry of something the client already has; rows at or below `ackedUpTo` are dropped.
  - **Queries bypass selection.** Read-only requests are answered from applied state by the runtime (DL-019) and never enter the request table.
  - **Rank-1 hint.** The node passed to `Core::set_rank1_hint` is the view member with the lowest quantized load score, ignoring client RTT, ties to the lower ID.
  - **Load knob.** A CPU burner (`selection::knob::CpuBurner`) burns a chosen share of a chosen number of cores in 10 ms busy/idle cycles. It affects the score only through measured CPU and slower planning, as real load would.
- Evidence: unit tests for the score (including the worked example), every policy (each returns a permutation of the view, is deterministic, and an idle cluster spreads requests), the request table, claim timeouts, competing claims, takeover on view change, retries, acknowledgment clean-up and the local correction; plus a three-node simulation that delays claims up to twice the claim timeout over 20 seeds and four policies, and checks that all nodes agree on exactly one executor per request.


---

Entries DL-039 to DL-045 record decisions made while implementing workstream E (`crates/client`, `crates/loadgen`, `analysis/`, `experiments/`). Numbered separately so they merge cleanly with the other workstreams' entries; renumber when merging if the team prefers.

## DL-039. One connection per node per process, shared by all simulated users

- Decision: a client process holds three TCP connections (one per cloud node) and multiplexes every user's requests over them. Replies are matched to waiting requests by `ReqId`.
- Alternative: three connections per simulated user.
- Reason: 1,000 users per machine would need 3,000 sockets and tasks, and the client machine would saturate before the cloud (DL-018). Nodes see fewer, busier connections, which is also how a real frontend behaves.

## DL-040. Client RTT is measured by TCP handshake probes

- Decision: every ~2 s the client times a fresh TCP connect to each node and keeps an exponential average (weight 1/4). That value goes into `ClientReq::rtt_us`.
- Alternatives: reply latency (includes planning time and exists only for the one node that replied); a ping message (needs a new `proto` message).
- Reason: the handshake is completed by the node's kernel, so it measures the network path and is not inflated by the node's load, which is what the score's communication-delay term should see. Limitation: it cannot see a silenced process (DL-014: the kernel still accepts). Silence is handled by the failure detector and by retries.

## DL-041. Retry policy details

- Decision: same `ReqId` on every attempt; all three nodes get every attempt. Timeout = 2 s until 200 single-attempt samples exist, then 3 x the p99 of the last 4,096 first-attempt latencies (clamped to 100 ms..30 s), times `backoff_factor`^attempt, with +-25% jitter. A `Busy` or `Error` reply cuts the wait short and resends after 50 ms x factor^attempt (50-150% jitter). At most `max_retries` resends; then the request is reported as `gave_up`.
- `acked_up_to` is a gap-free watermark: the highest n with replies for every seq 1..=n. A given-up request leaves a gap on purpose, so nodes keep its request-table row.
- Latency is measured on the client's own clock from the first send to the first `Ok` reply, so a failover shows up as retried requests with high latency, which is what a user experiences.

## DL-042. Event fields added by the client

- `client_send`/`client_reply`/`client_retry` carry `phase` (`warmup`, `measure`, `cooldown`) and `op`. `client_reply.latency_us` is from the first send; `attempt_latency_us` from the latest send. Outcomes: `ok`, `busy`, `error`, `gave_up` (no `from`). `resource_sample` from a client process adds `cpu_core_permille`, `submitted`, `completed`, `gave_up`, `dropped_client`; the frontend's has `scope = "machine"`. `clock_offset` adds `when` (`start`/`end`).
- Clock-offset convention (everyone who logs `clock_offset` must follow it): `offset_ns` is positive when the machine's clock is ahead of the reference; corrected = local - offset.

## DL-043. Workload: seeded mix, open and closed loop, explicit phases

- Decision: each user draws operations from `hash(seed, machine, process, user)`, so the sequence is the same in every run (which resource an update targets depends on which offers have completed). Open loop: Poisson arrivals on a drift-free schedule, dropped and counted when a process has `max_inflight` outstanding. Closed loop: one outstanding request per user plus optional exponential think time. All processes share one wall-clock `start_at`; warm-up, measured window and drain are fixed from it, and requests are classified by the phase in which they start.
- Reason: open loop for latency curves (a slow system cannot slow the offered load, so there is no coordinated omission); closed loop for peak throughput. `dropped_client > 0` is reported as a warning because it means the client machine limited the load.

## DL-044. Statistics

- Decision: percentiles are computed per run from every measured request; confidence intervals are the 95% Student-t interval over the per-seed values (at least five seeds). Failover time = from the start of the silence until the 0.5 s-window p99 of completions stays within max(1.5 x baseline, baseline + 5 ms) for 2 s; baseline = median window p99 over the 10 s before. Empty windows count as breaches. Also reported: time to the committed `VIEW` removal, recovery time (end of silence to `active`), retries during the excursion.
- Reason: runs are the independent samples; pooling requests across a run would give far too narrow intervals because requests within a run are correlated.

## DL-045. A stand-in cloud for client-side development

- Decision: `client::testkit::StubCluster` (and the `rm-stub` binary) speaks the real client protocol with a fixed service time, a bounded worker pool, `Busy` replies and silencing on the real seeded schedule. It writes node-side events so the analysis can be exercised end to end.
- Reason: the load generator, retry logic and analysis had to be tested before the nodes exist (M1). It is a test fixture; its numbers are never reported.

---

Entries DL-046 and DL-047 come from the review of the `selection` branch before it was merged, 2026-10-29.

## DL-046. Claims are kept per claimer, and valid while the claimer stays in the view

- Date: 2026-10-29. Status: agreed (integration review of workstream C). Refines DL-038 and protocol invariant I5.
- Problems found:
  - Each node kept only the lowest claim it had seen. A survivor that held a removed node's claim discarded a successor's higher claim, then took over after the view change and claimed too. In a reviewer's simulation, half of staggered view changes ended with two nodes executing the same request and disagreeing on who held it.
  - A claim carrying an older view number was ignored even when its sender never left the view. Every view change therefore produced competing executions for requests claimed just before it.
- Decision:
  - Each request row keeps every valid claim it has received, by claimer. The executor is the lowest key among them.
  - When a view change removes the executor, the lowest remaining claim takes over; that node resumes executing if it had aborted. Only when no claim remains is the request ranked again.
  - A claim is valid if its sender has been in the view continuously since the view number the claim carries. A claim from a view this node has not applied yet is kept and checked when that view is applied.
- Safety: the coordinator still refuses results from executors outside its current view (DL-035), and its duplicate check (I1) makes any remaining double execution harmless.
- Evidence: regression tests for staggered takeover and for resumed execution fail on the previous code and pass now. There are also tests for claims from continuing members, re-admitted members and not-yet-applied views.

## DL-047. The missed-heartbeat term is off by default

- Date: 2026-10-29. Status: agreed (integration review). Changes the DL-024 default weights.
- Problem: the score's reliability term `f` is each node's own count of a peer's missed heartbeats (protocol Section 5.1). Different nodes therefore rank from different inputs, which breaks the determinism rule of DL-023. A reviewer measured disagreement on 38 of 1,000 requests from a 5% miss rate seen by one node. Every disagreement costs a competing claim or a claim-timeout stall.
- Decision: the default weights are now 250 for queue, CPU, service time and client RTT, and 0 for missed heartbeats. The term stays in the code and the config for experiments.
- Reason: an unreliable node is already handled by the failure detector and removed from the view. A per-node opinion adds no protection and costs agreement.
- Alternative for later: carry the client's per-node failure ratio in the request, next to its RTTs. Every node would then see the same value, and it would match the spec's "communication reliability with the originating client". This needs a field in `ClientReq`.

---

Entries DL-048 and DL-049 come from the review of the `client-loadgen` branch before it was merged, 2026-10-29.

## DL-048. `ackedUpTo` counts given-up requests as finished

- Date: 2026-10-29. Status: agreed (integration review of workstream E). Refines protocol Section 5.2.
- Problem: the client's watermark counted only answered requests. One given-up request left a permanent gap, so for the rest of the run that user's `ackedUpTo` never advanced. Nodes could never forget its rows, and the client's set of out-of-order completions grew without bound. In the first stand-in run 12% of requests gave up, so almost every user would stall in a failure experiment.
- Decision: `ackedUpTo` is the highest sequence number such that the client is finished with every request up to it, answered or given up. The client never sends a given-up request again.
- Safety: a late execution of a given-up request is still harmless. The coordinator commits at most one result per request (I1), and selection keeps a per-incarnation high-water mark so a delayed copy is not executed again after its row is gone (DL-046).

## DL-049. Failover time is measured from the first out-of-band window

- Date: 2026-10-29. Status: agreed (integration review). Corrects the failover definition of DL-044.
- Problem: the analysis took the failover time as the first point after the silence where 2 s of windows were back in band, searching from the silence itself. When the latency spike started 2 s or more after the silence, which is typical (client timeout, failure detection, view change), the windows before the spike counted as recovery. The result was 0 s and "no excursion" for exactly the worst failovers; the first stand-in summary already contained one such row.
- Decision: recovery is searched for only after the first out-of-band window. `no_excursion` means no window after the silence was out of band. The hold is set in seconds (`hold_s`, 2 s), not in windows, so changing `--window-ms` keeps the 2 s rule. `retries_during` counts requests that needed a retry, not retry events.
- Evidence: a new test with a spike starting 2.5 s late fails on the previous code and passes now. A no-spike test checks the other branch.

---

Entries DL-050 and DL-051 close the minor items left from the 2026-10-29 integration review.

## DL-050. Under a baseline policy, competing claims follow the policy's ranking

- Date: 2026-10-29. Status: agreed. Supersedes the "competing claims use the MWBE key under every policy" point of DL-038.
- Problem: claims always carried the MWBE load key. Under a baseline (random, hash, round-robin, two-choices, highest-ID), suppose the node the policy ranked first was busy and the next node timed out and claimed too. The less loaded claimer then won. The baselines quietly leaned towards load balancing, which flatters them in the comparison with MWBE.
- Decision: under MWBE the claim key is still the quantized load score. Under every other policy it is the claimer's position in the policy's ranking (0 for first), with the same hash tie-break. The policy's own first choice therefore wins every competing claim.
- Evidence: a test with the random policy and a heavily loaded first choice fails under the old keys and passes now.

## DL-051. Load-generator timing: shared phase boundaries, latency from the scheduled arrival

- Date: 2026-10-29. Status: agreed. Refines DL-043 and DL-044.
- Decisions:
  - **Phase boundaries** (end of warm-up, end of measurement) are wall-clock times computed from the shared `start_at_ms`, not timers started when each worker begins. A worker that started late, for example after a slow connect, still measures the same window as every other worker.
  - **Open-loop latency** is measured from the request's scheduled arrival time, not from when the client got round to sending it. A busy client therefore cannot hide its own queueing delay, which would otherwise under-report the tail (coordinated omission). Closed-loop latency is unchanged.
  - **Resource figures** now show memory (client machines: used memory; cloud nodes: RSS) and network throughput in and out, computed as MB/s from the cumulative byte counters. A counter reset and a run of missing samples both show as gaps rather than as false spikes or straight lines.

## DL-052. The node's core task enforces the selector's runtime rules

- Date: 2026-10-29. Status: agreed. Owner: Abdallah (core-task wiring).
- Decision: `crates/node` gets a core task, `NodeTask`. It owns the replication core, the selector and the state machine, and talks to the planning pool through a small `PlanPool` interface that workstream D implements. Like the modules it drives, it performs no I/O and reads no clock, so it is tested in a deterministic simulator.
- Rules enforced:
  - **Never drop an `Execute`.** `PlanPool::submit` cannot refuse a job. A request already being planned is not submitted twice. A job aborted after its proposal was sent is kept, so its outcome is still answered.
  - **Reply `Busy` before selection.** A new write request, or a retry of one still unclaimed, that arrives while `PlanPool::is_full` is true is answered `Busy` and never reaches the selector. Retries of a request the selector holds as claimed or committed still go to the selector, which answers them without new planning work.
- Also:
  - Reads are answered from the state machine and never enter selection (DL-019).
  - A `Stale` outcome sends the request back to the pool with its attempt number. The planner returns `Refused` once the attempt exceeds `max_replans` (workstream D's rule).
  - Heartbeats older than the last one from the same process are dropped.
- Evidence: seven end-to-end tests with three full nodes and fake pools. Four deliberately broken versions are each caught: no Busy check, the Busy check skipped for new requests, `Execute` dropped when full, and no re-planning. A chaos test with message loss, injected silences, isolations and pools of 2 to 32 slots passes 50 seeds; every client gets exactly the committed result.

---

Entries DL-053 to DL-055 record decisions made while implementing workstream B (`crates/node`: `net`, `link`, `heartbeat`, `fd`, `runtime`), 2026-11-01.

## DL-053. Heartbeats and the φ-accrual failure detector

- Date: 2026-11-01. Status: agreed. Owner: Hanna. Implements Section 6 of the protocol and the detector values of DL-024.
- Decision:
  - Heartbeats are UDP datagrams sent every `timeouts.heartbeat_ms` from a dedicated OS thread (`node::heartbeat`), which also receives them and runs one detector per peer. The thread never waits for the core task or the planning pool. The core task refreshes the heartbeat's content (epoch, coordinator, view, commit, load) once per interval through a shared slot; the thread stamps its own sequence number `n`.
  - Detector (`node::fd`): sliding window of the last 100 inter-arrival times, normal model, `φ = −log10 Q((t − μ)/σ)`. Suspect when `φ > fd_phi_threshold`, which is the same as a timeout `T_d = μ + z·σ` with `Q(z) = 10^−threshold` (z ≈ 5.61 for φ = 8). `T_d` is clamped to `[fd_min_ms, fd_max_ms]` = 300–1000 ms. σ is kept at least a tenth of the heartbeat interval, so a perfectly regular LAN cannot make φ infinite. Until two samples exist, μ = the heartbeat interval and σ = a quarter of it.
  - **Rejoin.** A gap longer than `fd_max_ms` is never a sample. When a suspected peer is heard again its window is cleared and rebuilt, so a 20 s silence does not distort the estimate.
  - **Own silence.** While this node is silenced the thread sends nothing and drops what it receives. On waking it restarts every detector's timeout from now: the gap was its own.
  - The detector reports changes to the core task, which turns a suspicion into a `VIEW` entry (DL-006). The core's `suspect` event is enriched with `phi`, `silent_ms`, `timeout_ms` and `samples`, so detection time can be read from the logs without a second event kind.
- Alternatives: a fixed timeout (does not adapt to load or network); heartbeats on the async runtime or the core thread (planning load could delay them, the false-suspicion risk DL-017 exists to avoid).
- Evidence: unit tests for the tail function and its inverse, regular and erratic arrivals, the floor and the ceiling, rejoin, start-up and own-silence restart. On localhost (`crates/node/tests/runtime_localhost.rs`), a silenced follower is removed from the view about 250–280 ms after it went quiet, within the 300–1000 ms bound when measured from its last heartbeat; with every core saturated by planning threads (three nodes, each with as many spinning threads as cores, pools full), no node was suspected over a 6 s run.

## DL-054. Transport: threads, connections, preambles, acknowledgment

- Date: 2026-11-01. Status: agreed. Owner: Hanna. Implements DL-014; changes the "asynchronous task per connection" part of DL-017.
- Amends DL-014 in three ways, agreed at the integration review on 2026-11-02: two TCP connections per pair of nodes instead of one, retransmission with exponential backoff up to 1.6 s and bursts of at most 256 instead of a fixed `retransmit_ms`, and a cap of 4,096 unacknowledged bodies per peer. Above the cap the oldest bodies are dropped while the peer is still in the view; the replication core resends `Append`, `Propose` and `Join` and claims time out, so only a forwarded operator `Silence` can be lost.
- Decisions:
  - **Threads, not async tasks.** The node uses OS threads with blocking `std::net` sockets: one acceptor, one reader per inbound connection, one writer per peer and per client connection, the heartbeat thread, the core thread and the planning threads. With three nodes and a few dozen client processes per run (DL-039) this is under two hundred threads, well within what an OS schedules easily, and blocking code is simpler to reason about and to test. The single core task owning all protocol state (DL-017) is unchanged. `tokio` remains in use on the client side.
  - **One port, three kinds of connection,** told apart by the first four bytes: `RMP1` plus a node-id byte for a peer, `RMO1` for the operator tool, anything else for a client. A client frame starts with a 4-byte length of at most 16 MiB, whose first byte is 0x00 or 0x01, never `R`, so clients need no preamble and `crates/client` is unchanged.
  - **Two connections per pair of nodes,** one per direction: each node dials each peer for its own outgoing messages and reads incoming messages on the connections the peers dialed. A writer thread per peer reconnects with back-off (50 ms doubling to 1 s).
  - **Acknowledgment** (`node::link`): per-direction streams numbered from 1 under `msg_id.incarnation`; cumulative `ack_up_to` piggybacked on every envelope; a bare `Ack` with `n = 0` after `ack_delay_ms` if nothing else carried it; bare acks are never numbered, acknowledged or retransmitted. Receivers deliver each number once and accept out-of-order numbers (the protocol tolerates reordering; waiting for a gap would stall live traffic behind a retransmission).
  - **Retransmission** of the oldest unacknowledged messages after `retransmit_ms`, at most 256 per round, with the interval doubling up to 16 × `retransmit_ms` (at least 1 s) while nothing is acknowledged, and reset by any progress, a reconnect or waking from silence. A peer silenced for 20 s therefore costs a trickle, not a flood.
  - **Giving up on content.** "Retransmit until acknowledged or until the peer leaves the view" is implemented by turning queued bodies into `Ack` *fillers* that keep their numbers, when the peer leaves the committed view and when more than 4096 bodies are waiting. The receiver still gets every number, so its cumulative acknowledgment can advance; stale content (old votes, appends the core resends anyway) is not replayed.
  - **Peer restart** (outside the failure model) is detected from a new heartbeat incarnation, and the link to that peer then restarts its stream. Detecting it from stream incarnations instead could make two links restart each other forever.
  - **Silence switch** in the transport: inbound envelopes and client requests are read and dropped before they are acknowledged or delivered, nothing is sent, new outgoing messages are discarded, and TCP connections stay open, so the node looks exactly like a process that stopped responding.
  - **Replies** are routed by `ClientId` to the connection that last carried a request from that user.
  - **Operator channel:** after `RMO1`, one text command per line, `status` or `silence <1|2|3|coordinator|rank1> <ms>`; the answer is one line (`ok: …`, `error: …`, or the status as JSON). It needs no new types in `proto`. Status is answered even while the node is silenced, so the demo can watch it; a silenced node refuses `silence`. `rm-ctl` implements both commands, which `experiments/run_sweep.py` already calls.
  - **Shared code:** the node writes its event log with `client::EventLog` and takes its resource, machine and clock samples from `loadgen::sampler`, so every process logs the same way. Each node logs a `resource_sample` per second with `scope = "node"`, CPU, RSS, network bytes, queue length and the message counters (heartbeats, new messages, retransmissions, bare acks, duplicates, client requests and replies) needed for the overhead measurements, and a `clock_offset` every 30 s (nodes are killed at the end of a run, so there is no "end" sample).
- Evidence: unit tests for numbering, piggybacked and bare acks, backoff and the burst limit, stale acks, out-of-order delivery, fillers, the body cap, peer restart, and 1000 messages over a link with 30% loss in both directions (all delivered exactly once). Four localhost tests with real sockets: election and 60 client requests served by all three nodes; an operator silence detected, served around and rejoined; the coordinator silenced, a new one elected and the old one re-admitted; and no false suspicion under CPU saturation.

## DL-055. A placeholder planner until the incident service is merged

- Date: 2026-11-01. Status: superseded by DL-065 (the placeholder is kept for runtime stress tests only). Owner: Hanna (to be removed by Youssef).
- Decision: `node::dev` holds a minimal `StateMachine` and planning pool so `rm-node` runs end to end now. The planner follows the contract (pure, deterministic, read set recorded, `Refused` after `max_replans`) but plans no assignments. `rm-node --dev-spin-us N` burns N µs of CPU per plan for load tests. The runtime takes the state machine and a pool factory as parameters (`runtime::start`, `PoolFactory`, `PlanSink`), so workstream D replaces both in `crates/node/src/main.rs` and nothing else changes.
- Numbers measured with the placeholder are never reported.


---

Entries DL-056 to DL-061 record decisions made while implementing workstream D (`crates/incident`, `crates/node/src/workers.rs`), 2026-11-01. Owner: Youssef.

## DL-056. Planner details

- Date: 2026-11-01. Status: agreed (integration review, 2026-11-02). Refines DL-020.
- Decision:
  - **Priority** = 1000 × severity + 2 × seconds waited (capped at 600 s). Ten minutes of waiting is worth 1.2 severity levels, so age breaks ties within a level and can lift an old incident one level, no more.
  - Distance, responder capability and resource availability enter through the **assignment cost** of each incident–resource pair: walking distance on the campus map, plus 20 for the second-choice kind of responder (40 for the third), plus 30 per unit the resource is short.
  - **Leaving an incident unassigned** costs 100 000 + its priority. That is larger than any real pair, so an incident is always helped when a capable resource is free, and when two incidents want one resource the more urgent one gets it.
  - **Units needed**: 1 for severity 1–2, 2 for 3–4, 3 for 5. Each incident gets one responder.
  - **Capability**: a fixed table from category to responder kinds, best first (`incident::planner::capable`).
  - **Campus map**: a ring of 30 zones plus a chord to the zone five steps on, weights 1–9 from FNV-1a, so every node builds the identical map from the zone count alone.
- Alternatives: putting distance and availability into the priority as well. Rejected: the same quantities would be counted twice, and the plan would be harder to explain.
- Evidence: `crates/incident/tests/planner.rs` (more urgent wins, capability respected, units never over-given, determinism).

## DL-057. Plans re-dispatch waiting incidents; incidents are versioned at the coordinator

- Date: 2026-11-01. Status: agreed (integration review, 2026-11-02). Changes `replication::core` and the `PlanOutput::read_set` contract; Abdallah to review.
- Decision:
  - A report is planned as a fresh assignment of the top `top_k` open incidents, the new one always included, and **every incident the solution assigns is written**. An incident that found no help when reported is therefore picked up by a later plan.
  - The coordinator now versions **incidents as well as resources** (`versioned_ids` in `core.rs`), so two concurrent plans that both assign one waiting incident cannot both commit. `Snapshot::incidents` carries each incident's version.
  - The **read set is exactly the incidents and resources a plan writes**, with the versions it saw. Objects it only considered are left out.
- Alternatives:
  - Write only the new incident's assignment. Rejected: a waiting high-priority incident would win a resource in every later solve without ever being written, so that resource would sit idle until the incident expired.
  - Put every considered resource in the read set. Rejected: any commit that touches any resource would make every concurrent plan stale, which serializes commits and wastes most planning work.
- Reason: a change to an object a plan only considered can make the plan less than optimal, never invalid; I3 needs only the written objects unchanged.
- Evidence: `two_plans_writing_one_incident_cannot_both_commit` (replication) fails on the old coordinator and passes on the new; the real-planner chaos test (DL-061) finds incidents with two responders on the old coordinator and none on the new.

## DL-058. The planning snapshot holds open incidents only, and expired items are pruned

- Date: 2026-11-01. Status: agreed (integration review, 2026-11-02).
- Decision:
  - `Snapshot` holds every resource but only `Open` incidents, and no assignments. Full incidents and assignments are kept beside it for queries.
  - At every entry whose `seq` is a multiple of 1024, incidents and resources that expired more than 60 s before the latest client timestamp in a committed write are dropped from the snapshot.
- Reason: a planning thread holds an `Arc` of the snapshot, so applying an entry copies it. Over a run of tens of thousands of reports, copying every incident on every commit would make the core task, which applies entries, the bottleneck. Pruning uses log position and committed timestamps, never a local clock, so every node prunes identically.

## DL-059. A prioritized responder list makes the cost knob real

- Date: 2026-11-01. Status: agreed (integration review, 2026-11-02). Adds `Plan::responders`.
- Problem: measured on its own, the assignment step took 0.23 ms at `top_k` 100. Most incident–resource pairs are incompatible, so the Hungarian algorithm finishes far below its worst case. At that cost the nodes are network-bound and load balancing has nothing to balance.
- Decision: each plan also computes the **prioritized responder list** the project description gives as an example output (p. 3). For up to `top_k` capable responders, the assignment is solved again with the new incident forced onto that responder, and responders are ranked by the total cost. The list is the substitutes to send if the assigned responder cannot go.
- Measured (release build, 300 resources, 400 waiting incidents):

| `top_k` | ms per plan |
|---|---|
| 10 | 0.10 |
| 25 | 0.46 |
| 50 | 1.89 |
| 100 | 9.57 |
| 200 | 52.6 |

  Doubling `top_k` costs about 5×, so service time can be set predictably from the configuration.
- Alternatives: a busy-loop or repeated identical solves. Rejected: CPU spent on nothing the client receives is a sleep in disguise and would be hard to defend in the report.
- Evidence: `service_time_against_top_k` (ignored by default; run with `--release -- --ignored --nocapture`).
- Amended 2026-11-02 (DL-071): the list is still ranked over up to `top_k` candidates, so the cost knob is unchanged, but a plan keeps only the best 10. One hundred substitutes made every log entry about 1.2 KB (2.5–3 KB in memory) and bloated replication, catch-up and memory on all three nodes; ten are more than a dispatcher would use.

## DL-060. The planning worker pool

- Date: 2026-11-01. Status: agreed (integration review, 2026-11-02). Implements DL-017 and the `PlanPool` rules of DL-052.
- Decision:
  - `node::WorkerPool`: `planner.worker_threads` OS threads (default cores − 1), one shared queue under a mutex and condition variable. Results go back through a callback, which the runtime forwards to `NodeTask::on_planned`.
  - `submit` never refuses. `is_full` is true when queued plus running jobs reach `planner.queue_capacity`, the admission limit the core task checks before selection.
  - A job past `timeouts.max_replans` is answered `Refused` without planning, so the request commits and its retries are answered.
  - **Load sample**: queue length counts queued and running jobs; service time is a moving average with weight 1/8 on the newest plan; CPU is this whole process's share of the machine, from `sysinfo`, re-measured at most once per `sysinfo`'s minimum interval. Measuring the whole process means the load knob's burner threads raise the score, which the load-imbalance demo relies on.
- Evidence: five unit tests in `workers.rs` (every job planned once past the limit, re-plan cap, cancellation, service-time average, clean shutdown).

## DL-061. An executor answers from the committed entry, not its own copy

- Date: 2026-11-01. Status: agreed (integration review, 2026-11-02). Changes `node::task`; Abdallah to review.
- Problem found: with the real planner in the three-node chaos simulator, seed 5 answered a client with a plan different from the committed one. Sequence: a retransmitted `PROPOSE` drew two `Stale` replies; the executor re-planned on the first and that plan committed; the second, late `Stale` was taken as the answer to the new plan, so the executor re-planned again and overwrote its copy; the coordinator then reported `Committed` (it matches by executor) and the executor answered from the overwritten copy. The fake planner rarely produced `Stale`, so this had not appeared.
- Decision: on `Committed(seq)` the executor answers with the result in the committed entry. If the entry is not yet applied locally, it answers when that entry is applied.
- Alternatives: tag each proposal with an attempt number so late replies can be discarded. Rejected for now: it changes the wire protocol for a race that answering from the log already makes harmless.
- Evidence: `the_real_planner_keeps_assignments_consistent_under_faults` (node) passes 40 seeds, as does the existing chaos test; before the fix, seed 5 failed.

## DL-062. Waiting incidents are dispatched when capacity appears; updates never free assigned units

- Date: 2026-11-02. Status: agreed (integration review of workstream D). Changes DL-057 and the update semantics of DL-056.
- Problems found:
  - `UpdateResource` set `available` directly, ignoring units already assigned. A 3-unit vehicle assigned in full, then updated to 3, was handed out a second time: 6 units from a capacity of 3.
  - Every report plan also reassigned and wrote the waiting incidents it touched. Concurrent reports therefore all wrote the same waiting incidents, and all but one were rejected as stale. In the reviewer's chaos runs up to 46% of requests ended `Refused` after the re-plan cap; the client got an answer, but the report was lost.
- Decisions:
  - A report plan writes only its own new incident and, if it gets one, that incident's resource. The waiting incidents still take part in the solve, so the new incident does not take a responder a more urgent one should have.
  - Waiting incidents are dispatched by the plan that makes capacity appear: a new offer, or an update. That plan gives the resource's free units to the most urgent capable waiting incidents, at most `top_k` of them, and writes only that resource and those incidents.
  - `UpdateResource { available }` sets how many units the owner offers in total, assigned ones included. Capacity never drops below the assigned units, so `capacity == assigned + available` always holds and a unit can never be given out twice.
- Evidence:
  - New planner tests: an update after a full assignment hands out nothing more; an update that frees units dispatches a waiting incident; an offer dispatches waiting incidents, most urgent first; a report writes only its own incident and resource.
  - The real-planner chaos test now sends resource updates, and its audit of `assigned + available == capacity` covers them.
  - Give-ups after the re-plan cap fell from up to 46% to under 1% on 15 of 16 seeds (DL-063).

## DL-063. Re-plan only after catching up; known limit under overload

- Date: 2026-11-02. Status: agreed. Refines DL-052.
- Decision: after a `Stale` rejection the executor waits until it has applied the log up to the coordinator's position at the rejection, at most 500 ms, before planning again. Planning at once read the same old snapshot and was rejected again. A late duplicate `Stale` for a send already re-planned is ignored.
- Also:
  - The state machine copies its snapshot only for entries that write something, so view, no-op and injection entries never force a copy while planning jobs hold the snapshot.
  - A planner panic becomes a `Refused` result instead of killing a worker and stranding a claimed request.
  - `PROTOCOL_VERSION` is now 2, because `Plan` gained `responders`.
- Known limit: a job plans from the snapshot taken when it was queued. Under overload, with long queues and one planning thread at 200 requests/s, about 7% of requests still give up after the re-plan cap; on every other measured seed it is under 1%. The fix is to take the snapshot when a job starts. That changes the pool interface shared by workstreams B and D, so it is deferred past the 2 November code freeze.

## DL-064. Runtime fixes from the integration review

- Date: 2026-11-02. Status: agreed (integration review of workstream B).
- **No false suspicion on waking.** The heartbeat thread re-read the silence flag in its detection step, after a receive wait of up to 5 ms. The silence usually ended during that wait, so the detectors ran before being restarted, against heartbeats from before the silence. The woken node suspected both peers after every injection (3 of 3 in the reviewer's probe). That inflated the false-suspicion count and delayed its `Join`. The detection step now uses the value read at the top of the loop. A regression test checks that the victim logs no `suspect` after `inject_end`; it fails on the previous code.
- **Client connections are closed on every writer exit.** After a write error or the 5 s write timeout, the writer returned without shutting the socket. The node kept executing that client's requests but never answered them. Both directions are now shut down on every exit, so the client reconnects.
- **Test timing.** The localhost end-to-end test asserted more than 10 heartbeats after a run that takes about 0.2 s on a fast machine; it now waits for them.
- Follow-ups recorded, not done before the code freeze (all but retiring old peer incarnations were done in DL-067 and DL-073):
  - a bounded client input channel;
  - retiring old peer incarnations in the link;
  - logging version and decode errors in `resource_sample`;
  - `rm-ctl` reporting `ok` only when an injection actually starts;
  - logging `reply` only when it was sent;
  - resolving peer addresses at start;
  - a failing exit code after a core panic;
  - tests for `rm-ctl`.

## DL-065. Integration of workstreams B and D in `rm-node`

- Date: 2026-11-02. Status: agreed. Owner: Abdallah (integration).
- Decisions:
  - `rm-node` runs workstream D's state machine (`incident::State`), planner (`IncidentPlanner`) and worker pool (`WorkerPool`) on workstream B's runtime. Finished plans reach the core task through B's `PlanSink`. B's placeholder pool is kept for stress tests only, behind `--dev-spin-us`; the startup line names the planner in use.
  - The pool reports each plan's service time with the result. The core task logs it as `plan_end` (`service_us`, `read_set_len`), which the analysis needs for per-node busy time and Jain's index.
  - `inject_end` carries the injection number `k`, as the event schema says; the analysis matches it to its `inject_start` and no longer fails when `k` is missing.
  - The pool never starts with 0 planning threads, which would strand every claimed request on a one-core machine.
- First end-to-end run on one machine (3 nodes, real load generator, 300 requests/s open loop, coordinator silenced for 5 s): 9,002 requests, none lost. p50 4 ms, p99 49 ms. The silenced coordinator was removed from the view after 0.37 s and back in it 0.09 s after waking. Jain's index over busy time 0.986. All three nodes ended with identical logs.
- Open: the failover-time metric read 1.0 s and 6.5 s in two runs of the same scenario, while the other failure measures barely moved; its in-band threshold needs checking against real baselines before the experiments are reported.

---

Entries DL-066 to DL-070 record the fixes from the pre-deployment review of 2026-11-02: four reviewers checked the merged code against the Phase 1 requirements, and one end-to-end run on one machine (3 nodes, 3 load generators, 450 requests/s, periodic injection plus an operator silence of the coordinator) measured the effects. "Before" and "after" below are that same run (`e2e1`, `e2e2`), same seed and configuration. Owner: Abdallah (integration).

## DL-066. Replication to a silent peer: probes, not repeated batches; status, lease and injection fixes

- Date: 2026-11-02. Status: agreed. Changes `replication::core`; refines DL-030, DL-031 and DL-034.
- Problems found:
  - **Append flood.** Every commit flush (10 ms) reset `sent_up_to` to `matched` and resent from there, so a peer that acknowledged nothing got the same batch of up to 256 entries a hundred times a second, and the link kept every copy for retransmission. In the simulator a 5 s silence at 100 requests/s sent 27,909 entries for about 500 committed. In the real run the coordinator sent 66 MB in 8 s to a silenced node and its memory grew to 235 MB without coming back.
  - **Stuck in SYNCING.** A node waking from a short silence learned its catch-up target from the first `Append` but rechecked its status only when the commit position moved. On an idle cluster it never moved, so the node stayed SYNCING, claimed nothing, and its `active` event (the recovery-time measurement) was missing.
  - **Lease measured in the wrong place.** The lease counted acknowledgments when the core task processed them. Under overload they wait in its input queue, and a coordinator could step down with nothing wrong, pausing commits for more than a second.
  - **Injection precondition.** "All three ACTIVE" was approximated by "three in the view, none suspected", which a node that had just woken and not yet caught up satisfied.
- Decisions:
  - A peer is **responsive** if it acknowledged an `Append` within two heartbeat intervals. Responsive peers get new entries as they are appended, and on the heartbeat or commit flush only what was never sent; the link delivers what is in flight, and a loss shows up as a failed log-matching check, whose refusal repairs it. An unresponsive peer gets nothing but an empty probe each heartbeat interval; its first acknowledgment restarts the catch-up, one batch per round trip.
  - A follower rechecks its status after every `Append`.
  - The runtime tells the core when the transport last accepted anything from each peer (`Core::heard_from`); the lease counts that as well as processed acknowledgments. Safety still rests on epochs (DL-031); the lease only decides when a cut-off coordinator stops trying.
  - Scheduled and operator injections are refused while a peer is not responsive.
- Evidence:
  - New simulator tests: a silenced follower now gets 18 entries while 497 are committed (it was about 28,000); an idle node becomes ACTIVE after a short silence; a coordinator whose peers are heard keeps its lease, and steps down as before when they are not.
  - Real run after the fix: while node 3 was silent for 8 s the coordinator sent its normal 0.2 MB/s; the 14.5 MB when node 3 woke is the 3,600 entries it had missed, sent once. Coordinator memory ended at 120 MB instead of 235 MB.

## DL-067. Operator and runtime additions for the six-machine runs

- Date: 2026-11-02. Status: agreed. Changes `node::{runtime, net, link, main}`, `rm-ctl`, `proto::config`, `loadgen::sampler`.
- Decisions:
  - **Load knob** for the load-imbalance demo: `rm-ctl load <node> --threads N [--duty P] [--duration-ms D]` starts `selection::knob::CpuBurner` threads inside that node's process (operator line `load <threads> <permille> <ms>`), `--threads 0` stops it, and a duration stops it by itself. It has to run inside the node: the score's CPU term is this process's share of the machine (DL-060), so an outside CPU hog would not move it. Each change is logged as `load_knob` (`threads`, `duty_permille`, `duration_ms`, `on`). Before this the burner existed but nothing could start it.
  - **Clean stop.** `rm-node` flushes its event log and exits 0 on SIGTERM or SIGINT (a one-thread tokio runtime waits for the signals; the workspace forbids `unsafe`), and exits non-zero if the core thread panicked. Before, the runner's SIGINT was ignored and the following SIGTERM lost up to 0.5 s of events.
  - **Bounded client input.** At most `CLIENT_BACKLOG` = 4,096 client requests wait for the core task; more are dropped unread and counted (`client_overload_dropped`). The client sent the same request to the other two nodes and retries, so an overloaded node simply takes no part, as when its planning queue is full (DL-052).
  - **Measurements.** Each node `resource_sample` adds new peer messages sent by protocol role (`msgs_tx_election`, `msgs_tx_replication`, `msgs_tx_claim`, `msgs_tx_commit`, `msgs_tx_other`) for the election-overhead measurement; `core_lag_ms_max`, how late the core loop ran, as an overload signal; `events_dropped`; `client_backlog`, `client_overload_dropped`, `decode_errors`, `version_errors`; the round trip and missed-heartbeat share per peer (DL-069); the claim timeout in use; and the load knob. `reply` is logged only when the reply was actually queued for sending. `rm-ctl status` adds `plans`, `claims`, `queue_len`, `cpu_permille`, `load_knob` and `incarnation`, which the live demo reads.
  - **Operator answers.** A coordinator that declines an injection answers `error: injection refused: <reason>` instead of `ok`.
  - **Addresses.** `cluster.nodes[].host` must be an IPv4 address. Sockets bind IPv4, and the heartbeat thread sends to the first address a name resolves to; a name that resolved to IPv6 first (common for macOS `.local` names) would have lost every heartbeat silently.
  - **Clocks.** `clock_offset` falls back to systemd-timesyncd (`timedatectl timesync-status`, the Ubuntu default) when chrony is absent; `source` says which was used.
  - Smaller: the acceptor sleeps 10 ms after an `accept` error instead of spinning (for example out of file descriptors); the link hands delivered bodies on instead of deep-copying them; the stale "placeholder until D is merged" notes are gone.
- Evidence: localhost test `the_load_knob_raises_one_nodes_cpu_and_stops_on_time` (operator command, status fields, expiry, refusals); a 50 s run with the knob on node 2 for 20 s from second 15: plans per 5 s were about 740 on every node before, 0 on node 2 and about 1,080 on nodes 1 and 3 during, and back to about 740 each after; SIGTERM stopped all three nodes with complete logs. Parser tests for timesyncd output; config test for host names.

## DL-068. A planning job reads the newest snapshot when it starts

- Date: 2026-11-02. Status: agreed. Changes `PlanJob` (`node::task`), `WorkerPool`, `DevPool`. Resolves the known limit in DL-063.
- Problem: a job held the snapshot current when it was queued. Under overload each queued job could pin a different version of the state, up to `queue_capacity` (256) full copies, and a job planned from state that could be seconds old, which is why about 7% of requests gave up after the re-plan cap in DL-063's overload run.
- Decision: the core task publishes the newest snapshot in a shared `SnapshotSlot` after each batch of applied entries; a job carries the slot, and the planning thread reads it when it starts the job. The state still copies its snapshot on a write only while a planning thread holds the old one, as before.
- Effect: at most one snapshot per planning thread plus the current one stays alive, and every plan reads the freshest committed state. Re-plans in the real run fell from 3.5% to 2.4% of requests.
- Evidence: `a_job_reads_the_snapshot_current_when_it_starts_not_when_queued` (a job queued behind a blocked one sees the snapshot published after it was queued).

## DL-069. A node ranks itself from the heartbeat it actually sent; node round trips from heartbeat echoes

- Date: 2026-11-02. Status: agreed. Changes `node::{heartbeat, task, runtime}`, `selection::Selector`, `proto::Heartbeat` (`PROTOCOL_VERSION` 3). Refines DL-024 and DL-038.
- Problem: 25.7% of requests got two competing claims, 1.33 claims per request, and about 37% of requests waited one or two claim timeouts (20–40 ms) before anyone claimed, while planning took about 0.5 ms. In the logs, the typical case was two nodes each ranking the other first and both waiting.
- Cause: a node reset its own local correction (claims since its last heartbeat, Section 5.1) when its core task refreshed the heartbeat content, but the heartbeat thread sends on its own 100 ms schedule, so the datagram left up to an interval later, and peers reset their count of its claims only when it arrived. For most of each interval the node and its peers counted its claims from different starting points. A claim is worth about 4 score points against a quantum of 50, so with a dozen claims in the gap, one view in four crossed a step the other did not.
- Decisions:
  - The heartbeat thread hands each heartbeat back to the core task once sent (`Input::HeartbeatSent`); the node starts ranking itself from that load then, as its peers do on receipt.
  - **Round trips.** Each heartbeat echoes, per peer, the last heartbeat number received from it and how long it was held; the peer computes `now − sent − held` on its own clock, NTP-style, and smooths it (1/8). The heartbeat thread is independent of planning load, so this is the network round trip. The claim timeout is now `max(claim_timeout_min_ms, 3 × RTT)` as DL-024 intended; nothing called `set_peer_rtt_ms` before, so it was always the minimum. On localhost the round trip is 0.1–0.3 ms; on a congested network it raises the timeout instead of causing spurious claims.
  - **Missed heartbeats.** Each node now measures every peer's missed-heartbeat share (a moving average over about 32 heartbeats, from gaps in their numbers) and feeds it to the score's reliability term. Its weight stays 0 by default for the reason in DL-047; it is logged per peer so the report can show it.
- Alternatives: counting only claims for requests seen at least one claim timeout ago, on every node. It also makes the views agree, but delays the correction by a claim timeout and herds every request in that window onto one node. Not needed once the heartbeat cause was removed.
- Evidence, same run before and after: competing claims 25.7% → 0.46% of requests; claims per request 1.33 → 1.00; duplicate results suppressed by the coordinator 214 → 2; latency mean 18.9 → 6.8 ms, p50 5.2 → 3.1 ms, p95 46 → 10.5 ms, p99 55 → 27 ms; requests lost 0 in both. Load shares and Jain's index unchanged (0.968, 0.969).

## DL-070. Score inputs and defaults

- Date: 2026-11-02. Status: agreed. Changes `proto::config` defaults, `node::workers`, `node::task`. Refines DL-024 and DL-060.
- Decisions:
  - `score.service_cap_ms` 500 → 50. Plans take 3–20 ms at the default cost knob, so with a 500 ms cap the service-time term was at most 10 points of the score, less than one quantization step, and the term the protocol describes did nothing.
  - The service-time average decays by 1/8 per load sample once a node has had nothing to plan for 1 s. Otherwise a node slowed once, for example by the load knob, kept its old average while idle and might never be ranked first again to measure a new one.
  - **CPU** is measured as the process's accumulated CPU time over our own wall-clock window of at least 250 ms, not `sysinfo`'s `cpu_usage`. On Apple Silicon that divides by a wrong interval when refreshed under 200 ms apart, and about one reading in thirty came out near zero (seen in the review: 98‰ read as 5‰). The placeholder pool measures CPU the same way, so the load knob works with it too.
  - `config/cluster.example.toml` turns on periodic injection (`[inject] enabled = true`, a random 1–20 s silence every 45–75 s), which the project description requires of the servers themselves. Sweeps set it explicitly.
  - `policy.ranker = "mwbe_bids"` is refused at start-up. The fresh-bid round was the first item on the plan's cut list and was never built, so that setting ran exactly `mwbe_cached` under another name; a comparison between the two would have measured nothing.
  - An executor told `NotInView` keeps the request and plans it again after the usual wait instead of dropping it. If the view change that caused the refusal applies, the selector aborts the request first, as before; if it was uncommitted and a new coordinator discarded it, the node is still in the view and still holds the claim, and dropping the request would have stranded it until the client gave up.
- Evidence: `the_service_time_average_decays_while_idle`, `cpu_use_of_a_busy_thread_is_measured`, the localhost knob test; all simulator and chaos tests unchanged.

## DL-071. The planner's matrix columns are bounded, and the answer is checked

- Date: 2026-11-02. Status: agreed (pre-deployment review). Owner: Youssef; Abdallah reviewed. Refines DL-020, DL-056, DL-059.
- Problem: `top_k` bounded the rows of the assignment problem but not its columns. Every available resource able to serve any row became a column, and the matrix was solved up to `top_k + 1` times. Resources live for their lease plus 60 s, so during a stress run plans grew from 19 ms (300 resources) to 946 ms (30,000), and latency drifted upwards for reasons unrelated to load balancing.
- Decision:
  - Each waiting incident brings its `COLUMNS_PER_ROW` = 3 cheapest capable resources, and the new incident its `top_k` cheapest, which are also the responder candidates. They are found by walking resources grouped by kind, zone and free units, nearest zone first, never by scanning everything per row.
  - The solver returns the dual prices of the rows. If any row's price exceeds the cost of its c-th cheapest column, other rows took all of its cheap columns; that row's columns double and the problem is solved again. The forced solves that rank responders are checked the same way. The answer is therefore exactly the optimum of the unrestricted problem, not an approximation.
  - A plan keeps the best 10 responders (DL-059, amended): a report entry went from 1,152 to 257 bytes on the wire.
  - Rows and dispatch candidates are chosen with a partial selection instead of sorting every open incident.
- Alternatives: every resource a column (time grows with the resource count); a fixed c without the check (silently suboptimal under contention, which tied costs make common); a spatial index inside `Snapshot` (changes the shared API).
- Measured scaling: about k² when waiting incidents do not compete for resources, about k³ or steeper when they do. DL-020's "roughly cubic" holds only under contention.
- Evidence (release build, `top_k` 100):

| Resources / open incidents | Before | After |
|---|---|---|
| 300 / 300 | 18.9 ms | 15.5 ms |
| 3,000 / 2,000 | 82.1 ms | 12.1 ms |
| 10,000 / 5,000 | 322 ms | 13.0 ms |
| 30,000 / 10,000 | 946 ms | 13.7 ms |

  `bounding_the_columns_changes_no_total_cost` compares totals with the unbounded solve on 400 random campuses at c = 1, 2 and 3, and fails if the dual check is disabled. `service_time_against_resource_count` and `service_time_against_top_k` are the benches (ignored by default).

## DL-072. Assignments hold units until they expire; time-dependent state is read, not applied

- Date: 2026-11-02. Status: agreed (pre-deployment review). Owner: Youssef; Abdallah reviewed. Changes `ResourceOffer` (`PROTOCOL_VERSION` 3), DL-058, DL-062.
- Problem:
  - Units given to an incident never came back when the assignment expired, so over a long run every resource drained to zero.
  - `IncidentStatus::Expired` was never produced: a query for an incident past its TTL still said Open.
  - The full incident and assignment maps were never pruned, and a zone query scanned every incident on the core thread.
- Decision:
  - `ResourceOffer.holds` lists each assignment's units and expiry, with `capacity == available + Σ holds` always. A resource's free units at time t are its available units plus those of holds expired by t. Every plan that writes a resource first moves its expired holds back into `available`. Apply never moves units by itself.
  - A report whose solve gives a waiting incident a resource dispatches it as an offer would, each incident served once and version-checked, so returned units do not sit idle while a waiting incident claims them in every solve.
  - `UpdateResource` counts expired holds as free and keeps capacity at or above the units still held (DL-062).
  - Queries answer as of the request's `sent_at_ms`: `Expired` for an incident past its TTL, effective availability for a resource, a zone index for zone queries. Once pruned, an item is unknown.
  - Pruning, at the same log positions as before, also removes incidents, assignments and index entries, and copies the snapshot only if something goes.
  - **Pruning clock:** the median client timestamp of the last 31 committed entries that carry one (`CLOCK_WINDOW`), instead of the latest. Deterministic as before, and one client with a fast clock can no longer make every node prune live data.
- Alternatives: releasing units at apply or prune time. Rejected: the coordinator checks only the objects a plan writes, so a concurrent stale plan could overwrite the released units unnoticed, breaking I3. Deriving holds from assignment records was rejected because the snapshot has no per-resource index and the resource's version would not cover them.
- Reason: availability stays a pure function of committed writes and the reader's timestamp, and every change to it is version-checked like any other write.
- Evidence: tests for holds returning units, returned units going to the most urgent waiting incident, released units being version-checked, updates, a long run that never over-allocates and keeps reusing units, `Expired` on query, pruning of snapshot, state and index, the zone index against a scan, and `one_client_with_a_fast_clock_cannot_make_the_nodes_prune_live_data`. The real-planner chaos test now keeps its own ledger of assignments and checks that `available + Σ holds == capacity`, that every hold is a committed assignment held once, and that no hold is released early; it fails if a report's resource is dropped from its read set.
- Known limit: the snapshot is a set of `BTreeMap`s, so a committed write copies it whenever a planning thread holds the old one, about 0.77 ms at 30,000 resources and 10,000 open incidents. Sixty-second runs stay far below that size. A persistent map would remove the copy but changes the shared `proto` API, so it is left for Phase 2.
- Amended 2026-11-02 after a second review (Youssef; Abdallah reviewed):
  - **Dispatch keeps the matching.** A resource the solve gives to a waiting incident is dispatched to that incident first, and any units left go to the most urgent capable ones. Handing it to the most urgent instead could leave waiting an incident that only that resource can serve, while another resource sat idle.
  - **Returned units reach every waiting incident.** Rows are chosen among incidents some free resource can serve. A report also dispatches up to `top_k` resources with units back from expired holds when a capable incident waits outside the rows, which also covers `top_k` 1.
  - **One client's clock no longer decides expiry.** `Snapshot.clients_now_ms` carries the committed clock, the median above. Plans judge hold release, TTL and leases by the request's timestamp clamped to within 5 s of it (`CLOCK_SLACK_MS`). A client a minute fast could otherwise take a unit still held; the version check accepts that plan, because nothing it read had changed. The window is 31 entries rather than 255 because the median lags real time by half the window's span (13 s at 10 requests/s with 255), which would have returned held units late; at low load they can still come back up to the lag, less 5 s, late.
  - **Pruned items stay pruned.** `apply` ignores writes of anything that expired by the last prune's cut, so a plan computed before a prune and committed after it cannot bring an incident or resource back.
  - Evidence: six regression tests ported from the reviewer's reproductions fail before the change and pass after it; `bounding_the_columns_serves_the_same_incidents_under_contention` fails with the dual check disabled; benchmarks unchanged.

## DL-073. Corrections from the second review of DL-066 to DL-070

- Date: 2026-11-02. Status: agreed. Owner: Abdallah. Three independent reviewers re-read the fixes above; these are what they found in the runtime, with the corrections.
- **Probe position (DL-066).** The probe to a silent peer was placed at `matched`. A new coordinator starts with `matched = 0`, so its probe to the silenced old coordinator always matched at 0, and on waking the old coordinator was sent the whole log: in the simulator about 2,160 entries for 180 missed. The probe now sits at `next − 1`, where the peer's log is believed to end, and a refusal's hint moves it back if that is wrong. Now 202 entries for 182 missed (`a_woken_old_coordinator_is_sent_only_what_it_missed`, which fails on the first version).
- **Stale catch-up target (DL-066).** The first `Append` after waking may be a link retransmission from before the silence, carrying a commit position the node already applied; with the status check after every `Append`, the node could become ACTIVE at once although newer entries were committed. For two heartbeat intervals after waking the node now stays SYNCING and raises its target to the highest commit position any `Append` carries; at least one of them was sent after the wake. Recovery takes up to 0.2 s longer as a result. `a_retransmitted_append_from_before_a_silence_does_not_end_the_catch_up` delivers such a stale `Append` and fails without the window.
- **Snapshot slot (DL-068).** The slot held a strong reference to the current snapshot, so the state copied its whole snapshot at the first write of every batch even with no planning thread holding it. It now holds a weak reference; a planning thread upgrades it, and in the moment while the core moves a snapshot nobody holds, waits for the core to republish at the end of the batch. Copies happen again only while a plan actually holds the old snapshot. `StateMachine::snapshot` must therefore return a clone of the `Arc` the state keeps, which every implementation does.
- **Missed heartbeats (DL-069).** The average was kept in thousandths, where `m −= m / 32` stops at 31, so one lost heartbeat counted for ever; it is now kept in millionths and decays to 0. A node waking from its own silence no longer charges its peers for the heartbeats it dropped. `missed_heartbeats_are_not_charged_for_a_nodes_own_silence_and_decay_to_zero`.
- **NotInView (DL-070).** If the view change that removed this node applied while its proposal was out, the selector's abort was ignored and the request was planned again every 500 ms for as long as the node stayed out of the view. A `NotInView` now drops the request when this node's applied view no longer contains it, and plans again only when it does.
- **Smaller:** the first `core_lag_ms_max` sample no longer counts start-up; `rm-ctl silence` asks the coordinator first, so a refusal is reported instead of being hidden behind a follower's "ok", and tries the other nodes when the answer is only "no coordinator".

## DL-074. Third review: the injection margin survives a coordinator change; smaller fixes

- Date: 2026-11-02. Status: agreed. Owner: Abdallah. Five reviewers checked the merged work against the project description and the Phase 1 plan. The correctness reviewer ran the full suite three times (no flakiness), the socket tests ten times, about 5,500 simulator seeds and thirteen targeted scenarios. It found no safety violation: I1–I5 held, every request got one consistent answer, and rejoined nodes always had identical logs and state. Catch-up cost 1.1 entries sent per entry missed. It did find one gap in the plan's criterion A, "the rejoining node always reaches ACTIVE".
- **Injection margin across coordinator changes.**
  - The single-failure precondition required the previous injection to have ended `inject_margin_ms` (5 s) ago, but kept only as the coordinator's own "busy until" time. A new coordinator starts that at its election, so after the coordinator itself was silenced, the woken node could be silenced again about 30 ms after waking, before it was ever ACTIVE.
  - In the reviewer's sweeps this happened in 362 of 7,291 injections. The shipped schedules (an interval at least 20 s longer than the longest silence) cannot hit it, but `rm-ctl silence` could.
  - The coordinator now keeps, per peer, since when it has acknowledged without a gap. An injection is refused until every peer has been continuously responsive for the margin and has acknowledged the latest view entry, so a rejoining node is ACTIVE first.
  - Tests: `a_new_coordinator_keeps_the_injection_margin_for_a_woken_node` and `no_injection_while_a_reconnected_node_is_still_catching_up`; both fail without the change.
- **Every applied view reaches the selector.** The core task passed the selector only the last view of a batch of applied entries. A node that left and rejoined within one batch then counted as a continuous member, so its claims from before it left stayed valid (DL-046). The result could only be a duplicate execution, which I1 suppresses, but it is now exact.
- **Load generator stops on SIGTERM** as well as Ctrl-C, draining and writing its summary, as `rm-node` does (DL-067). The runner and the demo stop processes with SIGTERM.
- **`rm-ctl status` lists the peers each node suspects**, so readiness checks can tell a stable cluster from one that only looks ready between flaps (for example when UDP is blocked).
- Known and accepted: `Selector::tick` scans the request table each tick. Rows are dropped by the clients' `ackedUpTo`, which bounds the table in production; the simulators do not send it, so very long simulations slow down.

## DL-075. Overload: bounded, visible and recoverable; claim-timeout floor; workload TTL

- Date: 2026-11-02. Status: agreed. Owner: Abdallah. From the live performance review: 3 nodes on one 18-core machine, open-loop sweeps to 4,800 requests/s, closed loop, all six policies, the load knob, 20 s silences, automatic injection and a 9-minute soak. Normal operation met expectations: baseline p99 13 ms, detection 224–305 ms, recovery about 200 ms, no flood from a silent node, clean shutdown. Three problems appeared at the edges.
- **Collapse past saturation.**
  - Past about 3,600 requests/s the cluster did not degrade; it collapsed and stayed wedged after the load stopped. `rm-ctl status` timed out on every node, coordinator memory reached 0.95 GB, and a third of requests were never answered.
  - Causes:
    - executors resent every unanswered proposal every 200 ms with no backoff, although the link already delivers it to a coordinator that stays in place;
    - the coordinator added a new waiter for each resend of a proposal still waiting for its commit, so each resend also multiplied the replies;
    - client requests over the core's queue limit were dropped silently, so clients learned nothing until they timed out.
  - Changes:
    - Proposal resends back off, from `propose_retry_ms` doubling to 16 times it, and go at once to a new coordinator.
    - A resent proposal adds no second waiter.
    - The client queue limit is 1,024 (about a tenth of a second of the core's work), and a request over it is answered `Busy` at once.
    - Clients now resend early only when all three nodes answered `Busy`; one busy node takes no part while the others serve the request.
    - The analysis counts requests that never got a final reply (`unresolved`) and warns about overload signals (Busy, refused requests, a full queue), since core-loop lag stayed low during the collapse.
  - Evidence, the same offered load of 4,800 requests/s for 40 s:
    - Before: no recovery.
    - After: about 3,600 requests/s served and every request resolved (answered, or given up after the client's retries). The queue held at its 1,024 limit, with `Busy` answered at once. Peak node memory was 290 MB. After the load, all three nodes answered at once, ACTIVE at the same commit and applied position.
- **MWBE's tail.**
  - At 1,200 requests/s, MWBE's p99 was 22 ms against 7–9 ms for the hash, random and round-robin baselines; the medians were equal. In about 1% of requests the nodes still disagreed on the first-ranked node (a claim in flight while the next request is ranked), and the others waited out the 20 ms claim timeout.
  - The floor of the claim timeout is now 5 ms. It is still three times the measured node round trip when that is larger (DL-069), so it grows on a slower network.
  - Same run, before and after: p99 21.8 → 8.6 ms (hash 7.4 ms); competing claims 0.49% → 0.59%.
- **Drift over long runs.**
  - At a constant 600 requests/s over 9 minutes, plan time rose from 59 µs to 1 ms, node CPU from 11% to 46% and p50 latency from 2.7 to 6.9 ms.
  - Cause: the workload's incidents lived 10 minutes while offers lived 5. Units given to an incident came back only after 10 minutes, so unserved incidents piled up (about 240,000), and every plan and every snapshot copy grew with them.
  - The load generator's incident TTL is now 2 minutes, shorter than the resource lease, so held units return while their resource is still offered, and the backlog of waiting incidents is bounded by two minutes of unserved demand. Sweeps restart the nodes for each run, so their 60-second windows were barely affected.
  - Evidence (5-minute soak at 600 requests/s after the change): p99 latency held at 11.6–12.8 ms every minute (it had climbed from 11 to 24–30 ms), and p50 went from 3.3 to 4.8 ms.
  - Known limit: plan time still grows with the number of live resources, from 0.15 ms to about 1 ms over 5 minutes at 36,000 resources, because each plan regroups every live resource by kind and zone. It levels off once offers start expiring (lease 300 s plus 60 s of slack); node memory and CPU level off with it. Sixty-second sweep runs restart the nodes and never get there.
  - Phase 2 fix: keep that grouping inside the snapshot, updated as resources are written, instead of rebuilding it for every plan.

## DL-076. Sweeps and the demo without ssh: a shared timetable

- Date: 2026-11-03. Status: agreed. Owner: Abdallah.
- Problem: the university lab's six machines (Ubuntu 24.04, wired, one subnet, about 3 ms round trip) have no ssh server, and a university account has no sudo, so none can be installed. `run_sweep.py` and the ssh mode of `demo.sh` start every program over ssh, so neither can run there.
- Decision:
  - `experiments/lab_run.py` computes a timetable from the sweep files, the lab config and an agreed start time. Every machine gets the same timetable, checked by a printed id. Each machine runs its own role's part by itself:
    - the cloud roles start and stop `rm-node` in each slot;
    - the client roles start `rm-loadgen` with the slot's shared start time;
    - client1 also checks that the cluster is ready and sends the run's scheduled silences and load-knob changes with `rm-ctl`.
  - Nothing travels between the machines but the cluster's own traffic. The machines' synchronized clocks (systemd-timesyncd) keep them in step, as `--start-at-ms` already did for the load generators.
  - `pack` bundles each machine's logs. `collect` merges the six bundles into run folders with the same `meta.json` as `run_sweep.py`, so the analysis is unchanged. A run with missing logs, a cluster that was not ready, or a failed scheduled action goes to `_failed`.
  - The lab config (`config/lab.toml`) is kept in git, so all six machines get the same file with `git pull`.
  - `lab_run.py check` reports, per machine:
    - its role, commit and clock synchronization;
    - whether its binaries are older than their own sources (cargo's dep-info);
    - whether a cloud role's address matches the config.
  - For the demo, `lab_run.py demo` starts each machine's part, and `demo.sh --attach` watches the cluster and drives the scenarios with `rm-ctl` only.
  - Alongside: the load generator's frontend now passes SIGTERM on to its worker processes; a stop signal used to reach only the frontend, so workers with no end time ran on.
- Alternatives:
  - an ssh server run as the user: it isn't installed, and unpacking it from Ubuntu's package is fragile;
  - asking the lab to install one: requested, but not in time;
  - starting each run by hand on six machines: hundreds of coordinated starts for the full sweeps.
- Limits:
  - A slot's timing is fixed, not adaptive: a run that needs longer than its slot is cut at the slot's end.
  - A machine that misses its start skips the runs already begun, and `collect` reports them as missing.
- Evidence: all six roles run as separate processes on one machine with the smoke sweep, then pack, collect and analyse: the silences, the load knob and the recovery were all measured, as with `run_sweep.py`. `demo.sh --attach` ran scenarios 1–4 against hand-started roles. `experiments/test_lab_run.py` covers the timetable's determinism and non-overlap, placeholder addresses, and collection (including a run parked because its cluster was not ready).

## DL-077. Measurements in an emulated lab; claims logged after re-admission; no reads while catching up

- Date: 2026-11-04. Status: agreed. Owner: Abdallah.
- Problem: the lab is not available long enough before the deadline to run the full sweeps there (about 7 hours at 5 seeds).
- Decision: the sweeps run in an emulated lab on one laptop (`experiments/docker/lab.sh`), at commit `be3cf4e`:
  - six containers (Ubuntu 24.04, as in the lab), one per machine role, and a controller, on one private network (172.30.0.0/24, `config/docker.toml`);
  - each machine's container adds 1.65 ± 0.35 ms (normal) to every packet it sends (`tc netem`). The measured round trip between containers is 3.56 ms; the lab's is 3.3 ms;
  - a CPU quota of 3 CPUs per container (`CLOUD_CPUS=3 CLIENT_CPUS=3`, on an 18-core host), so nodes and clients do not take each other's CPU;
  - an ssh server in each container, so `run_sweep.py` and the analysis run unchanged, with `experiments/machines.docker.toml`.
- Limits:
  - The containers share one host's kernel, memory bus and clock. Clock offsets are near zero, smaller than in the lab (timesyncd, a few ms), so the one-way delays measured across machines are more exact here than they would be in the lab.
  - The delay is added on send only, with no loss and no switch queueing; the lab's wired network has neither under this load.
  - The demo and a short confirmation run use the real lab (`lab_run.py`, DL-076).
- Found while analysing the failure sweep, and fixed after it (not in the measured binaries; neither changes a measured number):
  - **Claims logged before `active`.** A node re-admitted to the view claims its held requests in the same step. The view was handed to selection while the batch was being applied, before the core logged `active`, so the claims came a few microseconds before it. The analysis counted them as work before converging. Views now go to selection after the batch, so `active` comes first. For runs measured before this change, the analysis ignores claims in the last 2 ms before `active`. With that, every run shows no work before converging.
  - **Reads while catching up.** A node that was not ACTIVE still answered reads from its applied state, which may be behind. It now leaves them to its peers, which got the same request, as it already did for writes.
  - **Counts that included the runner's own starts and stops.** The analysis counted suspicions, view removals, step-downs and elections over the whole node log. That includes the start-up, when the runner starts the nodes one by one (the first one suspects the others), and the teardown, when it stops them one by one after the load (a coordinator stopped first is suspected, removed and replaced). Every run showed about four false suspicions and one wrongful removal, and an "election after startup" whenever node 1 was the coordinator. These are now counted only while the clients run (`service_span`: first client send to last client event), and an election that starts after it is marked `teardown`. Recount: the failure sweep has no false suspicions or wrongful removals, and its 24 later elections are exactly the runs whose silenced node was the coordinator. The latency sweep has none in 119 of 120 runs.
  - **The one exception is real.** The highest-ID baseline at 1,500 requests/s (seed 5) is far past its capacity: 514,000 Busy replies, and backlogs full 84–90% of the time. Each node's core loop stalled for up to 2–3 s, so heartbeats were late, the peers suspected each other (7 times) and elected once. A timeout-based detector cannot tell a stalled node from a failed one. The other policies' runs at the same load had no false suspicions.
