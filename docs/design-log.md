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

- Date: 2026-10-21. Status: proposed (C), for review by B.
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
