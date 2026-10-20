# Phase 1 plan

Due: **Wednesday 4 November 2026, 8:00 PM**. Written 20 October 2026, which leaves 15 days.

Phase 1 is worth 30% of the course grade. Within it, the demo counts for 10% and the design, report and analysis for 20%. The project description says poor documentation gives a poor grade however good the demo is, so the report and the measurements are planned as first-class work, not left to the last two days.

## 1. What Phase 1 requires

Everything below is taken from `spec/RescueMesh_Term_Project_v2.pdf`; page numbers refer to it.

### 1.1 Deliverables listed for Phase 1 (pp. 4–5)

1. The three-node cloud and the Incident Processing and Coordination Service.
2. Distributed election for workload assignment, with the load metrics justified.
3. Concurrent request processing, with the client and cloud-node parallelization models documented.
4. Injected temporary server failures of up to 20 s, with transparent continuation of service.
5. Server rejoin with state recovery and convergence.
6. Stress tests across increasing client loads, with statistically meaningful performance data.

### 1.2 Requirements from the rest of the description that apply to Phase 1

**Incident processing service (p. 3)**
- The planning computation combines incident severity, age, location or distance, resource availability and responder capability into a priority and assignment decision.
- It must be deterministic, explainable and tunable, and expensive enough to create real load (p. 8). Weighted scoring, graph, matching or scheduling formulations are suggested.
- The response plan is returned to the client and replicated to the other cloud peers (p. 3). Example outputs: prioritized responder list, resource assignment, recommended route or sequence of actions.

**Data model (pp. 2–3)**
- Incident: ID, owner, category, logical location, severity, timestamp, status, TTL.
- Resource offer: ID, owner, type, quantity, availability, logical location, expiry or lease.
- Response assignment: which client or resource is assigned to which incident, when it was created, when it expires.
- Every state-changing operation has an unambiguous identifier, so retries are idempotent and duplicates are recognized.
- All data is synthetic.

**Cloud behaviour (pp. 2–4)**
- Clients are not hard-wired to one server and never need to know which peer executes a request or takes over (transparency).
- Faults are injected by the servers themselves: they periodically elect one machine to ignore communication for a random interval of up to 20 s.
- The design must state how in-flight requests are detected, retried or completed.
- Duplicate execution is prevented or made harmless.
- The recovery protocol says what state is copied, replayed or reconstructed, and how the returning node knows it is current.

**Demo scenarios for Phase 1 (p. 5)**

| Scenario | What must be shown |
|---|---|
| Normal cloud operation | Different cloud peers are elected as load conditions change. |
| Load imbalance | Load is raised on one server artificially, and the election responds. |
| Server failure | The elected server goes silent during active workload; the remaining peers keep serving. |
| Server recovery | The failed server returns and converges before it participates again. |
| Scale and stress | Large automated runs reporting latency, throughput, election overhead, failure-recovery behaviour and convergence. |

**Measurements (p. 5)**
- Tens of thousands of operations per configuration, collected automatically. Screenshots are not results.
- Request latency: mean, median, p95, p99.
- Throughput as concurrent clients increase.
- Election frequency, duration and message overhead.
- Load distribution among the peers, and whether the policy is fair and effective.
- Behaviour before, during and after an injected failure; failover time and recovery time under different loads (p. 4).
- State recovery and convergence time after rejoin.
- CPU, memory, network and queue behaviour.

**Report contents that apply to Phase 1 (p. 7)**
- System architecture and process/node model.
- Protocols and message formats, with timeouts, retries and duplicate handling.
- The election survey and the rationale for the chosen algorithms (reuse survey v2).
- Load-balancing policy and the measured evidence for it.
- Fault model, failure detection, failover, recovery protocol and convergence.
- Consistency model for incidents, resources and assignments.
- Test plan and use cases, including failures.
- Experimental methodology, raw-data description, plots and tables, analysis, limitations.
- Design decisions that changed, with reasons.
- A contribution section for each member (the description says four; we are five).
- Which functions come from libraries and which protocols we wrote ourselves (p. 8).

**Engineering practice (p. 6)**
- A shared design log (`design-log.md`).
- Version control with a clear history.
- Message formats, error handling, timeouts, retry policy and state ownership defined explicitly.
- Workload generators and fault-injection controls built early.
- Rust; middleware and libraries must not hide the mechanisms being assessed.
- Demo setup: three cloud machines and three client machines, each client machine running many simulated user processes from one frontend.

### 1.3 Decisions (settled 20 October)

The open decisions were settled on 20 October; see design log DL-014 to DL-029. The shared interfaces are in `crates/proto`, which builds and passes its tests, so M0 is complete except for three questions to the instructor:

- whether a victim chosen by the elected coordinator satisfies "elect one machine to ignore communication" (DL-011);
- whether a simulated user may be a task inside a process (DL-018);
- which six machines the demo uses.

### 1.4 The nodes are separate physical machines

The project description fixes the environment: three physical machines run the three cloud nodes, and three more run the client frontends and the simulated users (pp. 2, 4). Each cloud node is therefore one process on its own computer, talking over a real network. The description does not say whose computers these are, so confirm with the instructor whether the demo uses lab machines or our own laptops.

This changes how the system has to be built and measured:

- **Nothing hard-coded to one host.** Node IDs map to IP addresses and ports in the config file. Localhost with three processes is only for development; every milestone from M2 on runs across machines.
- **No reliance on IP multicast.** Campus and lab networks often block it. "Multicast to the cloud" is implemented in the client middleware as one send to each of the three addresses.
- **Clocks differ between machines.** Protocol safety does not depend on clocks (design log DL-010), but measurements do.
  - Request latency is timed on the client's clock only.
  - Timelines that combine events from several machines, such as failover, need the clocks aligned. Run NTP or chrony on all six machines and log each machine's measured offset with every run.
- **Machines may differ in speed.** Record each machine's hardware with every run. Fairness is judged against capacity, not equal shares (protocol Section 10).
- **The client machines can become the bottleneck.** Thousands of client processes on three computers can exhaust client CPU, file descriptors or ports before the servers saturate. Log client-machine CPU too, so we can show that measured limits are on the server side.
- **The failure is a silent process, not a pulled cable.** The injected node keeps running and ignores all traffic in our transport layer, which is what the description asks for.
- **Deployment must be scripted.**
  - The Rust binaries have to build and start on all six machines, which may run different operating systems.
  - Logs have to come back to one place after each run.
  - A run must start from one command; starting six terminals by hand will not survive repeated experiments.
- **Firewalls.** Our ports must be open between all six machines; check this before M2.

Owners: A for deployment scripts and the machine config, B for addressing and transport over the real network, E for clock offsets, hardware records and client-machine monitoring in the measurement pipeline.

## 2. Work split

The work is split along the architecture so each person owns a module end to end: code, tests, its demo scenario, its measurements and its report section. Abdallah takes the largest share (about 30%) because he did not work on Phase 0. Each of the other four takes about 17–18%.

The names on workstreams B–E are a starting suggestion. Swap them to match who is strongest at what; keep the size of A.

| | Owner | Workstream | Share |
|---|---|---|---|
| A | Abdallah Afifi | Replication core, coordinator election, recovery, integration lead | ~30% |
| B | Hanna Mostafa | Node runtime, transport, heartbeats, failure detector | ~18% |
| C | Joudy El Gayar | Executor selection (MWBE), load metrics, baselines | ~17% |
| D | Youssef Badawy | Incident service: data model, planner, worker pool | ~17% |
| E | Sara Faheem | Client middleware, workload generator, measurement and analysis | ~18% |

### A. Replication core and integration — Abdallah

This is the critical path. Everything that touches shared state goes through it.

**Builds** (`protocol-v2.md` Sections 3, 5.3 steps 6–9, 7, 9):
- The replicated log: entries, sequence numbers, `commitIndex`, apply in order.
- The coordinator's commit path:
  - `PROPOSE` handling, with the duplicate-`reqID` check and the resource-version check;
  - `APPEND` with log matching, acknowledgments and the commit notice.
- Coordinator election: candidacy delay, pre-vote, vote rule, epoch fencing, back-off, stepping down.
- The membership view: `VIEW` entries, removal on suspicion, re-admission.
- Rejoin: SYNCING state, `JOIN` from any active donor, buffering live entries, ACTIVE on applying the re-admission entry.
- Fault injection: `INJECT` entries, the hash-of-seed schedule, the all-ACTIVE precondition, and the `SILENCE` operator command, including making the victim actually ignore all traffic.

**Also owns:**
- Integration: the Cargo workspace, the end-to-end build, and the shared interfaces in Section 3 below.
- Deployment scripts, the machine config, and runs on the six physical machines (Section 1.4).
- Report assembly and final editing.

**Demo scenarios:** server failure, server recovery.

**Measurements:** failover time, recovery time at light, medium and heavy load, coordinator elections and their duration, how often a returning node was fenced.

**Report sections:**
- System architecture and process/node model.
- Consistency model.
- Fault model, failover and the recovery protocol.
- Design decisions that changed.

**Done when** the invariants I1–I5 hold in a test that silences each node in turn, including the coordinator, during load, and the rejoining node always reaches ACTIVE.

### B. Node runtime and transport — Hanna

**Builds:**
- The message crate: every message type in the protocol, the request and node identifiers, and serialization.
- Transport between nodes and from clients: connections, framing, reconnection.
- Application-level acknowledgment and retransmission, so that a silenced process is handled correctly even when TCP reports success.
- The node's async runtime skeleton and the heartbeat sender on its own thread, carrying epoch, coordinator, view, commit index and load metrics.
- The φ-accrual failure detector, with the window reset on rejoin and floor/ceiling bounds on `T_d`.
- The "ignore all traffic" switch that fault injection flips, at the transport layer.
- Structured event logging for all of the above, in the schema of Section 3.

**Demo scenario:** supports server failure (detection is what starts failover).

**Measurements:**
- Detection time.
- Heartbeat and protocol message overhead per node.
- Network usage.
- False-suspicion rate under heavy planning load.

**Report sections:**
- Protocols and message formats.
- Timeouts, retries and duplicate handling.
- Failure detection.

**Done when** three nodes on separate machines exchange heartbeats, a silenced node is suspected within the expected bound, and no false suspicion occurs during a CPU-saturating run.

### C. Executor selection and load balancing — Joudy

**Builds** (`protocol-v2.md` Section 5):
- The load score: q, c, s, f and the client RTT, with caps, quantization and the hash tie-break.
- The local correction for claims seen since the last heartbeat.
- The request table: SEEN, CLAIMED, COMMITTED, with clean-up by `ackedUpTo`.
- The claim logic: `CLAIM`, the claim timeout `T_c`, resolving competing claims, re-ranking when the claimer leaves the view, and handling client retries.
- The `rank()` interface with every baseline behind it: random, hash, round-robin, two-choices, always-highest-ID. Fresh bids are optional and come last.
- The artificial load knob for the load-imbalance demo, for example a CPU burner or an extra delay on one node.

**Demo scenarios:** normal cloud operation, load imbalance.

**Measurements:**
- Load distribution and Jain's index over busy time, per policy.
- Competing-claim rate.
- Election overhead per request.
- Response to induced imbalance.

**Report sections:**
- The election and load-balancing policy with its evidence.
- The justification of the load metrics. The project description asks for this explicitly.

**Done when** MWBE and all baselines run behind one switch, and the imbalance demo visibly shifts work away from the loaded node.

### D. Incident processing service — Youssef

**Builds:**
- The data model for incidents, resources and assignments, with all required fields, synthetic zones and a campus graph.
- The deterministic planner:
  - priority from severity, age, distance, availability and capability;
  - assignment, for example weighted bipartite matching;
  - a route over the zone graph.
- A tunable cost knob, so a request can take a chosen amount of CPU.
- Snapshot reads that record the version of every resource read, and plans emitted as the writes the coordinator checks.
- Applying committed `RESULT` entries to the state.
- The node's planning worker pool: bounded queue, backpressure (a BUSY reply when full) and queue-length reporting to C.

**Demo scenario:** supports all of them (it generates the load).

**Measurements:**
- Planner service time against problem size and the cost knob.
- Re-plan rate under resource contention.
- Queue length and CPU behaviour.

**Report sections:**
- The incident and resource model.
- The planning algorithm.
- The cloud-node parallelization model.

**Done when** the same snapshot and request always give the same plan, and service time scales predictably with the cost knob.

### E. Clients, workload and measurement — Sara

**Builds:**
- The client middleware:
  - multicasts each request to all three nodes;
  - builds `reqID` with an incarnation number;
  - measures RTT per server and sends it with the request;
  - times out and retries with exponential backoff and jitter;
  - reports `ackedUpTo`.
- The workload generator: many client processes per machine, open-loop (Poisson arrivals) and closed-loop modes, a seeded and repeatable mix of incident and resource operations, and load levels.
- A simple frontend or CLI on each client machine that starts and stops runs across its client processes.
- The experiment runner: sweeps configurations and seeds, triggers fault injection at a chosen time, and collects every node's event logs and resource traces into one place.
- Analysis scripts:
  - latency mean, median, p95 and p99 with confidence intervals;
  - throughput curves;
  - failure timelines with the latency excursion;
  - fairness;
  - CPU, memory, network and queue plots.

**Demo scenario:** scale and stress.

**Measurements:** owns the pipeline that produces everyone's numbers, and the latency and throughput results.

**Report sections:**
- The client parallelization model.
- The test plan.
- The experimental methodology.
- The raw-data description.
- Plots, tables and their interpretation.

**Done when** one command runs a full sweep on the six machines and produces every plot in the report from raw logs.

### Shared by everyone

- Your own design-log entries, each time you decide or change something in your module.
- Unit tests for your module.
- Your contribution section.
- Being able to explain the whole architecture at the demo. The project description says every member is expected to.

## 3. Interfaces to agree on day 1

Agreeing these first lets the five workstreams proceed in parallel. A owns this list and records each decision in the design log.

| Interface | Between | Content |
|---|---|---|
| Message types and IDs | everyone | Rust enum of all messages, `ReqId`, `NodeId`, `Epoch`, `ViewNo`, `Seq` |
| `Planner` trait | D → A, C | `plan(snapshot, request) -> (plan, read_set, writes)` |
| `StateMachine::apply` | A → D | applies a committed entry; exposes resource versions |
| `Ranker` trait | C → node | `rank(req_id, view, scores) -> Vec<NodeId>` |
| Load metrics | D, B → C | queue length, CPU, service time per heartbeat |
| Event log schema | everyone → E | one JSON line per event: timestamp, node, kind, req_id, fields |
| Config file | everyone | all timeouts, caps, weights, seeds, policy choice |

Proposed workspace layout:

```
crates/
  proto/     messages, IDs, codec                      (B)
  node/      cloud node binary
    net, fd, heartbeat                                 (B)
    log, election, view, recovery, inject              (A)
    select, request_table                              (C)
    plan, state, workers                               (D)
  client/    middleware library                        (E)
  loadgen/   workload generator and frontend           (E)
  ctl/       operator CLI: SILENCE, status             (A)
analysis/    scripts that turn logs into tables and plots (E)
results/     summaries committed; raw runs ignored
```

## 4. Schedule

| Dates | Milestone | Exit criterion |
|---|---|---|
| Tue 20 – Thu 22 Oct | **M0: contracts** | Open decisions settled; workspace builds; message crate and traits merged; instructor asked about fault-injection wording. |
| Fri 23 – Tue 27 Oct | **M1: modules** | Each module works alone with tests. Three nodes run on one machine (localhost) with heartbeats; the planner runs standalone; the load generator talks to a stub server. |
| Wed 28 – Fri 30 Oct | **M2: integration** | End-to-end request on one machine, then on the six physical machines with clocks synchronized. Fault injection and rejoin work under load. All five demo scenarios run at least once. |
| Sat 31 Oct – Mon 2 Nov | **M3: experiments** | Full sweeps with five or more seeds per configuration; raw data collected. **Code freeze Monday night**; after that, bug fixes only. |
| Tue 3 – Wed 4 Nov | **M4: report and demo** | Report assembled from the owners' sections; demo rehearsed end to end; submit by 8 PM Wednesday. |

Report sections are drafted while the code is being written: the design parts during M1, the results parts during M3.

If the schedule slips, cut in this order:
1. The fresh-bid variant.
2. Snapshots: keep catching up by log suffix only.
3. The round-robin baseline.

Do not cut fault injection, recovery or the measurement pipeline; they are graded directly.

## 5. Risks

- **The replication core is the critical path.** If A slips, nothing integrates. Mitigation: A starts with the commit path on a fixed coordinator (no election) so others can integrate by M1. Election and rejoin are added next.
- **Access to six machines.** Confirm with the instructor which machines the demo uses, book them for M2 and M3 now, and check that the network allows our ports between them.
- **Measurements arriving late.** E's pipeline must read logs from day 1 of M1 so every module emits usable events early.
- **One person knowing a module.** Each module has a reviewer: A reviews B, B reviews C, C reviews D, D reviews E, and E reviews A. Every merge needs that review, which also spreads the knowledge needed for the demo.
