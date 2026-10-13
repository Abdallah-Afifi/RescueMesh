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

- Date: 2026-10-13. Status: proposed (Abdallah); team to confirm.
- Supersedes: DL-002 "any executor commits after one peer ack" and "coordinator off the request path".
- Problem: the survey used a single "state version" for planning, recovery and the election key, but nothing ordered commits. Example: node 1 commits A through node 2 while node 2 commits B through node 3. Node 1 holds {A}, node 3 holds {B}; there is no version and no replay order.
- Decision: the executor sends its result to the coordinator, which assigns a sequence number, checks resource versions and replicates. The reply goes out after a majority holds the entry. The coordinator is on the commit path, not the compute path.
- Alternatives:
  - Version vectors with mergeable state. Keeps the coordinator off the path, but can only detect a double assignment after the fact. Kept for the Phase 2 offline layer.
  - Full Raft with the leader also executing. Rejected: concentrates compute on one node, which defeats the load-balancing experiments.
- Cost accepted: one extra message per request, commits pause during a coordinator election, and we now implement a small replicated log that the survey said it would avoid.
- Spec: `protocol-v2.md` Sections 3, 5.3, 7.

## DL-004. One result per request is enforced at the sequencer

- Date: 2026-10-13. Status: proposed.
- Supersedes: DL-002 "first committed result wins".
- Problem: with two executors for one request, each could refuse the other's result (client never answered) or each could accept (two different plans). The determinism argument fails because the two may plan from different snapshots.
- Decision: the coordinator returns the existing result for any `reqID` already in its log. Stated as invariant I1.
- Alternative: resolve between peers by claim key on replication messages. Rejected: more rules, and still needs an order for cross-request conflicts.

## DL-005. Resource-version check at commit

- Date: 2026-10-13. Status: proposed.
- Problem: two different requests planned from the same snapshot could both assign the same resource. Idempotency by request ID does not cover this.
- Decision: a plan carries the versions of the resources it read; the coordinator rejects it if any is out of date and the executor plans again. Stated as invariant I3.
- Alternative: lock resources during planning. Rejected: holds locks across a long computation and across a possible executor failure.
- Risk: high re-plan rate under contention. Measured and capped.

## DL-006. Agreed membership view; "suspected" is not a node state

- Date: 2026-10-13. Status: proposed.
- Supersedes: DL-002 node states.
- Problem: each node ranked its own private idea of who was active. The epoch changed only on coordinator elections, so a returning non-coordinator still had a valid epoch and its claims were accepted. A silence shorter than the suspicion threshold never triggered a resync.
- Decision: the view `(viewNo, activeSet)` changes only through committed log entries. Claims carry `viewNo`. Heartbeats carry `viewNo` and `commitIndex`. An injected node always resyncs on wake-up.
- Alternative: gossip-style membership (SWIM). Rejected as unnecessary for three nodes that already share a log.

## DL-007. Takeover on view change, not on elapsed time

- Date: 2026-10-13. Status: proposed.
- Supersedes: DL-002 watchdog `T_w`.
- Problem: under load a queued request looked like a dead executor, so the next node re-executed it and doubled the load. With cached scores, a silenced node's frozen score could keep it ranked first for every request until it was suspected.
- Decision: a short claim timeout `T_c` (a few round trips) covers "nobody claimed". A claimed request is taken over only when the claimer is removed from the view. The request table has states SEEN / CLAIMED / COMMITTED so a client retry cannot start a second execution.
- Alternative: keep `T_w` and tune it. Rejected: any value is wrong at some load.

## DL-008. Hash tie-break and local score correction

- Date: 2026-10-13. Status: proposed.
- Supersedes: DL-002 static-ID tie-break.
- Problem: with quantised scores and a static-ID tie-break, one node receives every request when the cluster is idle. With scores cached for a heartbeat interval, all nodes agree on the same "best" node for the whole interval.
- Decision: tie-break by `hash(reqID, nodeID)`; add claims seen since a peer's last heartbeat to that peer's queue length.
- Alternative: fresh bids on every request. Kept as an optional comparison; costs six messages and a wait per request.

## DL-009. Score inputs made obtainable

- Date: 2026-10-13. Status: proposed.
- Problem: client RTT is per client and cannot ride on server heartbeats; "normalised to [0, 1]" was undefined for queue length and service time; the failure-rate term was self-reported.
- Decision: the client measures RTT per server and sends it in the request; cluster-wide caps `Q_MAX`, `S_MAX`, `R_MAX` define normalisation; the reliability term is the heartbeat miss ratio observed by the ranking node.

## DL-010. Coordinator election details

- Date: 2026-10-13. Status: proposed.
- Problem: with one node silenced, the two remaining nodes could both stand in the same epoch and refuse each other, with no retry rule. The survey also said both that a new leader must wait for the old lease and that failover takes detection plus two round trips.
- Decision: candidacy delay ordered by ID plus random jitter; random backoff and a new epoch on failure; vote key `(lastEntryEpoch, lastSeq)`. Safety comes from the majority rule and the epoch, because every coordinator action is a log entry; the lease only makes a cut-off coordinator stop sooner. No lease wait before the new coordinator acts.
- Alternative: lease-based safety. Rejected: adds a clock assumption we do not need.

## DL-011. Fault injection through the log

- Date: 2026-10-13. Status: proposed. Open question for the instructor.
- Supersedes: DL-002 seeded generator held by the coordinator.
- Problem: repeatability broke when the coordinator changed; nothing prevented silencing a node while another was still syncing; no way to target a specific node for the required demo.
- Decision: victim and duration are derived from `hash(seed, k)`, with the counter `k` in replicated state; injection only when all three nodes are active; an operator `SILENCE` command for targeted injection.
- Open: the project description says the servers "elect" the machine to silence. Ask whether a deterministic choice committed by the elected coordinator satisfies this, or whether a bidding round is expected.

## DL-012. Recovery completes when the node applies its re-admission entry

- Date: 2026-10-13. Status: proposed.
- Supersedes: DL-002 "matches the donor's committed version at the time of the check" and "donor named by the coordinator".
- Problem: under sustained load the donor's version keeps moving; rejoin blocked whenever a coordinator election was running.
- Decision: the joining node takes the missing suffix (or snapshot) from any active node while buffering live entries; the coordinator re-admits it with a `VIEW` entry; the node is active when it applies that entry.

## DL-013. Evaluation baselines and method

- Date: 2026-10-13. Status: proposed.
- Supersedes: survey Section 7.8 baselines.
- Problem: plain Bully and a per-request ring are easy to beat and cannot show that a metric election is better than a zero-message policy.
- Decision: baselines are random, hash, round-robin, two-choices and always-highest-ID. Failover is measured as a latency excursion. Fairness uses busy time. Five or more seeds with confidence intervals. One failure detector (φ-accrual); the ring baseline and the second detector are dropped to fit the Phase 1 schedule.
