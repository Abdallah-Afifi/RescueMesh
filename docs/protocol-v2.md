# RescueMesh cloud protocol, v2

Status: proposed 2026-10-13. This is the design we implement in Phase 1. It replaces Section 7 of the submitted Phase 0 survey (`phase0/104_survey_v1_submitted.pdf`), and version 2 of the survey (`phase0/104_survey_v2.docx`) describes the same design. Each change from the submitted survey has an entry in `design-log.md`.

All numeric values are starting points to be tuned by measurement, not results.

## 1. What changed from the survey, in one paragraph

The survey let any node commit a result after one peer acknowledged it, and at the same time relied on a single "state version" for planning, recovery and coordinator election. Nothing ordered the commits, so that version did not exist. In v2 the elected executor still computes the plan, but every state change is committed through the coordinator, which assigns it a sequence number in a small replicated log. The coordinator is on the commit path (one small message per request) and never on the compute path. Membership changes and fault injections go through the same log.

## 2. Model

- Three cloud nodes with static IDs 1, 2, 3, fully connected.
- Failure model: a node ignores all inbound and outbound messages for up to 20 s, then resumes with its memory intact. At most one node is silenced at a time; the protocol enforces this for injected failures (Section 8).
- Messages can be delayed, lost or duplicated. Every protocol message is acknowledged at application level and retransmitted; a successful TCP send proves nothing when the drop happens inside the receiving process.
- Byzantine behaviour is out of scope.

## 3. Roles and state

| Role | How chosen | What it does |
|---|---|---|
| Executor | Per request, by MWBE (Section 5) | Computes the response plan |
| Coordinator | Rarely, by quorum election (Section 7) | Orders commits, changes the view, schedules fault injection |

Each node holds:

- **Log**: entries `(seq, epoch, kind, payload)`. `seq` is gapless and increasing. Kinds: `RESULT`, `VIEW`, `INJECT`, `NOOP` (Phase 2 adds lease entries).
- **commitIndex**: highest `seq` known to be held by a majority. Entries are applied in `seq` order up to `commitIndex` only.
- **Applied state**: incidents, resources, assignments, the request table, the current view and the injection counter. Each resource carries `version` = `seq` of the last entry that wrote it.
- **epoch**, and the candidate it voted for in that epoch.
- **View**: `(viewNo, activeSet)`, changed only by a committed `VIEW` entry.

"State version" everywhere in this document means the applied `seq`.

### Node states

| State | Meaning | Claims requests? | Votes / acks log? |
|---|---|---|---|
| ACTIVE | In the committed view | Yes | Yes |
| SYNCING | Not in the view, catching up | No | Acks log entries; does not stand for coordinator |

"Suspected" is not a node state. It is the output of one node's failure detector about another, and it only has an effect when the coordinator turns it into a committed `VIEW` entry.

## 4. Invariants

These are the properties the implementation must preserve and the tests must check.

- **I1. One result per request.** At most one `RESULT` entry is ever committed for a given `reqID`.
- **I2. No lost commits.** An entry acknowledged to a client is held by a majority, so it survives any single silenced node.
- **I3. No double assignment.** A committed plan's assignments were computed from the resource versions that are current at its `seq`.
- **I4. One coordinator per epoch.** Entries and commands from an older epoch are rejected.
- **I5. Only view members execute.** A `CLAIM` carrying an older `viewNo` is ignored.

Liveness: with two of three nodes ACTIVE and messages eventually delivered, every request is eventually committed and answered.

## 5. Executor selection (MWBE)

### 5.1 Score

Lower is better. Each term is clamped to [0, 1].

```
S_i = w_q * min(q_i / Q_MAX, 1)      queue length
    + w_c * c_i                      CPU utilisation, already in [0, 1]
    + w_s * min(s_i / S_MAX, 1)      EWMA of recent service time
    + w_f * f_i                      fraction of i's recent heartbeats missed, as seen by the ranking node
    + w_r * min(rtt_i / R_MAX, 1)    client-measured RTT to node i, taken from the request
```

- `Q_MAX`, `S_MAX`, `R_MAX` and the weights are cluster-wide configuration constants, identical on all nodes, so scores are comparable.
- `q`, `c`, `s` travel on heartbeats. `rtt` travels in the request, because it is per client and cannot ride on server heartbeats.
- **Local correction:** when ranking, a node adds to `q_i` the number of `CLAIM`s it has seen from node `i` since `i`'s last heartbeat. This stops every request in one heartbeat interval from going to the same node.
- Scores are quantised to steps of 0.05.
- In code every term, weight and the quantization step are integers in thousandths, so all nodes compute identical claim keys (design log DL-023).
- **Tie-break:** lower `hash(reqID, nodeID)` wins. A static-ID tie-break sends everything to one node when the cluster is idle.

The selection rule is a pluggable function `rank(reqID, view, scores)`, so the baselines in Section 10 are alternative implementations of the same interface.

### 5.2 Request table

Each node tracks every `reqID` it has seen:

| State | Meaning | On a client retry |
|---|---|---|
| SEEN | Received, no valid claim yet | Rank again |
| CLAIMED(by, viewNo) | A view member is executing | Do nothing |
| COMMITTED(seq) | Result is in the applied log | Return the stored result |

`reqID = (clientID, incarnation, clientSeq)`. The incarnation changes when a client process restarts, so a restarted client cannot reuse an old ID. Each request also carries `ackedUpTo`, the highest `clientSeq` whose reply the client has received; nodes may drop request-table rows at or below it.

### 5.3 Flow for one request

1. **Request.** The client middleware sends `REQ(reqID, payload, rtt[1..3], ackedUpTo)` to all three nodes.
2. **Rank.** Each ACTIVE node ranks the members of its current view.
3. **Claim.** The node that ranks itself first sends `CLAIM(reqID, viewNo, key)` to its peers, where `key = (quantised S, hash)`, and starts planning from a snapshot at its applied `seq`. It records the version of every resource it reads.
4. **Claim timeout.** If a node sees no `CLAIM` within `T_c` from the node it ranked first, the next-ranked node claims. `T_c` is a few peer round trips and is unrelated to service time.
5. **Competing claims.** If two claims for one `reqID` meet, the lower `key` wins and the other executor aborts if it has not yet proposed. If both propose, step 7 picks one.
6. **Propose.** The executor sends `PROPOSE(reqID, epoch, viewNo, readSet, writes, plan)` to the coordinator. If the executor is the coordinator this is a local call.
7. **Sequence.** The coordinator:
   - returns the existing result if `reqID` is already in its log (this enforces I1);
   - replies `REJECT(STALE)` if any resource in `readSet` has a newer version, and the executor plans again from a fresh snapshot (this enforces I3);
   - otherwise appends a `RESULT` entry with the next `seq` and sends `APPEND` to both peers.
8. **Commit.** The entry is committed when one peer has acknowledged it. The coordinator advances `commitIndex` and tells the executor.
9. **Reply.** The executor replies to the client only after it knows the entry is committed.

Message cost per request when the executor is not the coordinator: 2 `CLAIM`, 1 `PROPOSE`, 2 `APPEND`, 2 acknowledgments, 1 commit notice. This replaces the survey's "0 extra messages" figure.

### 5.4 Takeover of a claimed request

A request in state CLAIMED is taken over only when its claimer leaves the view, never because time has passed. When a node applies a `VIEW` entry that removes node X, every request it holds as CLAIMED(by X) goes back to SEEN and is ranked again. If X's result had already been committed, step 7 returns it and nothing is executed twice.

If the coordinator is lost while a `PROPOSE` is outstanding, the executor resends it to the new coordinator after the election. The `reqID` check in step 7 makes this safe.

### 5.5 Client retry

If the client gets no reply within its timeout it resends the same `REQ` to all nodes, with exponential backoff and random jitter. The request table (5.2) decides what each node does with it. A retry never starts a second execution while a view member holds the claim.

## 6. Failure detection and view changes

- **Heartbeats** every 100 ms on a dedicated thread, separate from the planning worker pool, so heavy planning does not delay them. Each carries `(epoch, coordinatorID, viewNo, commitIndex, q, c, s)`.
- **Detector:** φ-accrual over a sliding window of inter-arrival times, with a floor and ceiling on the resulting timeout `T_d`. The window for a peer is reset when that peer rejoins, so a 20 s gap does not distort it.
- **Removing a node:** when the coordinator suspects a peer, it commits `VIEW(viewNo+1, activeSet without that peer)`. A wrongly removed node costs one resync, nothing more.
- **Losing the coordinator:** handled by election (Section 7). The new coordinator's first entry is a `VIEW` without the old one.
- **Finding out you were removed or are behind:** every heartbeat carries `viewNo` and `commitIndex`. A node that sees a view it is not in enters SYNCING.
- **Short silences:** a node silenced for less than `T_d` is never removed. It has missed `APPEND`s, which the coordinator retransmits, so it is merely behind. A plan it computes from a stale snapshot is caught by the read-set check in step 7.

## 7. Coordinator election

Quorum election with Bully-ordered candidacy. The vote rule and the log repair are Raft's, written by us; the candidacy order is Bully's.

1. **Trigger.** A node has had no coordinator heartbeat for `T_d`.
2. **Candidacy delay.** It waits `T_base * (3 - nodeID) + random jitter` before standing, so the highest ID stands first and two nodes rarely stand together.
3. **Pre-vote.** It asks its peers, without changing any epoch: "have you also lost the coordinator, and is my log at least as current as yours?" It continues only if one peer agrees. A node returning from silence fails this check and cannot disturb a healthy coordinator.
4. **Vote.** It increments its epoch and sends `ELECTION(epoch, lastEntryEpoch, lastSeq)`. A peer grants its vote only if all of these hold:
   - the epoch is newer than any it has seen;
   - it has not voted in that epoch;
   - it has not heard from a coordinator within `T_d`;
   - the candidate's `(lastEntryEpoch, lastSeq)` is at least its own.
5. **Win.** With its own vote plus one, the candidate sends `COORDINATOR(epoch)`. It then appends a `VIEW` entry in the new epoch. Entries inherited from earlier epochs count as committed only once that entry is committed.
6. **Lose or collide.** If no majority arrives, it waits a random backoff and retries with a new epoch.
7. **Log repair.** Every `APPEND` carries the `(seq, epoch)` of the preceding entry. A follower whose log does not match at that point deletes its unmatched suffix and accepts the coordinator's. Only uncommitted entries can be deleted this way, and no client was ever told they succeeded.
8. **Stepping down.** A coordinator that sees a higher epoch, or gets no acknowledgment from any peer for the lease period `L`, stops acting and becomes a follower. Its status changes only through the view or after an injected silence (design log DL-031).

A recovered higher-ID node does not take the role back. Safety rests on the majority rule and the epoch, not on clocks: every coordinator action is a log entry that needs a peer's acknowledgment. The lease only makes a cut-off coordinator stop trying sooner. Time to a new coordinator therefore stays at `T_d` plus two round trips, with no lease wait.

## 8. Fault injection

- The coordinator commits `INJECT(k, victim, duration)` on a timer, where `k` is the injection counter in replicated state, `victim = hash(seed, k) mod 3 + 1` and `duration` is derived from `hash(seed, k)` in (0, 20 s]. Because `k` is replicated, the schedule is repeatable even when the coordinator changes.
- **Precondition:** all three nodes are ACTIVE and no injection is in progress. This enforces "one silenced node at a time".
- The victim starts ignoring all traffic, including client traffic, when it applies the entry. The coordinator may pick itself; that exercises the election.
- **Operator command** `SILENCE(nodeID | current-coordinator | current-rank-1, duration)`, accepted by any node and committed as an `INJECT` entry with an explicit victim. This is what the "force the elected server to become nonresponsive" demo needs.
- When its interval ends, the victim always enters SYNCING, even if it was never removed from the view.

The project description says the servers "elect one machine to ignore communication". Our reading: the victim is fixed by a function of replicated state that every node can compute, and the decision is committed by the elected coordinator. Confirm this reading with the instructor; the alternative is a literal bidding round for the victim.

## 9. Rejoin and recovery

As implemented (design log DL-030, DL-032):

1. After an injected silence the node always enters SYNCING. It learns the coordinator and the commit position from the first `APPEND` it receives.
2. The coordinator replicates to all three nodes whether or not they are in the view, so the missing entries arrive through the normal `APPEND` stream; log repair fills any gap. No separate donor is involved.
3. If the node is still in the view (a short silence), it becomes ACTIVE once it has applied the commit position it learned in step 1.
4. If it was removed from the view, it sends `JOIN` to the coordinator. Once the coordinator sees that the node holds everything committed, it commits a `VIEW` with the node added, and the node becomes ACTIVE when it applies that entry.

In both cases the node is sufficiently current because it has applied every entry committed before it returned. Recovery time is measured from the end of the silence to the `active` event.

## 10. Evaluation plan corrections

- **Baselines** (same `rank` interface): random, `hash(reqID)`, round-robin, best of two random choices, and always-highest-ID as a "no balancing" reference. The ring baseline is dropped.
- **Variants:** cached scores with local correction is the primary design. Fresh bids per request is an optional comparison.
- **Statistics:** at least five seeds per configuration, a discarded warm-up period, and 95% confidence intervals. Open-loop arrivals for latency curves, closed-loop clients for peak throughput.
- **Failover time:** from the start of the silence until windowed p99 latency returns to its pre-failure band, together with the time to the committed `VIEW` removal and the number of requests that needed a client retry.
- **Fairness:** Jain's index over per-node busy time, not request counts. Under induced imbalance, compare each node's share with its available capacity.
- **Additional counters:** re-plan rate (`REJECT(STALE)`), competing-claim rate, wrongful-removal rate, coordinator elections, election duration and messages.
- **Resource traces:** CPU, memory, network and queue length per node, as the project description requires.

## 11. Phase 2 hooks

- Lease and reservation grants are log entries with the same resource-version check as step 7. A client sends a claim to any cloud node, which forwards it as a `PROPOSE`.
- Offline peers mark their claims as speculative and reconcile on reconnection. A claim that fails the version check moves the item to a visible reconciliation state.
- Version vectors are used on the peer side for offline changes, where no sequencer is reachable.

## 12. Known limitations

- **Commit pause.** Commits pause during a coordinator election (about `T_d` plus two round trips). Planning continues.
- **Coordinator load.** The coordinator handles one small message per request on top of its share of planning. Its CPU and queue must be measured for this effect.
- **Re-planning under contention.** When many incidents compete for few resources, `REJECT(STALE)` rates rise. Re-plans are capped per request and the rate is reported.
- **Majority.** With two nodes down nothing commits. This is outside the single-failure model.
- **Memory.** The log and state are held in memory, which matches the failure model. A real process crash is not covered unless the log is also written to disk.

## 13. Decided implementation details

The questions this section used to list were settled on 2026-10-20; see design log DL-014 to DL-029:

- Transport, acknowledgment and wire format: DL-014, DL-015.
- Starting timeout and cap values: DL-024.
- Snapshots: none in Phase 1 (DL-022).
- Parallelization model: DL-017 (cloud node), DL-018 (clients).
- Planning algorithm: DL-020.

The types, messages and configuration they describe are implemented in `crates/proto`.

Still open, waiting on the instructor:
- Whether a victim chosen by the elected coordinator satisfies "elect one machine to ignore communication" (DL-011).
- Whether a simulated user may be a task inside a process (DL-018).
- Which six machines the demo uses.
