# Workstream E: clients, workload and measurement

How to use what is in `crates/client`, `crates/loadgen`, `analysis/` and `experiments/`.

Python: 3.11 or newer (tomllib) and `pip install -r requirements.txt` (numpy, pandas, scipy, matplotlib, pytest). The runner and the demo need only the standard library; the analysis needs the packages.

## Try it on one machine (no cloud nodes needed)

```sh
cargo build --release
mkdir -p logs
# terminal 1: stand-in cloud on the ports of config/local.toml, silencing a node every 15 s
target/release/rm-stub --config config/local.toml --silence-every-s 15 --silence-max-ms 5000
# terminal 2: 4 processes x 50 users, open loop, 600 ops/s, 5 s warm-up, 40 s measured
target/release/rm-loadgen run --config config/local.toml --machine 1 \
    --processes 4 --users 50 --mode open --rate 600 --warmup-secs 5 --duration-secs 40
python analysis/analyze.py logs --out results/dev
```

Or let the runner do all of it, with three real nodes on localhost:

```sh
python experiments/run_sweep.py experiments/sweeps/smoke.toml --local --out results/dev/smoke   # ~1.5 min
python analysis/analyze.py results/dev/smoke --out results/dev/smoke-summary
```

`--local` runs every role on this machine and uses `config/local.toml` when the sweep's base config is not on localhost, so every sweep file works locally (`--seeds 1 --only mwbe` to keep it short). `--stub` replaces the three nodes with `rm-stub`.

## On the six machines

1. On all six: `git checkout <the same commit>`, `cargo build --release`, chrony running.
2. `cp config/cluster.example.toml config/cluster.toml` and set the three node hosts; `cp experiments/machines.example.toml experiments/machines.toml` and set the ssh targets (the cloud entries must be the same machines as the config's nodes; `addr` if ssh uses an alias). There is no need to copy `config/cluster.toml` to the other machines: the runner pushes each run's config to them.
3. Run the sweeps (estimated times are at the top of each file):

   | Sweep | What it measures | Runs | Time |
   |---|---|---|---|
   | `experiments/sweeps/latency.toml` | open-loop latency vs offered load, MWBE and all five baselines | 120 | ~2 h 50 |
   | `experiments/sweeps/clients.toml` | closed-loop throughput vs concurrent users, MWBE and hash | 60 | ~1 h 25 |
   | `experiments/sweeps/failure.toml` | rank-1 and coordinator silences at 200/500/1000 ops/s | 30 | ~55 min |
   | `experiments/sweeps/imbalance.toml` | load knob on node 2 mid-run: MWBE, hash, and MWBE with each score term alone | 30 | ~1 h |
   | `experiments/sweeps/autoinject.toml` | `[inject] enabled = true`, 2-3 automatic silences per run | 10 | ~26 min |
   | `experiments/sweeps/planner.toml` | planner cost knob `top_k` 10/50/100/200 at 500 ops/s: service time and latency | 20 | ~30 min |
   | `experiments/sweeps/weights.toml` | MWBE score weights: each term alone (queue, CPU, service time, client RTT) vs equal, at 500 and 1000 ops/s | 50 | ~1 h 10 |

   ```sh
   python experiments/run_sweep.py experiments/sweeps/failure.toml --machines experiments/machines.toml --out results/raw/failure
   ```
   **Calibrate first with `clients.toml` and `latency.toml`** (one seed each is enough): the saturation point they show depends on the machines and sets the load levels (`rate`) of every other sweep. Weight variants must keep the weights summing to 1000 (`Config::validate`).
4. Give each sweep its own `--out results/raw/<sweep>`, then `python analysis/analyze.py results/raw --out results/summary`. The analysis finds runs one directory down, takes the sweep from that directory (else from meta.json's `sweep`, else the run id's first word), and keeps every sweep's configurations, tables and figures apart, even when two sweeps share labels or an x-axis name. Analyse one sweep alone with `analyze.py results/raw/<sweep> --out results/summary-<sweep>`. Every sweep also has its own `x_name`.

`run_sweep.py` writes each run to `<out>/<run_id>/` (default `results/raw/`, git-ignored) with a `meta.json`; commit `results/summary/` only, and only from real sweeps. Stand-in-cloud and local output goes to `results/dev/`, which is git-ignored (stub numbers are never reported, DL-045).

## What the runner does and checks

Once per sweep (preflight; any failure refuses to start):
- every host answers over ssh;
- every host has the same `git rev-parse HEAD`;
- every host's binaries were built from that commit:
  - no uncommitted changes to tracked files under `crates/`, `Cargo.toml` or `Cargo.lock` (refused; `--allow-dirty` makes it a warning; other uncommitted changes only warn);
  - `target/release/rm-node`, `rm-loadgen` and `rm-ctl` (and `rm-stub` with `--stub`) exist and are newer than their own sources, as listed in cargo's dep-info file `target/release/rm-X.d`, plus `Cargo.lock`. A change to tests, or to a crate the binary does not use, therefore never makes it stale. Without the `.d` file every tracked source under `crates/` counts, and the message suggests `touch target/release/rm-*` after a correct build.
- Byte-identical binaries are not required. Release builds embed absolute paths (home directory, cargo registry), so different users, linkers or C libraries always give different hashes. The sha256 of each binary is recorded in meta.json for information only;
- no node port (TCP) or heartbeat port (UDP) is in use, with the listener named (ss, else lsof; a warning, because strays of ours are killed before each run, then checked again per run, which fails if a port is still busy);
- no rm-node, rm-loadgen or rm-stub is running that the runner did not start from this checkout (for example one started by hand as `target/release/rm-node`, which the anchored cleanup cannot match): a warning naming the process;
- the cloud hosts of `machines.toml` are the node hosts of the cluster config;
- the hosts' clocks agree: each host's clock is read over ssh (3 samples, the one with the shortest round trip). The run is refused if the spread exceeds 50 ms plus the largest ssh round trip; `--allow-clock-skew` makes it a warning. The offsets and round trips go into meta.json;
- chrony is installed and synchronized (`chronyc tracking`). This one only prints a loud warning, because the clock offsets are also logged and corrected.
- Per-host commit, binary hashes, OS, core count and chrony state go into every run's `meta.json` (`preflight`).

For every run:
1. Stray `rm-node`, `rm-loadgen` and `rm-stub` processes from earlier runs are killed on every host (TERM, then KILL). Only this checkout's binaries match: the pattern is the absolute path `<repo>/target/release/rm-…`, and the runner starts every process by that path.
2. Node ports are checked to be free. Nodes start; `rm-ctl status` is polled (default 45 s, `--ready-timeout`, or `ready_timeout_secs` in the sweep) until all three are Active, in one view of three members, with one agreed coordinator and an empty `suspected` list, and have stayed so (same view and coordinator) for 3 s over at least 4 polls (`ready_stable_secs` in the sweep). A UDP block between two nodes shows up here as suspicions or a changing view. Then every node pid must be alive.
3. The start time and every scheduled action are timed on `clients[0]`'s clock, read over ssh, not on the controller's clock (recorded as `client0_minus_controller_ms`).
4. Each loadgen call has a timeout of lead + warm-up + duration + drain + `--loadgen-margin` (60 s). A watchdog checks the node pids every 5 s.
5. Processes stop with TERM, a 2 s grace (`--stop-grace`), then KILL. Every host's `logs/<run_id>/` is copied back with `ssh host 'cd repo && COPYFILE_DISABLE=1 tar cf - logs/<run_id>' | tar xf -` (no macOS `._*` files), including each process's stdout/stderr: `cloudN-node.out`, `clientN-loadgen.out`.
6. **A run fails**, gets no `meta.json`, and is moved to `<out>/_failed/<run_id>-<time>/` with a `failure.txt`, when a node dies, a node's machine cannot be asked about it three times in a row (an ssh failure is "unknown", retried every 2 s, never taken as a dead node), readiness times out, a port is busy, a remote command returns nothing usable, a scheduled `rm-ctl` exits non-zero, a loadgen exits non-zero or times out, or any `cloudN-node.jsonl`, `clientN-frontend.jsonl`, `clientN-<p>.jsonl` or `clientN-summary.json` is missing or empty. A failing run stops its load at once. It is retried `--retries` times (default 1); the sweep goes on and exits 2 listing the failed runs, and rerunning the same command retries them. The analysis skips `_failed/`.
8. **Resuming.** Each meta.json holds a fingerprint of the run: its config text, loadgen arguments and durations, scheduled actions, the hosts' commit, and the build (every host's binary sha256 and, when the tree is dirty, a hash of `git diff HEAD`; with `--skip-preflight`, this machine's), so a rebuild under `--allow-dirty` or `--skip-preflight` cannot reuse old runs. A rerun of the sweep skips a finished run only when the fingerprint matches. If any finished run in `--out` was made with other settings or code, the sweep refuses to start and lists the runs and what changed. `--rerun-changed` redoes those runs, `--force` redoes everything, or use a fresh `--out`.
7. Ctrl-C or `kill` of the runner kills our processes on every host before exiting (exit 130); the interrupted run is parked in `_failed/`.
9. **Before anything runs**, every failure target (1, 2, 3, coordinator, rank1; 1..20000 ms) and load-knob setting (node 1, 2, 3 or coordinator; threads a count or "cores"; duty 1..1000; `off_at_s` after `at_s` and in the window) is checked. A knob action's meta records the node's answer and the effective thread count (`threads_effective`), since the node caps the request at cores − 1.
10. `--clean-remote` deletes `logs/<run_id>/` and `runs/<run_id>.toml` on every host after the run's logs are collected and verified (never the directory they were collected into). A full set of sweeps otherwise leaves 7-9 GB per host.

## Sweep file reference

```toml
name = "failure"                    # run ids are <name>-<label>-s<seed>
base_config = "config/cluster.toml" # --base-config overrides; --local falls back to config/local.toml
seeds = [1, 2, 3, 4, 5]
warmup_secs = 10                    # also settable per [[run]]
duration_secs = 50
drain_secs = 10
settle_secs = 3                     # after readiness, before the load starts
start_lead_ms = 8000                # load starts this long after clients[0]'s clock is read
ready_timeout_secs = 45
x_name = "offered load (ops/s)"     # axis label in the analysis (also per [[run]])

[config]                            # overrides for every run's cluster config (dotted keys)
"policy.ranker" = "mwbe_cached"
"inject.enabled" = true             # automatic periodic injection (see autoinject.toml)

[loadgen]                           # rm-loadgen flags; rate is the total over all client machines
processes = 4
users = 50
mode = "open"                       # or "closed" (with think_ms)

[[failure]]                         # any number; times are seconds into the measured window
at_s = 15
target = "coordinator"              # 1, 2, 3, "coordinator" or "rank1"
duration_ms = 10000

[[load_knob]]                       # any number: rm-ctl load <node> --threads N --duty P --duration-ms D
at_s = 20
node = 2                            # or "coordinator" (resolved from rm-ctl status at that moment)
threads = "cores"                   # or a number; "cores" = that node machine's core count
duty = 900                          # permille, default 900
duration_ms = 20000                 # 0 = until stopped; or off_at_s = 40 sends --threads 0

[[run]]
label = "coord-r500"
series = "silence coordinator"
x = 500
config = { "policy.ranker" = "hash" }      # merged over [config]
loadgen = { rate = 500 }                   # merged over [loadgen]
failure = [{ at_s = 15, target = "coordinator", duration_ms = 10000 }]   # replaces the sweep's; [] = none
```

The old single-table form still works: a sweep-level `[failure]` with `enabled = true`, merged with a run's `failure = {...}`.

Any non-zero exit of a scheduled action (`rm-ctl silence` refused, `rm-ctl load` failed) fails the run.

Other runner flags: `--only TEXT` (labels containing TEXT), `--seeds 1,2`, `--force` (redo finished runs), `--rerun-changed` (redo finished runs whose fingerprint changed), `--allow-dirty`, `--dry-run` (print the plan), `--local-clients N`, `--skip-preflight` (development only; recorded in meta.json).

`experiments/sweeps/smoke.toml` is a check of the runner, not a measurement: always give it `--out results/dev/smoke`. Its series is `smoke` and its `x_name` says "not for the report", so it can never land on a report curve.

## The live demo

```sh
experiments/demo.sh --local            # three nodes on this machine
experiments/demo.sh --pause            # six machines; wait for Enter before each scenario
```

`experiments/demo.sh` runs the runner's preflight on all hosts (on by default for the six machines, off with `--local`; `--preflight` / `--no-preflight`), starts the cluster (killing strays first) with automatic injection off whatever the base config says, waits until it is ready, and runs:

1. **Normal operation.** Starts open-loop load, then prints every second, per node, plans per second (deltas of `plans` in `rm-ctl status`), share with a bar, role, status, the machine's CPU and the planning queue. Every request is a separate election on the nodes' current scores, so the split moves from second to second. After 8 s a 10 s burst adds `--burst-x` (default 4) times the base rate, 5× in total (750/s at the default 150), and the view runs 14 s so the burst and its drain end inside it. At demo scale the cloud keeps spare capacity (plans take well under a millisecond), so the split stays near even, and the narration says so. Step 2 shows the election reacting to a real imbalance. `--top-k` raises the planner's cost knob for the demo if the machines need more load to show it.
2. **Load imbalance.** `rm-ctl load <node> --threads N` (`--knob-node`, `--knob-threads`; default node 2, 16 threads), 15 s of the same view with that node marked, then `--threads 0` and 8 s more.
3. **Server failure.** `rm-ctl silence coordinator --duration-ms 15000` (`--silence rank1` or a node id, `--silence-ms`). The victim is then read from `rm-ctl status` (the node reporting `silenced: true`), so `rank1` works too. The view follows with the victim marked SILENCED, then the new coordinator and view.
4. **Recovery.** A watcher starts the moment the silence is seen and polls every 0.1 s, because SYNCING usually lasts well under a second. It prints a line at each state change (SILENCED, SYNCING, ACTIVE) with the applied position against the peers' commit, the SYNCING time and the entries applied during it, and the moment the node plans again. Step 4 replays that log and follows it live, however long step 3 and its pause took.
5. **Automatic injection** (skip with `--skip-auto`). Restarts the cluster with `[inject] enabled = true` (every 20-25 s, 4-8 s silences) and watches 60 s. It runs last, after a restart, so automatic silences cannot collide with the scripted ones.

Other flags: `--rate` (base load, default 150/s; it runs until the demo stops it, so a long talk cannot outlast it), `--config`, `--machines`, `--keep` (leave it running). Ctrl-C or `kill -TERM <pid of demo.sh>` stops the current step at once (every step runs in the background and is waited for), then the cluster; the helper exits quietly on either. A demo started as a background job (`demo.sh &`) has SIGINT ignored by the shell, so stop it with `kill -TERM`. Only `--local` needs binaries on the controller. In six-machine mode the nodes and load generators are started over ssh as the runner does, and rm-ctl runs on the controller if it has `target/release/rm-ctl`, otherwise on client machine 1. The pieces are also available one by one: `python experiments/demo_helper.py [--local] preflight|setup|load|watch|status|ctl|coordinator|silenced|recovery|stop`. Event logs land in `logs/demo-*/` on each machine. With an rm-node that does not report `plans` yet, the view shows roles and views only, and step 2 is skipped if rm-ctl has no `load`.

## Analysis outputs and definitions

`python analysis/analyze.py <runs dir> --out <dir>` writes, from the logs only. It processes one run at a time: only the event kinds and columns it uses are parsed (repeated strings as categoricals, resource samples in their own frame), and each run is freed before the next is loaded. On three 70 s, 450 ops/s runs, peak memory fell from 1.70 GB to 0.33 GB, and 12 runs peak at 0.32 GB (0.16 GB of that is importing pandas, scipy and matplotlib). A sweep's size no longer matters.

Every `*_by_config.csv` (and `summary.md`) first averages a measure within each seed, then gives the mean and the 95% Student-t half-width (`_ci95`) over seeds (DL-044): runs are the independent samples, not injections or elections.

| Output | Content |
|---|---|
| `tables/per_run.csv`, `aggregate.csv` | latency mean/p50/p95/p99, throughput, retries, give-ups; mean and 95% CI over seeds. `n_started`, `unresolved` and `unresolved_frac`: requests started in the measured window that never got a final reply (no `ok`, no `gave_up`; `busy` and `error` replies are per attempt and retried), for example past saturation when the drain ends first. **Throughput and the latency percentiles are over `ok` replies only and exclude unresolved requests**; above 0.1% unresolved, a loud warning says so. Also `mode`, `concurrent_users` (users × processes × client machines, from meta.json), `x_name` |
| `tables/failures.csv`, `failures_by_config.csv` | per injection inside the measured window (silences during warm-up or after the load stopped are left out of failures, phases and timelines; `counters.csv` counts them as `injections_outside_window`): `failover_s`, `failover_lb_s` (the horizon when censored, a lower bound), `onset_s`, `detect_s` (first `suspect` of the victim), `view_removal_s` (first committed view without it), `recovery_s` (end of silence to `active`), `peak_p99_ms`, `retries_during` (requests that needed a retry), `victim_role` (coordinator or follower), `censored`; and convergence: `sync_s` (victim's `sync_start` to `active`), `catch_up_entries` (applied at `active` minus its log position at waking), `worked_before_active` (its `plan_end`/`claim` events between waking and `active`: must be 0, else a warning). By configuration and role: CIs over seeds, with `injections`, `censored` and `no_baseline` counts. A censored failover counts as its horizon, so it never silently drops out of the mean |
| `tables/injection_phases.csv`, `phases_by_config.csv` | per injection: throughput, mean, p50, p99 before (≤10 s, after the previous injection settled), during, and after (until the next injection or 30 s) the silence, all clipped to the measured window; a phase shorter than 0.5 s is left out |
| `tables/elections.csv`, `elections_by_config.csv` | per election (one per epoch won): `duration_s` from the first `pre_vote` start, `from_suspect_s`, `from_inject_s`, `startup` |
| `tables/counters.csv`, `counters_by_config.csv`, `fairness_by_config.csv` (Jain over busy time, and `jain_busy_per_core`: busy time divided by each node's cores from `machine_info`, fairness against capacity as protocol Section 10 asks, with `capacity_share_n*`) | `elections`, `election_attempts` (pre_vote starts), `elections_per_min` (in the measured window), `views` and `view_changes` (distinct view numbers, not events summed over nodes), election duration mean/max, `msgs_tx_*` totals, election messages per election and per second, claim/commit/replication messages per request, claim and re-plan rates |
| `tables/suspicions.csv` | false suspicions (a `suspect` of a peer that was not silenced: no `inject_start` of it in the preceding 5 s and not during its silence + 2 s) per node-hour; wrongful view removals. `suspicions_by_config.csv` pools them: total count over total node-hours, not a sum of per-seed rates |
| `tables/imbalance.csv`, `imbalance_by_config.csv` | per load-knob episode, the loaded node's share of completed plans and plans/s before, during and after (windows clipped to the offered load) |
| `tables/health.csv` | `events_dropped` (nodes, clients), `core_lag_ms_max` max and p99 per node, client-dropped arrivals, and `clock_uncorrected`: machines with no usable clock offset (`source` "unavailable" or "*_unparsed", or no `clock_offset` event). Their timestamps are used uncorrected, which is harmless when every process ran on one machine but not across the six. On the lab machines check chrony (or timesyncd, which the node now falls back to), and that a Homebrew `chronyc` is on the PATH of a non-interactive ssh shell. Overload is also flagged from signals other than core lag, which can stay low (28-45 ms) while a node collapses: Busy replies in the measured window, `client_overload_dropped` (requests a node dropped because its client backlog was full), and a `client_backlog` of 256 or more in over a quarter of the window's samples. A client machine averaging over 80% CPU in the measured window is flagged too: the client side may be the bottleneck. A single late core-loop tick is reported as "overload, or a stall of the whole machine", not as overload. Non-zero drops, lag over 100 ms, busy clients and uncorrected clocks are listed as warnings at the top of `summary.md` and on stderr |
| `tables/machines.csv` | every machine of every run: cores, CPU model, memory, OS (`machine_info`), clock source and mean offset (`clock_offset`) |
| `tables/overhead.csv`, `overhead_by_config.csv` | per node: heartbeats/s and heartbeat bytes/s, network bytes/s in and out, peer messages/s, retransmissions and bare acks/s, and messages/s per protocol role (`msgs_tx_*`) |
| `tables/service_by_config.csv` (and `per_run.csv`) | planner service time of the plans finished in the measured window: mean, p50, p99 (µs) and count; with planner.toml, against the cost knob |
| `figures/load_curves*.png` | throughput and latency vs x, one figure per (sweep, x_name), one colour per series, line style per percentile (p50 dotted, p95 dashed, p99 solid), CI whiskers clipped at 0. Runs without an x (imbalance) are left out: they have their own figure |
| `figures/throughput_vs_clients*.png` | closed-loop throughput, p50 and p99 vs concurrent users, one figure per sweep |
| `figures/timeline_<run>_k<k>.png` | for `--timelines` runs (default 1) of every configuration (sweep and label), so every failure target of every sweep gets one: one per injection: completions/s and p99 in the failover metric's windows, with silence, suspicion, view removal, new coordinator, victim active, back-in-band and the in-band limit marked |
| `figures/imbalance_<run>.png` | for `--imbalance` runs (default 1) of every configuration with a load knob: plans/s and share per node per second, knob on/off shaded, CPU and queue per node |
| `figures/resources_<run>.png` | CPU, memory, network, queue and core-loop lag per client machine and node, with the measured window shaded green and the silences red |

### Failover time (refines DL-044 and DL-049)

The old metric read 4.25 s for a run whose p99 was back in band within about 0.5 s, and 1.0 s and 6.5 s for the same scenario (DL-065). Causes found:
- **Noisy windows against a tight band.** At a few hundred requests per second a 0.5 s window holds ~150 replies, so its "p99" is about the second-largest reply. The band, max(1.5 × median, median + 5 ms), sat inside the normal spread of those windows, so the 2 s hold (four windows in a row) kept failing on noise long after the real excursion.
- **Single-window noise as the excursion.** Any one window over the band counted as the start of an excursion, even seconds after a silence that had no effect.
- **Unbounded search.** The search ran to 30 s after the silence ended, past a following injection, so a later spike could be matched.
- **Empty windows ignored.** A window with no completions (a full stall) counted neither as good nor bad, so a stall could read "no excursion".

Definition now (`analyze.failures`):
- Windows of 0.5 s, widened so the baseline expects at least 20 completions each.
- Baseline windows: the 10 s before the silence, never before the load started or before 5 s after the previous injection ended. Band = max(1.5 × median, median + 5 ms, the 95th percentile of the baseline windows).
- A window is in band if its p99 ≤ band and it has at least a quarter of the baseline's completions.
- The excursion is the first out-of-band window within 5 s of the silence that is confirmed: starved, over 2 × band, or followed by another breach. Without one, `no_excursion` and failover 0.
- Failover = from the silence to the start of the first 2 s of in-band windows after the excursion.
- The search stops at the next injection, the end of the load, or 30 s after the silence, whichever comes first. If there is no recovery by then, the failover is NaN and `censored` is set.

On three local runs of the DL-065 scenario (300 ops/s, 5 s silences) the coordinator failover now reads 1.0 s in all three runs. Follower silences read 0, 0 and 2.0 s; the 2.0 s is a real but mild rise (p99 46 ms against a 26 ms baseline). The values do not change with the band percentile (90–100) or the hold (1–2 s). Flags: `--window-ms`, `--hold-s`, `--onset-max-s`.

On the review's three e2e runs, every run had a fourth automatic silence at about 74 s, after the load had stopped. It used to be reported as a 0 s failover, and its near-empty "during" window pulled the mean "during" throughput down to 339 ops/s. It is now left out, and "during" reads 449–453 ops/s, as it should.

## Command reference

`rm-loadgen run`: `--config`, `--machine 1..3`, `--processes K`, `--users M`, `--mode open|closed`, `--rate` (open loop, ops/s for this machine), `--think-ms` (closed loop), `--warmup-secs`, `--duration-secs`, `--drain-secs`, `--seed`, `--mix report=60,offer=20,update=15,query=5`, `--max-inflight`, `--start-at-ms` (same value on all client machines). Ctrl-C stops the run cleanly.

`rm-ctl --config C status | silence <1|2|3|coordinator|rank1> --duration-ms D | load <node> --threads N [--duty P] [--duration-ms D]` (`--threads 0` stops the burner).

`run_sweep.py SWEEP [--machines M | --local [--local-clients N]] [--stub] [--base-config C] [--out DIR] [--only TEXT] [--seeds 1,2] [--force] [--rerun-changed] [--dry-run] [--retries N] [--ready-timeout S] [--loadgen-margin S] [--stop-grace S] [--allow-dirty] [--allow-clock-skew] [--clean-remote] [--skip-preflight]`

`analyze.py RUNS [--out DIR] [--window-ms 500] [--hold-s 2] [--onset-max-s 5] [--timelines 1] [--imbalance 1]` (`RUNS` holds run directories, or one directory per sweep; `--timelines` and `--imbalance` are runs per configuration).

`demo.sh [--local | --machines M] [--config C] [--pause] [--rate R] [--burst-x F] [--top-k K] [--knob-node N] [--knob-threads T] [--silence TARGET] [--silence-ms MS] [--skip-auto] [--preflight | --no-preflight] [--keep]`

Tests: `python -m pytest analysis experiments -q` (also in CI).

## What each piece records

Every client process writes `logs/<run_id>/client<machine>-<process>.jsonl` (events per `proto::event`, plus the fields in DL-042) and a `.summary.json`; the frontend writes `client<machine>-frontend.jsonl` (hardware, clock offsets, machine CPU/network once a second) and `client<machine>-summary.json` (with `events_dropped`).

## For the other workstreams

- Log events with the same schema; `client::EventLog` is a ready-made non-blocking writer (`EventLog::create(dir, run_id, machine, process, Some(node))`).
- The analysis expects these node events: `inject_start`/`inject_end` (with `k`, `victim`, `duration_ms`), `view_change` (with `view` and `active`), `active`, `suspect` (with `peer`), `plan_end` (with `service_us`), `claim`, `commit`, `replan`, `election_start` (with `epoch`, `phase` = `pre_vote` or `vote`), `election_won` (with `epoch`), `load_knob` (`threads`, `duty_permille`, `duration_ms`, `on`), `resource_sample` (`scope = "node"`, with `cpu_permille`, `queue_len`, `events_dropped`, `core_lag_ms_max`, `msgs_tx_election`, `msgs_tx_replication`, `msgs_tx_claim`, `msgs_tx_commit`, `msgs_tx_other`), `clock_offset`.
- The demo reads `rm-ctl status` fields `plans`, `queue_len`, `cpu_permille`, `load_knob.threads`, `status`, `silenced`, `view`, `active`, `coordinator`, `commit`, `applied`.
- The client expects frames exactly as in `proto::codec` (4-byte length + `ClientMsg`), `Reply` from the executor node, `Busy` when the planning queue is full.
