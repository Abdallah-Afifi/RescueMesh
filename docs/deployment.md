# Running on the six machines

How to set up the Phase 1 test environment the project description asks for (pp. 2, 4): three machines run the cloud nodes, three run the client frontends with many simulated users. A seventh computer, usually a laptop, is the controller that starts runs and collects logs; it can be one of the six. The steps take about an hour the first time.

| Machine | Runs | Inbound ports |
|---|---|---|
| cloud1, cloud2, cloud3 | `rm-node --id 1/2/3` | TCP 7000 from all six machines, UDP 7100 from the other two cloud machines |
| client1, client2, client3 | `rm-loadgen` (one frontend, K processes × M users) | none |
| controller | `run_sweep.py`, `demo.sh`, `analyze.py`, `rm-ctl` | none |

Linux (Ubuntu 22.04 or 24.04) is the tested platform. macOS works; the differences are noted.

## 1. Network

1. Put all six machines on one private wired network: a lab LAN, or a small switch or travel router you bring. Campus Wi-Fi often isolates clients from each other, hands out changing addresses, and stalls for 200 ms to 1 s when laptops scan or roam, which the failure detector reads as failures. If you must use Wi-Fi, use the Wi-Fi timeouts below, and record which network you used in the report.
2. Give each machine a fixed IPv4 address: a DHCP reservation on your own router, or a static address. The config needs IPv4 addresses, not host names (DL-067).
3. Check that every machine can reach every other's ports, not only ping (macOS stealth mode drops ping). Once the nodes run (section 6), from each machine:
   ```sh
   nc -vz <cloud-ip> 7000            # TCP, from all six machines
   nc -vzu <cloud-ip> 7100           # UDP, between the cloud machines (nc reports UDP optimistically; rm-ctl status is the real test)
   ```
4. Open the ports on the three cloud machines:
   - **Ubuntu (ufw):**
     ```sh
     sudo ufw allow 7000/tcp
     sudo ufw allow from <cloud-subnet> to any port 7100 proto udp
     ```
   - **macOS:** nobody is at the screen to answer the firewall prompt when nodes start over ssh. After the final build, either allow the binary:
     ```sh
     sudo /usr/libexec/ApplicationFirewall/socketfilterfw --add "$PWD/target/release/rm-node"
     sudo /usr/libexec/ApplicationFirewall/socketfilterfw --unblockapp "$PWD/target/release/rm-node"
     ```
     or turn the firewall off for the session (`--setglobalstate off`). Rebuilding changes the binary, so repeat this after every build.
   - **Lab machines** may block ports centrally; ask the lab administrator before the demo day.
5. Turn off sleep on all six machines, keep them on mains power and laptop lids open. A machine that sleeps looks exactly like an injected failure:
   - Ubuntu: `sudo systemctl mask sleep.target suspend.target hibernate.target hybrid-sleep.target`
   - macOS: `sudo pmset -a disablesleep 1` (undo with `0`), or keep `caffeinate -dimsu` running.

**Wi-Fi timeouts.** The defaults suspect a node after about 300 ms without heartbeats, which suits a wired LAN. On Wi-Fi, add to the cluster config:

```toml
[timeouts]
fd_min_ms = 1000
fd_max_ms = 3000
lease_ms = 800
```

Detection and failover then take about a second longer; report the values used.

If UDP 7100 is blocked but TCP works, nodes keep suspecting each other and the view flaps. Step 6 checks for this.

## 2. Software on all six machines

```sh
# Ubuntu
sudo apt update && sudo apt install -y build-essential git curl chrony python3 python3-venv
# Ubuntu 22.04 ships Python 3.10; the runner and the demo need 3.11 (tomllib):
sudo apt install -y python3.11 python3.11-venv      # then use python3.11 below
# macOS
xcode-select --install

curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source "$HOME/.cargo/env"

git clone <repository-url> ~/RescueMesh
cd ~/RescueMesh
git checkout <the commit you will demo>     # the same commit everywhere
cargo build --release                       # rust-toolchain.toml installs the pinned 1.99.1 compiler
ls target/release/rm-node target/release/rm-loadgen target/release/rm-ctl
```

Every machine must run the same commit. Nodes on different `PROTOCOL_VERSION`s drop each other's messages and count `version_errors`. The sweep runner checks commits and that the binaries were built after the checkout before it starts (section 7). Commit and push everything first, and tag the commit you will demo, so every machine can check out exactly that.

Cloud machines: allow enough open files for the client connections (each client process keeps one connection to each node), e.g. `ulimit -n 4096` in the shell that starts the nodes.

## 3. Clocks

Latency is always timed on one clock, the client's, so clocks don't affect it. Cross-machine timelines do depend on them: failover, view removal and recovery, which combine node and client events. Every process logs its clock offset (`clock_offset` events), and the analysis corrects for it.

- **Ubuntu:** chrony, installed above, is enough. To make all six follow the same source, edit `/etc/chrony/chrony.conf` to use one shared NTP server (or one of the six machines as the server), then `sudo systemctl restart chrony`.
- **macOS:** has no chrony. `brew install chrony` installs it but does not start it: run `sudo chronyd` (pointed at the shared server). The nodes and load generators look for `chronyc` on the PATH and in `/opt/homebrew/bin`, `/usr/local/bin`, `/usr/sbin` and `/usr/bin`, since a non-interactive ssh shell's PATH lacks Homebrew's directory. Otherwise set `RM_CLOCK_OFFSET_NS` (positive when this machine's clock is ahead). Without either, offsets are logged as `unavailable` and those machines' timelines are uncorrected.
- **A local time server.** Without internet NTP, make one cloud machine the server: in its `chrony.conf` add `allow <subnet>` and `local stratum 10`, and point the others at it with `server <its ip> iburst`.
- **Check on each machine:** `chronyc tracking` should show `Leap status: Normal` and a system time offset well under 1 ms on a LAN. If chrony is missing on Ubuntu, `timedatectl timesync-status` is used as a fallback (DL-067). Then compare the machines with each other: `python3 -c 'import time; print(int(time.time()*1000))'` on all six at once (e.g. from the controller over ssh) should agree to within a few ms plus the ssh delay.

## 4. Configuration

On one machine:

```sh
cp config/cluster.example.toml config/cluster.toml
```

Edit the three `[[cluster.nodes]]` entries with the cloud machines' IPv4 addresses, and set `run_id`. The sweep runner and the demo copy their own config to every machine on each run. For the smoke test by hand (section 6), copy it yourself and check all six have the same file:

```sh
for h in <six ssh targets>; do scp config/cluster.toml $h:RescueMesh/config/; done
for h in <six ssh targets>; do ssh $h 'shasum -a 256 RescueMesh/config/cluster.toml'; done
```

- `[inject] enabled = true` is the periodic automatic fault injection the project description requires: every 45–75 s the coordinator silences one node for 1–20 s. Leave it on for the demo.
- Sweeps set `inject.enabled` themselves: off when they inject one controlled failure with `rm-ctl silence`, on for the automatic-injection sweep.
- `policy.ranker` selects MWBE or a baseline.
- `planner.top_k` is the cost knob (how much CPU one plan takes).

## 5. ssh from the controller

The controller drives the six machines over ssh without passwords:

```sh
ssh-keygen -t ed25519            # if you have no key yet
ssh-copy-id user@<each of the six>
ssh -o BatchMode=yes user@<each> true && echo ok
```

Then:

```sh
cp experiments/machines.example.toml experiments/machines.toml
```

In it, fill in the six `ssh` targets and `repo_dir`. The cloud ids must match the cluster config.

Python on the controller (3.11 or later; `python3.11` on Ubuntu 22.04):

```sh
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
```

The controller also needs current binaries (`cargo build --release`): the demo and the scheduled failures run its `rm-ctl`.

## 6. Smoke test by hand

Do this once after setup, and again on the demo day.

1. On cloud1, cloud2 and cloud3 respectively:
   ```sh
   target/release/rm-node --config config/cluster.toml --id 1   # 2 on cloud2, 3 on cloud3
   ```
2. On any machine, check the cluster:
   ```sh
   target/release/rm-ctl --config config/cluster.toml status
   ```
   Expect three lines, all `"status":"Active"`, the same `view` with `"active":[1,2,3]`, and the same `coordinator`. If the view number keeps rising, UDP 7100 is blocked between some pair (section 1).
3. On each client machine, with the same `--start-at-ms`: a time about a minute ahead, in Unix ms, so you can start all three. Get it on any synchronized machine with `python3 -c 'import time; print(int(time.time()*1000) + 60000)'` (BSD `date` on macOS has no `%N`). `--rate` is per client machine: 200 on each of three is 600 requests/s in total.
   ```sh
   target/release/rm-loadgen run --config config/cluster.toml --machine N \
       --processes 4 --users 50 --mode open --rate 200 --warmup-secs 5 --duration-secs 60 \
       --start-at-ms <same value on all three>
   ```
4. During the load, from any machine:
   ```sh
   target/release/rm-ctl --config config/cluster.toml silence coordinator --duration-ms 10000
   target/release/rm-ctl --config config/cluster.toml status      # a new coordinator, then the old one Syncing, then Active
   target/release/rm-ctl --config config/cluster.toml load 2 --threads 8 --duration-ms 20000
   ```
5. Each loadgen prints `gave up 0` (or close to it) and its latency percentiles. Stop the nodes with Ctrl-C; they flush their logs.

If one cloud process dies or a machine reboots, restart the whole cluster rather than one node: a restarted process has lost its in-memory state and votes, which is outside the failure model.
6. Make sure these hand-started nodes are stopped before running the runner or the demo. They were started by a relative path, so the runner's cleanup (which matches `<repo>/target/release/rm-…` only) does not kill them. The preflight warns about them and about any busy node port.

## 7. Experiments

From the controller:

```sh
# a 1.5-minute check that the runner works on the real machines (it also runs the preflight);
# always into results/dev, never next to the report's runs
python3 experiments/run_sweep.py experiments/sweeps/smoke.toml --base-config config/cluster.toml --out results/dev/smoke
# calibrate first: these two set the load levels of all the others
python3 experiments/run_sweep.py experiments/sweeps/clients.toml --seeds 1 --out results/raw/clients
python3 experiments/run_sweep.py experiments/sweeps/latency.toml --seeds 1 --only mwbe --out results/raw/latency
# then the report's sweeps, each into its own --out
python3 experiments/run_sweep.py experiments/sweeps/failure.toml --out results/raw/failure
python3 analysis/analyze.py results/raw --out results/summary
```

| Sweep | Measures | Time |
|---|---|---|
| `latency.toml` | open-loop latency vs load, MWBE and the five baselines | ~2 h 50 |
| `clients.toml` | closed-loop throughput vs concurrent users | ~1 h 25 |
| `failure.toml` | rank-1 and coordinator silences at three loads | ~55 min |
| `imbalance.toml` | load knob on node 2: MWBE, hash, and MWBE with each score term alone | ~1 h |
| `autoinject.toml` | `[inject] enabled = true`, 2–3 automatic silences per run | ~26 min |
| `planner.toml` | the planner cost knob `top_k` 10–200: service time and latency | ~30 min |
| `weights.toml` | MWBE score-weight ablation (each term alone vs equal weights) at two loads | ~1 h 10 |

Calibrate first with `clients.toml` and `latency.toml`: the saturation point they show depends on the machines and sets the load levels of the other sweeps (edit the `rate` values in their files). Give each sweep its own `--out` under `results/raw/`. `analyze.py results/raw` then finds every sweep's runs one level down and keeps each sweep's figures and tables apart; run it on a single sweep's directory to analyse that sweep alone.

`--machines` defaults to `experiments/machines.toml` and `--out` to `results/raw`. Rerunning a sweep resumes it. A finished run is skipped only if its `meta.json` fingerprint matches: config, load, scheduled actions, commit and build (binary hashes, plus the uncommitted changes when the tree is dirty). If a finished run was made with other settings or code, the runner refuses and lists it: `--rerun-changed` redoes those runs, and `--force` redoes everything. `--clean-remote` deletes each run's `logs/<run_id>/` and `runs/<run_id>.toml` on the hosts once its logs are collected and checked; a full set of sweeps otherwise leaves several GB per host.

Before the first run, the runner checks every host:
- it is reachable;
- it is at the same commit, with no uncommitted changes under `crates/` or `Cargo.*` (`--allow-dirty` downgrades that to a warning);
- its binaries are newer than their own sources, as listed in cargo's `target/release/rm-*.d` (so `cargo build --release` was run after the last change to them; a change to tests only does not count). Without the `.d` files every tracked source counts, and `touch target/release/rm-*` clears a false alarm after a correct build;
- its clock agrees with the others within 50 ms plus the ssh round trip (`--allow-clock-skew` downgrades this to a warning), and chrony reports it synchronized (a warning).

It refuses to start on a mismatch. Binary hashes are recorded but need not match: release builds embed absolute paths, so different users or OSes always differ. It also warns about busy node ports and about rm- processes it did not start. Each run then:

1. kills stray processes from earlier runs and checks the node ports are free;
2. starts the three nodes and waits until all are Active in one three-member view, agree on the coordinator and suspect nobody, continuously for 3 s (a blocked UDP path shows up here);
3. starts the three load generators on a shared start time;
4. applies the scheduled failures and load-knob changes;
5. stops everything with SIGTERM;
6. collects every log into `<out>/<run_id>/`.

A run that fails any check gets no `meta.json`, and is retried when you rerun the sweep.

The sweeps in `experiments/sweeps/` each give their estimated wall-clock time in a header comment. Use at least five seeds per configuration (protocol Section 10). Commit `results/summary/`, not the raw logs.

## 8. The live demo

`experiments/demo.sh --pause` (six machines; `--local` for one) first runs the runner's preflight on all hosts (commit, binaries, clocks, ports; `--no-preflight` skips it). It then starts the cluster with automatic injection off, waits until it is ready and walks through the four Phase 1 cloud scenarios and automatic injection, pausing before each:

1. **Normal operation:** every request is a separate election, so the per-second split of plans between the nodes moves from second to second. A 10 s burst (5× the base rate by default) shows queues and CPU rising and the work moving away from the busier node. At demo scale the cloud keeps spare capacity, so the split stays near even, and the narration says so; step 2 is the real imbalance.
2. **Load imbalance:** `rm-ctl load` on one node; its share drops, then recovers.
3. **Server failure:** the coordinator or the rank-1 node is silenced during load; service continues and a new coordinator is elected.
4. **Recovery:** watched every 0.1 s from the moment of the silence. The silenced node goes Syncing, catches up to the peers' commit position, and only then becomes Active and takes work again.
5. **Automatic injection:** the cluster restarts with `[inject] enabled = true`, and the cloud silences nodes by itself.

On machines with many cores, pass `--knob-threads` of about the cloud machine's core count (the node caps it at cores − 1), so the burner moves the node's CPU score visibly. See `docs/workstream-e.md` for all options. Rehearse it on the real machines at least once. Ctrl-C or `kill -TERM <pid of demo.sh>` stops every step and the cluster. A demo started as a background job ignores Ctrl-C, so use `kill -TERM` there. `python3 experiments/demo_helper.py stop` also cleans up, and the demo can be rerun from the start.

## 9. Troubleshooting

| Symptom | Likely cause |
|---|---|
| `invalid config: … must be an IPv4 address` | A host name in `cluster.nodes`; use the IP. |
| View number keeps rising, nodes flap between Active and Syncing | UDP 7100 blocked, or a machine went to sleep. |
| `version_errors` rising in `resource_sample` | Machines on different commits; rebuild everywhere. |
| `rm-node` exits at once with "Address already in use" | A node from an earlier run is still running: `pkill -f target/release/rm-node`. |
| `clock_offset` says `unavailable` | No chrony or timesyncd on that machine (section 3). |
| Client machine CPU near 100% in `client*-frontend.jsonl` | The clients are the bottleneck; use fewer users per machine or more processes. |
| `core_lag_ms_max` above about 100 (the analysis warns), or `client_overload_dropped` rising | That node is overloaded; that run is past the saturation point. |
| A node keeps getting suspected on Wi-Fi | Use the Wi-Fi timeouts (section 1), or move to a wired network. |
| `._*.jsonl` files in collected logs | macOS `tar` metadata; set `COPYFILE_DISABLE=1` (the runner does). |
