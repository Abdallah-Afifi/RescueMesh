# Workstream E: clients, workload and measurement

How to use what is in `crates/client`, `crates/loadgen`, `analysis/` and `experiments/`.

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

Swap `rm-stub` for `rm-node --config ... --id N` (three terminals) once the nodes exist; nothing else changes.

## On the six machines

1. Build with `cargo build --release` on all six; run chrony on all six.
2. `cp experiments/machines.example.toml experiments/machines.toml`, fill in the hosts.
3. `python experiments/run_sweep.py experiments/example-sweep.toml`
4. `python analysis/analyze.py results/raw --out results/summary`

`run_sweep.py` writes each run to `results/raw/<run_id>/` (git-ignored) with a `meta.json`; commit `results/summary/` only, and only from real sweeps. Stand-in-cloud and local demo output goes to `results/dev/`, which is git-ignored (stub numbers are never reported, DL-045).

## Command reference

`rm-loadgen run`: `--config`, `--machine 1..3`, `--processes K`, `--users M`, `--mode open|closed`, `--rate` (open loop, ops/s for this machine), `--think-ms` (closed loop), `--warmup-secs`, `--duration-secs`, `--drain-secs`, `--seed`, `--mix report=60,offer=20,update=15,query=5`, `--max-inflight`, `--start-at-ms` (same value on all client machines). Ctrl-C stops the run cleanly.

## What each piece records

Every client process writes `logs/<run_id>/client<machine>-<process>.jsonl` (events per `proto::event`, plus the fields in DL-042) and a `.summary.json`; the frontend writes `client<machine>-frontend.jsonl` (hardware, clock offsets, machine CPU/network once a second) and `client<machine>-summary.json`.

## For the other workstreams

- Log events with the same schema; `client::EventLog` is a ready-made non-blocking writer (`EventLog::create(dir, run_id, machine, process, Some(node))`).
- The analysis expects these node events: `inject_start`/`inject_end` (with `k`, `victim`, `duration_ms`), `view_change` (with `active`), `active`, `plan_end` (with `service_us`), `claim`, `commit`, `replan`, `election_start`/`election_won` (with `epoch`), `resource_sample`, `clock_offset`.
- The client expects frames exactly as in `proto::codec` (4-byte length + `ClientMsg`), `Reply` from the executor node, `Busy` when the planning queue is full.
