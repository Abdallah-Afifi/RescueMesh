#!/usr/bin/env python3
"""Sweeps and the demo on lab machines that cannot ssh to each other.

The lab machines have no ssh server, so `run_sweep.py` cannot start programs
on them. Their clocks are synchronized, though, so every machine can follow
the same timetable by itself: this script computes it from the sweep files
and an agreed start time, and each machine runs its own part. Nothing is sent
between the machines except the cluster's own traffic and, from client1, the
scheduled `rm-ctl` silences and load-knob changes (TCP, the node port).

On every machine, from ~/RescueMesh at the same commit, with the same lab
config (keep it in git: config/lab.toml):

    python3 experiments/lab_run.py check --role cloud1 --config config/lab.toml --start 18:30 SWEEP...
    python3 experiments/lab_run.py run   --role cloud1 --config config/lab.toml --start 18:30 SWEEP...

(roles cloud1, cloud2, cloud3, client1, client2, client3; same --start, sweeps
and options on all six). `check` and `run` print a timetable id: it must be
the same on all six. Afterwards, on every machine:

    python3 experiments/lab_run.py pack --role cloud1 --start 18:30 --config config/lab.toml SWEEP...

and bring the six .tgz files to one computer (USB stick, or serve them with
`python3 -m http.server 7000 --directory ~` once the nodes are stopped), then:

    python3 experiments/lab_run.py collect --config config/lab.toml --start 18:30 \\
        --from rescuemesh-*.tgz --out results/raw SWEEP...
    python3 analysis/analyze.py results/raw --out results/summary

Each run's slot: nodes start; 6 s later the load generators start and
connect; the load begins at 10 s (warm-up, measured window, drain); the
nodes stop 3 s after the drain; 4 s later the next slot begins. A machine
started after its slot began skips that slot. A run whose cluster was not
ready, or whose logs are incomplete, is put in <out>/_failed by `collect`.

The live demo without ssh: on each machine `lab_run.py demo --role ...`
(nodes, or open-loop load until Ctrl-C), then on the presenting computer
`experiments/demo.sh --attach --config config/lab.toml`.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import ipaddress
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
import tomllib
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import run_sweep as rs  # noqa: E402

ROLES = ["cloud1", "cloud2", "cloud3", "client1", "client2", "client3"]
N_CLIENTS = 3
BOOT_S = 10      # slot start -> load begins (nodes elect in well under a second)
LG_START_S = 6   # slot start -> load generators start and connect
STOP_AFTER_S = 3  # end of the drain -> nodes stop
GAP_S = 4        # nodes stopped -> next slot
REPO = Path(__file__).resolve().parent.parent


def binary(name: str) -> str:
    return str(REPO / "target" / "release" / name)


# ------------------------------------------------------------------ timetable


def parse_start(text: str) -> float:
    """`HH:MM` (today, local time), `YYYY-MM-DD HH:MM`, or Unix seconds."""
    if text.replace(".", "", 1).isdigit() and len(text) >= 9:
        return float(text)
    for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M", "%H:%M"):
        try:
            t = dt.datetime.strptime(text, fmt)
        except ValueError:
            continue
        if fmt == "%H:%M":
            t = dt.datetime.combine(dt.date.today(), t.time())
        return t.timestamp()
    raise SystemExit(f"--start {text!r}: use HH:MM (today), 'YYYY-MM-DD HH:MM' or Unix seconds")


def timetable(sweep_files: list[Path], base_cfg: dict, start: float, seeds_arg: str | None,
              only: str | None) -> list[dict]:
    """Every run of the sweeps, in order, with its slot times (Unix seconds)."""
    slots, t = [], start
    for f in sweep_files:
        sweep = tomllib.loads(f.read_text())
        specs = [s for s in sweep["run"] if not only or only in s["label"]]
        seeds = [int(s) for s in seeds_arg.split(",")] if seeds_arg else sweep.get("seeds", [1, 2, 3, 4, 5])
        for seed in seeds:
            for spec in specs:
                plan = rs.plan_run(sweep, spec, seed, base_cfg, N_CLIENTS)
                load_at = t + BOOT_S
                drain_end = load_at + plan["warmup"] + plan["duration"] + plan["drain"]
                stop = drain_end + STOP_AFTER_S
                slots.append({"sweep_file": str(f), "sweep": sweep, "spec": spec, "seed": seed, "plan": plan,
                              "start": t, "lg_start": t + LG_START_S, "load_at": load_at,
                              "drain_end": drain_end, "stop": stop})
                t = stop + GAP_S
    return slots


def timetable_id(slots: list[dict]) -> str:
    key = [[s["plan"]["run_id"], s["plan"]["cfg_text"], s["plan"]["lg"], s["plan"]["fails"], s["plan"]["knobs"],
            round(s["start"], 3)] for s in slots]
    return hashlib.sha256(json.dumps(key, sort_keys=True, default=str).encode()).hexdigest()[:8]


def show(slots: list[dict], role: str | None = None):
    end = slots[-1]["stop"] if slots else 0
    print(f"{len(slots)} runs from {fmt_t(slots[0]['start'])} to {fmt_t(end)} "
          f"({(end - slots[0]['start']) / 3600:.1f} h); timetable id {timetable_id(slots)}"
          + (f"; this machine is {role}" if role else ""))


def fmt_t(t: float) -> str:
    return dt.datetime.fromtimestamp(t).strftime("%Y-%m-%d %H:%M:%S")


def load_inputs(args) -> tuple[dict, list[dict]]:
    base_cfg = tomllib.loads(Path(args.config).read_text())
    slots = timetable([Path(s) for s in args.sweeps], base_cfg, parse_start(args.start), args.seeds, args.only)
    if not slots:
        raise SystemExit("no runs: check the sweep files and --only")
    return base_cfg, slots


# ------------------------------------------------------------------ checks


def my_addresses() -> set[str]:
    out = subprocess.run(["hostname", "-I"], capture_output=True, text=True).stdout.split()
    if not out:  # macOS
        out = subprocess.run(["ipconfig", "getifaddr", "en0"], capture_output=True, text=True).stdout.split()
    return set(out) | {"127.0.0.1"}


def role_problems(role: str, base_cfg: dict) -> list[str]:
    nodes = {int(n["id"]): n for n in base_cfg.get("cluster", {}).get("nodes", [])}
    probs = []
    for i, n in sorted(nodes.items()):
        try:
            ipaddress.IPv4Address(n["host"])
        except ValueError:
            probs.append(f"node {i}'s host {n['host']!r} in the config is not an IPv4 address: set the real IPs")
    if probs:
        return probs
    if role.startswith("cloud"):
        n = nodes.get(int(role[-1]))
        if n is None:
            probs.append(f"the config has no node {role[-1]}")
        elif n["host"] not in my_addresses():
            probs.append(f"the config says node {role[-1]} is {n['host']}, but this machine is "
                         f"{' '.join(sorted(my_addresses() - {'127.0.0.1'}))}: wrong role or stale IP")
    return probs


def build_problems() -> list[str]:
    probs = []
    bins = [Path(binary(b)) for b in rs.CHECKED_BINARIES]
    missing = [b.name for b in bins if not b.exists()]
    if missing:
        return [f"missing {missing}: run cargo build --release"]
    for b in bins:
        # cargo's dep-info lists exactly the sources this binary was built
        # from; a test-only change must not make it look stale.
        d = b.with_suffix(".d")
        if d.exists():
            srcs = [Path(x) for x in d.read_text().split(":", 1)[-1].replace("\\\n", " ").split()]
            srcs.append(REPO / "Cargo.lock")
        else:
            srcs = [REPO / f for f in subprocess.run(["git", "ls-files", "crates", "Cargo.lock"], cwd=REPO,
                                                     capture_output=True, text=True).stdout.split()]
        newer = [x for x in srcs if x.exists() and x.stat().st_mtime > b.stat().st_mtime]
        if newer:
            probs.append(f"{b.name} is older than {newer[0].relative_to(REPO) if newer[0].is_relative_to(REPO) else newer[0]}"
                         ": run cargo build --release")
    dirty = subprocess.run(["git", "status", "--porcelain", "--untracked-files=no", "crates", "Cargo.toml",
                            "Cargo.lock"], cwd=REPO, capture_output=True, text=True).stdout.strip()
    if dirty:
        probs.append("uncommitted changes in crates/: every machine must build the same commit")
    return probs


def cmd_check(args) -> int:
    base_cfg, slots = load_inputs(args)
    commit = subprocess.run(["git", "rev-parse", "--short", "HEAD"], cwd=REPO, capture_output=True,
                            text=True).stdout.strip()
    sync = subprocess.run(["timedatectl", "show", "-p", "NTPSynchronized", "--value"], capture_output=True,
                          text=True).stdout.strip() if shutil.which("timedatectl") else "unknown"
    print(f"{socket.gethostname()}  role {args.role}  commit {commit}  clock synchronized: {sync or 'unknown'}  "
          f"cores {os.cpu_count()}")
    show(slots, args.role)
    probs = role_problems(args.role, base_cfg) + build_problems()
    if sync not in ("yes", "unknown"):
        probs.append("the clock is not synchronized (timedatectl): the machines would not keep the timetable")
    if slots[0]["start"] < time.time() + 30:
        probs.append(f"the first slot starts at {fmt_t(slots[0]['start'])}, less than 30 s from now")
    for p in probs:
        print(f"  PROBLEM: {p}")
    if not probs:
        print("  ok: compare the commit and the timetable id with the other five machines")
    return 1 if probs else 0


# ------------------------------------------------------------------ run


def wait_until(t: float, stop: threading.Event | None = None) -> bool:
    """Sleeps until Unix time t; False if `stop` was set meanwhile."""
    while True:
        left = t - time.time()
        if left <= 0:
            return True
        if stop is not None:
            if stop.wait(min(left, 1.0)):
                return False
        else:
            time.sleep(min(left, 1.0))


def kill_ours():
    """Our own leftover node and load-generator processes on this machine."""
    pat = f"^{REPO}/target/release/rm-(node|loadgen)"
    subprocess.run(["pkill", "-TERM", "-f", pat], capture_output=True)
    time.sleep(1)
    subprocess.run(["pkill", "-KILL", "-f", pat], capture_output=True)


def stop_proc(p: subprocess.Popen, grace_s: float = 3.0):
    if p.poll() is None:
        p.send_signal(signal.SIGTERM)
        try:
            p.wait(grace_s)
        except subprocess.TimeoutExpired:
            p.kill()
            p.wait()


def ctl(cfg_path: str, args: str, timeout: float = 20) -> subprocess.CompletedProcess:
    return subprocess.run(f'"{binary("rm-ctl")}" --config {cfg_path} {args}', shell=True, capture_output=True,
                          text=True, timeout=timeout)


def run_actions(slot: dict, cfg_path: str, record: dict, stop: threading.Event):
    """client1: the run's scheduled silences and load-knob changes."""
    plan = slot["plan"]
    actions = [{**f, "type": "failure"} for f in plan["fails"]]
    for k in plan["knobs"]:
        actions.append({**k, "type": "load_knob"})
        if k.get("off_at_s") is not None:
            actions.append({**k, "type": "load_knob_off", "at_s": k["off_at_s"]})
    actions.sort(key=lambda a: a["at_s"])
    log = record.setdefault("actions", [])
    for a in actions:
        if not wait_until(slot["load_at"] + plan["warmup"] + a["at_s"], stop):
            return
        rec = {"type": a["type"], "at_s": a["at_s"]}
        try:
            if a["type"] == "failure":
                rec.update(target=a.get("target", "rank1"), duration_ms=a["duration_ms"])
                cmd = f"silence {rec['target']} --duration-ms {int(a['duration_ms'])}"
            else:
                node = str(a.get("node", 1))
                if node == "coordinator":
                    st = rs.parse_status(ctl(cfg_path, "status").stdout)
                    co = {s.get("coordinator") for s in st.values() if "error" not in s} - {None}
                    node = str(co.pop()) if len(co) == 1 else "?"
                th = a.get("threads", 8)
                threads = 0 if a["type"] == "load_knob_off" else (os.cpu_count() if th == "cores" else int(th))
                cmd = f"load {node} --threads {threads}"
                if threads:
                    cmd += f" --duty {int(a.get('duty', 900))}"
                    if a.get("duration_ms"):
                        cmd += f" --duration-ms {int(a['duration_ms'])}"
                rec.update(node=int(node) if node.isdigit() else node, threads=threads, duty=a.get("duty", 900),
                           duration_ms=a.get("duration_ms", 0))
            rec["sent_ms_client0"] = int(time.time() * 1000)
            r = ctl(cfg_path, cmd)
            rec.update(rc=r.returncode, output=(r.stdout + r.stderr).strip()[-500:])
            print(f"    t+{a['at_s']} s {a['type']}: {rec['output'][:100]}")
        except Exception as e:  # noqa: BLE001
            rec["error"] = str(e)
        log.append(rec)


def run_slot(slot: dict, role: str, stop: threading.Event):
    plan, run_id = slot["plan"], slot["plan"]["run_id"]
    logdir = REPO / "logs" / run_id
    logdir.mkdir(parents=True, exist_ok=True)
    # Remove only this role's files from an earlier attempt: on one machine
    # (testing) all six roles share the folder.
    mine = (f"cloud{role[-1]}-", ) if role.startswith("cloud") else (f"client{role[-1]}-", )
    for f in logdir.iterdir():
        if f.name.startswith(mine) or f.name == f"{role}-lab.json":
            f.unlink()
    cfg_path = f"runs/{run_id}.toml"
    (REPO / "runs").mkdir(exist_ok=True)
    (REPO / cfg_path).write_text(plan["cfg_text"])
    record = {"role": role, "run_id": run_id, "host": socket.gethostname(), "slot_start": slot["start"],
              "load_at_ms": int(slot["load_at"] * 1000), "started": time.time()}
    print(f"=== {fmt_t(slot['start'])} {run_id}")
    try:
        if role.startswith("cloud"):
            i = role[-1]
            with open(logdir / f"cloud{i}-node.out", "w") as out:
                p = subprocess.Popen([binary("rm-node"), "--config", cfg_path, "--id", i], cwd=REPO,
                                     stdout=out, stderr=subprocess.STDOUT)
            if not wait_until(slot["stop"], stop):
                stop_proc(p)
                return
            record["node_exit_before_stop"] = p.poll()
            stop_proc(p)
            record["node_exit"] = p.returncode
        else:
            m = role[-1]
            lg = plan["lg"]
            flags = []
            for k in rs.LOADGEN_FLAGS:
                if k in lg:
                    flags += [f"--{k.replace('_', '-')}", str(lg[k])]
            argv = [binary("rm-loadgen"), "run", "--config", cfg_path, "--machine", m,
                    "--start-at-ms", str(int(slot["load_at"] * 1000)), "--seed", str(slot["seed"]),
                    "--duration-secs", str(plan["duration"]), "--warmup-secs", str(plan["warmup"]),
                    "--drain-secs", str(plan["drain"]), *flags]
            side = None
            if not wait_until(slot["lg_start"], stop):
                return
            with open(logdir / f"client{m}-loadgen.out", "w") as out:
                p = subprocess.Popen(argv, cwd=REPO, stdout=out, stderr=subprocess.STDOUT)
            if role == "client1":
                # Just before the load begins: is the cluster up? Then the
                # scheduled silences and load-knob changes.
                wait_until(slot["load_at"] - 2, stop)
                st = rs.parse_status(ctl(cfg_path, "status").stdout)
                ok, why = rs.cluster_ready(st)
                record["ready"], record["ready_why"] = ok, why
                print(f"    cluster {'ready' if ok else 'NOT READY'}: {why}")
                side = threading.Thread(target=run_actions, args=(slot, cfg_path, record, stop), daemon=True)
                side.start()
            while p.poll() is None and time.time() < slot["stop"] + 1 and not stop.is_set():
                time.sleep(0.5)
            record["loadgen_exit"] = p.poll() if p.poll() is not None else "killed at slot end"
            stop_proc(p)
            if side is not None:
                side.join(timeout=5)
    finally:
        record["finished"] = time.time()
        (logdir / f"{role}-lab.json").write_text(json.dumps(record, indent=2))


def cmd_run(args) -> int:
    base_cfg, slots = load_inputs(args)
    show(slots, args.role)
    probs = role_problems(args.role, base_cfg) + build_problems()
    for p in probs:
        print(f"  PROBLEM: {p}")
    if probs and not args.force:
        print("refusing to run (fix the problems above, or --force)")
        return 1
    stop = threading.Event()

    def on_signal(*_):
        print("\nstopping: ending this machine's part of the timetable")
        stop.set()
    signal.signal(signal.SIGINT, on_signal)
    signal.signal(signal.SIGTERM, on_signal)
    done = skipped = 0
    for slot in slots:
        if stop.is_set():
            break
        if time.time() > slot["start"] + 2:
            print(f"--- skip {slot['plan']['run_id']} (its slot began at {fmt_t(slot['start'])})")
            skipped += 1
            continue
        if not wait_until(slot["start"] - 3, stop):
            break
        kill_ours()
        if not wait_until(slot["start"], stop):
            break
        run_slot(slot, args.role, stop)
        done += 1
    kill_ours()
    print(f"finished: {done} run(s), {skipped} skipped. Next: lab_run.py pack (same arguments)")
    return 0


# ------------------------------------------------------------------ pack and collect


def cmd_pack(args) -> int:
    _, slots = load_inputs(args)
    out = Path(args.output or Path.home() / f"rescuemesh-{args.role}-{timetable_id(slots)}.tgz")
    n = 0
    with tarfile.open(out, "w:gz") as tar:
        for s in slots:
            d = REPO / "logs" / s["plan"]["run_id"]
            if d.is_dir():
                tar.add(d, arcname=d.name)
                n += 1
    print(f"wrote {out}: {n} run folder(s) of {len(slots)}")
    return 0


def cmd_collect(args) -> int:
    base_cfg, slots = load_inputs(args)
    show(slots)
    tmp = Path(tempfile.mkdtemp(prefix="rm-collect-"))
    sources = []
    for i, src in enumerate(args.sources):
        p = Path(src)
        if p.is_dir():
            sources.append(p)
        else:
            d = tmp / f"src{i}"
            d.mkdir()
            with tarfile.open(p) as tar:
                tar.extractall(d, filter="data")
            sources.append(d)
    commit = subprocess.run(["git", "rev-parse", "HEAD"], cwd=REPO, capture_output=True, text=True).stdout.strip()
    ok = bad = 0
    for s in slots:
        plan, sweep, spec = s["plan"], s["sweep"], s["spec"]
        run_id = plan["run_id"]
        out = Path(args.out) / sweep["name"]
        run_dir = out / run_id
        if (run_dir / "meta.json").exists() and not args.force:
            ok += 1
            continue
        shutil.rmtree(run_dir, ignore_errors=True)
        run_dir.mkdir(parents=True)
        for src in sources:
            d = src / run_id
            if d.is_dir():
                for f in d.iterdir():
                    if f.is_file():
                        shutil.copy2(f, run_dir / f.name)
        (run_dir / "config.toml").write_text(plan["cfg_text"])
        records = {r.stem.removesuffix("-lab"): json.loads(r.read_text()) for r in run_dir.glob("*-lab.json")}
        lg = plan["lg"]
        procs = int(lg.get("processes", 4))
        reasons = []
        missing = rs.missing_logs(run_dir, False, N_CLIENTS, procs)
        if missing:
            reasons.append(f"missing logs: {missing}")
        c1 = records.get("client1", {})
        if c1 and c1.get("ready") is False:
            reasons.append(f"cluster not ready at the start: {c1.get('ready_why')}")
        actions = c1.get("actions", [])
        failed_actions = [a for a in actions if a.get("rc") not in (0, None) or "error" in a]
        if failed_actions:
            reasons.append(f"scheduled action failed: {failed_actions[0]}")
        n_actions = len(plan["fails"]) + sum(1 + (k.get("off_at_s") is not None) for k in plan["knobs"])
        if c1 and len(actions) < n_actions:
            reasons.append(f"only {len(actions)} of {n_actions} scheduled actions ran")
        if reasons:
            where = rs.park_failed(out, run_dir, reasons, actions)
            print(f"  FAILED {run_id}: {'; '.join(reasons)[:200]} -> {where}")
            bad += 1
            continue
        fp = rs.fingerprint(plan, commit, False, N_CLIENTS, "lab-timetable")
        meta = {"sweep": sweep["name"], "label": spec["label"], "series": spec.get("series", spec["label"]),
                "seed": s["seed"], "x": spec.get("x", float("nan")),
                "x_name": spec.get("x_name", sweep.get("x_name", "load level")),
                "config_overrides": plan["cfg_over"], "base_config": str(args.config), "loadgen": lg,
                "mode": lg.get("mode", "closed"), "processes_per_machine": procs,
                "users_per_process": int(lg.get("users", 50)), "client_machines": N_CLIENTS,
                "concurrent_users": int(lg.get("users", 50)) * procs * N_CLIENTS,
                "warmup_secs": plan["warmup"], "duration_secs": plan["duration"], "drain_secs": plan["drain"],
                "failure": plan["fails"][0] if plan["fails"] else {}, "failures": plan["fails"],
                "load_knobs": plan["knobs"], "actions": actions,
                "failure_injections": [a for a in actions if a["type"] == "failure"],
                "clock_host": "client1", "start_at_ms": int(s["load_at"] * 1000), "git_commit": commit,
                "stub": False, "local": False, "lab_timetable": timetable_id(slots),
                "lab_records": records, "fingerprint": fp,
                "preflight": {"skipped": True, "mode": "lab timetable (lab_run.py check on each machine)"}}
        (run_dir / "meta.json").write_text(json.dumps(meta, indent=2))
        ok += 1
    shutil.rmtree(tmp, ignore_errors=True)
    print(f"collected {ok} run(s) into {args.out}/<sweep>/, {bad} failed (in <sweep>/_failed)")
    return 0 if bad == 0 else 2


# ------------------------------------------------------------------ demo


def cmd_demo(args) -> int:
    base = tomllib.loads(Path(args.config).read_text())
    probs = role_problems(args.role, base) + build_problems()
    for p in probs:
        print(f"  PROBLEM: {p}")
    if probs and not args.force:
        return 1
    run_id = f"demo-{dt.date.today():%Y%m%d}" + ("-auto" if args.auto_inject else "")
    over = {"run.run_id": run_id, "run.seed": 7, "run.log_dir": "logs", "inject.enabled": bool(args.auto_inject)}
    if args.auto_inject:
        over.update({"inject.interval_min_ms": 20000, "inject.interval_max_ms": 25000,
                     "inject.duration_min_ms": 4000, "inject.duration_max_ms": 8000})
    cfg_path = f"runs/{run_id}.toml"
    (REPO / "runs").mkdir(exist_ok=True)
    (REPO / cfg_path).write_text(rs.dump_toml(rs.apply_overrides(base, over)))
    (REPO / "logs" / run_id).mkdir(parents=True, exist_ok=True)
    if args.role.startswith("cloud"):
        argv = [binary("rm-node"), "--config", cfg_path, "--id", args.role[-1]]
    else:
        argv = [binary("rm-loadgen"), "run", "--config", cfg_path, "--machine", args.role[-1], "--mode", "open",
                "--rate", str(args.rate), "--processes", "2", "--users", "50", "--warmup-secs", "0",
                "--duration-secs", "0", "--drain-secs", "3"]
    print(f"{args.role}: {' '.join(argv[1:])}   (Ctrl-C to stop)")

    def interrupt(*_):
        raise KeyboardInterrupt
    # Also when started in the background, where SIGINT is ignored.
    signal.signal(signal.SIGINT, interrupt)
    signal.signal(signal.SIGTERM, interrupt)
    p = subprocess.Popen(argv, cwd=REPO)
    try:
        return p.wait()
    except KeyboardInterrupt:
        stop_proc(p, grace_s=6.0)
        return 0


# ------------------------------------------------------------------ main


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    def common(p, role=True, sweeps=True):
        if role:
            p.add_argument("--role", required=True, choices=ROLES)
        p.add_argument("--config", default="config/lab.toml", help="the lab cluster config (same on all six)")
        if sweeps:
            p.add_argument("--start", required=True, help="first slot: HH:MM today, 'YYYY-MM-DD HH:MM' or Unix s")
            p.add_argument("--seeds", help="comma-separated seeds overriding the sweeps'")
            p.add_argument("--only", help="only configurations whose label contains this text")
            p.add_argument("sweeps", nargs="+", help="sweep files, run in this order")

    p = sub.add_parser("check", help="this machine's role, build, clock and the timetable id")
    common(p)
    p = sub.add_parser("run", help="run this machine's part of the timetable")
    common(p)
    p.add_argument("--force", action="store_true", help="run despite check problems")
    p = sub.add_parser("pack", help="this machine's logs of the timetable, as one .tgz")
    common(p)
    p.add_argument("--output", help="the .tgz to write (default ~/rescuemesh-<role>-<id>.tgz)")
    p = sub.add_parser("collect", help="merge the six machines' logs into run folders with meta.json")
    common(p, role=False)
    p.add_argument("--from", dest="sources", nargs="+", required=True, help=".tgz files or log folders")
    p.add_argument("--out", default="results/raw")
    p.add_argument("--force", action="store_true", help="rebuild runs that already have meta.json")
    p = sub.add_parser("demo", help="this machine's part of the live demo, until Ctrl-C")
    common(p, sweeps=False)
    p.add_argument("--rate", type=float, default=50, help="client roles: requests/s from this machine")
    p.add_argument("--auto-inject", action="store_true", help="cloud roles: automatic fault injection on")
    p.add_argument("--force", action="store_true")
    args = ap.parse_args(argv)
    os.chdir(REPO)
    return {"check": cmd_check, "run": cmd_run, "pack": cmd_pack, "collect": cmd_collect,
            "demo": cmd_demo}[args.cmd](args)


if __name__ == "__main__":
    sys.exit(main())
