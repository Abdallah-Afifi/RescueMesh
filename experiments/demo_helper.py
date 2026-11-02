#!/usr/bin/env python3
"""Helper for experiments/demo.sh: starts and stops the demo cluster and load,
and shows what the audience should see (plans per second per node, roles,
views, recovery). Python 3.11+. Run by demo.sh; each subcommand can also be
run by hand:

    demo_helper.py [--local | --machines M] [--base-config C] <command> ...

    setup [--auto-inject]        kill strays, write the demo config, start 3 nodes, wait until ready
    load --rate R [--secs S] [--tag T] start open-loop load on every client machine (background;
                                 --secs 0, the default, runs until `stop`)
    watch --secs S [--mark N]    one line per second: plans/s per node (from `plans` deltas), share,
                                 role, status, view, CPU, queue, load knob
    status                       one line per node
    ctl ARGS...                  rm-ctl with the demo config (from a machine that reaches the nodes)
    coordinator                  print the current coordinator's id
    recovery --node N [--secs S] watch N go Syncing -> Active and catch up before it takes work again
    stop                         stop nodes and load everywhere (TERM, then KILL)

State (hosts, config, pids) is kept in runs/demo/state.json on the controller.
"""

from __future__ import annotations

import argparse
import json
import shutil
import signal
import subprocess
import sys
import time
import tomllib
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import run_sweep as rs  # noqa: E402

STATE = Path("runs/demo/state.json")
BAR = 30  # characters for a 100% share


def hosts_of(args):
    ns = argparse.Namespace(local=args.local, local_clients=1, machines=args.machines)
    return rs.load_hosts(ns, {})


def base_path(args) -> Path:
    if args.base_config:
        return Path(args.base_config)
    return Path("config/local.toml") if args.local else Path("config/cluster.toml")


def save(state: dict):
    STATE.parent.mkdir(parents=True, exist_ok=True)
    STATE.write_text(json.dumps(state, indent=2))


def load_state() -> dict:
    if not STATE.exists():
        sys.exit("no demo running (runs/demo/state.json missing): run `demo.sh` or `demo_helper.py setup` first")
    return json.loads(STATE.read_text())


class Ctl:
    """Runs rm-ctl with the demo config: on this machine if it has the binary,
    otherwise on the first client machine over ssh."""

    def __init__(self, state: dict, cloud, clients):
        self.cfg = state["config"]
        local_bin = Path("target/release/rm-ctl")
        self.local = state["local"] or local_bin.exists()
        self.host = clients[0]

    def run(self, args: str, timeout=20) -> subprocess.CompletedProcess:
        if self.local:
            cmd = f"target/release/rm-ctl --config {self.cfg} {args}"
            return subprocess.run(["bash", "-c", cmd], capture_output=True, text=True, timeout=timeout)
        return self.host.run(f'"$(pwd -P)/target/release/rm-ctl" --config {self.cfg} {args}', check=False,
                             timeout=timeout)

    def status(self) -> dict[int, dict]:
        try:
            return rs.parse_status(self.run("status").stdout)
        except (subprocess.TimeoutExpired, RuntimeError):
            return {}


def ctx(args):
    state = load_state()
    a = argparse.Namespace(local=state["local"], machines=state.get("machines"))
    cloud, clients = hosts_of(a)
    return state, cloud, clients, Ctl(state, cloud, clients)


# ------------------------------------------------------------------ commands


def cmd_setup(args):
    cloud, clients = hosts_of(args)
    base = tomllib.loads(base_path(args).read_text())
    errs = rs.check_cluster_hosts(cloud, base, args.local)
    if errs:
        sys.exit("\n".join(errs))
    run_id = f"demo-{time.strftime('%Y%m%d-%H%M%S')}" + ("-auto" if args.auto_inject else "")
    # The scripted steps 1-4 must not be disturbed by automatic silences,
    # whatever the base config says (cluster.example.toml turns them on).
    over = {"run.run_id": run_id, "run.seed": 7, "run.log_dir": "logs", "inject.enabled": False}
    if args.top_k:
        over["planner.top_k"] = args.top_k
    if args.auto_inject:
        # Automatic fault injection by the cloud itself (DL-011, DL-025), with
        # short intervals so the audience sees a few in two minutes.
        over.update({"inject.enabled": True, "inject.interval_min_ms": 20000, "inject.interval_max_ms": 25000,
                     "inject.duration_min_ms": 4000, "inject.duration_max_ms": 8000})
    text = rs.dump_toml(rs.apply_overrides(base, over))
    cfg = f"runs/demo/{run_id}.toml"
    Path(cfg).parent.mkdir(parents=True, exist_ok=True)
    Path(cfg).write_text(text)
    hosts = rs.unique_hosts([*cloud, *clients])
    rs.cleanup_all(hosts, "before the demo")
    for h in hosts:
        if h.ssh is not None:
            h.put_text(text, cfg)
        h.run(f"mkdir -p logs/{run_id}")
    nodes = []
    for i, h in enumerate(cloud, start=1):
        nodes.append([i, h.spawn("rm-node", f"--config {cfg} --id {i}", f"logs/{run_id}/cloud{i}-node.out")])
    state = {"local": args.local, "machines": str(args.machines) if args.machines else None, "config": cfg,
             "run_id": run_id, "nodes": nodes, "loads": [], "auto_inject": args.auto_inject}
    save(state)
    ctl = Ctl(state, cloud, clients)
    deadline, why = time.time() + 30, ""
    while time.time() < deadline:
        ok, why = rs.cluster_ready(ctl.status())
        if ok:
            print(f"cluster ready: {why}")
            return 0
        time.sleep(0.5)
    print(f"cluster NOT ready after 30 s: {why}")
    return 1


def cmd_load(args):
    state, cloud, clients, _ = ctx(args)
    cfg = state["config"]
    if args.tag:
        # A second load generator on the same machines needs its own run id (its own log files).
        text = Path(cfg).read_text().replace(f'run_id = "{state["run_id"]}"', f'run_id = "{state["run_id"]}-{args.tag}"')
        cfg = cfg.replace(".toml", f"-{args.tag}.toml")
        Path(cfg).write_text(text)
        for h in rs.unique_hosts(clients):
            if h.ssh is not None:
                h.put_text(text, cfg)
    c0, _ = clients[0].now_ms()
    start = c0 + 2000
    rate = args.rate / len(clients)
    for i, h in enumerate(clients, start=1):
        log = f"logs/{state['run_id']}/client{i}-loadgen{'-' + args.tag if args.tag else ''}.out"
        pid = h.spawn("rm-loadgen", f"run --config {cfg} --machine {i} --mode open --rate {rate:g} "
                                    f"--processes {args.processes} --users {args.users} --warmup-secs 0 "
                                    f"--duration-secs {args.secs} --drain-secs 3 --seed 7 --start-at-ms {start}", log)
        state["loads"].append([i, pid])
    save(state)
    how_long = f"for {args.secs} s" if args.secs else "until the demo stops it"
    if args.tag:  # an extra load on top of the running one (demo.sh prints the new total)
        print(f"extra load ({args.tag}): +{args.rate:g} requests/s from {len(clients)} client machine(s) {how_long}")
    else:
        print(f"load: {args.rate:g} requests/s in total from {len(clients)} client machine(s) {how_long}")
    time.sleep(2.2)
    return 0


def fmt_node(n: int, s: dict | None, d_plans, share, mark: int | None) -> str:
    if not s or "error" in s:
        return f"n{n} {'unreachable':<{BAR + 18}}"
    tags = []
    if s.get("silenced"):
        # A silenced node hears nothing, so its own idea of the coordinator
        # is stale; never show it as one next to the real coordinator.
        tags.append("SILENCED")
    elif s.get("coordinator") == n:
        tags.append("COORD")
    if s.get("status") != "Active":
        tags.append(str(s.get("status", "?")).upper())
    kn = s.get("load_knob") or {}
    if kn.get("threads"):
        tags.append(f"KNOB {kn['threads']}t")
    if mark == n:
        tags.append("<-")
    if d_plans is None:
        body = f"{'plans n/a':>10}"
        bar = ""
    else:
        bar = "#" * int(round(share * BAR)) if share == share else ""
        body = f"{d_plans:5.0f}/s {share * 100 if share == share else 0:3.0f}%"
    extra = ""
    if "cpu_permille" in s:
        extra += f" machine cpu {s['cpu_permille'] / 10:5.1f}%"
    if "queue_len" in s:
        extra += f" queue {s['queue_len']:3}"
    return f"n{n} {body} {bar:<{BAR}}{extra} {' '.join(tags)}"


def cmd_watch(args):
    state, cloud, clients, ctl = ctx(args)
    prev, prev_t = ctl.status(), time.time()
    have_plans = any("plans" in s for s in prev.values())
    if not have_plans:
        print("(this rm-node does not report `plans` in its status yet: showing roles and views only)")
    t_start = time.time()
    while time.time() - t_start < args.secs:
        time.sleep(max(0.0, 1.0 - (time.time() - prev_t)))
        cur, now = ctl.status(), time.time()
        dt = max(now - prev_t, 1e-3)
        deltas = {}
        for n in (1, 2, 3):
            a, b = prev.get(n, {}), cur.get(n, {})
            deltas[n] = ((b["plans"] - a["plans"]) / dt) if "plans" in a and "plans" in b else None
        tot = sum(v for v in deltas.values() if v)
        coord = {s.get("coordinator") for s in cur.values() if "error" not in s and not s.get("silenced")} - {None}
        views = {(s.get("view"), tuple(s.get("active") or [])) for s in cur.values()
                 if "error" not in s and not s.get("silenced")}
        head = f"t+{now - t_start:4.0f}s  coordinator {','.join(map(str, coord)) or '?'}  " + \
               "  ".join(f"view {v} {list(a)}" for v, a in sorted(views, key=lambda x: x[0] or 0))
        if dt > 3:
            head += f"  (status took {dt:.0f} s: a node or this machine is overloaded)"
        print(head)
        for n in (1, 2, 3):
            d = deltas[n]
            share = (d / tot) if d is not None and tot else float("nan")
            print("   " + fmt_node(n, cur.get(n), d, share, args.mark))
        sys.stdout.flush()
        prev, prev_t = cur, now
    return 0


def cmd_status(args):
    _, _, _, ctl = ctx(args)
    for n, s in sorted(ctl.status().items()):
        print(json.dumps(s))
    return 0


def cmd_ctl(args):
    _, _, _, ctl = ctx(args)
    r = ctl.run(" ".join(args.rest), timeout=30)
    out = (r.stdout + r.stderr).strip()
    print(out)
    return r.returncode


def cmd_coordinator(args):
    _, _, _, ctl = ctx(args)
    co = {s.get("coordinator") for s in ctl.status().values() if "error" not in s and not s.get("silenced")} - {None}
    if len(co) != 1:
        return 1
    print(co.pop())
    return 0


def silenced_node(st: dict[int, dict]) -> int | None:
    """The node that reports `silenced: true` (a silenced node still answers status)."""
    ids = [n for n, s in st.items() if "error" not in s and s.get("silenced")]
    return ids[0] if len(ids) == 1 else None


def cmd_silenced(args):
    """Prints the id of the silenced node, waiting up to --wait seconds for
    the silence to take effect (rank1 or coordinator is resolved by the cloud,
    so this is how the demo learns which node it was)."""
    _, _, _, ctl = ctx(args)
    deadline = time.time() + args.wait
    while True:
        n = silenced_node(ctl.status())
        if n is not None:
            print(n)
            return 0
        if time.time() >= deadline:
            return 1
        time.sleep(0.3)


def mark_arg(v: str) -> int | None:
    return int(v) if v.strip().isdigit() else None


def cmd_recovery(args):
    """Watches node N rejoin. Polls every --poll seconds (0.1 by default),
    because SYNCING usually lasts well under a second. Prints a line at every
    state change (with applied position against the peers' commit), a
    progress line once a second, and stops once N is ACTIVE in the view and
    has planned again."""
    _, _, _, ctl = ctx(args)
    n, t0 = args.node, time.time()
    prev_state, last_line = None, 0.0
    t_sync = t_active = None
    sync_applied = None
    plans_at_active = None
    while time.time() - t0 < args.secs:
        st = ctl.status()
        now = time.time() - t0
        me = st.get(n, {})
        peers = [x for p_, x in st.items() if p_ != n and "error" not in x]
        peer_commit = max((x.get("commit", 0) for x in peers), default=0)
        state = "SILENCED" if me.get("silenced") else str(me.get("status", "?")).upper()
        applied = me.get("applied")
        behind = (peer_commit - applied) if isinstance(applied, int) else "?"
        line = (f"t+{now:5.2f}s  n{n} {state:<9} applied {applied!s:>7} | peers' commit {peer_commit:>7} "
                f"(behind {behind}) view {me.get('view', '?')} {me.get('active', '')}")
        if state != prev_state:
            print(line)
            if prev_state == "SILENCED" and state == "SYNCING":
                t_sync, sync_applied = now, applied
                print(f"   -> n{n} woke up and is SYNCING: it takes no work while it catches up "
                      "from the coordinator")
            elif state == "ACTIVE" and prev_state in ("SYNCING", "SILENCED"):
                t_active = now
                if t_sync is None:
                    print(f"   -> (SYNCING lasted less than one {args.poll:g} s poll)")
                else:
                    gained = (applied - sync_applied) if isinstance(applied, int) and isinstance(sync_applied, int) else "?"
                    print(f"   -> SYNCING took {now - t_sync:.2f} s and applied {gained} entries")
                print(f"   -> n{n} is ACTIVE in view {me.get('view')} {me.get('active')}: applied {applied} "
                      f"vs peers' commit {peer_commit}")
                plans_at_active = me.get("plans")
            prev_state, last_line = state, now
        elif now - last_line >= 1.0:
            print(line)
            last_line = now
        if t_active is not None:
            plans = me.get("plans")
            if plans is None or plans_at_active is None:
                return 0
            if plans > plans_at_active:
                print(f"t+{now:5.2f}s  -> n{n} is planning again ({plans - plans_at_active} plans since it "
                      f"became ACTIVE, {now - t_active:.2f} s later)")
                return 0
        time.sleep(args.poll)
    print(f"n{n} did not finish recovering within {args.secs} s")
    return 1


def cmd_preflight(args):
    """run_sweep's preflight for the demo's hosts: commit, binaries built from
    it, clocks, ports, stray processes. Exits non-zero on a refusal."""
    cloud, clients = hosts_of(args)
    base = tomllib.loads(base_path(args).read_text())
    ns = argparse.Namespace(stub=False, local=args.local, allow_dirty=args.allow_dirty,
                            allow_clock_skew=args.allow_clock_skew)
    rs.preflight(cloud, clients, base, ns)
    return 0


def cmd_stop(args):
    if not STATE.exists():
        print("nothing to stop")
        return 0
    state, cloud, clients, _ = ctx(args)
    by_id = {i: h for i, h in enumerate(cloud, start=1)}
    for i, pid in state["nodes"]:
        try:
            by_id[i].stop([str(pid)])
        except Exception as e:  # noqa: BLE001
            print(f"  [{by_id[i].name}] {e}")
    rs.cleanup_all([*cloud, *clients], "stopping the demo")
    print(f"stopped. Event logs: logs/{state['run_id']}/ on each machine")
    STATE.unlink()
    return 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--local", action="store_true")
    ap.add_argument("--machines", type=Path, default=Path("experiments/machines.toml"))
    ap.add_argument("--base-config")
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("setup")
    p.add_argument("--auto-inject", action="store_true")
    p.add_argument("--top-k", type=int, help="planner cost knob for the demo (default: the config's)")
    p = sub.add_parser("preflight")
    p.add_argument("--allow-dirty", action="store_true")
    p.add_argument("--allow-clock-skew", action="store_true")
    p = sub.add_parser("load")
    p.add_argument("--rate", type=float, default=200)
    p.add_argument("--secs", type=int, default=0, help="measured seconds; 0 = until stopped")
    p.add_argument("--processes", type=int, default=2)
    p.add_argument("--users", type=int, default=50)
    p.add_argument("--tag")
    p = sub.add_parser("watch")
    p.add_argument("--secs", type=float, default=15)
    p.add_argument("--mark", type=mark_arg)
    sub.add_parser("status")
    sub.add_parser("coordinator")
    p = sub.add_parser("silenced")
    p.add_argument("--wait", type=float, default=5)
    p = sub.add_parser("ctl")
    p.add_argument("rest", nargs=argparse.REMAINDER)
    p = sub.add_parser("recovery")
    p.add_argument("--node", type=int, required=True)
    p.add_argument("--secs", type=float, default=40)
    p.add_argument("--poll", type=float, default=0.1, help="seconds between status polls")
    sub.add_parser("stop")
    args = ap.parse_args(argv)
    if shutil.which("ssh") is None and not args.local:
        sys.exit("ssh is needed for the six-machine demo")
    return {"setup": cmd_setup, "load": cmd_load, "watch": cmd_watch, "status": cmd_status, "ctl": cmd_ctl,
            "coordinator": cmd_coordinator, "silenced": cmd_silenced, "recovery": cmd_recovery,
            "preflight": cmd_preflight, "stop": cmd_stop}[args.cmd](args)


def _term(*_):
    raise KeyboardInterrupt


if __name__ == "__main__":
    # demo.sh runs long steps in the background and forwards Ctrl-C as TERM;
    # either way, stop quietly.
    signal.signal(signal.SIGTERM, _term)
    signal.signal(signal.SIGINT, signal.default_int_handler)
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("(interrupted)", file=sys.stderr)
        sys.exit(130)
