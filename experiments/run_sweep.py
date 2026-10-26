#!/usr/bin/env python3
"""Run a sweep of experiments: every configuration x every seed, one command.

    python experiments/run_sweep.py experiments/example-sweep.toml \
        --machines experiments/machines.toml            # the six machines
    python experiments/run_sweep.py experiments/example-sweep.toml --local --stub
                                                        # dev: one machine, rm-stub

For each (configuration, seed) it
  1. writes a run configuration (base config + the overrides + run id/seed),
  2. copies it to every machine and starts the three cloud nodes,
  3. starts `rm-loadgen run` on every client machine with one shared
     wall-clock start time,
  4. at the chosen time during the measured window runs `rm-ctl silence`,
  5. waits for the load to finish, stops the nodes,
  6. copies every machine's event logs into <out>/<run_id>/ and writes meta.json.

Runs that already have a meta.json are skipped (use --force to redo them), so
an interrupted sweep can be resumed. Clocks: run chrony on all machines first;
each process logs its measured offset and the analysis applies it.

Requires Python 3.11+, ssh access without a password to every machine, and the
release binaries built at <repo_dir>/target/release on every machine.
"""

from __future__ import annotations

import argparse
import copy
import json
import shlex
import shutil
import subprocess
import sys
import time
import tomllib
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path


# ------------------------------------------------------------------ TOML output


def toml_value(v) -> str:
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (int, float)):
        return repr(v)
    if isinstance(v, str):
        return json.dumps(v)
    if isinstance(v, list):
        return "[" + ", ".join(toml_value(x) for x in v) + "]"
    raise TypeError(f"cannot write {v!r} as TOML")


def dump_toml(d: dict) -> str:
    """Enough TOML for our configuration: tables, scalars, one array of tables."""
    out: list[str] = []

    def table(prefix: str, t: dict):
        scalars = {k: v for k, v in t.items() if not isinstance(v, dict) and not _is_aot(v)}
        subs = {k: v for k, v in t.items() if isinstance(v, dict)}
        aots = {k: v for k, v in t.items() if _is_aot(v)}
        if prefix and scalars:
            out.append(f"[{prefix}]")
        for k, v in scalars.items():
            out.append(f"{k} = {toml_value(v)}")
        if scalars:
            out.append("")
        for k, v in subs.items():
            table(f"{prefix}.{k}" if prefix else k, v)
        for k, items in aots.items():
            for item in items:
                out.append(f"[[{prefix}.{k}]]" if prefix else f"[[{k}]]")
                for ik, iv in item.items():
                    out.append(f"{ik} = {toml_value(iv)}")
                out.append("")

    table("", d)
    return "\n".join(out)


def _is_aot(v) -> bool:
    return isinstance(v, list) and bool(v) and all(isinstance(x, dict) for x in v)


def apply_overrides(cfg: dict, overrides: dict) -> dict:
    cfg = copy.deepcopy(cfg)
    for dotted, value in overrides.items():
        *path, leaf = dotted.split(".")
        node = cfg
        for p in path:
            node = node.setdefault(p, {})
        node[leaf] = value
    return cfg


# ------------------------------------------------------------------ hosts


class Host:
    """A machine we can run commands on: over ssh, or this one (local mode)."""

    def __init__(self, name: str, ssh: str | None, repo: str, ssh_opts: list[str]):
        self.name, self.ssh, self.repo, self.ssh_opts = name, ssh, repo, ssh_opts

    def _wrap(self, cmd: str) -> list[str]:
        full = f"cd {self.repo} && {cmd}"
        if self.ssh is None:
            return ["bash", "-c", full]
        return ["ssh", *self.ssh_opts, self.ssh, full]

    def run(self, cmd: str, check=True, **kw) -> subprocess.CompletedProcess:
        r = subprocess.run(self._wrap(cmd), capture_output=True, text=True, **kw)
        if check and r.returncode != 0:
            raise RuntimeError(f"[{self.name}] `{cmd}` failed ({r.returncode}):\n{r.stderr or r.stdout}")
        return r

    def spawn(self, cmd: str, log: str) -> str:
        """Start a background process; returns its pid on that machine."""
        r = self.run(f"{{ nohup {cmd} > {log} 2>&1 < /dev/null & echo $!; }}")
        return r.stdout.strip().splitlines()[-1]

    def kill(self, pid: str):
        self.run(f"kill -INT {pid} 2>/dev/null; sleep 1; kill {pid} 2>/dev/null; true", check=False)

    def put(self, local: Path, remote: str):
        if self.ssh is None:
            dst = Path(self.repo).expanduser() / remote
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(local, dst)
        else:
            self.run(f"mkdir -p {shlex.quote(str(Path(remote).parent))}")
            subprocess.run(["scp", "-q", *self.ssh_opts, str(local), f"{self.ssh}:{self.repo}/{remote}"], check=True)

    def get_dir(self, remote: str, local: Path):
        local.mkdir(parents=True, exist_ok=True)
        if self.ssh is None:
            src = Path(self.repo).expanduser() / remote
            if src.exists() and src.resolve() != local.resolve():
                shutil.copytree(src, local, dirs_exist_ok=True)
        else:
            subprocess.run(["scp", "-q", "-r", *self.ssh_opts, f"{self.ssh}:{self.repo}/{remote}/.", str(local)],
                           check=False)


def load_hosts(args, base_cfg: dict):
    if args.local:
        repo = str(Path.cwd())
        cloud = [Host(f"cloud{i}", None, repo, []) for i in (1, 2, 3)]
        clients = [Host(f"client{i}", None, repo, []) for i in range(1, args.local_clients + 1)]
        return cloud, clients
    m = tomllib.loads(Path(args.machines).read_text())
    opts = m.get("ssh_opts", ["-o", "BatchMode=yes", "-o", "ConnectTimeout=10"])
    repo = m.get("repo_dir", "~/RescueMesh")
    cloud = [Host(f"cloud{c['id']}", c["ssh"], c.get("repo_dir", repo), opts)
             for c in sorted(m["cloud"], key=lambda c: c["id"])]
    clients = [Host(f"client{c['machine']}", c["ssh"], c.get("repo_dir", repo), opts)
               for c in sorted(m["client"], key=lambda c: c["machine"])]
    if len(cloud) != 3:
        sys.exit("machines.toml needs exactly three [[cloud]] entries")
    return cloud, clients


# ------------------------------------------------------------------ one run


LOADGEN_FLAGS = ["processes", "users", "mode", "rate", "think_ms", "mix", "max_inflight"]


def run_one(sweep: dict, spec: dict, seed: int, cloud, clients, base_cfg: dict, args, out: Path) -> None:
    run_id = f"{sweep['name']}-{spec['label']}-s{seed}"
    run_dir = out / run_id
    if (run_dir / "meta.json").exists() and not args.force:
        print(f"skip {run_id} (done)")
        return
    print(f"=== {run_id}")
    shutil.rmtree(run_dir, ignore_errors=True)
    run_dir.mkdir(parents=True)

    overrides = {"run.run_id": run_id, "run.seed": seed, "run.log_dir": "logs", **spec.get("config", {})}
    cfg = apply_overrides(base_cfg, overrides)
    cfg_path = run_dir / "config.toml"
    cfg_path.write_text(dump_toml(cfg))
    remote_cfg = f"runs/{run_id}.toml"

    lg = {**sweep.get("loadgen", {}), **spec.get("loadgen", {})}
    n_clients = len(clients)
    if "rate" in lg:  # the sweep states the total rate; each machine offers its share
        lg["rate"] = lg["rate"] / n_clients
    duration, warmup, drain = (sweep.get(k) for k in ("duration_secs", "warmup_secs", "drain_secs"))
    fail = sweep.get("failure", {})
    fail = {**fail, **spec.get("failure", {})}

    hosts = {h.name: h for h in [*cloud, *clients]}.values()
    for h in hosts:
        h.run(f"mkdir -p logs runs && rm -rf logs/{run_id}")
        h.put(cfg_path, remote_cfg)

    pids: list[tuple[Host, str]] = []
    try:
        if args.stub:
            cmd = f"target/release/rm-stub --config {remote_cfg}"
            if fail.get("enabled"):
                cmd += f" --silence-every-s {int(warmup + fail['at_s'])} --silence-max-ms {fail['duration_ms']}"
            pids.append((cloud[0], cloud[0].spawn(cmd, f"logs/{run_id}-stub.out")))
        else:
            for i, h in enumerate(cloud, start=1):
                pids.append((h, h.spawn(f"target/release/rm-node --config {remote_cfg} --id {i}",
                                        f"logs/{run_id}-node{i}.out")))
        time.sleep(sweep.get("settle_secs", 3))

        start_at_ms = int(time.time() * 1000) + 8000
        flags = " ".join(f"--{k.replace('_', '-')} {shlex.quote(str(lg[k]))}" for k in LOADGEN_FLAGS if k in lg)
        base_cmd = (f"target/release/rm-loadgen run --config {remote_cfg} --start-at-ms {start_at_ms} "
                    f"--seed {seed} --duration-secs {duration} --warmup-secs {warmup} --drain-secs {drain} {flags}")

        def go(item):
            m, h = item
            return h.run(f"{base_cmd} --machine {m}", check=False)

        injected = []

        def inject():
            wait = start_at_ms / 1000 + warmup + fail["at_s"] - time.time()
            time.sleep(max(wait, 0))
            if args.stub:
                return  # the stub silences on its own schedule
            target = fail.get("target", "rank1")
            r = clients[0].run(f"target/release/rm-ctl --config {remote_cfg} silence {target} "
                               f"--duration-ms {fail['duration_ms']}", check=False)
            injected.append({"at_s": fail["at_s"], "target": target, "rc": r.returncode,
                             "output": (r.stdout + r.stderr).strip()})

        with ThreadPoolExecutor(max_workers=len(clients) + 1) as pool:
            futs = [pool.submit(go, (i, h)) for i, h in enumerate(clients, start=1)]
            inj = pool.submit(inject) if fail.get("enabled") else None
            results = [f.result() for f in futs]
            if inj:
                inj.result()
        for (i, _), r in zip(enumerate(clients, start=1), results):
            (run_dir / f"loadgen-client{i}.out").write_text(r.stdout + r.stderr)
            if r.returncode != 0:
                print(f"  WARNING: loadgen on client{i} exited {r.returncode}")
    finally:
        for h, pid in pids:
            h.kill(pid)
        time.sleep(1)
        for h in hosts:
            h.get_dir(f"logs/{run_id}", run_dir)

    commit = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip()
    meta = {"label": spec["label"], "series": spec.get("series", spec["label"]), "seed": seed,
            "x": spec.get("x", float("nan")), "x_name": spec.get("x_name", "load level"),
            "config_overrides": spec.get("config", {}), "loadgen": lg, "failure": fail,
            "failure_injections": injected, "start_at_ms": start_at_ms, "git_commit": commit,
            "stub": bool(args.stub), "client_machines": n_clients}
    (run_dir / "meta.json").write_text(json.dumps(meta, indent=2))
    print(f"  logs in {run_dir}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("sweep", type=Path)
    ap.add_argument("--machines", type=Path, default=Path("experiments/machines.toml"))
    ap.add_argument("--out", type=Path, default=Path("results/raw"))
    ap.add_argument("--local", action="store_true", help="run everything on this machine (development)")
    ap.add_argument("--local-clients", type=int, default=1, help="client machines to emulate with --local")
    ap.add_argument("--stub", action="store_true", help="use rm-stub instead of the three rm-node processes")
    ap.add_argument("--force", action="store_true", help="redo runs that already have a meta.json")
    ap.add_argument("--only", help="run only configurations whose label contains this text")
    ap.add_argument("--dry-run", action="store_true", help="print the plan and write nothing")
    args = ap.parse_args()

    sweep = tomllib.loads(args.sweep.read_text())
    base_cfg = tomllib.loads(Path(sweep["base_config"]).read_text())
    specs = [s for s in sweep["run"] if not args.only or args.only in s["label"]]
    seeds = sweep.get("seeds", [1, 2, 3, 4, 5])
    per = (sweep["warmup_secs"] + sweep["duration_secs"] + sweep["drain_secs"] + sweep.get("settle_secs", 3) + 12)
    print(f"{len(specs)} configurations x {len(seeds)} seeds = {len(specs) * len(seeds)} runs, "
          f"about {len(specs) * len(seeds) * per / 60:.0f} minutes")
    if args.dry_run:
        for s in specs:
            print(" ", s["label"], s.get("config", {}), s.get("loadgen", {}))
        return 0
    cloud, clients = load_hosts(args, base_cfg)
    args.out.mkdir(parents=True, exist_ok=True)
    for seed in seeds:  # seeds outermost, so a partial sweep has every configuration
        for spec in specs:
            run_one(sweep, spec, seed, cloud, clients, base_cfg, args, args.out)
    print("sweep complete; analyse with: python analysis/analyze.py", args.out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
