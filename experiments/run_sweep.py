#!/usr/bin/env python3
"""Run a sweep of experiments: every configuration x every seed, one command.

    python experiments/run_sweep.py experiments/sweeps/latency.toml \
        --machines experiments/machines.toml            # the six machines
    python experiments/run_sweep.py experiments/sweeps/smoke.toml --local
                                                        # dev: everything on this machine
    python experiments/run_sweep.py experiments/example-sweep.toml --local --stub
                                                        # dev: one machine, rm-stub

Once per sweep, a preflight checks that every host is reachable, runs the same
git commit and byte-identical release binaries, that chrony is synchronized
(a loud warning if not), and that the cloud hosts in machines.toml are the
node hosts of the cluster config. A mismatch of commit, binaries or hosts
stops the sweep before anything runs.

For each (configuration, seed) it
  1. kills stray rm-node / rm-loadgen / rm-stub processes of this checkout on
     every host (matched by the absolute binary path, so nothing else is hit),
  2. writes a run configuration (base config + the overrides + run id/seed)
     and copies it to every host,
  3. starts the three cloud nodes and polls `rm-ctl status` until all three
     are Active, in the same three-member view, with one coordinator,
  4. reads clients[0]'s clock and starts `rm-loadgen run` on every client
     machine with one shared start time in that clock,
  5. runs the scheduled actions (`[[failure]]` silences and `[[load_knob]]`
     CPU burners) at their times in the measured window, on clients[0]'s clock,
  6. waits for the load to finish (with a timeout), stops every process
     (TERM, then KILL after a grace period),
  7. copies every host's `logs/<run_id>/` (event logs and the `.out`
     stdout/stderr files) into <out>/<run_id>/ with tar over ssh,
  8. checks that every node and client log is there and writes meta.json.

A run FAILS, and gets no meta.json, if a node process dies, the cluster does
not become ready in time, a scheduled rm-ctl action exits non-zero, a loadgen
exits non-zero or times out, or a node/client log is missing. Its directory
is moved to <out>/_failed/ with a `failure.txt`, the sweep goes on, and a
later run of the same command (resume) retries it. Runs that already have a
meta.json are skipped (use --force to redo them).

On Ctrl-C or an exception every host is cleaned up (stray processes killed),
so no remote loadgen keeps running into the next run.

Requires Python 3.11+ (tomllib) on the controller, password-less ssh to every
machine, a POSIX shell, git, pkill and tar on every machine, and the release
binaries built at <repo_dir>/target/release on every machine.
"""

from __future__ import annotations

import argparse
import copy
import datetime as dt
import hashlib
import json
import re
import shlex
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import tomllib
from pathlib import Path

SAFE_NAME = re.compile(r"[A-Za-z0-9._-]+")
# ServerAlive*: a dead network path ends the ssh call within ~15 s instead of hanging.
DEFAULT_SSH_OPTS = ["-o", "BatchMode=yes", "-o", "ConnectTimeout=10",
                    "-o", "ServerAliveInterval=5", "-o", "ServerAliveCountMax=3"]
CHECKED_BINARIES = ["rm-node", "rm-loadgen", "rm-ctl"]
OUR_PROCESSES = "rm-(node|loadgen|stub)"
CMD_TIMEOUT = 60  # seconds, for every short remote command


class RunFailed(Exception):
    """The run is invalid; it gets no meta.json so a resumed sweep retries it."""


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


def _pkill_script(signal: str) -> str:
    """Kill this checkout's own binaries only: the pattern is anchored at the
    absolute path of <repo>/target/release, with regex characters escaped."""
    return (
        "R=$(pwd -P | sed 's/[][\\\\.*^$+?(){}|]/\\\\&/g'); "
        f"pkill -{signal} -f \"^$R/target/release/{OUR_PROCESSES}( |$)\" 2>/dev/null; true"
    )


class Host:
    """A machine we can run commands on: over ssh, or this one (local mode)."""

    def __init__(self, name: str, ssh: str | None, repo: str, ssh_opts: list[str], addr: str | None = None):
        self.name, self.ssh, self.repo, self.ssh_opts = name, ssh, repo, ssh_opts
        self.addr = addr  # the address the cluster config uses for this machine

    @property
    def key(self) -> tuple:
        """Identifies the physical machine + checkout (several roles can share one)."""
        return (self.ssh or "local", self.repo)

    def _wrap(self, cmd: str) -> list[str]:
        if self.ssh is None:
            return ["bash", "-c", f"cd {shlex.quote(str(Path(self.repo).expanduser()))} && {cmd}"]
        # repo_dir is left unquoted on purpose so the remote shell expands `~`.
        return ["ssh", *self.ssh_opts, self.ssh, f"cd {self.repo} && {cmd}"]

    def run(self, cmd: str, check=True, timeout: float | None = CMD_TIMEOUT, input: str | None = None
            ) -> subprocess.CompletedProcess:
        try:
            r = subprocess.run(self._wrap(cmd), capture_output=True, text=True, timeout=timeout,
                               input=input, stdin=None if input is not None else subprocess.DEVNULL)
        except subprocess.TimeoutExpired as e:
            raise RuntimeError(f"[{self.name}] `{cmd}` timed out after {timeout} s") from e
        if check and r.returncode != 0:
            raise RuntimeError(f"[{self.name}] `{cmd}` failed ({r.returncode}):\n{(r.stderr or r.stdout).strip()}")
        return r

    def spawn(self, binary: str, args: str, log: str) -> str:
        """Start one of our binaries in the background by its absolute path
        (so cleanup can match it); returns its pid on that machine."""
        r = self.run(f'R=$(pwd -P); {{ nohup "$R/target/release/{binary}" {args} > {log} 2>&1 < /dev/null & '
                     f'echo $!; }}')
        lines = r.stdout.strip().splitlines()
        pid = lines[-1].strip() if lines else ""
        if not pid.isdigit():
            raise RuntimeError(f"[{self.name}] could not start {binary}: {r.stdout!r} {r.stderr!r}")
        return pid

    def alive(self, pid: str, tries: int = 3, pause_s: float = 2.0) -> bool | None:
        """True or False when the machine answered; None when it could not be
        asked `tries` times in a row (ssh exit 255, a timeout, garbled output).
        An unreachable machine is not proof that the node died."""
        for i in range(tries):
            try:
                r = self.run(f"if kill -0 {pid} 2>/dev/null; then echo alive; else echo dead; fi",
                             check=False, timeout=20)
                out = r.stdout.strip().splitlines()
                if r.returncode == 0 and out and out[-1] in ("alive", "dead"):
                    return out[-1] == "alive"
            except RuntimeError:
                pass
            if i + 1 < tries:
                time.sleep(pause_s)
        return None

    def stop(self, pids: list[str], grace_s: float = 2.0):
        """TERM, wait up to grace_s for the processes to exit (rm-node flushes
        its event log on TERM), then KILL whatever is left."""
        if not pids:
            return
        p = " ".join(pids)
        polls = max(1, int(grace_s / 0.2))
        self.run(f"kill -TERM {p} 2>/dev/null; i=0; while [ $i -lt {polls} ]; do "
                 f"alive=0; for x in {p}; do kill -0 $x 2>/dev/null && alive=1; done; "
                 f"[ $alive = 0 ] && break; sleep 0.2; i=$((i+1)); done; "
                 f"kill -KILL {p} 2>/dev/null; true", check=False, timeout=grace_s + 30)

    def cleanup(self, grace_s: float = 2.0) -> int:
        """Kill stray processes of our binaries (TERM, then KILL). Returns how
        many were found."""
        r = self.run("R=$(pwd -P | sed 's/[][\\\\.*^$+?(){}|]/\\\\&/g'); "
                     f"pgrep -f \"^$R/target/release/{OUR_PROCESSES}( |$)\" | wc -l",
                     check=False, timeout=30)
        try:
            n = int(r.stdout.strip() or 0)
        except ValueError:
            n = 0
        if n:
            self.run(f"{_pkill_script('TERM')}; sleep {grace_s}; {_pkill_script('KILL')}", check=False,
                     timeout=grace_s + 30)
        return n

    def clock_sample(self, n: int = 3) -> tuple[float, float]:
        """(this machine's clock minus the controller's, ssh round trip), in
        ms, from the sample with the shortest round trip."""
        best = None
        for _ in range(n):
            a = time.time()
            remote, _ = self.now_ms()
            b = time.time()
            rtt = (b - a) * 1000
            off = remote - (a + b) / 2 * 1000
            if best is None or rtt < best[1]:
                best = (off, rtt)
        return best

    def busy_ports(self, tcp: list[int], udp: list[int]) -> list[str]:
        """Who is listening on these ports here (ss, else lsof; nothing if neither exists)."""
        parts = [f"(ss -Hltnp 'sport = :{p}' 2>/dev/null || lsof -nP -iTCP:{p} -sTCP:LISTEN 2>/dev/null) "
                 f"| sed 's/^/busy tcp {p}: /'" for p in tcp]
        parts += [f"(ss -Hlunp 'sport = :{p}' 2>/dev/null || lsof -nP -iUDP:{p} 2>/dev/null) "
                  f"| sed 's/^/busy udp {p}: /'" for p in udp]
        if not parts:
            return []
        try:
            r = self.run("; ".join(parts) + "; true", check=False, timeout=30)
        except RuntimeError:
            return []
        return [x for x in r.stdout.splitlines() if x.startswith("busy") and "COMMAND" not in x]

    def put_text(self, text: str, remote: str):
        d = shlex.quote(str(Path(remote).parent))
        self.run(f"mkdir -p {d} && cat > {shlex.quote(remote)}", input=text)

    def get_dir(self, remote: str, local: Path, timeout: float = 600):
        """Copies <repo>/<remote>/* into local/ (tar over ssh: no scp, no `~` quirks)."""
        local.mkdir(parents=True, exist_ok=True)
        if self.ssh is None:
            src = Path(self.repo).expanduser() / remote
            if src.exists() and src.resolve() != local.resolve():
                shutil.copytree(src, local, dirs_exist_ok=True)
            return
        with tempfile.TemporaryDirectory() as tmp:
            # COPYFILE_DISABLE: macOS tar would add ._* AppleDouble files.
            ssh = subprocess.Popen(self._wrap(f"test -d {shlex.quote(remote)} && "
                                              f"COPYFILE_DISABLE=1 tar cf - {shlex.quote(remote)}"),
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.DEVNULL)
            try:
                tar = subprocess.run(["tar", "xf", "-", "-C", tmp], stdin=ssh.stdout, capture_output=True,
                                     timeout=timeout)
                ssh.stdout.close()
                ssh.wait(timeout=60)
            except subprocess.TimeoutExpired:
                print(f"  WARNING: [{self.name}] copying logs/{remote} timed out after {timeout} s")
                return
            finally:
                if ssh.poll() is None:  # a stuck tar or ssh must not outlive the copy
                    ssh.kill()
                    ssh.wait()
            if ssh.returncode != 0:
                err = ssh.stderr.read().decode(errors="replace").strip()
                print(f"  WARNING: [{self.name}] no logs/{remote} to collect ({ssh.returncode}) {err}")
                return
            if tar.returncode != 0:
                print(f"  WARNING: [{self.name}] tar failed: {tar.stderr.decode(errors='replace')}")
            src = Path(tmp) / remote
            if src.exists():
                shutil.copytree(src, local, dirs_exist_ok=True)

    def now_ms(self) -> tuple[int, int]:
        """This machine's wall clock in Unix ms, and the controller's clock at
        the midpoint of the call (to convert between the two)."""
        a = time.time()
        r = self.run("perl -MTime::HiRes=time -e 'printf(\"%d\\n\", time()*1000)' 2>/dev/null"
                     " || python3 -c 'import time; print(int(time.time()*1000))' 2>/dev/null"
                     " || echo $(( $(date +%s) * 1000 ))", timeout=30)
        b = time.time()
        lines = r.stdout.strip().splitlines()
        try:
            return int(lines[-1]), int((a + b) / 2 * 1000)
        except (IndexError, ValueError):
            raise RuntimeError(f"[{self.name}] could not read the clock: {r.stdout!r} {r.stderr!r}") from None


def unique_hosts(hosts) -> list[Host]:
    seen, out = set(), []
    for h in hosts:
        if h.key not in seen:
            seen.add(h.key)
            out.append(h)
    return out


def load_hosts(args, base_cfg: dict):
    if args.local:
        repo = str(Path.cwd())
        cloud = [Host(f"cloud{i}", None, repo, [], "127.0.0.1") for i in (1, 2, 3)]
        clients = [Host(f"client{i}", None, repo, []) for i in range(1, args.local_clients + 1)]
        return cloud, clients
    m = tomllib.loads(Path(args.machines).read_text())
    opts = list(m.get("ssh_opts", DEFAULT_SSH_OPTS))
    # Always keep the keep-alive and connect timeouts, whatever machines.toml says.
    have = " ".join(opts)
    for k, v in (("BatchMode", "yes"), ("ConnectTimeout", "10"), ("ServerAliveInterval", "5"),
                 ("ServerAliveCountMax", "3")):
        if k not in have:
            opts += ["-o", f"{k}={v}"]
    repo = m.get("repo_dir", "~/RescueMesh")
    cloud = [Host(f"cloud{c['id']}", c["ssh"], c.get("repo_dir", repo), opts, c.get("addr"))
             for c in sorted(m["cloud"], key=lambda c: c["id"])]
    clients = [Host(f"client{c['machine']}", c["ssh"], c.get("repo_dir", repo), opts)
               for c in sorted(m["client"], key=lambda c: c["machine"])]
    if len(cloud) != 3:
        sys.exit("machines.toml needs exactly three [[cloud]] entries")
    if not clients:
        sys.exit("machines.toml needs at least one [[client]] entry")
    return cloud, clients


def cleanup_all(hosts, why: str):
    for h in unique_hosts(hosts):
        try:
            n = h.cleanup()
            if n:
                print(f"  [{h.name}] {why}: killed {n} stray process(es)")
        except Exception as e:  # noqa: BLE001 -- cleanup must try every host
            print(f"  WARNING: [{h.name}] cleanup failed: {e}")


# ------------------------------------------------------------------ preflight


PREFLIGHT_SCRIPT = r"""
# Homebrew and sbin tools are often missing from a non-interactive ssh shell's PATH.
PATH="$PATH:/opt/homebrew/bin:/usr/local/bin:/usr/sbin:/sbin"; export PATH
echo "repo=$(pwd -P)"
echo "commit=$(git rev-parse HEAD 2>/dev/null || echo none)"
echo "dirty=$(git status --porcelain --untracked-files=no 2>/dev/null | wc -l | tr -d ' ')"
echo "dirty_src=$(git status --porcelain --untracked-files=no -- crates Cargo.toml Cargo.lock 2>/dev/null | wc -l | tr -d ' ')"
if [ "$(git status --porcelain --untracked-files=no 2>/dev/null | wc -l | tr -d ' ')" != 0 ]; then
  echo "diff_sha=$(git diff HEAD 2>/dev/null | (sha256sum 2>/dev/null || shasum -a 256) | cut -c1-16)"
else
  echo "diff_sha=clean"
fi
echo "os=$(uname -srm)"
echo "cores=$(getconf _NPROCESSORS_ONLN 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo ?)"
for b in rm-node rm-loadgen rm-ctl rm-stub; do
  f=target/release/$b
  if [ -f "$f" ]; then
    h=$( (sha256sum "$f" 2>/dev/null || shasum -a 256 "$f") | cut -d' ' -f1)
    echo "sha256_$b=$h"
    # Sources modified after the binary was built: it is stale. Cargo's
    # dep-info file (rm-X.d) lists exactly the sources that binary was built
    # from, so a change to tests or to another binary does not count; without
    # it, every tracked source file under crates/ counts.
    if [ -f "$f.d" ]; then
      newer=$( { sed -e 's/^[^:]*://' -e 's/\\$//' "$f.d" | tr ' ' '\n'; echo Cargo.lock; } |
              while read -r x; do [ -n "$x" ] && [ -e "$x" ] && [ "$x" -nt "$f" ] && echo "$x"; done)
      echo "stale_basis_$b=dep-info"
    else
      newer=$(git ls-files -z crates Cargo.toml Cargo.lock 2>/dev/null |
              xargs -0 sh -c 'for x; do [ "$x" -nt "$0" ] && echo "$x"; done' "$f" 2>/dev/null)
      echo "stale_basis_$b=all-sources"
    fi
    echo "stale_$b=$(printf '%s' "$newer" | grep -c .)"
    echo "stale_example_$b=$(printf '%s\n' "$newer" | head -1)"
  else
    echo "sha256_$b=missing"
  fi
done
if command -v chronyc >/dev/null 2>&1; then
  chronyc -n tracking 2>&1 | sed 's/^/chrony| /'
else
  echo "chrony=absent"
fi
if command -v timedatectl >/dev/null 2>&1; then
  echo "timesyncd=$(timedatectl show -p NTPSynchronized --value 2>/dev/null)"
fi
ps -eo pid=,args= 2>/dev/null | grep -E 'rm-(node|loadgen|stub)( |$)' | grep -v -e grep -e 'sh -s' | sed 's/^ */proc| /'
"""


def parse_preflight(text: str) -> dict:
    info: dict = {}
    c: dict = {}
    for line in text.splitlines():
        if line.startswith("chrony| "):
            k, _, v = line[8:].partition(":")
            c[k.strip()] = v.strip()
        elif line.startswith("proc| "):
            info.setdefault("procs", []).append(line[6:].strip())
        elif "=" in line:
            k, _, v = line.partition("=")
            info[k.strip()] = v.strip()
    if info.pop("chrony", None) == "absent" or not c:
        info["chrony"] = {"present": False, "synchronized": False}
    else:
        ref = c.get("Reference ID", "")
        synced = c.get("Leap status", "") == "Normal" and not ref.startswith("00000000")
        info["chrony"] = {"present": True, "synchronized": synced, "leap_status": c.get("Leap status"),
                          "reference": ref, "system_time": c.get("System time"),
                          "stratum": c.get("Stratum")}
    return info


def _addrs(host: str) -> set[str]:
    try:
        return {a[4][0] for a in socket.getaddrinfo(host, None)}
    except OSError:
        return set()


def check_cluster_hosts(cloud: list[Host], base_cfg: dict, local: bool) -> list[str]:
    """The cloud hosts in machines.toml must be the node hosts of the cluster config."""
    errors = []
    nodes = {int(n["id"]): n["host"] for n in base_cfg.get("cluster", {}).get("nodes", [])}
    if sorted(nodes) != [1, 2, 3]:
        return [f"the cluster config must define nodes 1, 2 and 3 (it has {sorted(nodes)})"]
    if local:
        for i, cfg_host in sorted(nodes.items()):
            a = _addrs(cfg_host)
            if not (a and all(x.startswith("127.") or x == "::1" for x in a)):
                errors.append(f"--local needs node {i} on this machine; the config says {cfg_host}")
        return errors
    for i, h in enumerate(cloud, start=1):
        cfg_host = nodes[i]
        mine = h.addr or h.ssh.split("@")[-1].split(":")[0]
        if mine != cfg_host and not (_addrs(mine) & _addrs(cfg_host)):
            errors.append(f"machines.toml cloud {i} is {mine} but the cluster config's node {i} is {cfg_host} "
                          f"(set `addr` in machines.toml if ssh uses another name)")
    return errors


def foreign_processes(info: dict) -> list[str]:
    """Our binaries running on a host but not started by this runner from
    this checkout (e.g. by hand as `target/release/rm-node`): cleanup does
    not touch them, and they may hold the ports."""
    mine = info.get("repo", "") + "/target/release/rm-"
    out = []
    for line in info.get("procs", []):
        pid, _, cmd = line.partition(" ")
        if not cmd.strip().startswith(mine):
            out.append(f"pid {pid}: {cmd.strip()[:120]}")
    return out


def check_build(info: dict, needed: list[str], allow_dirty: bool) -> tuple[list[str], list[str]]:
    """(errors, warnings) for one host: the binaries must be built from the
    checked-out commit. Byte-identical binaries cannot be required: release
    builds embed absolute paths (home directory, cargo registry), so hosts
    with different users, linkers or C libraries always differ."""
    errors, warnings = [], []
    if info.get("dirty_src", "0") not in ("0", ""):
        msg = (f"has {info['dirty_src']} uncommitted change(s) under crates/ or Cargo.*: the binaries are not "
               "the commit's (commit or stash them, rebuild, or pass --allow-dirty)")
        (warnings if allow_dirty else errors).append(msg)
    elif info.get("dirty", "0") not in ("0", ""):
        warnings.append(f"has {info['dirty']} uncommitted change(s) outside the source (docs, scripts)")
    for b in needed:
        if info.get(f"sha256_{b}", "missing") == "missing":
            errors.append(f"target/release/{b} is missing; run `cargo build --release`")
            continue
        n = info.get(f"stale_{b}", "0")
        if n not in ("0", ""):
            hint = "" if info.get(f"stale_basis_{b}") == "dep-info" else (
                f" (no target/release/{b}.d, so every tracked source counts; if only tests or other binaries "
                "changed since the build, `touch target/release/rm-*`)")
            errors.append(f"target/release/{b} is older than {n} of its source file(s) "
                          f"(e.g. {info.get(f'stale_example_{b}', '?')}); run `cargo build --release`{hint}")
    return errors, warnings


def clock_spread(samples: dict[str, tuple[float, float]], slack_ms: float = 50.0) -> tuple[float, float]:
    """(spread of the hosts' clock offsets, allowed spread). A sample's
    offset is uncertain by up to its round trip, so the allowance is
    slack_ms plus the largest round trip."""
    if not samples:
        return 0.0, slack_ms
    offs = [o for o, _ in samples.values()]
    return max(offs) - min(offs), slack_ms + max(r for _, r in samples.values())


def preflight(cloud, clients, base_cfg, args) -> dict:
    hosts = unique_hosts([*cloud, *clients])
    print(f"preflight on {len(hosts)} host(s)")
    errors, warnings, info = [], [], {}
    for h in hosts:
        try:
            r = h.run("sh -s", input=PREFLIGHT_SCRIPT, timeout=30)
        except RuntimeError as e:
            errors.append(f"[{h.name}] unreachable or failing: {e}")
            continue
        info[h.name] = {"ssh": h.ssh or "local", **parse_preflight(r.stdout)}
    if errors:
        sys.exit("PREFLIGHT FAILED\n  " + "\n  ".join(errors))

    commits = {n: i["commit"] for n, i in info.items()}
    if len(set(commits.values())) != 1:
        errors.append(f"hosts run different git commits: {commits}")
    controller = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip()
    if controller and controller not in commits.values():
        warnings.append(f"the controller is at {controller[:12]}, the hosts at "
                        f"{sorted({c[:12] for c in commits.values()})}")
    needed = CHECKED_BINARIES + (["rm-stub"] if args.stub else [])
    for n, i in info.items():
        e, w = check_build(i, needed, args.allow_dirty)
        errors += [f"[{n}] {x}" for x in e]
        warnings += [f"[{n}] {x}" for x in w]
        for x in foreign_processes(i):
            warnings.append(f"[{n}] an rm- process not started by this runner is running ({x}); it will not be "
                            "cleaned up and may hold the ports: stop it by hand")
    nodes = {int(x["id"]): x for x in base_cfg.get("cluster", {}).get("nodes", [])}
    for i, h in enumerate(cloud, start=1):
        if i in nodes:
            for line in h.busy_ports([nodes[i]["port"]], [nodes[i]["heartbeat_port"]]):
                warnings.append(f"[{h.name}] {line[:160]} (a stray of ours is killed before each run; "
                                "anything else must be stopped)")
    clocks = {}
    for h in hosts:
        try:
            clocks[h.name] = h.clock_sample()
        except RuntimeError as e:
            warnings.append(f"[{h.name}] could not read its clock: {e}")
    spread, allowed = clock_spread(clocks)
    for n, (o, r) in clocks.items():
        info[n]["clock_offset_ms"], info[n]["ssh_rtt_ms"] = round(o, 1), round(r, 1)
    if spread > allowed:
        msg = (f"host clocks disagree by {spread:.0f} ms (allowed {allowed:.0f} ms: 50 ms plus the ssh round trip): "
               + ", ".join(f"{n} {o:+.0f} ms" for n, (o, _) in clocks.items())
               + "; fix chrony, or pass --allow-clock-skew")
        (warnings if args.allow_clock_skew else errors).append(msg)
    hashes = {b: {n: i.get(f"sha256_{b}", "missing")[:12] for n, i in info.items()} for b in needed}
    if any(len(set(v.values())) > 1 for v in hashes.values()):
        print("  note: binary hashes differ between hosts (expected across users, OSes or toolchains; "
              "they are recorded in meta.json for information)")
    errors += check_cluster_hosts(cloud, base_cfg, args.local)
    for n, i in info.items():
        c = i["chrony"]
        if not c["present"]:
            ts = i.get("timesyncd")
            warnings.append(f"[{n}] CHRONY IS NOT INSTALLED (timesyncd synchronized: {ts or 'unknown'}); "
                            "cross-machine timelines may be off")
        elif not c["synchronized"]:
            warnings.append(f"[{n}] CHRONY IS NOT SYNCHRONIZED (leap status {c.get('leap_status')}, "
                            f"reference {c.get('reference')})")
    for w in warnings:
        print("!" * 78 + f"\n!! WARNING: {w}\n" + "!" * 78)
    if errors:
        sys.exit("PREFLIGHT FAILED, refusing to run:\n  " + "\n  ".join(errors))
    for n, i in info.items():
        print(f"  {n}: {i['commit'][:12]} {i['os']}, chrony "
              f"{'synced' if i['chrony']['synchronized'] else 'NOT synced'}")
    return {"controller_commit": controller, "hosts": info, "warnings": warnings,
            "clock_spread_ms": round(spread, 1)}


# ------------------------------------------------------------------ sweep spec


VALID_TARGETS = {"1", "2", "3", "coordinator", "rank1"}
VALID_KNOB_NODES = {"1", "2", "3", "coordinator"}
LOADGEN_FLAGS = ["processes", "users", "mode", "rate", "think_ms", "mix", "max_inflight"]


def scheduled(sweep: dict, spec: dict, key: str, dict_needs_enabled: bool) -> list[dict]:
    """One kind of scheduled action of a run, as a list sorted by `at_s`.

    A single `[key]` table in the sweep is merged with the run's `key = {...}`
    (the old form). A `[[key]]` list, in the sweep or in a run, lets one run do
    the action several times; a run's own list replaces the sweep's, and
    `key = []` in a run turns it off. List entries are on unless
    `enabled = false`; a single `[failure]` table still needs `enabled = true`.
    """
    sv, rv = sweep.get(key), spec.get(key)
    if isinstance(sv, dict) and isinstance(rv, dict):
        items, single = [{**sv, **rv}], True
    else:
        v = rv if rv is not None else sv
        if v is None:
            return []
        single = isinstance(v, dict)
        items = [v] if single else list(v)
    default = not (single and dict_needs_enabled)
    return sorted((i for i in items if i.get("enabled", default)), key=lambda a: a["at_s"])


def scheduled_actions(sweep: dict, spec: dict) -> tuple[list[dict], list[dict]]:
    """(failures, load-knob actions) of one run."""
    return (scheduled(sweep, spec, "failure", dict_needs_enabled=True),
            scheduled(sweep, spec, "load_knob", dict_needs_enabled=False))


# ------------------------------------------------------------------ one run


def parse_status(text: str) -> dict[int, dict]:
    st = {}
    for line in text.splitlines():
        line = line.strip()
        if line.startswith("{"):
            try:
                o = json.loads(line)
            except json.JSONDecodeError:
                continue
            if "node" in o:
                st[int(o["node"])] = o
    return st


def cluster_ready(st: dict[int, dict]) -> tuple[bool, str]:
    if sorted(st) != [1, 2, 3]:
        return False, f"status from nodes {sorted(st)}"
    errs = {n: s["error"] for n, s in st.items() if "error" in s}
    if errs:
        return False, f"unreachable: {errs}"
    if any(s.get("status") != "Active" for s in st.values()):
        return False, "not all Active: " + ", ".join(f"n{n}={s.get('status')}" for n, s in st.items())
    views = {s.get("view") for s in st.values()}
    if len(views) != 1:
        return False, f"views differ: {views}"
    if any(sorted(s.get("active") or []) != [1, 2, 3] for s in st.values()):
        return False, "view does not hold all three nodes: " + str({n: s.get("active") for n, s in st.items()})
    coords = {s.get("coordinator") for s in st.values()}
    if len(coords) != 1 or None in coords:
        return False, f"no agreed coordinator: {coords}"
    sus = {n: s.get("suspected") for n, s in st.items() if s.get("suspected")}
    if sus:
        return False, "suspicions: " + ", ".join(f"n{n} suspects {v}" for n, v in sus.items())
    return True, f"view {views.pop()}, coordinator {coords.pop()}"


def wait_ready(poll, timeout_s: float, stable_s: float = 3.0, interval_s: float = 0.5, min_polls: int = 4,
               check=None, clock=time.time, sleep=time.sleep) -> str:
    """Polls until the cluster is ready (cluster_ready) and has stayed ready,
    with the same view and coordinator, for stable_s seconds and at least
    min_polls polls. One good instant is not enough: a blocked heartbeat
    path (UDP) shows only as suspicions and view changes a moment later.
    Raises RunFailed on timeout."""
    deadline = clock() + timeout_s
    since, sig, n_ok, why = None, None, 0, ""
    while True:
        if check:
            check()
        st = poll()
        ok, why = cluster_ready(st)
        cur = (st[1].get("view"), st[1].get("coordinator")) if ok else None
        if ok and cur == sig:
            n_ok += 1
        elif ok:
            since, sig, n_ok = clock(), cur, 1
        else:
            since, sig, n_ok = None, None, 0
        if ok and n_ok >= min_polls and clock() - since >= stable_s:
            return f"{why}, stable for {clock() - since:.1f} s"
        if clock() > deadline:
            raise RunFailed(f"cluster not ready (and stable for {stable_s:g} s) after {timeout_s:.0f} s: {why}")
        sleep(interval_s)


def missing_logs(run_dir: Path, stub: bool, n_clients: int, procs: int) -> list[str]:
    """Event logs a complete run must have; an empty file counts as missing."""
    present = {p.name for p in run_dir.iterdir() if p.is_file() and p.stat().st_size > 0}
    missing = []
    if stub:
        if not any(n.startswith("cloud-stub") and n.endswith(".jsonl") for n in present):
            missing.append("cloud-stub*.jsonl")
    else:
        missing += [f"cloud{i}-node.jsonl" for i in (1, 2, 3) if f"cloud{i}-node.jsonl" not in present]
    for i in range(1, n_clients + 1):
        want = [f"client{i}-frontend.jsonl", f"client{i}-summary.json"] + [f"client{i}-{p}.jsonl" for p in range(procs)]
        missing += [w for w in want if w not in present]
    return missing


def check_nodes(node_pids, when: str):
    """Raises RunFailed if a node process is gone, or its machine cannot be
    asked (after retries)."""
    states = [(n, h, h.alive(p)) for h, n, p in node_pids]
    dead = [n for n, _, a in states if a is False]
    if dead:
        raise RunFailed(f"{dead} not running {when}")
    unknown = [f"{n} on {h.name}" for n, h, a in states if a is None]
    if unknown:
        raise RunFailed(f"cannot check {unknown} {when}: ssh failed 3 times in a row")


class Ctx:
    """Shared state of one run: abort flag, failure reasons, the clock map."""

    def __init__(self):
        self.abort = threading.Event()
        self.reasons: list[str] = []
        self.lock = threading.Lock()

    def fail(self, why: str):
        with self.lock:
            self.reasons.append(why)
        print(f"  RUN FAILING: {why}")
        self.abort.set()


def park_failed(out: Path, run_dir: Path, reasons: list[str], actions_log: list | None = None) -> Path:
    """Moves a failed run out of the way (to <out>/_failed/), so neither a
    resumed sweep nor the analysis mistakes it for a finished run."""
    failed_dir = out / "_failed" / f"{run_dir.name}-{dt.datetime.now():%Y%m%d-%H%M%S}"
    failed_dir.parent.mkdir(parents=True, exist_ok=True)
    if run_dir.exists():
        (run_dir / "failure.txt").write_text("\n".join(reasons) + "\n")
        if actions_log is not None:
            (run_dir / "actions.json").write_text(json.dumps(actions_log, indent=2))
        shutil.move(str(run_dir), failed_dir)
    return failed_dir


def plan_run(sweep: dict, spec: dict, seed: int, base_cfg: dict, n_clients: int) -> dict:
    """Everything that defines a run, computed without running it."""
    run_id = f"{sweep['name']}-{spec['label']}-s{seed}"
    # run_id goes into remote shell commands (including rm -rf), so it must be
    # a plain name: no spaces, globs, slashes or shell syntax.
    if not SAFE_NAME.fullmatch(run_id):
        raise SystemExit(f"unsafe run id {run_id!r}: sweep name and labels may use only letters, digits, '.', '_' and '-'")
    cfg_over = {**sweep.get("config", {}), **spec.get("config", {})}
    overrides = {"run.run_id": run_id, "run.seed": seed, "run.log_dir": "logs", **cfg_over}
    cfg_text = dump_toml(apply_overrides(base_cfg, overrides))
    lg = {**sweep.get("loadgen", {}), **spec.get("loadgen", {})}
    if "rate" in lg:  # the sweep states the total rate; each machine offers its share
        lg["rate"] = lg["rate"] / n_clients
    duration, warmup, drain = (int(spec.get(k, sweep.get(k))) for k in ("duration_secs", "warmup_secs", "drain_secs"))
    fails, knobs = scheduled_actions(sweep, spec)
    for a in [*fails, *knobs]:
        if not 0 <= a["at_s"] <= duration:
            raise SystemExit(f"{run_id}: scheduled action at_s={a['at_s']} is outside the {duration} s window")
    for f in fails:
        if str(f.get("target", "rank1")) not in VALID_TARGETS:
            raise SystemExit(f"{run_id}: failure target {f.get('target')!r} must be 1, 2, 3, coordinator or rank1")
        if not 1 <= int(f.get("duration_ms", 0)) <= 20000:
            raise SystemExit(f"{run_id}: failure duration_ms must be 1..20000 (spec p. 3), not {f.get('duration_ms')}")
    for k in knobs:
        if str(k.get("node", 1)) not in VALID_KNOB_NODES:
            raise SystemExit(f"{run_id}: load_knob node {k.get('node')!r} must be 1, 2, 3 or coordinator")
        th = k.get("threads", 8)
        if not (th == "cores" or (isinstance(th, int) and not isinstance(th, bool) and th >= 0)):
            raise SystemExit(f"{run_id}: load_knob threads must be a count or \"cores\", not {th!r}")
        if not 1 <= int(k.get("duty", 900)) <= 1000:
            raise SystemExit(f"{run_id}: load_knob duty must be 1..1000 permille, not {k.get('duty')}")
    for k in knobs:
        off = k.get("off_at_s")
        if off is not None and not (k["at_s"] < off <= duration):
            raise SystemExit(f"{run_id}: load_knob off_at_s={off} must be after at_s={k['at_s']} "
                             f"and within the {duration} s window")
    return {"run_id": run_id, "cfg_over": cfg_over, "cfg_text": cfg_text, "lg": lg, "duration": duration,
            "warmup": warmup, "drain": drain, "fails": fails, "knobs": knobs}


def _h(x) -> str:
    return hashlib.sha256(json.dumps(x, sort_keys=True, default=str).encode()).hexdigest()[:16]


def fingerprint(plan: dict, commit: str, stub: bool, n_clients: int, build: str = "") -> dict:
    """What a finished run was made with. A resumed sweep skips a run only if
    this matches, so changing the config, the load, the actions, the commit
    or the build (binary hashes, and uncommitted changes when the tree is
    dirty) cannot silently reuse old results."""
    parts = {"config": _h(plan["cfg_text"]),
             "loadgen": _h([plan["lg"], plan["duration"], plan["warmup"], plan["drain"], n_clients, stub]),
             "actions": _h([plan["fails"], plan["knobs"]]),
             "commit": commit, "build": build}
    return {**parts, "all": _h(parts)}


FP_PARTS = ("config", "loadgen", "actions", "commit", "build")


def fingerprint_diff(meta: dict, fp: dict) -> list[str]:
    old = meta.get("fingerprint")
    if not old:
        return ["made by an older runner (no fingerprint)"]
    return [k for k in FP_PARTS if old.get(k) != fp[k]]


def build_identity(pre: dict | None, stub: bool) -> str:
    """The binaries' sha256 on every host and, for a dirty tree, a hash of
    `git diff HEAD`, from the preflight; with --skip-preflight, this
    machine's own (exact for --local)."""
    names = CHECKED_BINARIES + (["rm-stub"] if stub else [])
    hosts = (pre or {}).get("hosts") or {}
    if hosts:
        return _h({n: [i.get(f"sha256_{b}") for b in names] + [i.get("diff_sha")] for n, i in sorted(hosts.items())})
    return _h(local_build(names))


def local_build(names) -> dict:
    out = {}
    for b in names:
        f = Path("target/release") / b
        out[b] = hashlib.sha256(f.read_bytes()).hexdigest() if f.exists() else "missing"
    dirty = subprocess.run(["git", "status", "--porcelain", "--untracked-files=no"], capture_output=True, text=True)
    if dirty.stdout.strip():
        diff = subprocess.run(["git", "diff", "HEAD"], capture_output=True).stdout
        out["diff"] = hashlib.sha256(diff).hexdigest()
    return out


def sweep_commit(pre: dict | None) -> str:
    hosts = (pre or {}).get("hosts") or {}
    commits = {i.get("commit") for i in hosts.values()} - {None}
    if len(commits) == 1:
        return commits.pop()
    return subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip()


def run_one(sweep: dict, spec: dict, seed: int, cloud, clients, base_cfg: dict, args, out: Path,
            pre: dict | None) -> str:
    """Returns "done" or "skipped"; raises RunFailed. An interrupted run is
    parked in _failed/ too."""
    run_dir = out / f"{sweep['name']}-{spec['label']}-s{seed}"
    try:
        return _run_one(sweep, spec, seed, cloud, clients, base_cfg, args, out, pre)
    except (KeyboardInterrupt, SystemExit):
        if run_dir.exists() and not (run_dir / "meta.json").exists():
            park_failed(out, run_dir, ["interrupted"])
        raise


def _run_one(sweep: dict, spec: dict, seed: int, cloud, clients, base_cfg: dict, args, out: Path,
             pre: dict | None) -> str:
    n_clients = len(clients)
    plan = plan_run(sweep, spec, seed, base_cfg, n_clients)
    run_id, cfg_over, cfg_text, lg = plan["run_id"], plan["cfg_over"], plan["cfg_text"], plan["lg"]
    duration, warmup, drain, fails, knobs = (plan[k] for k in ("duration", "warmup", "drain", "fails", "knobs"))
    fp = fingerprint(plan, sweep_commit(pre), bool(args.stub), n_clients, build_identity(pre, bool(args.stub)))
    run_dir = out / run_id
    if (run_dir / "meta.json").exists() and not args.force:
        diff = fingerprint_diff(json.loads((run_dir / "meta.json").read_text()), fp)
        if not diff:
            print(f"skip {run_id} (done)")
            return "skipped"
        if not args.rerun_changed:
            raise SystemExit(f"{run_id} exists but was made with a different {', '.join(diff)}; "
                             "pass --rerun-changed to redo such runs, --force to redo all, or use another --out")
        print(f"redo {run_id}: {', '.join(diff)} changed")
    print(f"=== {run_id}  ({dt.datetime.now():%H:%M:%S})")
    shutil.rmtree(run_dir, ignore_errors=True)
    run_dir.mkdir(parents=True)
    (run_dir / "config.toml").write_text(cfg_text)
    remote_cfg = f"runs/{run_id}.toml"
    ctl = f'"$(pwd -P)/target/release/rm-ctl" --config {remote_cfg}'

    all_hosts = [*cloud, *clients]
    ctx = Ctx()
    node_pids: list[tuple[Host, str, str]] = []
    actions_log: list[dict] = []
    clock: dict = {}
    loadgen_rc: dict = {}
    t_run = time.time()
    try:
        cleanup_all(all_hosts, "before the run")
        nodes = {int(x["id"]): x for x in base_cfg.get("cluster", {}).get("nodes", [])}
        for i, h in enumerate(cloud, start=1):
            if args.stub and i > 1:
                break
            ids = [1, 2, 3] if args.stub else [i]
            busy = h.busy_ports([nodes[j]["port"] for j in ids if j in nodes],
                                [nodes[j]["heartbeat_port"] for j in ids if j in nodes])
            if busy:
                raise RunFailed(f"[{h.name}] port still in use after cleanup: {busy[0][:160]}")
        for h in unique_hosts(all_hosts):
            h.run(f"rm -rf logs/{run_id} && mkdir -p logs/{run_id} runs")
            h.put_text(cfg_text, remote_cfg)

        # ---- cloud
        if args.stub:
            a = f"--config {remote_cfg}"
            if fails:
                a += f" --silence-every-s {int(warmup + fails[0]['at_s'])} --silence-max-ms {fails[0]['duration_ms']}"
            node_pids.append((cloud[0], "stub", cloud[0].spawn("rm-stub", a, f"logs/{run_id}/cloud-stub.out")))
        else:
            for i, h in enumerate(cloud, start=1):
                pid = h.spawn("rm-node", f"--config {remote_cfg} --id {i}", f"logs/{run_id}/cloud{i}-node.out")
                node_pids.append((h, f"node{i}", pid))

        ctl_host = clients[0]
        ready_timeout = float(sweep.get("ready_timeout_secs", args.ready_timeout))
        if not args.stub:
            why = wait_ready(lambda: parse_status(ctl_host.run(f"{ctl} status", check=False, timeout=30).stdout),
                             ready_timeout, stable_s=float(sweep.get("ready_stable_secs", 3)),
                             check=lambda: check_nodes(node_pids, f"during start-up (see logs/{run_id}/cloudN-node.out)"))
            print(f"  ready after {time.time() - t_run:.1f} s: {why}")
        check_nodes(node_pids, "after start-up")
        time.sleep(sweep.get("settle_secs", 3))

        # ---- clocks: start time and action times in clients[0]'s clock
        c0_ms, ctrl_ms = ctl_host.now_ms()
        skew_ms = c0_ms - ctrl_ms  # client0 clock minus controller clock
        start_at_ms = c0_ms + int(sweep.get("start_lead_ms", 8000))
        clock = {"clock_host": ctl_host.name, "client0_minus_controller_ms": skew_ms,
                 "start_at_ms": start_at_ms}
        if abs(skew_ms) > 1000:
            print(f"  note: {ctl_host.name}'s clock is {skew_ms} ms from the controller's; using {ctl_host.name}'s")

        def local_time_of(c0_target_ms: float) -> float:
            return (c0_target_ms - skew_ms) / 1000.0

        flags = " ".join(f"--{k.replace('_', '-')} {shlex.quote(str(lg[k]))}" for k in LOADGEN_FLAGS if k in lg)
        lg_args = (f"run --config {remote_cfg} --start-at-ms {start_at_ms} --seed {seed} "
                   f"--duration-secs {duration} --warmup-secs {warmup} --drain-secs {drain} {flags}")
        lg_timeout = (start_at_ms - c0_ms) / 1000 + warmup + duration + drain + args.loadgen_margin

        def go(i: int, h: Host):
            try:
                r = h.run(f'R=$(pwd -P); "$R/target/release/rm-loadgen" {lg_args} --machine {i} '
                          f"> logs/{run_id}/client{i}-loadgen.out 2>&1", check=False, timeout=lg_timeout)
                loadgen_rc[i] = r.returncode
                if r.returncode != 0 and not ctx.abort.is_set():
                    ctx.fail(f"loadgen on client{i} exited {r.returncode} (see client{i}-loadgen.out)")
            except RuntimeError as e:
                loadgen_rc[i] = "timeout"
                ctx.fail(f"loadgen on client{i}: {e}")

        def resolve_node(target) -> str:
            if str(target) in ("1", "2", "3"):
                return str(target)
            if target == "coordinator":
                st = parse_status(ctl_host.run(f"{ctl} status", check=False, timeout=30).stdout)
                co = {s.get("coordinator") for s in st.values() if "error" not in s} - {None}
                if len(co) == 1:
                    return str(co.pop())
            raise RuntimeError(f"cannot resolve load_knob node {target!r}")

        def resolve_threads(a: dict, node: str):
            th = a.get("threads", 8)
            if th == "cores":
                r = cloud[int(node) - 1].run("getconf _NPROCESSORS_ONLN 2>/dev/null || sysctl -n hw.ncpu",
                                             check=False, timeout=30)
                th = int(r.stdout.strip() or 8)
            return int(th)

        def do_action(a: dict) -> dict:
            rec = {"type": a["type"], "at_s": a["at_s"]}
            try:
                if a["type"] == "failure":
                    target = a.get("target", "rank1")
                    rec["target"] = target
                    rec["duration_ms"] = a["duration_ms"]
                    if args.stub:
                        rec["note"] = "rm-stub silences on its own schedule"
                        return rec
                    cmd = f"{ctl} silence {target} --duration-ms {int(a['duration_ms'])}"
                else:
                    node = resolve_node(a.get("node", 1))
                    threads = 0 if a["type"] == "load_knob_off" else resolve_threads(a, node)
                    cmd = f"{ctl} load {node} --threads {threads}"
                    if threads:
                        cmd += f" --duty {int(a.get('duty', 900))}"
                        if a.get("duration_ms"):
                            cmd += f" --duration-ms {int(a['duration_ms'])}"
                    rec.update(node=int(node), threads=threads, duty=a.get("duty", 900),
                               duration_ms=a.get("duration_ms", 0))
                rec["sent_ms_client0"] = int(time.time() * 1000) + skew_ms
                r = ctl_host.run(cmd, check=False, timeout=30)
                rec.update(rc=r.returncode, output=(r.stdout + r.stderr).strip()[-500:])
                m = re.search(r"burning (\d+) threads", rec["output"])
                if m:  # the node may cap the request (e.g. at cores - 1)
                    rec["threads_effective"] = int(m.group(1))
                if r.returncode != 0:
                    ctx.fail(f"rm-ctl {a['type']} at {a['at_s']} s exited {r.returncode}: {rec['output']}")
                else:
                    print(f"  t+{a['at_s']} s {a['type']}: {rec['output'][:120]}")
            except Exception as e:  # noqa: BLE001
                rec["error"] = str(e)
                ctx.fail(f"{a['type']} at {a['at_s']} s: {e}")
            return rec

        actions = [{**f, "type": "failure"} for f in fails]
        for k in knobs:
            actions.append({**k, "type": "load_knob"})
            if k.get("off_at_s") is not None:
                actions.append({**k, "type": "load_knob_off", "at_s": k["off_at_s"]})
        actions.sort(key=lambda a: a["at_s"])

        def scheduler():
            for a in actions:
                when = local_time_of(start_at_ms + (warmup + a["at_s"]) * 1000)
                if ctx.abort.wait(max(0.0, when - time.time())):
                    return
                actions_log.append(do_action(a))

        def monitor():
            # A node that dies mid-run invalidates it; stop the load early.
            while not ctx.abort.wait(5):
                for h, n, p in node_pids:
                    a = h.alive(p)
                    if a is False:
                        ctx.fail(f"{n} (pid {p} on {h.name}) died during the run")
                        return
                    if a is None:
                        ctx.fail(f"cannot reach {h.name} to check {n} (pid {p}): ssh failed 3 times in a row")
                        return

        workers = [threading.Thread(target=go, args=(i, h), daemon=True) for i, h in enumerate(clients, start=1)]
        side = [threading.Thread(target=scheduler, daemon=True), threading.Thread(target=monitor, daemon=True)]
        for t in [*workers, *side]:
            t.start()
        print(f"  load: {lg_args.split('--seed')[0].strip()} ... measured {duration} s, "
              f"{len(actions)} scheduled action(s)")
        while any(t.is_alive() for t in workers):
            if ctx.abort.wait(0.5):
                # Stop the load everywhere at once; the loadgen ssh calls then return.
                cleanup_all(clients, "aborting the run")
                for t in workers:
                    t.join(timeout=30)
                break
        if not ctx.reasons:
            try:
                check_nodes(node_pids, "at the end of the load")
            except RunFailed as e:
                ctx.fail(str(e))
        ctx.abort.set()
        for t in side:
            t.join(timeout=35)
        if len(actions_log) < len(actions) and not ctx.reasons:
            ctx.fail(f"only {len(actions_log)} of {len(actions)} scheduled actions ran")
    except RunFailed as e:
        ctx.fail(str(e))
    except RuntimeError as e:
        ctx.fail(f"remote command failed: {e}")
    finally:
        ctx.abort.set()
        by_host: dict = {}
        for h, _, p in node_pids:
            by_host.setdefault(h.key, (h, []))[1].append(p)
        for h, pids in by_host.values():
            try:
                h.stop(pids, grace_s=args.stop_grace)
            except Exception as e:  # noqa: BLE001
                print(f"  WARNING: [{h.name}] stop failed: {e}")
        cleanup_all(all_hosts, "after the run")
        for h in unique_hosts(all_hosts):
            try:
                h.get_dir(f"logs/{run_id}", run_dir)
            except Exception as e:  # noqa: BLE001
                print(f"  WARNING: [{h.name}] log collection failed: {e}")

    # ---- every log must be there
    procs = int(lg.get("processes", 4))
    missing = missing_logs(run_dir, args.stub, n_clients, procs)
    if missing and not ctx.reasons:
        ctx.reasons.append(f"missing after collection: {missing}")

    if ctx.reasons:
        failed_dir = park_failed(out, run_dir, ctx.reasons, actions_log)
        raise RunFailed("; ".join(ctx.reasons) + f"  (logs kept in {failed_dir})")

    commit = fp["commit"]
    users = int(lg.get("users", 50)) * procs * n_clients
    meta = {"sweep": sweep["name"], "label": spec["label"], "series": spec.get("series", spec["label"]), "seed": seed,
            "x": spec.get("x", float("nan")), "x_name": spec.get("x_name", sweep.get("x_name", "load level")),
            "config_overrides": cfg_over, "base_config": str(args.base_config_used), "loadgen": lg, "mode": lg.get("mode", "closed"),
            "processes_per_machine": procs, "users_per_process": int(lg.get("users", 50)),
            "client_machines": n_clients, "concurrent_users": users,
            "warmup_secs": warmup, "duration_secs": duration, "drain_secs": drain,
            "failure": fails[0] if fails else {}, "failures": fails, "load_knobs": knobs,
            "actions": actions_log, "failure_injections": [a for a in actions_log if a["type"] == "failure"],
            **clock, "git_commit": commit, "stub": bool(args.stub), "local": bool(args.local),
            "loadgen_exit": loadgen_rc, "wall_secs": round(time.time() - t_run, 1),
            "fingerprint": fp, "preflight": pre}
    (run_dir / "meta.json").write_text(json.dumps(meta, indent=2))
    if args.clean_remote:
        clean_remote(unique_hosts(all_hosts), run_id, run_dir)
    print(f"  ok: logs in {run_dir} ({time.time() - t_run:.0f} s)")
    return "done"


def clean_remote(hosts, run_id: str, run_dir: Path):
    """After a verified collection: delete logs/<run_id>/ and
    runs/<run_id>.toml on every host (a full sweep leaves several GB per host).
    Never deletes the directory the logs were collected into."""
    for h in hosts:
        src = (Path(h.repo).expanduser() / "logs" / run_id).resolve() if h.ssh is None else None
        if src is not None and (src == run_dir.resolve() or src in run_dir.resolve().parents):
            continue
        try:
            h.run(f"rm -rf logs/{run_id} runs/{run_id}.toml", check=False, timeout=60)
        except RuntimeError as e:
            print(f"  WARNING: [{h.name}] --clean-remote failed: {e}")


def choose_base_config(sweep: dict, args) -> Path:
    if args.base_config:
        return args.base_config
    p = Path(sweep["base_config"])
    if args.local:
        local_ok = p.exists() and not check_cluster_hosts([], tomllib.loads(p.read_text()), True)
        if not local_ok:
            print(f"--local: using config/local.toml instead of {p} (its nodes are not on this machine)")
            return Path("config/local.toml")
    if not p.exists():
        sys.exit(f"{p} does not exist. For the lab machines: cp config/cluster.example.toml {p} and set the "
                 "node hosts (or pass --base-config)")
    return p


def stale_runs(sweep, specs, seeds, base_cfg, n_clients, args, pre) -> list[tuple[str, list[str]]]:
    """Finished runs (with meta.json) whose fingerprint no longer matches."""
    commit, build, out = sweep_commit(pre), build_identity(pre, bool(args.stub)), []
    for seed in seeds:
        for spec in specs:
            plan = plan_run(sweep, spec, seed, base_cfg, n_clients)
            mf = args.out / plan["run_id"] / "meta.json"
            if mf.exists():
                d = fingerprint_diff(json.loads(mf.read_text()),
                                     fingerprint(plan, commit, bool(args.stub), n_clients, build))
                if d:
                    out.append((plan["run_id"], d))
    return out


def est_run_secs(sweep: dict, spec: dict) -> float:
    g = lambda k, d=0: spec.get(k, sweep.get(k, d))  # noqa: E731
    return g("warmup_secs") + g("duration_secs") + g("drain_secs") + g("settle_secs", 3) + 8 + 25


def _interrupt(*_):
    raise KeyboardInterrupt


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("sweep", type=Path)
    ap.add_argument("--machines", type=Path, default=Path("experiments/machines.toml"))
    ap.add_argument("--base-config", type=Path,
                    help="cluster config to build every run's config from (default: the sweep's base_config; "
                         "with --local, config/local.toml unless the sweep's is already on this machine)")
    ap.add_argument("--out", type=Path, default=Path("results/raw"))
    ap.add_argument("--local", action="store_true", help="run everything on this machine (development)")
    ap.add_argument("--local-clients", type=int, default=1, help="client machines to emulate with --local")
    ap.add_argument("--stub", action="store_true", help="use rm-stub instead of the three rm-node processes")
    ap.add_argument("--force", action="store_true", help="redo runs that already have a meta.json")
    ap.add_argument("--only", help="run only configurations whose label contains this text")
    ap.add_argument("--seeds", help="comma-separated seeds overriding the sweep's, e.g. 1,2")
    ap.add_argument("--dry-run", action="store_true", help="print the plan and write nothing")
    ap.add_argument("--retries", type=int, default=1, help="immediate retries of a failed run (default 1)")
    ap.add_argument("--ready-timeout", type=float, default=45, help="seconds to wait for the cluster to be ready")
    ap.add_argument("--loadgen-margin", type=float, default=60,
                    help="seconds beyond warm-up + duration + drain before a loadgen is killed")
    ap.add_argument("--stop-grace", type=float, default=2.0, help="seconds between TERM and KILL")
    ap.add_argument("--allow-dirty", action="store_true",
                    help="run although a host has uncommitted changes under crates/ or Cargo.* (warns instead)")
    ap.add_argument("--rerun-changed", action="store_true",
                    help="redo finished runs whose config, load, actions or commit changed (default: refuse)")
    ap.add_argument("--allow-cross-platform-binaries", action="store_true", help=argparse.SUPPRESS)
    ap.add_argument("--allow-clock-skew", action="store_true",
                    help="warn instead of refusing when host clocks disagree by more than 50 ms + the ssh round trip")
    ap.add_argument("--clean-remote", action="store_true",
                    help="after a run's logs are collected and verified, delete logs/<run_id>/ and "
                         "runs/<run_id>.toml on every host")
    ap.add_argument("--skip-preflight", action="store_true", help="development only; recorded in meta.json")
    args = ap.parse_args(argv)
    # Ctrl-C and `kill` both end in the cleanup below, even when the shell that
    # started us ignores SIGINT (background jobs).
    signal.signal(signal.SIGINT, signal.default_int_handler)
    signal.signal(signal.SIGTERM, _interrupt)

    sweep = tomllib.loads(args.sweep.read_text())
    base_path = choose_base_config(sweep, args)
    args.base_config_used = base_path
    base_cfg = tomllib.loads(base_path.read_text())
    specs = [s for s in sweep["run"] if not args.only or args.only in s["label"]]
    seeds = [int(s) for s in args.seeds.split(",")] if args.seeds else sweep.get("seeds", [1, 2, 3, 4, 5])
    total = sum(est_run_secs(sweep, s) for s in specs) * len(seeds)
    print(f"{len(specs)} configurations x {len(seeds)} seeds = {len(specs) * len(seeds)} runs, "
          f"about {total / 60:.0f} minutes")
    if args.dry_run:
        for s in specs:
            f, k = scheduled_actions(sweep, s)
            print(" ", s["label"], {**sweep.get("config", {}), **s.get("config", {})}, s.get("loadgen", {}),
                  f"failures={[(a['at_s'], a.get('target'), a.get('duration_ms')) for a in f]}",
                  f"knobs={[(a['at_s'], a.get('node'), a.get('threads')) for a in k]}")
        return 0
    if args.allow_cross_platform_binaries:
        print("note: --allow-cross-platform-binaries is no longer needed (binaries are checked against the commit)")
    cloud, clients = load_hosts(args, base_cfg)
    pre = {"skipped": True} if args.skip_preflight else preflight(cloud, clients, base_cfg, args)
    args.out.mkdir(parents=True, exist_ok=True)
    if not args.force:
        changed = stale_runs(sweep, specs, seeds, base_cfg, len(clients), args, pre)
        if changed and not args.rerun_changed:
            sys.exit("these finished runs were made with different settings or code:\n  "
                     + "\n  ".join(f"{r}: {', '.join(d)}" for r, d in changed)
                     + "\npass --rerun-changed to redo them, --force to redo everything, or use a fresh --out")
    failed: list[str] = []
    try:
        for seed in seeds:  # seeds outermost, so a partial sweep has every configuration
            for spec in specs:
                for attempt in range(args.retries + 1):
                    try:
                        run_one(sweep, spec, seed, cloud, clients, base_cfg, args, args.out, pre)
                        break
                    except RunFailed as e:
                        print(f"  FAILED (attempt {attempt + 1}/{args.retries + 1}): {e}")
                        if attempt == args.retries:
                            failed.append(f"{sweep['name']}-{spec['label']}-s{seed}")
    except KeyboardInterrupt:
        print("\ninterrupted: cleaning up every host")
        cleanup_all([*cloud, *clients], "interrupted")
        return 130
    except BaseException:
        print("\nerror: cleaning up every host")
        cleanup_all([*cloud, *clients], "error")
        raise
    if failed:
        print(f"{len(failed)} run(s) FAILED and have no meta.json; rerun the same command to retry them:")
        for f in failed:
            print("  ", f)
        return 2
    print("sweep complete; analyse with: python analysis/analyze.py", args.out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
