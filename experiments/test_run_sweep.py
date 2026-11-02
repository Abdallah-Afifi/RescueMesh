"""Unit tests for the experiment runner's pure parts (no ssh, no cluster).

Run:  python -m pytest experiments -q
The end-to-end check is a real local sweep: see docs/workstream-e.md.
"""

import sys
import tempfile
import tomllib
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import run_sweep as rs  # noqa: E402


def test_single_failure_table_keeps_its_old_meaning():
    sweep = {"failure": {"enabled": True, "at_s": 20, "target": "rank1", "duration_ms": 10000}}
    f, k = rs.scheduled_actions(sweep, {"failure": {"duration_ms": 5000}})
    assert f == [{"enabled": True, "at_s": 20, "target": "rank1", "duration_ms": 5000}] and k == []
    # A single table without enabled = true is off, as before.
    assert rs.scheduled_actions({"failure": {"at_s": 20, "duration_ms": 1}}, {}) == ([], [])
    # A run can switch it off.
    assert rs.scheduled_actions(sweep, {"failure": {"enabled": False}})[0] == []


def test_lists_of_failures_and_load_knobs():
    sweep = tomllib.loads("""
[[failure]]
at_s = 30
target = "coordinator"
duration_ms = 5000
[[failure]]
at_s = 10
target = "rank1"
duration_ms = 3000
[load_knob]
at_s = 15
node = 2
threads = 8
""")
    f, k = rs.scheduled_actions(sweep, {})
    assert [a["at_s"] for a in f] == [10, 30], "sorted by time"
    assert k == [{"at_s": 15, "node": 2, "threads": 8}], "a single [load_knob] needs no enabled flag"
    f, _ = rs.scheduled_actions(sweep, {"failure": []})
    assert f == [], "a run's empty list switches the sweep's failures off"
    f, _ = rs.scheduled_actions(sweep, {"failure": [{"at_s": 5, "duration_ms": 1, "enabled": False}]})
    assert f == []


def status(n, **kw):
    s = {"node": n, "status": "Active", "view": 3, "active": [1, 2, 3], "coordinator": 2}
    s.update(kw)
    return s


def test_cluster_readiness():
    ok = {n: status(n) for n in (1, 2, 3)}
    assert rs.cluster_ready(ok)[0]
    assert not rs.cluster_ready({1: ok[1], 2: ok[2]})[0]
    assert not rs.cluster_ready({**ok, 3: {"node": 3, "error": "refused"}})[0]
    assert not rs.cluster_ready({**ok, 3: status(3, status="Syncing")})[0]
    assert not rs.cluster_ready({**ok, 3: status(3, view=2)})[0]
    assert not rs.cluster_ready({**ok, 3: status(3, coordinator=1)})[0]
    assert not rs.cluster_ready({n: status(n, active=[1, 2]) for n in (1, 2, 3)})[0]
    assert not rs.cluster_ready({n: status(n, coordinator=None) for n in (1, 2, 3)})[0]


def test_parse_status_skips_noise():
    text = '{"node":1,"status":"Active"}\nwarning: x\n{"node":2,"error":"refused"}\n{broken\n'
    st = rs.parse_status(text)
    assert sorted(st) == [1, 2] and st[2]["error"] == "refused"


CHRONY = """commit=abc
dirty=0
os=Linux 6.8.0 x86_64
sha256_rm-node=11
chrony| Reference ID    : C0A80001 (gw)
chrony| Stratum         : 3
chrony| System time     : 0.000012 seconds fast of NTP time
chrony| Leap status     : Normal
"""


def test_preflight_parsing():
    i = rs.parse_preflight(CHRONY)
    assert i["commit"] == "abc" and i["sha256_rm-node"] == "11"
    assert i["chrony"]["present"] and i["chrony"]["synchronized"]
    i = rs.parse_preflight(CHRONY.replace("Normal", "Not synchronised"))
    assert i["chrony"]["present"] and not i["chrony"]["synchronized"]
    i = rs.parse_preflight("commit=abc\nchrony=absent\ntimesyncd=yes\n")
    assert not i["chrony"]["present"] and i["timesyncd"] == "yes"


def test_cluster_hosts_must_match_machines():
    cfg = {"cluster": {"nodes": [{"id": i, "host": f"10.0.0.1{i}"} for i in (1, 2, 3)]}}
    cloud = [rs.Host(f"cloud{i}", f"user@10.0.0.1{i}", "~/r", []) for i in (1, 2, 3)]
    assert rs.check_cluster_hosts(cloud, cfg, local=False) == []
    cloud[2] = rs.Host("cloud3", "user@10.0.0.99", "~/r", [])
    assert len(rs.check_cluster_hosts(cloud, cfg, local=False)) == 1
    cloud[2] = rs.Host("cloud3", "lab3", "~/r", [], addr="10.0.0.13")  # ssh alias + addr
    assert rs.check_cluster_hosts(cloud, cfg, local=False) == []
    assert len(rs.check_cluster_hosts([], cfg, local=True)) == 3, "--local needs loopback nodes"
    local = {"cluster": {"nodes": [{"id": i, "host": "127.0.0.1"} for i in (1, 2, 3)]}}
    assert rs.check_cluster_hosts([], local, local=True) == []


def test_missing_logs():
    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp)
        for name in ["cloud1-node.jsonl", "cloud2-node.jsonl", "client1-frontend.jsonl", "client1-summary.json",
                     "client1-0.jsonl", "client1-1.jsonl"]:
            (d / name).write_text("{}\n")
        (d / "cloud3-node.jsonl").write_text("")  # empty counts as missing
        assert rs.missing_logs(d, False, 1, 2) == ["cloud3-node.jsonl"]
        assert rs.missing_logs(d, False, 1, 3)[-1] == "client1-2.jsonl"
        assert "client2-frontend.jsonl" in rs.missing_logs(d, False, 2, 2)
        assert rs.missing_logs(d, True, 1, 2) == ["cloud-stub*.jsonl"]


def test_dump_toml_round_trips_a_run_config():
    base = tomllib.loads(Path(__file__).parent.parent.joinpath("config", "local.toml").read_text())
    cfg = rs.apply_overrides(base, {"run.run_id": "x-s1", "policy.ranker": "hash", "inject.enabled": True,
                                    "inject.interval_min_ms": 30000})
    back = tomllib.loads(rs.dump_toml(cfg))
    assert back == cfg
    assert back["inject"] == {"enabled": True, "interval_min_ms": 30000}
    assert len(back["cluster"]["nodes"]) == 3


def test_local_host_copies_files_and_cleans_up_nothing_foreign():
    with tempfile.TemporaryDirectory() as tmp:
        h = rs.Host("local", None, tmp, [])
        h.put_text("a = 1\n", "runs/x.toml")
        assert (Path(tmp) / "runs/x.toml").read_text() == "a = 1\n"
        (Path(tmp) / "logs/r1").mkdir(parents=True)
        (Path(tmp) / "logs/r1/cloud1-node.jsonl").write_text("{}\n")
        out = Path(tmp) / "collected"
        h.get_dir("logs/r1", out)
        assert (out / "cloud1-node.jsonl").exists()
        assert h.cleanup() == 0, "no binaries of this (temporary) checkout are running"


# ---------------------------------------------------------------- review fixes (H2, M4, M5, L2, L3, L4, L6)


def test_build_check_means_built_from_this_commit():
    # H2: hashes may differ between hosts (paths are embedded in release
    # binaries); what matters is a clean source tree and binaries newer than it.
    clean = {"dirty": "0", "dirty_src": "0", "sha256_rm-node": "aa", "stale_rm-node": "0"}
    assert rs.check_build(clean, ["rm-node"], allow_dirty=False) == ([], [])
    e, _ = rs.check_build({**clean, "dirty_src": "2"}, ["rm-node"], allow_dirty=False)
    assert e and "uncommitted" in e[0]
    e, w = rs.check_build({**clean, "dirty_src": "2"}, ["rm-node"], allow_dirty=True)
    assert not e and w
    _, w = rs.check_build({**clean, "dirty": "3"}, ["rm-node"], allow_dirty=False)
    assert w and "outside the source" in w[0]
    e, _ = rs.check_build({**clean, "stale_rm-node": "4", "stale_example_rm-node": "crates/x.rs"}, ["rm-node"], False)
    assert e and "older than 4" in e[0] and "crates/x.rs" in e[0]
    e, _ = rs.check_build({**clean, "sha256_rm-node": "missing"}, ["rm-node"], False)
    assert e and "missing" in e[0]


def test_preflight_script_reports_staleness_in_a_real_checkout():
    # H2, end to end on a throwaway git repository: a binary built before the
    # last source change is stale; touching it afterwards makes it fresh.
    import os
    import subprocess
    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp)
        (d / "crates/a/src").mkdir(parents=True)
        (d / "target/release").mkdir(parents=True)
        (d / "Cargo.toml").write_text("[workspace]\n")
        (d / "crates/a/src/lib.rs").write_text("// v1\n")
        git = ["git", "-c", "user.name=t", "-c", "user.email=t@t", "-C", str(d)]
        subprocess.run([*git, "init", "-q"], check=True)
        subprocess.run([*git, "add", "."], check=True)
        subprocess.run([*git, "commit", "-qm", "x"], check=True)
        binary = d / "target/release/rm-node"
        binary.write_text("old build")
        os.utime(binary, (1_000_000_000, 1_000_000_000))  # built long before the sources
        h = rs.Host("local", None, str(d), [])
        info = rs.parse_preflight(h.run("sh -s", input=rs.PREFLIGHT_SCRIPT).stdout)
        assert info["dirty_src"] == "0" and info["stale_rm-node"] == "2", info
        os.utime(binary, None)  # rebuilt now
        info = rs.parse_preflight(h.run("sh -s", input=rs.PREFLIGHT_SCRIPT).stdout)
        assert info["stale_rm-node"] == "0", info
        (d / "crates/a/src/lib.rs").write_text("// v2, not committed\n")
        info = rs.parse_preflight(h.run("sh -s", input=rs.PREFLIGHT_SCRIPT).stdout)
        assert info["dirty_src"] == "1"
        assert rs.check_build(info, ["rm-node"], allow_dirty=False)[0]


def test_hand_started_processes_are_reported():
    # L2: an rm-node not started from this checkout's absolute path is not
    # cleaned up by the anchored pkill, so the preflight warns about it.
    info = {"repo": "/home/u/RescueMesh",
            "procs": ["101 /home/u/RescueMesh/target/release/rm-node --config runs/x.toml --id 1",
                      "102 target/release/rm-node --config config/cluster.toml --id 2",
                      "103 /home/other/RescueMesh/target/release/rm-loadgen run"]}
    f = rs.foreign_processes(info)
    assert len(f) == 2 and f[0].startswith("pid 102") and f[1].startswith("pid 103")


def test_busy_ports_are_found():
    # L2: a listener on a node's port is reported (ss or lsof).
    import shutil
    import socket
    if not (shutil.which("ss") or shutil.which("lsof")):
        return
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    s.listen()
    port = s.getsockname()[1]
    try:
        with tempfile.TemporaryDirectory() as tmp:
            busy = rs.Host("local", None, tmp, []).busy_ports([port], [])
            assert busy and busy[0].startswith(f"busy tcp {port}"), busy
    finally:
        s.close()


class FakeHost(rs.Host):
    """A host whose remote command is replaced by a local one."""

    def __init__(self, argv):
        super().__init__("fake", "fake-host", "/tmp", [])
        self.argv = argv

    def _wrap(self, cmd):
        return self.argv


def test_an_unreachable_host_is_unknown_not_dead():
    # M4: ssh exit 255 (or a timeout) says nothing about the node.
    import os
    with tempfile.TemporaryDirectory() as tmp:
        local = rs.Host("local", None, tmp, [])
        assert local.alive(str(os.getpid())) is True
        assert local.alive("999999") is False
    assert FakeHost(["sh", "-c", "echo 'ssh: connect: refused' >&2; exit 255"]).alive("1", pause_s=0) is None
    assert FakeHost(["sh", "-c", "echo dead"]).alive("1", pause_s=0) is False
    nodes = [(FakeHost(["sh", "-c", "exit 255"]), "node1", "1")]
    nodes[0][0].alive = lambda p, **k: None
    try:
        rs.check_nodes(nodes, "now")
        assert False, "an unknown node must fail the check"
    except rs.RunFailed as e:
        assert "cannot check" in str(e)


def test_empty_output_fails_the_run_not_the_sweep():
    # L3: spawn and now_ms raise RuntimeError (a run failure), not IndexError.
    for fn in (lambda h: h.spawn("rm-node", "", "x.out"), lambda h: h.now_ms()):
        try:
            fn(FakeHost(["true"]))
            assert False
        except RuntimeError:
            pass


def test_log_copy_that_hangs_is_killed():
    # L6: the ssh side of the tar pipe must not outlive a timed-out copy.
    import subprocess
    import time
    procs = []
    real = subprocess.Popen

    def spy(*a, **k):
        p = real(*a, **k)
        procs.append(p)
        return p

    subprocess.Popen = spy
    try:
        with tempfile.TemporaryDirectory() as tmp:
            t = time.time()
            FakeHost(["sleep", "30"]).get_dir("logs/x", Path(tmp) / "out", timeout=1)
            assert time.time() - t < 10
    finally:
        subprocess.Popen = real
    assert procs and procs[0].poll() is not None


def sweep_for_plans():
    return tomllib.loads("""
name = "t"
seeds = [1]
warmup_secs = 5
duration_secs = 30
drain_secs = 5
[loadgen]
rate = 300
processes = 2
[[run]]
label = "a"
""")


def test_off_at_s_is_validated():
    # L4
    base = tomllib.loads(Path(__file__).parent.parent.joinpath("config", "local.toml").read_text())
    sw = sweep_for_plans()
    for bad in ({"at_s": 10, "off_at_s": 5, "node": 2}, {"at_s": 10, "off_at_s": 31, "node": 2}):
        try:
            rs.plan_run(sw, {"label": "a", "load_knob": [bad]}, 1, base, 3)
            assert False, bad
        except SystemExit as e:
            assert "off_at_s" in str(e)
    rs.plan_run(sw, {"label": "a", "load_knob": [{"at_s": 10, "off_at_s": 20, "node": 2}]}, 1, base, 3)


def test_resume_skips_only_runs_with_the_same_fingerprint():
    # M5: a finished run is reused only if its config, load, actions and
    # commit are the same; otherwise the sweep refuses (or reruns on request).
    import argparse
    import json
    base = tomllib.loads(Path(__file__).parent.parent.joinpath("config", "local.toml").read_text())
    sw = sweep_for_plans()
    spec = sw["run"][0]
    plan = rs.plan_run(sw, spec, 1, base, 3)
    assert plan["lg"]["rate"] == 100  # per machine
    pre = {"hosts": {"cloud1": {"commit": "c1"}}}
    fp = rs.fingerprint(plan, "c1", False, 3, rs.build_identity(pre, False))
    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp)
        (out / plan["run_id"]).mkdir()
        (out / plan["run_id"] / "meta.json").write_text(json.dumps({"fingerprint": fp}))
        args = argparse.Namespace(out=out, stub=False)
        pre = {"hosts": {"cloud1": {"commit": "c1"}}}
        assert rs.stale_runs(sw, [spec], [1], base, 3, args, pre) == []
        assert rs.stale_runs(sw, [spec], [1], base, 3, args, {"hosts": {"cloud1": {"commit": "c2"}}}) == \
            [(plan["run_id"], ["commit"])]
        sw2 = {**sw, "loadgen": {"rate": 600, "processes": 2}}
        assert rs.stale_runs(sw2, [spec], [1], base, 3, args, pre) == [(plan["run_id"], ["loadgen"])]
        sw3 = {**sw, "config": {"policy.ranker": "hash"}}
        assert rs.stale_runs(sw3, [spec], [1], base, 3, args, pre) == [(plan["run_id"], ["config"])]
        spec4 = {**spec, "failure": [{"at_s": 5, "duration_ms": 1000}]}
        assert rs.stale_runs(sw, [spec4], [1], base, 3, args, pre) == [(plan["run_id"], ["actions"])]
        (out / plan["run_id"] / "meta.json").write_text(json.dumps({"label": "a"}))
        assert "older runner" in rs.stale_runs(sw, [spec], [1], base, 3, args, pre)[0][1][0]


def test_smoke_sweep_never_lands_on_a_report_curve():
    # M1
    sw = tomllib.loads(Path(__file__).parent.joinpath("sweeps", "smoke.toml").read_text())
    report = {r.get("series") for f in ("latency", "clients", "failure", "imbalance", "autoinject")
              for r in tomllib.loads(Path(__file__).parent.joinpath("sweeps", f"{f}.toml").read_text())["run"]}
    for r in sw["run"]:
        assert r.get("series") not in report and "smoke" in r.get("series", "")
        assert "not for the report" in r.get("x_name", sw.get("x_name", ""))




# ---------------------------------------------------------------- third review (R1, R2, R3, R5, R6)


def _git_repo(d: Path):
    import subprocess
    (d / "crates/a/src").mkdir(parents=True)
    (d / "crates/a/tests").mkdir(parents=True)
    (d / "target/release").mkdir(parents=True)
    (d / "Cargo.toml").write_text("[workspace]\n")
    (d / "Cargo.lock").write_text("# lock\n")
    (d / "crates/a/src/main.rs").write_text("// v1\n")
    (d / "crates/a/tests/t.rs").write_text("// test v1\n")
    git = ["git", "-c", "user.name=t", "-c", "user.email=t@t", "-C", str(d)]
    subprocess.run([*git, "init", "-q"], check=True)
    subprocess.run([*git, "add", "."], check=True)
    subprocess.run([*git, "commit", "-qm", "x"], check=True)


def test_staleness_uses_the_binarys_own_sources():
    # R1: after `cargo build`, a change to a test file (not one of rm-node's
    # sources) must not make rm-node stale forever; a change to its source must.
    import os
    import time
    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp)
        _git_repo(d)
        b = d / "target/release/rm-node"
        b.write_text("binary")
        (d / "target/release/rm-node.d").write_text(f"{b}: {d}/crates/a/src/main.rs\n")
        past = time.time() - 100
        for f in (d / "crates/a/src/main.rs", d / "Cargo.lock"):
            os.utime(f, (past, past))
        h = rs.Host("local", None, str(d), [])

        def info():
            return rs.parse_preflight(h.run("sh -s", input=rs.PREFLIGHT_SCRIPT).stdout)

        os.utime(d / "crates/a/tests/t.rs", None)  # a test changed after the build
        i = info()
        assert i["stale_basis_rm-node"] == "dep-info" and i["stale_rm-node"] == "0", i
        os.utime(d / "crates/a/src/main.rs", (time.time() + 5, time.time() + 5))  # its source changed
        i = info()
        assert i["stale_rm-node"] == "1" and i["stale_example_rm-node"].endswith("main.rs"), i
        (d / "target/release/rm-node.d").unlink()  # no dep-info: every source counts, with a hint
        i = info()
        assert i["stale_basis_rm-node"] == "all-sources"
        e, _ = rs.check_build(i, ["rm-node"], allow_dirty=False)
        assert e and "touch target/release/rm-*" in e[0]


def _status(view=3, coord=2, suspected=()):
    return {n: {"node": n, "status": "Active", "view": view, "active": [1, 2, 3], "coordinator": coord,
                "suspected": list(suspected) if n == 1 else []} for n in (1, 2, 3)}


class FakeClock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t

    def sleep(self, dt):
        self.t += dt


def test_readiness_must_be_stable_and_unsuspected():
    # R2: one good poll is not enough; a suspicion or a view change resets it.
    c = FakeClock()
    seq = iter([_status(), _status(suspected=[3]), _status(), _status(view=4), *[_status(view=4)] * 20])
    why = rs.wait_ready(lambda: next(seq), timeout_s=30, stable_s=3, interval_s=0.5, clock=c, sleep=c.sleep)
    assert "stable" in why and "view 4" in why
    assert c.t >= 1.5 + 3, c.t  # the clock restarted at the view change (third sleep)
    assert not rs.cluster_ready(_status(suspected=[3]))[0]
    c = FakeClock()
    flapping = iter([_status(view=v) for v in range(100)])  # a new view every poll: never stable
    try:
        rs.wait_ready(lambda: next(flapping), timeout_s=10, clock=c, sleep=c.sleep)
        assert False
    except rs.RunFailed as e:
        assert "stable" in str(e)


def test_clock_spread_allows_the_round_trip():
    # R3
    spread, allowed = rs.clock_spread({"a": (0.0, 4.0), "b": (30.0, 6.0), "c": (-10.0, 2.0)})
    assert spread == 40.0 and allowed == 56.0
    spread, allowed = rs.clock_spread({"a": (0.0, 4.0), "b": (120.0, 6.0)})
    assert spread > allowed
    with tempfile.TemporaryDirectory() as tmp:
        off, rtt = rs.Host("local", None, tmp, []).clock_sample()
        assert abs(off) < 1000 + rtt and rtt >= 0


def test_a_rebuild_changes_the_fingerprint():
    # R5: same commit, different binaries (or different uncommitted changes)
    # must not reuse old runs, even with --allow-dirty or --skip-preflight.
    base = tomllib.loads(Path(__file__).parent.parent.joinpath("config", "local.toml").read_text())
    sw = sweep_for_plans()
    plan = rs.plan_run(sw, sw["run"][0], 1, base, 3)
    host = {"commit": "c1", "sha256_rm-node": "aa", "sha256_rm-loadgen": "bb", "sha256_rm-ctl": "cc",
            "diff_sha": "clean"}
    b1 = rs.build_identity({"hosts": {"cloud1": host}}, False)
    b2 = rs.build_identity({"hosts": {"cloud1": {**host, "sha256_rm-node": "zz"}}}, False)
    b3 = rs.build_identity({"hosts": {"cloud1": {**host, "diff_sha": "0123"}}}, False)
    assert len({b1, b2, b3}) == 3
    old = {"fingerprint": rs.fingerprint(plan, "c1", False, 3, b1)}
    assert rs.fingerprint_diff(old, rs.fingerprint(plan, "c1", False, 3, b1)) == []
    assert rs.fingerprint_diff(old, rs.fingerprint(plan, "c1", False, 3, b2)) == ["build"]
    assert rs.build_identity({"skipped": True}, False) == rs.build_identity(None, False)  # this machine's


def test_targets_and_knob_settings_are_validated_up_front():
    # R6
    base = tomllib.loads(Path(__file__).parent.parent.joinpath("config", "local.toml").read_text())
    sw = sweep_for_plans()
    bad = [{"failure": [{"at_s": 5, "target": "leader", "duration_ms": 1000}]},
           {"failure": [{"at_s": 5, "target": "rank1", "duration_ms": 25000}]},
           {"load_knob": [{"at_s": 5, "node": "rank1"}]},
           {"load_knob": [{"at_s": 5, "node": 2, "threads": "lots"}]},
           {"load_knob": [{"at_s": 5, "node": 2, "duty": 1500}]}]
    for extra in bad:
        try:
            rs.plan_run(sw, {"label": "a", **extra}, 1, base, 3)
            assert False, extra
        except SystemExit:
            pass
    rs.plan_run(sw, {"label": "a", "failure": [{"at_s": 5, "target": 2, "duration_ms": 1000}],
                     "load_knob": [{"at_s": 5, "node": "coordinator", "threads": "cores"}]}, 1, base, 3)
# ---------------------------------------------------------------- demo helper (M7, M8, L1)


def test_demo_finds_the_silenced_node_and_tolerates_a_named_target():
    # M8: with `--silence rank1` the victim is whoever reports silenced, and
    # `watch --mark rank1` (or "none") must not crash the demo.
    import demo_helper as dh
    st = {1: {"node": 1, "silenced": False}, 2: {"node": 2, "silenced": True}, 3: {"node": 3, "error": "x"}}
    assert dh.silenced_node(st) == 2
    assert dh.silenced_node({1: {"node": 1, "silenced": False}}) is None
    assert dh.mark_arg("3") == 3 and dh.mark_arg("rank1") is None and dh.mark_arg("none") is None


def test_demo_base_load_runs_until_stopped():
    # M7: the base load has no fixed length, so it cannot run out mid-talk;
    # L1: the six-machine mode does not require binaries on the controller.
    import subprocess
    import sys
    here = Path(__file__).parent
    h = subprocess.run([sys.executable, str(here / "demo_helper.py"), "--local", "load", "--help"],
                       capture_output=True, text=True)
    assert "0 = until stopped" in h.stdout
    demo = (here / "demo.sh").read_text()
    assert "--secs 900" not in demo and "--secs 300" not in demo
    assert 'load --rate "$RATE"   # until the demo stops it' in demo
    assert "if [ $LOCAL = 1 ]; then\n  for b in rm-node rm-loadgen rm-ctl" in demo


def test_demo_recovery_shows_a_short_syncing_phase(monkeypatch, capsys):
    # D1: polled fast enough, the watcher reports SILENCED -> SYNCING -> ACTIVE
    # with the catch-up, then the first plans after ACTIVE.
    import argparse
    import demo_helper as dh

    def st(state, applied, peer, plans, silenced=False):
        return {1: {"node": 1, "status": state, "silenced": silenced, "applied": applied, "commit": applied,
                    "view": 3, "active": [1, 2, 3], "plans": plans},
                2: {"node": 2, "status": "Active", "commit": peer}, 3: {"node": 3, "status": "Active", "commit": peer}}

    seq = iter([st("Active", 100, 100, 5, silenced=True), st("Active", 100, 300, 5, silenced=True),
                st("Syncing", 150, 320, 5), st("Active", 330, 331, 5), st("Active", 340, 341, 9)])

    class FakeCtl:
        def status(self):
            return next(seq)

    monkeypatch.setattr(dh, "ctx", lambda args: (None, None, None, FakeCtl()))
    monkeypatch.setattr(dh.time, "sleep", lambda s: None)
    rc = dh.cmd_recovery(argparse.Namespace(node=1, secs=30, poll=0.1))
    out = capsys.readouterr().out
    assert rc == 0
    assert "SYNCING" in out and "applied 180 entries" in out, out
    assert "is ACTIVE in view 3" in out and "planning again (4 plans" in out, out


def test_every_sweep_plans_and_keeps_weights_valid():
    # S1, S2, R6: every run of every sweep file plans without error, and any
    # score weights sum to 1000 (Config::validate), counting the defaults.
    base = tomllib.loads(Path(__file__).parent.parent.joinpath("config", "local.toml").read_text())
    defaults = {"w_queue": 250, "w_cpu": 250, "w_service": 250, "w_missed": 0, "w_rtt": 250}
    names = set()
    for f in sorted(Path(__file__).parent.joinpath("sweeps").glob("*.toml")):
        sw = tomllib.loads(f.read_text())
        names.add(sw["name"])
        for spec in sw["run"]:
            plan = rs.plan_run(sw, spec, 1, base, 3)
            score = {**defaults, **tomllib.loads(plan["cfg_text"]).get("score", {})}
            assert sum(v for k, v in score.items() if k.startswith("w_")) == 1000, (f.name, spec["label"], score)
    assert {"planner", "weights"} <= names
    xs = {}
    for f in sorted(Path(__file__).parent.joinpath("sweeps").glob("*.toml")):
        sw = tomllib.loads(f.read_text())
        xs.setdefault(sw.get("x_name"), []).append(f.name)
    assert all(len(v) == 1 for v in xs.values()), xs  # S3: every sweep has its own x_name
