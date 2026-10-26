"""Checks the analysis against synthetic logs whose answers are known.

Run:  python -m pytest analysis -q      (or: python analysis/test_analyze.py)
"""

import json
import sys
import tempfile
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).parent))
import analyze  # noqa: E402

T0 = 1_800_000_000 * 10**9  # arbitrary wall-clock origin, ns


def ev(t, kind, machine="client1", node=None, **f):
    d = {"ts_ns": int(T0 + t * 1e9), "mono_ns": 0, "run_id": "x", "machine": machine, "kind": kind}
    if node is not None:
        d["node"] = node
    d.update(f)
    return d


def synthetic_run(root: Path, name="synthetic-s1", client_offset_s=0.0):
    """60 s at 500 ops/s, 5 ms latency. Node 1 is silenced from 30 s for 5 s:
    for the first 2 s of it, replies take 300 ms. The view change is at +0.6 s
    and the node is active again 0.4 s after the silence ends."""
    rng = np.random.default_rng(1)
    rows = []
    for i in range(30_000):
        t = 5 + i * 0.002  # warm-up until t=5 is not marked, measure from 10
        phase = "warmup" if t < 10 else "measure"
        lat = 5_000 + int(rng.integers(0, 1500))
        if 30 <= t < 32:
            lat = 300_000
        rows.append(ev(t + client_offset_s, "client_send", req=f"r{i}", attempt=0, phase=phase))
        rows.append(ev(t + lat / 1e6 + client_offset_s, "client_reply", req=f"r{i}", attempt=int(lat > 100_000),
                       outcome="ok", latency_us=lat, phase=phase, **{"from": 2}))
        if lat > 100_000:
            rows.append(ev(t + 0.15 + client_offset_s, "client_retry", req=f"r{i}", attempt=1, phase=phase))
    rows.append(ev(0, "clock_offset", offset_ns=int(client_offset_s * 1e9), when="start"))
    rows.append(ev(30, "inject_start", machine="cloud1", node=1, k=1, victim=1, duration_ms=5000))
    rows.append(ev(30.6, "view_change", machine="cloud2", node=2, view=2, active=[2, 3]))
    rows.append(ev(35, "inject_end", machine="cloud1", node=1, k=1))
    rows.append(ev(35.4, "active", machine="cloud1", node=1, applied=100, view=3))
    for i in range(3000):
        rows.append(ev(10 + i * 0.01, "plan_end", machine=f"cloud{i % 3 + 1}", node=i % 3 + 1, service_us=1000))
    d = root / name
    d.mkdir(parents=True)
    (d / "client1-0.jsonl").write_text("\n".join(json.dumps(r) for r in rows) + "\n")
    return d


def test_latency_throughput_and_warmup_exclusion():
    with tempfile.TemporaryDirectory() as tmp:
        synthetic_run(Path(tmp))
        run = analyze.load_runs(Path(tmp))[0]
        r = analyze.run_latency(run)
        assert 480 < r["throughput"] < 520, r
        assert r["p50_ms"] < 8
        assert r["p99_ms"] > 100, "the stalled replies are in the tail"
        assert r["n_ok"] < 30_000, "warm-up requests are excluded"
        assert r["retried_frac"] > 0


def test_failure_metrics_match_the_construction():
    with tempfile.TemporaryDirectory() as tmp:
        synthetic_run(Path(tmp))
        f = analyze.failures(analyze.load_runs(Path(tmp))[0])
        assert len(f) == 1
        f = f[0]
        assert abs(f["view_removal_s"] - 0.6) < 0.01
        assert abs(f["recovery_s"] - 0.4) < 0.01
        # Slow replies *complete* between 30.3 s and 32.3 s, so the p99 is back in
        # band after about 2.3 s; the 0.5 s windows make it 2.0 to 3.0.
        assert 2.0 <= f["failover_s"] <= 3.0, f
        assert f["peak_p99_ms"] > 250
        assert f["retries_during"] > 100


def test_clock_offsets_are_corrected():
    with tempfile.TemporaryDirectory() as tmp:
        synthetic_run(Path(tmp), client_offset_s=2.0)  # client clock 2 s fast
        f = analyze.failures(analyze.load_runs(Path(tmp))[0])[0]
        # Without correction the client replies would look 2 s late and the
        # baseline window would be empty; with it the result is unchanged.
        assert 2.0 <= f["failover_s"] <= 3.0, f


def test_fairness_and_jain_index():
    with tempfile.TemporaryDirectory() as tmp:
        synthetic_run(Path(tmp))
        fr = analyze.fairness(analyze.load_runs(Path(tmp))[0])
        assert fr["jain_busy"] > 0.99
    assert analyze.jain([1, 0, 0]) == 1 / 3
    assert analyze.jain([5, 5, 5]) == 1.0


def test_confidence_interval():
    mean, half, n = analyze.ci95([10.0, 12.0, 11.0, 13.0, 9.0])
    assert n == 5 and abs(mean - 11) < 1e-9
    assert abs(half - 2.776 * np.std([10, 12, 11, 13, 9], ddof=1) / np.sqrt(5)) < 1e-3
    assert np.isnan(analyze.ci95([4.0])[1])


def test_timeline_handles_durations_that_are_not_window_multiples():
    with tempfile.TemporaryDirectory() as tmp:
        d = synthetic_run(Path(tmp))
        lines = [json.loads(x) for x in (d / "client1-0.jsonl").read_text().splitlines()]
        for r in lines:
            if r["kind"] == "inject_start":
                r["duration_ms"] = 2869  # 10 + 2.869 + 30 is not a multiple of 0.25
        (d / "client1-0.jsonl").write_text("\n".join(json.dumps(r) for r in lines) + "\n")
        analyze.plot_timeline(analyze.load_runs(Path(tmp))[0], Path(tmp))


def test_end_to_end_cli_writes_outputs():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp) / "runs"
        for s in (1, 2, 3):
            synthetic_run(root, f"cfg-s{s}")
        out = Path(tmp) / "out"
        assert analyze.main([str(root), "--out", str(out)]) == 0
        assert (out / "summary.md").exists()
        assert (out / "tables" / "aggregate.csv").exists()
        assert (out / "figures" / "load_curves.png").exists()
        assert any((out / "figures").glob("timeline_*.png"))


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            print("ok", name)
