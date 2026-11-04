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


def synthetic_run(root: Path, name="synthetic-s1", client_offset_s=0.0, spike_delay_s=0.0, spike=True):
    """60 s at 500 ops/s, 5 ms latency. Node 1 is silenced from 30 s for 5 s:
    for the first 2 s of it, replies take 300 ms. The view change is at +0.6 s
    and the node is active again 0.4 s after the silence ends."""
    rng = np.random.default_rng(1)
    rows = []
    for i in range(30_000):
        t = 5 + i * 0.002  # warm-up until t=5 is not marked, measure from 10
        phase = "warmup" if t < 10 else "measure"
        lat = 5_000 + int(rng.integers(0, 1500))
        if spike and 30 + spike_delay_s <= t < 32 + spike_delay_s:
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
        # Counted per request: each slow request in the synthetic run retried once.
        assert f["retries_during"] <= 1000


def test_a_spike_that_starts_late_is_still_measured():
    # The slow replies start 2.5 s after the silence: the first windows are
    # in band, but they must not count as recovery (failover would read 0).
    with tempfile.TemporaryDirectory() as tmp:
        synthetic_run(Path(tmp), spike_delay_s=2.5)
        f = analyze.failures(analyze.load_runs(Path(tmp))[0])[0]
        assert not f["no_excursion"], f
        assert 4.5 <= f["failover_s"] <= 5.5, f
        assert f["peak_p99_ms"] > 250


def test_no_spike_is_reported_as_no_excursion():
    with tempfile.TemporaryDirectory() as tmp:
        synthetic_run(Path(tmp), spike=False)
        f = analyze.failures(analyze.load_runs(Path(tmp))[0])[0]
        assert f["no_excursion"], f
        assert f["failover_s"] == 0.0, f


def test_network_rate_from_cumulative_counters():
    import pandas as pd
    t = pd.Series([0.0, 1.0, 2.0, 3.0, 4.0])
    total = pd.Series([0, 2e6, 4e6, 1e6, 3e6])  # restarted between 2 s and 3 s
    r = analyze.counter_rate_mb_s(t, total).to_numpy()
    assert np.isnan(r[0])
    assert r[1] == r[2] == 2.0
    assert np.isnan(r[3]), "a counter reset is a gap, not a negative rate"
    assert r[4] == 2.0


def test_missing_samples_show_as_a_gap():
    t = np.array([0.0, 1.0, 2.0, 8.0, 9.0])
    y = np.array([1.0, 1.0, 1.0, 2.0, 2.0])
    tt, yy = analyze.break_gaps(t, y)
    assert len(tt) == 6 and np.isnan(yy[3]), (tt, yy)
    tt, yy = analyze.break_gaps(np.array([0.0, 1.0, 2.0]), np.array([1.0, 2.0, 3.0]))
    assert not np.isnan(yy).any()


def test_resource_figure_includes_memory_and_network():
    with tempfile.TemporaryDirectory() as tmp:
        d = synthetic_run(Path(tmp))
        rows = []
        for i in range(20):
            rows.append(ev(10 + i, "resource_sample", machine="client1", scope="machine", cpu_permille=300,
                           mem_used_bytes=2_000_000_000, net_rx_bytes=i * 1_000_000, net_tx_bytes=i * 500_000))
            rows.append(ev(10 + i, "resource_sample", machine="cloud1", node=1, cpu_permille=500,
                           rss_bytes=50_000_000, net_rx_bytes=i * 3_000_000, net_tx_bytes=i * 3_000_000, queue_len=4))
        with open(d / "client1-0.jsonl", "a") as f:
            f.write("\n".join(json.dumps(r) for r in rows) + "\n")
        run = analyze.load_runs(Path(tmp))[0]
        captured = {}
        real = analyze.plt.subplots

        def spy(n, *a, **k):
            captured["panels"] = n
            return real(n, *a, **k)

        analyze.plt.subplots = spy
        try:
            analyze.plot_resources(run, Path(tmp))
        finally:
            analyze.plt.subplots = real
        assert captured["panels"] == 7, captured
        assert (Path(tmp) / f"resources_{run.id}.png").exists()


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
            d = synthetic_run(root, f"cfg-s{s}")
            # Load curves need an x (runs without one are left out of them).
            (d / "meta.json").write_text(json.dumps({"label": "cfg", "seed": s, "x": 500,
                                                     "x_name": "offered load (ops/s)"}))
        out = Path(tmp) / "out"
        assert analyze.main([str(root), "--out", str(out)]) == 0
        assert (out / "summary.md").exists()
        assert (out / "tables" / "aggregate.csv").exists()
        assert (out / "figures" / "load_curves.png").exists()
        assert any((out / "figures").glob("timeline_*.png"))


# ---------------------------------------------------------------- new measurements (synthetic, known answers)


def gen_run(root: Path, name="gen-s1", rate=500, t_end=65.0, spikes=(), blips_every=0, drops=(),
            injections=(), extra=(), meta=None):
    """Like synthetic_run, but configurable. Requests every 1/rate s from 5 s
    (measured from 10 s), 5-6.5 ms latency.
    spikes:      (send_start, send_end, latency_us): slow replies
    blips_every: every n-th 0.5 s window has 5 replies of 30 ms (normal noise)
    drops:       (send_start, send_end): these requests never complete (gave_up)
    injections:  (t0, victim, duration_ms, k): inject_start/inject_end by the victim
    extra:       more event rows (use ev())"""
    rng = np.random.default_rng(7)
    rows, i, t = [], 0, 5.0
    while t < t_end:
        phase = "warmup" if t < 10 else "measure"
        lat = 5_000 + int(rng.integers(0, 1500))
        if blips_every and int((t - 5) / 0.5) % blips_every == 0 and int(((t - 5) % 0.5) * rate) < 5:
            lat = 30_000
        for a, b, slow in spikes:
            if a <= t < b:
                lat = slow
        rows.append(ev(t, "client_send", req=f"r{i}", attempt=0, phase=phase))
        if any(a <= t < b for a, b in drops):
            rows.append(ev(t + 10, "client_reply", req=f"r{i}", attempt=3, outcome="gave_up", phase=phase))
        else:
            rows.append(ev(t + lat / 1e6, "client_reply", req=f"r{i}", attempt=int(lat > 100_000), outcome="ok",
                           latency_us=lat, phase=phase, **{"from": 2}))
            if lat > 100_000:
                rows.append(ev(t + 0.15, "client_retry", req=f"r{i}", attempt=1, phase=phase))
        i += 1
        t = 5.0 + i / rate
    for t0, victim, dur_ms, k in injections:
        rows.append(ev(t0, "inject_start", machine=f"cloud{victim}", node=victim, k=k, victim=victim,
                       duration_ms=dur_ms, by_operator=True))
        rows.append(ev(t0 + dur_ms / 1000, "inject_end", machine=f"cloud{victim}", node=victim, k=k))
    rows += list(extra)
    d = root / name
    d.mkdir(parents=True)
    (d / "client1-0.jsonl").write_text("\n".join(json.dumps(r) for r in rows) + "\n")
    if meta is not None:
        (d / "meta.json").write_text(json.dumps(meta))
    return d


def one_run(tmp, **kw):
    gen_run(Path(tmp), **kw)
    return analyze.load_runs(Path(tmp))[0]


def test_failover_ignores_baseline_noise_and_the_next_injection():
    # Every third window has a few 30 ms replies (normal for this system), and
    # a second injection 15 s later causes its own spike. The old definition
    # (band from the median only, search 30 s past the silence) never saw
    # 2 s in band, so it returned NaN.
    with tempfile.TemporaryDirectory() as tmp:
        run = one_run(tmp, blips_every=3, spikes=[(30, 32, 300_000), (45, 47, 300_000)],
                      injections=[(30, 1, 5000, 1), (45, 2, 5000, 2)])
        f = analyze.failures(run)
        assert len(f) == 2
        for x in f:
            assert not x["no_excursion"], x
            assert 2.0 <= x["failover_s"] <= 3.0, x
        assert abs(f[0]["horizon_s"] - 15.0) < 1e-6, "the search stops at the next injection"
        assert f[0]["band_ms"] >= 30, "the band includes the noise seen before the silence"


def test_a_later_unrelated_spike_does_not_count():
    # One injection at 30 s with no effect; an unrelated spike 8 s later. The
    # failover must read 0 (no excursion), not about 10 s as before.
    with tempfile.TemporaryDirectory() as tmp:
        run = one_run(tmp, spikes=[(38, 40, 300_000)], injections=[(30, 1, 5000, 1)])
        f = analyze.failures(run)[0]
        assert f["no_excursion"], f
        assert f["failover_s"] == 0.0, f


def test_a_single_noisy_window_is_not_an_excursion():
    # A few replies 3 s after the silence are slightly over the band, in one
    # window: noise, not a failover (the old code read 3.5 s).
    with tempfile.TemporaryDirectory() as tmp:
        run = one_run(tmp, spikes=[(33.0, 33.012, 14_000)], injections=[(30, 1, 5000, 1)])
        f = analyze.failures(run)[0]
        assert f["no_excursion"], f


def test_a_stall_with_no_completions_is_a_breach():
    # Nothing completes for 1.5 s (the requests give up): empty windows are
    # breaches (DL-044). The old code skipped them and saw no excursion.
    with tempfile.TemporaryDirectory() as tmp:
        run = one_run(tmp, drops=[(30, 31.5)], injections=[(30, 1, 5000, 1)])
        f = analyze.failures(run)[0]
        assert not f["no_excursion"], f
        assert 1.5 <= f["failover_s"] <= 2.0, f


def test_failure_detection_view_removal_and_recovery_times():
    extra = [ev(30.3, "suspect", machine="cloud2", node=2, peer=1, phi=9.0),
             ev(30.6, "view_change", machine="cloud2", node=2, view=2, active=[2, 3]),
             ev(30.61, "view_change", machine="cloud3", node=3, view=2, active=[2, 3]),
             ev(35.4, "active", machine="cloud1", node=1, applied=10, view=3)]
    with tempfile.TemporaryDirectory() as tmp:
        f = analyze.failures(one_run(tmp, injections=[(30, 1, 5000, 1)], extra=extra))[0]
        assert abs(f["detect_s"] - 0.3) < 0.01
        assert abs(f["view_removal_s"] - 0.6) < 0.01
        assert abs(f["recovery_s"] - 0.4) < 0.01


def election_rows():
    """Start-up election by n3; n3 silenced at 30 s, suspected at 30.3 s; n1
    and n2 both pre-vote (30.4, 30.45), n2 wins epoch 2 at 30.6 s."""
    r = []
    for n in (1, 2, 3):
        r.append(ev(0.0 + n * 0.001, "view_change", machine=f"cloud{n}", node=n, view=1, active=[1, 2, 3]))
    r += [ev(0.0, "election_start", machine="cloud3", node=3, epoch=1, phase="pre_vote"),
          ev(0.001, "election_start", machine="cloud3", node=3, epoch=1, phase="vote"),
          ev(0.002, "election_won", machine="cloud3", node=3, epoch=1),
          ev(30.3, "suspect", machine="cloud1", node=1, peer=3, phi=9.0),
          ev(30.31, "suspect", machine="cloud2", node=2, peer=3, phi=9.0),
          ev(30.4, "election_start", machine="cloud1", node=1, epoch=2, phase="pre_vote"),
          ev(30.45, "election_start", machine="cloud2", node=2, epoch=2, phase="pre_vote"),
          ev(30.5, "election_start", machine="cloud2", node=2, epoch=2, phase="vote"),
          ev(30.6, "election_won", machine="cloud2", node=2, epoch=2),
          ev(30.6, "view_change", machine="cloud2", node=2, view=2, active=[1, 2]),
          ev(30.61, "view_change", machine="cloud1", node=1, view=2, active=[1, 2])]
    for n in (1, 2, 3):
        r.append(ev(35.5, "view_change", machine=f"cloud{n}", node=n, view=3, active=[1, 2, 3]))
    # Cumulative election messages per node: 4 from start-up, then +6 for the second election.
    for n in (1, 2, 3):
        for s in range(0, 61):
            r.append(ev(s + 0.5, "resource_sample", machine=f"cloud{n}", node=n, scope="node",
                        msgs_tx_election=4 + (6 if s >= 31 else 0), msgs_tx_replication=100 * s,
                        msgs_tx_claim=10 * s, msgs_tx_commit=10 * s, msgs_tx_other=0))
    return r


def test_elections_are_counted_once_and_timed_from_the_first_pre_vote():
    with tempfile.TemporaryDirectory() as tmp:
        run = one_run(tmp, injections=[(30, 3, 5000, 0)], extra=election_rows())
        el = analyze.elections(run)
        assert len(el) == 2 and el[0]["startup"] and not el[1]["startup"]
        assert not el[0]["teardown"] and not el[1]["teardown"]
        e = el[1]
        assert abs(e["duration_s"] - 0.2) < 1e-6, e         # first pre_vote 30.4 -> won 30.6
        assert abs(e["from_suspect_s"] - 0.3) < 1e-6, e     # first suspect 30.3 -> 30.6
        assert abs(e["from_inject_s"] - 0.6) < 1e-6, e      # silence 30.0 -> 30.6
        assert e["pre_vote_starts"] == 2
        c = analyze.counters(run)
        assert c["elections"] == 2 and c["elections_after_startup"] == 1
        assert c["election_attempts"] == 3                  # pre_vote starts, not pre_vote + vote events
        assert c["views"] == 3 and c["view_changes"] == 2   # distinct views, not 8 events
        assert abs(c["election_mean_s"] - 0.2) < 1e-6
        # One election (30.6 s) inside the measured window 10..65 s.
        assert abs(c["elections_per_min"] - 60 / 55) < 0.01, c
        # The start-up messages were sent before the first sample: 3 nodes x 6.
        assert c["msgs_tx_election"] == 18, c
        assert c["election_msgs_per_election"] == 9
        assert abs(c["election_msgs_per_s"] - 18 / 60) < 1e-6


def test_false_suspicions_and_wrongful_removals():
    extra = [ev(1.0, "view_change", machine="cloud2", node=2, view=1, active=[1, 2, 3]),
             ev(30.3, "suspect", machine="cloud2", node=2, peer=1, phi=9.0),            # justified
             ev(30.6, "view_change", machine="cloud2", node=2, view=2, active=[2, 3]),  # justified removal
             ev(35.5, "view_change", machine="cloud2", node=2, view=3, active=[1, 2, 3]),
             ev(36.0, "suspect", machine="cloud1", node=1, peer=3, phi=9.0),            # false: n3 was never silenced
             ev(50.0, "suspect", machine="cloud1", node=1, peer=2, phi=9.0),            # false
             ev(50.1, "view_change", machine="cloud1", node=1, view=4, active=[1, 3]),  # wrongful
             # After the clients stopped, the runner stops n1 first: not counted.
             ev(3500.0, "suspect", machine="cloud2", node=2, peer=1, phi=9.0),
             ev(3500.1, "view_change", machine="cloud2", node=2, view=5, active=[3])]
    for n in (1, 2, 3):
        extra += [ev(0, "resource_sample", machine=f"cloud{n}", node=n, scope="node"),
                  ev(3600, "resource_sample", machine=f"cloud{n}", node=n, scope="node")]
    with tempfile.TemporaryDirectory() as tmp:
        run = one_run(tmp, injections=[(30, 1, 5000, 1)], extra=extra)
        lo, hi = analyze.service_span(run)
        assert hi < 3500, (lo, hi)
        s = analyze.suspicions(run)
        assert s["suspects"] == 3 and s["false_suspects"] == 2, s
        hours = 3 * (hi - lo) / 3600  # node time while the clients ran
        assert abs(s["node_hours"] - hours) < 1e-6, s
        assert abs(s["false_suspects_per_node_hour"] - 2 / hours) < 1e-6
        assert s["view_removals"] == 2 and s["wrongful_removals"] == 1, s


def test_before_during_after_table():
    with tempfile.TemporaryDirectory() as tmp:
        run = one_run(tmp, spikes=[(30, 32, 300_000)], injections=[(30, 1, 5000, 1), (50, 2, 3000, 2)])
        ph = analyze.injection_phases(run)
        assert len(ph) == 6
        by = {(p["k"], p["phase"]): p for p in ph}
        b, d, a = by[(1, "before")], by[(1, "during")], by[(1, "after")]
        assert b["p99_ms"] < 10 and a["p99_ms"] < 10 and d["p99_ms"] > 250
        assert 480 < b["throughput"] < 520 and 480 < a["throughput"] < 520
        assert abs(a["end_s"] - 20.0) < 1e-6, "the after window stops at the next injection"
        b2 = by[(2, "before")]
        assert abs(b2["start_s"] - (-10.0)) < 1e-6 and b2["p99_ms"] < 10


def test_every_injection_gets_a_timeline():
    with tempfile.TemporaryDirectory() as tmp:
        run = one_run(tmp, spikes=[(30, 32, 300_000)], injections=[(30, 1, 5000, 1), (50, 2, 3000, 2)])
        paths = analyze.plot_timeline(run, Path(tmp))
        assert len(paths) == 2 and all(p.exists() for p in paths)


def closed_meta(users, seed):
    return {"label": f"closed-u{users}", "series": "mwbe_cached", "seed": seed, "x": users,
            "x_name": "users per process", "mode": "closed", "client_machines": 3,
            "loadgen": {"mode": "closed", "users": users, "processes": 4}}


def test_throughput_against_concurrent_clients_uses_meta_and_x_name():
    import pandas as pd
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp) / "runs"
        for users in (10, 20):
            for seed in (1, 2):
                gen_run(root, f"closed-u{users}-s{seed}", rate=200 if users == 10 else 300, t_end=40,
                        meta=closed_meta(users, seed))
        runs = analyze.load_runs(root)
        per = pd.DataFrame([analyze.run_latency(r) for r in runs])
        assert set(per.concurrent_users) == {120, 240}      # users x 4 processes x 3 machines
        agg = analyze.aggregate(per)
        assert set(agg.concurrent_users) == {120, 240} and set(agg["mode"]) == {"closed"}
        labels = []
        real = analyze.plt.Axes.set_xlabel

        def spy(self, text, *a, **k):
            labels.append(text)
            return real(self, text, *a, **k)

        analyze.plt.Axes.set_xlabel = spy
        try:
            analyze.plot_load(agg, Path(tmp))
            analyze.plot_clients(agg, Path(tmp))
        finally:
            analyze.plt.Axes.set_xlabel = real
        assert "users per process" in labels, labels
        assert "load level" not in labels
        assert (Path(tmp) / "throughput_vs_clients.png").exists()


def test_failed_runs_are_skipped():
    with tempfile.TemporaryDirectory() as tmp:
        synthetic_run(Path(tmp), "ok-s1")
        synthetic_run(Path(tmp) / "_failed", "bad-s1")
        assert [r.id for r in analyze.load_runs(Path(tmp))] == ["ok-s1"]


def test_load_imbalance_share_and_figure():
    # Plans spread evenly over three nodes, except node 2 gets 1 in 10 while
    # its load knob is on (20-40 s).
    extra = []
    for i in range(6000):
        t = 10 + i * 0.01
        if 20 <= t < 40:
            n = 2 if i % 10 == 0 else (1 if i % 2 else 3)
        else:
            n = i % 3 + 1
        extra.append(ev(t, "plan_end", machine=f"cloud{n}", node=n, service_us=1000))
    extra += [ev(20, "load_knob", machine="cloud2", node=2, threads=8, duty_permille=900, duration_ms=0, on=True),
              ev(40, "load_knob", machine="cloud2", node=2, threads=8, duty_permille=900, duration_ms=0, on=False)]
    for n in (1, 2, 3):
        for s in range(10, 70):
            extra.append(ev(s, "resource_sample", machine=f"cloud{n}", node=n, scope="node", queue_len=2,
                            cpu_permille=900 if n == 2 and 20 <= s < 40 else 300))
    with tempfile.TemporaryDirectory() as tmp:
        run = one_run(tmp, extra=extra)
        eps = analyze.knob_episodes(run)
        assert len(eps) == 1 and eps.node.iloc[0] == 2
        # The run's first event is at 5 s, so t = 15 and 35.
        assert abs(eps.t_on.iloc[0] - 15) < 1e-6 and abs(eps.t_off.iloc[0] - 35) < 1e-6
        im = analyze.imbalance(run)[0]
        assert 0.3 < im["share_before"] < 0.36 and im["share_during"] < 0.12 and 0.3 < im["share_after"] < 0.36
        assert analyze.plot_imbalance(run, Path(tmp)).exists()


def test_knob_times_fall_back_to_the_runner_actions():
    with tempfile.TemporaryDirectory() as tmp:
        meta = {"actions": [{"type": "load_knob", "node": 2, "threads": 8, "duty": 900, "duration_ms": 5000,
                             "rc": 0, "sent_ms_client0": (T0 + 20 * 10**9) // 10**6}]}
        run = one_run(tmp, t_end=30, meta=meta)
        eps = analyze.knob_episodes(run)
        assert len(eps) == 1
        # The run's first event is at 5 s, so t = wall clock - 5 s.
        assert abs(eps.t_on.iloc[0] - 15.0) < 0.01 and abs(eps.t_off.iloc[0] - 20.0) < 0.01


def test_dropped_events_and_core_lag_are_reported():
    extra = []
    for s in range(10, 30):
        extra.append(ev(s, "resource_sample", machine="cloud1", node=1, scope="node",
                        events_dropped=0 if s < 20 else 7, core_lag_ms_max=3 if s != 25 else 250,
                        cpu_permille=500, queue_len=1))
        extra.append(ev(s, "resource_sample", machine="cloud2", node=2, scope="node", events_dropped=0,
                        core_lag_ms_max=2, cpu_permille=500, queue_len=1))
    with tempfile.TemporaryDirectory() as tmp:
        d = gen_run(Path(tmp), t_end=30, extra=extra)
        (d / "client1-summary.json").write_text(json.dumps({"events_dropped": 3, "send_dropped": 0}))
        run = analyze.load_runs(Path(tmp))[0]
        row, warn = analyze.health(run)
        assert row["node_events_dropped"] == 7 and row["client_events_dropped"] == 3
        assert row["core_lag_max_ms_n1"] == 250
        assert any("node 1 dropped 7" in w for w in warn)
        assert any("core-loop lag up to 250" in w for w in warn)
        assert any("clients dropped 3" in w for w in warn)
        assert not any("node 2" in w for w in warn)
        captured = {}
        real = analyze.plt.subplots

        def spy(n, *a, **k):
            captured["panels"] = n
            return real(n, *a, **k)

        analyze.plt.subplots = spy
        try:
            analyze.plot_resources(run, Path(tmp))
        finally:
            analyze.plt.subplots = real
        assert captured["panels"] == 3, captured  # node CPU, queue length and core-loop lag


def test_end_to_end_cli_with_the_new_tables():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp) / "runs"
        gen_run(root, "fail-s1", spikes=[(30, 32, 300_000)], injections=[(30, 3, 5000, 0)], extra=election_rows())
        out = Path(tmp) / "out"
        assert analyze.main([str(root), "--out", str(out)]) == 0
        for t in ("failures", "injection_phases", "elections", "suspicions", "counters", "health"):
            assert (out / "tables" / f"{t}.csv").exists(), t
        md = (out / "summary.md").read_text()
        assert "Before / during / after" in md and "Coordinator elections" in md


# ---------------------------------------------------------------- review fixes (H1, H3, M2, M3, M6, L5)


def test_loader_skips_unused_kinds_and_keeps_compact_columns():
    # H1: unused kinds are counted but never parsed; resource samples live in
    # their own frame; repeated strings are categoricals.
    extra = []
    for i in range(200):
        extra += [ev(10 + i * 0.01, "ranked", machine="cloud1", node=1, req=f"r{i}", rank=[1, 2, 3]),
                  ev(10 + i * 0.01, "propose", machine="cloud1", node=1, req=f"r{i}", attempt=0)]
    for s in range(10, 20):
        extra.append(ev(s, "resource_sample", machine="cloud1", node=1, scope="node", cpu_permille=5,
                        dropped_silenced=s, queue_len=1))
    with tempfile.TemporaryDirectory() as tmp:
        run = one_run(tmp, t_end=20, extra=extra)
        assert run.kind_counts["ranked"] == 200 and run.kind_counts["propose"] == 200
        assert not run.ev.kind.isin(["ranked", "propose", "resource_sample"]).any()
        assert len(run.rs) == 10 and "dropped_silenced" not in run.rs and "cpu_permille" in run.rs
        assert str(run.ev.kind.dtype) == "category" and str(run.ev.phase.dtype) == "category"
        assert run.ev.req[run.ev.kind == "client_send"].isna().all(), "request ids are kept only where used"
        assert len(analyze.node_samples(run)) == 10


def test_main_keeps_one_run_in_memory_at_a_time(monkeypatch):
    # H1: main() computes each run's rows and figures and frees it before
    # loading the next, so memory does not grow with the sweep.
    import gc
    import weakref
    alive, peak = [], [0]
    real = analyze.load_run

    def tracking(path, *a):
        gc.collect()
        peak[0] = max(peak[0], sum(w() is not None for w in alive))
        r = real(path, *a)
        if r is not None:
            alive.append(weakref.ref(r))
        return r

    monkeypatch.setattr(analyze, "load_run", tracking)
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp) / "runs"
        for s in (1, 2, 3):
            gen_run(root, f"cfg-s{s}", t_end=30)
        assert analyze.main([str(root), "--out", str(Path(tmp) / "out")]) == 0
    assert len(alive) == 3 and peak[0] == 0, peak


def test_injections_outside_the_measured_window_are_left_out():
    # H3: a silence during warm-up and one after the load stopped are not
    # failovers (the old code reported a 0 s failover and a near-empty
    # "during" window for the second).
    with tempfile.TemporaryDirectory() as tmp:
        run = one_run(tmp, spikes=[(30, 32, 300_000)],
                      injections=[(7, 3, 2000, 0), (30, 1, 5000, 1), (66, 2, 5000, 2)])
        inj = analyze.injections(run)
        assert list(inj.in_window) == [False, True, False]
        f = analyze.failures(run)
        assert [x["k"] for x in f] == [1] and 2.0 <= f[0]["failover_s"] <= 3.0
        ph = analyze.injection_phases(run)
        assert {p["k"] for p in ph} == {1}
        during = [p for p in ph if p["phase"] == "during"][0]
        assert 480 < during["throughput"] < 520, during
        c = analyze.counters(run)
        assert c["injections"] == 1 and c["injections_outside_window"] == 2
        assert len(analyze.plot_timeline(run, Path(tmp))) == 1


def test_a_phase_cut_by_the_end_of_the_load_is_clipped():
    # The load ends 2 s into a 5 s silence: "during" covers only those 2 s
    # and "after" is left out instead of showing an empty window.
    with tempfile.TemporaryDirectory() as tmp:
        run = one_run(tmp, t_end=40, injections=[(36, 1, 5000, 1)])
        ph = {p["phase"]: p for p in analyze.injection_phases(run)}
        assert set(ph) == {"before", "during"}, ph
        assert ph["during"]["end_s"] < 4.01 and 480 < ph["during"]["throughput"] < 520


def test_censored_failover_counts_as_its_horizon():
    # M3: the latency never comes back before the load ends: failover is
    # censored, and the per-configuration mean uses the horizon (a lower
    # bound) instead of dropping the injection.
    import pandas as pd
    with tempfile.TemporaryDirectory() as tmp:
        run = one_run(tmp, spikes=[(30, 65, 300_000)], injections=[(30, 1, 5000, 1)])
        f = analyze.failures(run)[0]
        assert f["censored"] and np.isnan(f["failover_s"])
        assert f["failover_lb_s"] == f["horizon_s"] > 30
        t = analyze.failures_by_config(pd.DataFrame([f]))
        assert t.censored.iloc[0] == 1 and t.failover_s.iloc[0] == f["horizon_s"]


def test_confidence_intervals_are_over_seeds_not_injections():
    # M3: seed 1 has three injections at 1 s, seed 2 one at 3 s, seed 3 one
    # at 2 s. Per seed: 1, 3, 2 -> mean 2 with a t-interval over 3 seeds
    # (the mean over injections would be 1.6).
    import pandas as pd
    base = dict(label="L", victim_role="coordinator", censored=False)
    rows = [dict(base, seed=1, failover_s=1.0, failover_lb_s=1.0)] * 3
    rows += [dict(base, seed=2, failover_s=3.0, failover_lb_s=3.0), dict(base, seed=3, failover_s=2.0, failover_lb_s=2.0)]
    t = analyze.failures_by_config(pd.DataFrame(rows))
    assert t.seeds.iloc[0] == 3 and t.injections.iloc[0] == 5
    assert abs(t.failover_s.iloc[0] - 2.0) < 1e-9
    assert abs(t.failover_s_ci95.iloc[0] - analyze.ci95([1.0, 3.0, 2.0])[1]) < 1e-9
    el = analyze.seed_ci(pd.DataFrame([dict(label="L", seed=s, duration_s=d) for s, d in [(1, 0.1), (1, 0.3), (2, 0.4)]]),
                         ["label"], ["duration_s"])
    assert abs(el.duration_s.iloc[0] - 0.3) < 1e-9 and el.seeds.iloc[0] == 2


def test_false_suspicion_rate_is_pooled_over_seeds():
    # M2: (2 + 0) false suspicions over (1 + 3) node-hours = 0.5 per node-hour,
    # not the sum of the per-seed rates (2.0).
    import pandas as pd
    susp = pd.DataFrame([dict(label="L", seed=1, node_hours=1.0, suspects=5, false_suspects=2, view_removals=1,
                              wrongful_removals=1, false_suspects_per_node_hour=2.0),
                         dict(label="L", seed=2, node_hours=3.0, suspects=3, false_suspects=0, view_removals=1,
                              wrongful_removals=0, false_suspects_per_node_hour=0.0)])
    r = analyze.suspicion_rates(susp)
    assert r.false_suspects_per_node_hour.iloc[0] == 0.5
    assert r.wrongful_removals_per_node_hour.iloc[0] == 0.25


def test_missing_clock_offsets_are_warned():
    # M6: a machine whose clock_offset says "unavailable" or "*_unparsed", or
    # that logged none, is uncorrected; the summary must say so.
    extra = [ev(5, "clock_offset", machine="client1", offset_ns=1000, source="chrony", when="start"),
             ev(5, "clock_offset", machine="cloud1", node=1, source="unavailable", when="start"),
             ev(6, "clock_offset", machine="cloud3", node=3, source="chrony_unparsed", when="start"),
             ev(7, "suspect", machine="cloud2", node=2, peer=1)]
    with tempfile.TemporaryDirectory() as tmp:
        run = one_run(tmp, t_end=20, extra=extra)
        bad = analyze.clock_problems(run)
        assert bad == {"cloud1": "unavailable", "cloud2": "no clock_offset event", "cloud3": "chrony_unparsed"}, bad
        row, warn = analyze.health(run)
        assert row["clock_uncorrected"] == "cloud1,cloud2,cloud3"
        assert any("no usable clock offset" in w and "cloud1 (unavailable)" in w for w in warn)
        assert not any("client1 (" in w for w in warn)


def test_imbalance_rates_are_clipped_to_the_load():
    # L5: the knob starts 7 s after the load does; plans/s before it is over
    # those 7 s, not diluted over a 20 s window that starts before the load.
    extra = [ev(5 + i / 30, "plan_end", machine="cloud1", node=1, service_us=1000) for i in range(30 * 40)]
    extra += [ev(12, "load_knob", machine="cloud2", node=2, threads=8, duty_permille=900, duration_ms=10000, on=True),
              ev(22, "load_knob", machine="cloud2", node=2, threads=8, duty_permille=900, duration_ms=10000, on=False)]
    with tempfile.TemporaryDirectory() as tmp:
        im = analyze.imbalance(one_run(tmp, t_end=45, extra=extra))[0]
        assert 28 < im["plans_per_s_before"] < 32, im
        assert 28 < im["plans_per_s_during"] < 32 and 28 < im["plans_per_s_after"] < 32, im


# ---------------------------------------------------------------- third review (A1, A2, A3, A4, A8)


def _meta(sweep, label, seed, x=None, series=None, x_name="offered load (ops/s)"):
    m = {"sweep": sweep, "label": label, "seed": seed, "series": series or label, "x_name": x_name}
    if x is not None:
        m["x"] = x
    return m


def test_runs_nested_by_sweep_keep_their_sweep_apart():
    # Two sweeps with the same run ids (e.g. a rerun into another --out) are
    # different configurations: the sweep comes from the directory.
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        gen_run(root / "failure", "failure-coord-s1", t_end=30, meta=_meta("failure", "coord", 1, 200))
        gen_run(root / "failure_rr", "failure-coord-s1", t_end=30, meta=_meta("failure", "coord", 1, 200))
        gen_run(root / "_failed", "failure-coord-s9", t_end=30)
        runs = analyze.load_runs(root)
        assert sorted(r.id for r in runs) == ["failure/failure-coord-s1", "failure_rr/failure-coord-s1"]
        assert sorted(r.sweep for r in runs) == ["failure", "failure_rr"]
        import pandas as pd
        agg = analyze.aggregate(pd.DataFrame([analyze.run_latency(r) for r in runs]))
        assert len(agg) == 2, "same label in two sweeps is two configurations"
    with tempfile.TemporaryDirectory() as tmp:
        gen_run(Path(tmp), "latency-mwbe-r200-s1", t_end=20)  # flat, no meta: sweep from the run id
        assert analyze.load_runs(Path(tmp))[0].sweep == "latency"


def test_figures_are_chosen_per_sweep_and_label():
    # A1: the default no longer takes the first runs alphabetically: every
    # failure target of every sweep gets a timeline and every knob policy an
    # imbalance figure.
    knob = [ev(20, "load_knob", machine="cloud2", node=2, threads=8, duty_permille=900, duration_ms=5000, on=True),
            ev(25, "load_knob", machine="cloud2", node=2, threads=8, duty_permille=900, duration_ms=5000, on=False)]
    knob += [ev(10 + i * 0.05, "plan_end", machine=f"cloud{i % 3 + 1}", node=i % 3 + 1, service_us=100)
             for i in range(600)]
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp) / "raw"
        for sweep in ("autoinject", "failure"):
            for label in ("coord-r200", "rank1-r200"):
                for seed in (1, 2):
                    gen_run(root / sweep, f"{sweep}-{label}-s{seed}", t_end=40, spikes=[(25, 26, 300_000)],
                            injections=[(25, 1, 3000, 0)], meta=_meta(sweep, label, seed, 200))
        for label in ("hash-knob", "mwbe-knob"):
            for seed in (1, 2):
                gen_run(root / "imbalance", f"imbalance-{label}-s{seed}", t_end=40, extra=knob,
                        meta=_meta("imbalance", label, seed))
        out = Path(tmp) / "out"
        assert analyze.main([str(root), "--out", str(out)]) == 0
        figs = {p.name for p in (out / "figures").iterdir()}
        tl = [f for f in figs if f.startswith("timeline_")]
        assert len(tl) == 4, tl  # one run (all its injections) per (sweep, label)
        for sweep in ("autoinject", "failure"):
            for label in ("coord", "rank1"):
                assert any(f"{sweep}__{sweep}-{label}" in f for f in tl), (sweep, label, tl)
        imb = sorted(f for f in figs if f.startswith("imbalance_"))
        assert len(imb) == 2 and "hash" in imb[0] and "mwbe" in imb[1], imb


def test_load_curves_are_per_sweep_coloured_by_series_and_skip_runs_without_x():
    # A2, A3
    import pandas as pd
    rows = []
    for sweep in ("latency", "failure"):
        for series in ("mwbe", "hash"):
            for x in (200, 600):
                rows.append({"sweep": sweep, "series": series, "label": f"{series}-{x}", "x": x,
                             "x_name": "offered load (ops/s)", "throughput": x, "throughput_ci95": 5.0,
                             **{c: 10.0 for c in ("p50_ms", "p95_ms", "p99_ms")},
                             **{c + "_ci95": 50.0 for c in ("p50_ms", "p95_ms", "p99_ms")}})
    rows.append({"sweep": "imbalance", "series": "mwbe", "label": "knob", "x": float("nan"),
                 "x_name": "policy", "throughput": 1, "throughput_ci95": 0,
                 **{c: 1.0 for c in ("p50_ms", "p95_ms", "p99_ms")},
                 **{c + "_ci95": 0.0 for c in ("p50_ms", "p95_ms", "p99_ms")}})
    calls = []
    real = analyze.plt.Axes.errorbar

    def spy(self, x, y, *a, **k):
        calls.append((self.get_yscale(), k.get("label"), k.get("color"), k.get("yerr")))
        return real(self, x, y, *a, **k)

    analyze.plt.Axes.errorbar = spy
    try:
        with tempfile.TemporaryDirectory() as tmp:
            paths = analyze.plot_load(pd.DataFrame(rows), Path(tmp))
            assert sorted(p.name for p in paths) == ["load_curves_failure_offered_load_ops_s.png",
                                                     "load_curves_latency_offered_load_ops_s.png"]
    finally:
        analyze.plt.Axes.errorbar = real
    colour = {}
    for _, label, c, _ in calls:
        colour.setdefault(label.split()[0], set()).add(c)
    assert all(len(v) == 1 for v in colour.values()), colour  # one colour per series...
    assert colour["mwbe"] != colour["hash"]                   # ...different between series
    for scale, label, _, yerr in calls:
        if label.endswith("p50"):
            lower = np.asarray(yerr[0])
            assert (lower < 10.0).all(), "a whisker reaching 0 on a log axis is clipped"
    assert not any("knob" in str(lbl) for _, lbl, _, _ in calls), "runs without x are not drawn"


def test_a_victim_that_works_before_converging_is_flagged():
    # A8: between waking and `active` the victim must not plan or claim.
    extra = [ev(35.0, "sync_start", machine="cloud1", node=1, last_seq=1000, commit=1000),
             ev(35.1, "plan_end", machine="cloud1", node=1, service_us=100),  # too early
             ev(35.2, "claim", machine="cloud1", node=1, req="x"),            # too early
             ev(35.4, "active", machine="cloud1", node=1, applied=1450, view=3),
             ev(35.5, "plan_end", machine="cloud1", node=1, service_us=100)]  # fine
    with tempfile.TemporaryDirectory() as tmp:
        f = analyze.failures(one_run(tmp, injections=[(30, 1, 5000, 1)], extra=extra))[0]
        assert f["worked_before_active"] == 2, f
        assert abs(f["sync_s"] - 0.4) < 1e-6 and f["catch_up_entries"] == 450, f


def test_fairness_against_capacity_uses_cores():
    # A4: node 3 has twice the cores and does twice the work: fair against
    # capacity (Jain 1 per core) although the plain index is below 1.
    extra = [ev(0.5, "machine_info", machine=f"cloud{n}", node=n, cores=c, cpu_model="x", mem_bytes=1e9, os="y")
             for n, c in ((1, 4), (2, 4), (3, 8))]
    extra += [ev(10 + i * 0.01, "plan_end", machine=f"cloud{n}", node=n, service_us=1000)
              for i in range(2000) for n in ([1] if i % 4 == 0 else [2] if i % 4 == 1 else [3])]
    with tempfile.TemporaryDirectory() as tmp:
        fr = analyze.fairness(one_run(tmp, extra=extra))
        assert fr["jain_busy"] < 0.95 and fr["jain_busy_per_core"] > 0.999, fr
        assert fr["capacity_share_n3"] == 0.5


# ---------------------------------------------------------------- performance audit (U1, U2)


def _saturated_run(root: Path):
    """1000 requests started in the measured window: 880 ok, 20 gave up,
    50 got only Busy replies and 50 nothing at all before the logs ended."""
    rows = []
    for i in range(1000):
        t = 10 + i * 0.01
        rows.append(ev(t, "client_send", req=f"r{i}", attempt=0, phase="measure"))
        if i < 880:
            rows.append(ev(t + 0.005, "client_reply", req=f"r{i}", attempt=0, outcome="ok", latency_us=5000,
                           phase="measure", **{"from": 1}))
        elif i < 900:
            rows.append(ev(t + 9, "client_reply", req=f"r{i}", attempt=3, outcome="gave_up", latency_us=9e6,
                           phase="measure"))
        elif i < 950:
            rows.append(ev(t + 0.01, "client_reply", req=f"r{i}", attempt=0, outcome="busy", latency_us=1e4,
                           phase="measure", **{"from": 2}))
            rows.append(ev(t + 0.2, "client_send", req=f"r{i}", attempt=1, phase="measure"))
    rows.append(ev(5, "client_send", req="w0", attempt=0, phase="warmup"))  # not in the window: ignored
    for s in range(10, 21):
        rows.append(ev(s, "resource_sample", machine="cloud2", node=2, scope="node", client_backlog=600,
                       client_overload_dropped=0 if s < 15 else 40, cpu_permille=900, queue_len=200))
        rows.append(ev(s, "resource_sample", machine="cloud1", node=1, scope="node", client_backlog=0,
                       client_overload_dropped=0, cpu_permille=100, queue_len=0))
    d = root / "sat-s1"
    d.mkdir(parents=True)
    (d / "client1-0.jsonl").write_text("\n".join(json.dumps(r) for r in rows) + "\n")
    return d


def test_unresolved_requests_are_counted_and_warned():
    # U1: busy replies are not final; requests with no ok/gave_up reply are
    # unresolved, excluded from throughput and percentiles, and warned about.
    with tempfile.TemporaryDirectory() as tmp:
        _saturated_run(Path(tmp))
        run = analyze.load_runs(Path(tmp))[0]
        r = analyze.run_latency(run)
        assert r["n_started"] == 1000 and r["unresolved"] == 100, r
        assert abs(r["unresolved_frac"] - 0.1) < 1e-9
        assert r["n_ok"] == 880 and r["n_gave_up"] == 20
        row, warn = analyze.health(run)
        assert row["unresolved"] == 100
        assert any("100 of 1000 requests" in w and "EXCLUDE" in w for w in warn), warn
        out = Path(tmp) / "out"
        assert analyze.main([str(tmp), "--out", str(out)]) == 0
        md = (out / "summary.md").read_text()
        assert "unresolved_frac" in md and "EXCLUDED" in md


def test_overload_signals_other_than_core_lag_are_warned():
    # U2: Busy replies, requests dropped for a full client backlog, and a
    # backlog that stays high are overload even when core lag is low.
    with tempfile.TemporaryDirectory() as tmp:
        _saturated_run(Path(tmp))
        row, warn = analyze.health(analyze.load_runs(Path(tmp))[0])
        assert row["busy_replies"] == 50 and any("50 Busy replies" in w for w in warn), warn
        assert row["overload_dropped_n2"] == 40 and any("node 2 dropped 40 client requests" in w for w in warn)
        assert any("node 2 had a client backlog" in w for w in warn)
        assert not any("node 1" in w for w in warn)


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            print("ok", name)


def test_an_election_after_the_clients_stopped_is_teardown():
    """The runner stops the nodes after the load; when the coordinator goes
    first the other two elect one. That is not an election of the run."""
    extra = election_rows() + [
        ev(3000.0, "suspect", machine="cloud1", node=1, peer=2, phi=9.0),
        ev(3000.1, "election_start", machine="cloud3", node=3, epoch=3, phase="pre_vote"),
        ev(3000.2, "election_won", machine="cloud3", node=3, epoch=3),
        ev(3000.2, "step_down", machine="cloud2", node=2, epoch=3)]
    with tempfile.TemporaryDirectory() as tmp:
        run = one_run(tmp, injections=[(30, 3, 5000, 0)], extra=extra)
        el = analyze.elections(run)
        assert [e["teardown"] for e in el] == [False, False, True]
        c = analyze.counters(run)
        assert c["elections"] == 3 and c["elections_after_startup"] == 1 and c["elections_teardown"] == 1, c
        assert c["step_down"] == 0 and c["suspect"] == 2, c
