#!/usr/bin/env python3
"""Turn raw event logs into the tables and plots of the Phase 1 report.

    python analysis/analyze.py results/raw --out results/summary

Input: a directory of run directories. Each run directory holds the
`*.jsonl` event logs of every process (schema: crates/proto/src/event.rs)
and, if the experiment runner made it, a `meta.json`:

    {"label": "mwbe-load400", "series": "mwbe_cached", "x": 400,
     "x_name": "offered load (ops/s)", "seed": 3}

Without meta.json the seed is read from a run id ending in `-s<seed>` and
the label is the rest of the id.

Every number in the output is computed from the logs only. Requests are
classified by the phase in which they started (warm-up is discarded). All
timestamps are corrected by each machine's logged clock offset (offset is
positive when the machine's clock is ahead; corrected = local - offset).

Outputs (under --out):
    tables/per_run.csv          one row per run: latency, throughput, errors
    tables/aggregate.csv        per configuration, mean and 95% CI over seeds
    tables/failures.csv         one row per injected failure
    tables/fairness.csv         per-run load distribution and Jain's index
    tables/counters.csv         election / claim / re-plan counters per run
    figures/*.png               throughput, latency, failure timelines, resources
    summary.md                  the aggregate tables in Markdown
"""

from __future__ import annotations

import argparse
import json
import math
import re
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from scipy import stats as st  # noqa: E402

PCTS = {"p50": 50, "p95": 95, "p99": 99}


# ---------------------------------------------------------------- loading


class Run:
    def __init__(self, path: Path, ev: pd.DataFrame, meta: dict):
        self.path, self.ev, self.meta = path, ev, meta
        self.id = path.name

    @property
    def label(self):
        return self.meta["label"]

    @property
    def seed(self):
        return self.meta["seed"]


def parse_meta(path: Path) -> dict:
    meta = {}
    mf = path / "meta.json"
    if mf.exists():
        meta = json.loads(mf.read_text())
    m = re.match(r"^(.*)-s(\d+)$", path.name)
    meta.setdefault("label", m.group(1) if m else path.name)
    meta.setdefault("seed", int(m.group(2)) if m else 0)
    meta.setdefault("series", meta["label"])
    meta.setdefault("x", float("nan"))
    meta.setdefault("x_name", "load level")
    return meta


def ensure_cols(df: pd.DataFrame, cols) -> pd.DataFrame:
    for c in cols:
        if c not in df.columns:
            df[c] = np.nan
    return df


def load_run(path: Path) -> Run | None:
    frames = []
    for f in sorted(path.glob("*.jsonl")):
        rows = []
        with f.open() as fh:
            for line in fh:
                line = line.strip()
                if line:
                    try:
                        rows.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass  # a line cut off by a killed process
        if rows:
            df = pd.DataFrame(rows)
            df["file"] = f.stem
            frames.append(df)
    if not frames:
        return None
    ev = pd.concat(frames, ignore_index=True)
    ev = ensure_cols(
        ev,
        ["kind", "machine", "node", "req", "phase", "outcome", "attempt", "latency_us",
         "service_us", "epoch", "victim", "duration_ms", "k", "active", "view",
         "offset_ns", "cpu_permille", "queue_len", "scope", "rss_bytes", "when",
         "mem_used_bytes", "net_rx_bytes", "net_tx_bytes"],
    )
    # Clock correction: mean of the offsets logged by each machine.
    offs = ev[ev.kind == "clock_offset"].groupby("machine").offset_ns.mean()
    off = ev.machine.map(offs).fillna(0.0)
    t0 = int(ev.ts_ns.min())
    ev["t"] = (ev.ts_ns.astype("int64") - t0 - off.round().astype("int64")) / 1e9
    return Run(path, ev, parse_meta(path))


def load_runs(root: Path) -> list[Run]:
    runs = []
    for d in sorted(p for p in root.iterdir() if p.is_dir()):
        r = load_run(d)
        if r is not None:
            runs.append(r)
    return runs


# ---------------------------------------------------------------- latency


def pct(a: np.ndarray, p: float) -> float:
    return float(np.percentile(a, p)) if len(a) else float("nan")


def run_latency(run: Run) -> dict:
    ev = run.ev
    rep = ev[ev.kind == "client_reply"]
    ok = rep[(rep.outcome == "ok") & (rep.phase == "measure")]
    gu = rep[(rep.outcome == "gave_up") & (rep.phase == "measure")]
    busy = rep[(rep.outcome == "busy") & (rep.phase == "measure")]
    first = ev[(ev.kind == "client_send") & (ev.phase == "measure") & (ev.attempt == 0)]
    span = float(first.t.max() - first.t.min()) if len(first) > 1 else float("nan")
    lat = ok.latency_us.to_numpy(dtype=float) / 1000.0
    out = {
        "run": run.id, "label": run.label, "series": run.meta["series"],
        "x": run.meta["x"], "seed": run.seed,
        "n_ok": len(ok), "n_gave_up": len(gu), "n_busy_replies": len(busy),
        "window_s": span,
        "throughput": len(ok) / span if span and span > 0 else float("nan"),
        "mean_ms": float(lat.mean()) if len(lat) else float("nan"),
        "max_ms": float(lat.max()) if len(lat) else float("nan"),
        "retried_frac": float((ok.attempt > 0).mean()) if len(ok) else float("nan"),
        "gave_up_frac": len(gu) / max(len(ok) + len(gu), 1),
    }
    for name, p in PCTS.items():
        out[f"{name}_ms"] = pct(lat, p)
    return out


def ci95(values) -> tuple[float, float, int]:
    v = np.asarray([x for x in values if not math.isnan(x)], dtype=float)
    n = len(v)
    if n == 0:
        return float("nan"), float("nan"), 0
    if n == 1:
        return float(v[0]), float("nan"), 1
    half = st.t.ppf(0.975, n - 1) * v.std(ddof=1) / math.sqrt(n)
    return float(v.mean()), float(half), n


METRICS = ["throughput", "mean_ms", "p50_ms", "p95_ms", "p99_ms", "retried_frac", "gave_up_frac"]


def aggregate(per_run: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for (series, label), g in per_run.groupby(["series", "label"], sort=False):
        row = {"series": series, "label": label, "x": g.x.iloc[0], "seeds": len(g)}
        for m in METRICS:
            mean, half, _ = ci95(g[m])
            row[m], row[m + "_ci95"] = mean, half
        rows.append(row)
    return pd.DataFrame(rows)


# ---------------------------------------------------------------- failures


def window_p99(t: np.ndarray, lat: np.ndarray, lo: float, hi: float, w: float) -> np.ndarray:
    """p99 of latency in consecutive windows of w seconds over [lo, hi); NaN if empty."""
    n = int(round((hi - lo) / w))
    idx = np.floor((t - lo) / w).astype(int)
    out = np.full(n, np.nan)
    for i in range(n):
        sel = lat[idx == i]
        if len(sel):
            out[i] = np.percentile(sel, 99)
    return out


def first_by_k(df: pd.DataFrame, victim_col="node") -> pd.DataFrame:
    """One row per injection k, preferring the victim's own event."""
    rows = []
    for k, g in df.groupby("k"):
        mine = g[g.node == g.victim] if "victim" in g and g.victim.notna().any() else g.iloc[0:0]
        rows.append((mine if len(mine) else g).sort_values("t").iloc[0])
    return pd.DataFrame(rows)


def failures(run: Run, w=0.5, band_mult=1.5, band_abs_ms=5.0, hold_s=2.0, baseline_s=10.0) -> list[dict]:
    """Per injection: the p99 excursion and how long until it is back in band.

    Failover time = from the start of the silence to the start of the first
    run of `hold_s` seconds of in-band windows that FOLLOWS the first
    out-of-band window. The spike often starts late (client timeout, failure
    detection, view change), so in-band windows before it do not count.
    `no_excursion` is true only when no window after the silence is out of band.
    """
    hold = max(1, int(round(hold_s / w)))
    ev = run.ev
    starts = ev[ev.kind == "inject_start"]
    if starts.empty:
        return []
    starts = first_by_k(starts)
    ends = ev[ev.kind == "inject_end"]
    ends = first_by_k(ends) if len(ends) else ends
    ok = ev[(ev.kind == "client_reply") & (ev.outcome == "ok")]
    t_ok, lat_ok = ok.t.to_numpy(), ok.latency_us.to_numpy(dtype=float) / 1000.0
    out = []
    for _, s in starts.iterrows():
        t0, victim, dur = float(s.t), s.victim, float(s.duration_ms) / 1000.0
        row = {"run": run.id, "label": run.label, "seed": run.seed, "k": s.k,
               "victim": victim, "duration_s": dur, "t0": t0,
               "retries_during": float("nan")}
        base = window_p99(t_ok, lat_ok, t0 - baseline_s, t0, w)
        base = base[~np.isnan(base)]
        post_hi = t0 + dur + 30.0
        post = window_p99(t_ok, lat_ok, t0, post_hi, w)
        if len(base) >= 3:
            b = float(np.median(base))
            band = max(b * band_mult, b + band_abs_ms)
            good = ~np.isnan(post) & (post <= band)
            bad = ~np.isnan(post) & (post > band)
            if bad.any():
                first_bad = int(np.argmax(bad))
                k_ok = next((i for i in range(first_bad + 1, len(post) - hold + 1)
                             if good[i:i + hold].all()), None)
            else:
                k_ok = 0
            row.update(baseline_p99_ms=b, band_ms=band,
                       peak_p99_ms=float(np.nanmax(post)) if np.isfinite(post).any() else float("nan"),
                       failover_s=(k_ok * w) if k_ok is not None else float("nan"),
                       no_excursion=not bool(bad.any()))
            if k_ok is not None:
                # Requests that needed a retry (protocol Section 10), not retry events.
                rt = ev[(ev.kind == "client_retry") & (ev.t >= t0) & (ev.t < t0 + k_ok * w + w)]
                row["retries_during"] = int(rt.req.nunique()) if "req" in rt else len(rt)
        vc = ev[(ev.kind == "view_change") & (ev.t >= t0)]
        for _, v in vc.sort_values("t").iterrows():
            a = v.active
            if isinstance(a, list) and victim not in a:
                row["view_removal_s"] = float(v.t - t0)
                break
        e = ends[(ends.k == s.k)]
        if len(e):
            t_end = float(e.t.iloc[0])
            row["t_end"] = t_end
            act = ev[(ev.kind == "active") & (ev.node == victim) & (ev.t >= t_end)]
            if len(act):
                row["recovery_s"] = float(act.t.min() - t_end)
        out.append(row)
    return out


# ---------------------------------------------------------------- fairness, counters


def jain(x) -> float:
    x = np.asarray(x, dtype=float)
    return float(x.sum() ** 2 / (len(x) * (x ** 2).sum())) if (x ** 2).sum() > 0 else float("nan")


def fairness(run: Run) -> dict | None:
    ev = run.ev
    first = ev[(ev.kind == "client_send") & (ev.phase == "measure")]
    pe = ev[ev.kind == "plan_end"]
    if first.empty or pe.empty:
        return None
    pe = pe[(pe.t >= first.t.min()) & (pe.t <= first.t.max())]
    busy = pe.groupby("node").service_us.sum().reindex([1, 2, 3]).fillna(0.0)
    cnt = pe.groupby("node").size().reindex([1, 2, 3]).fillna(0)
    row = {"run": run.id, "label": run.label, "series": run.meta["series"], "seed": run.seed,
           "jain_busy": jain(busy), "jain_count": jain(cnt)}
    for n in (1, 2, 3):
        row[f"busy_share_n{n}"] = float(busy[n] / busy.sum()) if busy.sum() else float("nan")
    return row


def counters(run: Run) -> dict:
    ev = run.ev
    n = lambda k: int((ev.kind == k).sum())  # noqa: E731
    row = {"run": run.id, "label": run.label, "seed": run.seed}
    for k in ["election_start", "election_won", "step_down", "suspect", "view_change",
              "replan", "duplicate_suppressed", "busy", "commit", "inject_start"]:
        row[k] = n(k)
    cl = ev[ev.kind == "claim"]
    if len(cl):
        per = cl.groupby("req").size()
        row["claims_per_req"] = float(per.mean())
        row["competing_claim_rate"] = float((per > 1).mean())
    won, started = ev[ev.kind == "election_won"], ev[ev.kind == "election_start"]
    durs = []
    for _, w in won.iterrows():
        s = started[(started.node == w.node) & (started.epoch == w.epoch) & (started.t <= w.t)]
        if len(s):
            durs.append(float(w.t - s.t.max()))
    if durs:
        row["election_mean_s"], row["election_max_s"] = float(np.mean(durs)), float(np.max(durs))
    if n("commit"):
        row["replan_rate"] = n("replan") / n("commit")
    return row


# ---------------------------------------------------------------- plots


def plot_load(agg: pd.DataFrame, fig_dir: Path):
    if agg.empty:
        return
    fig, ax = plt.subplots(1, 2, figsize=(11, 4))
    for series, g in agg.groupby("series", sort=False):
        g = g.sort_values("x") if g.x.notna().all() else g
        x = g.x if g.x.notna().all() else np.arange(len(g))
        ax[0].errorbar(x, g.throughput, yerr=g.throughput_ci95.fillna(0), marker="o", capsize=3, label=series)
        for col, ls in (("p50_ms", ":"), ("p95_ms", "--"), ("p99_ms", "-")):
            ax[1].errorbar(x, g[col], yerr=g[col + "_ci95"].fillna(0), ls=ls, marker=".", capsize=2,
                           label=f"{series} {col[:3]}")
    ax[0].set_ylabel("throughput (ok replies / s)")
    ax[1].set_ylabel("latency (ms)")
    ax[1].set_yscale("log")
    xl = "load level"
    ax[0].set_xlabel(xl)
    ax[1].set_xlabel(xl)
    ax[0].grid(alpha=.3)
    ax[1].grid(alpha=.3)
    ax[0].legend(fontsize=8)
    ax[1].legend(fontsize=7, ncol=2)
    fig.suptitle("Throughput and latency vs load (mean over seeds, 95% CI)")
    fig.tight_layout()
    fig.savefig(fig_dir / "load_curves.png", dpi=140)
    plt.close(fig)


def plot_timeline(run: Run, fig_dir: Path, w=0.25, pre=10.0, post=30.0):
    ev = run.ev
    starts = ev[ev.kind == "inject_start"]
    if starts.empty:
        return
    s = first_by_k(starts).iloc[0]
    t0, dur = float(s.t), float(s.duration_ms) / 1000
    ok = ev[(ev.kind == "client_reply") & (ev.outcome == "ok")]
    lo, hi = t0 - pre, t0 + dur + post
    t, lat = ok.t.to_numpy(), ok.latency_us.to_numpy(dtype=float) / 1000
    n = int(round((hi - lo) / w))
    p99 = window_p99(t, lat, lo, hi, w)
    thr = np.histogram(t, bins=n, range=(lo, hi))[0] / w
    xs = lo + w * (np.arange(n) + .5) - t0
    fig, ax = plt.subplots(2, 1, figsize=(9, 5), sharex=True)
    ax[0].plot(xs, thr)
    ax[0].set_ylabel("completions / s")
    ax[1].plot(xs, p99)
    ax[1].set_ylabel(f"p99 latency per {w:g} s (ms)")
    ax[1].set_yscale("log")
    ax[1].set_xlabel("seconds since the injected silence began")
    marks = [(0, "silence starts", "r"), (dur, "silence ends", "g")]
    vc = ev[(ev.kind == "view_change") & (ev.t >= t0)].sort_values("t")
    if len(vc):
        marks.append((float(vc.t.iloc[0] - t0), "first view change", "k"))
    for a in ax:
        for x, label, c in marks:
            a.axvline(x, color=c, ls="--", lw=1)
        a.grid(alpha=.3)
    for x, label, c in marks:
        ax[0].text(x, ax[0].get_ylim()[0], " " + label, color=c, fontsize=7, va="bottom", rotation=90)
    fig.suptitle(f"Failure timeline: {run.id}, victim n{int(s.victim) if pd.notna(s.victim) else '?'}")
    fig.tight_layout()
    fig.savefig(fig_dir / f"timeline_{run.id}.png", dpi=140)
    plt.close(fig)


def counter_rate_mb_s(t: pd.Series, total: pd.Series) -> pd.Series:
    """MB/s from a cumulative byte counter sampled at times t (seconds).

    A counter that goes backwards (a restarted process) gives NaN for that
    interval rather than a negative spike.
    """
    order = np.argsort(t.to_numpy())
    tt = t.to_numpy(dtype=float)[order]
    yy = total.to_numpy(dtype=float)[order]
    dt, dy = np.diff(tt, prepend=np.nan), np.diff(yy, prepend=np.nan)
    with np.errstate(divide="ignore", invalid="ignore"):
        r = np.where((dy >= 0) & (dt > 0), dy / dt / 1e6, np.nan)
    out = np.empty_like(r)
    out[order] = r
    return pd.Series(out, index=t.index)


def break_gaps(t: np.ndarray, y: np.ndarray, max_gap_s: float = 2.5):
    """Insert NaN where samples are missing for more than max_gap_s, so a
    silenced node shows as a gap instead of a straight line across it."""
    order = np.argsort(t)
    t, y = t[order], y[order]
    gaps = np.where(np.diff(t) > max_gap_s)[0] + 1
    return np.insert(t, gaps, t[gaps - 1] + 1e-9), np.insert(y, gaps, np.nan)


def plot_resources(run: Run, fig_dir: Path):
    """CPU, memory, network and queue traces per client machine and cloud node."""
    rs = run.ev[run.ev.kind == "resource_sample"].copy()
    if rs.empty:
        return
    for c in ("net_rx_bytes", "net_tx_bytes"):
        rs[c] = pd.to_numeric(rs[c], errors="coerce")
    rs["net_bytes"] = rs.net_rx_bytes + rs.net_tx_bytes
    machines = rs[rs.scope == "machine"]
    nodes = rs[rs.node.notna()]
    # (title, rows, column, group by, transform)
    panels = [
        ("client machine CPU (‰)", machines, "cpu_permille", "machine", None),
        ("client machine memory used (MB)", machines, "mem_used_bytes", "machine", "mb"),
        ("client machine network in+out (MB/s)", machines, "net_bytes", "machine", "rate"),
        ("cloud node CPU (‰)", nodes, "cpu_permille", "node", None),
        ("cloud node memory, RSS (MB)", nodes, "rss_bytes", "node", "mb"),
        ("cloud node network in+out (MB/s)", nodes, "net_bytes", "node", "rate"),
        ("cloud node queue length", nodes, "queue_len", "node", None),
    ]
    panels = [p for p in panels if len(p[1]) and pd.to_numeric(p[1][p[2]], errors="coerce").notna().any()]
    if not panels:
        return
    fig, axes = plt.subplots(len(panels), 1, figsize=(9, 2.3 * len(panels)), sharex=True, squeeze=False)
    for ax, (title, df, col, key, how) in zip(axes[:, 0], panels):
        for name, g in df.groupby(key):
            y = pd.to_numeric(g[col], errors="coerce")
            if how == "mb":
                y = y / 1e6
            elif how == "rate":
                y = counter_rate_mb_s(g.t, y)
            tt, yy = break_gaps(g.t.to_numpy(dtype=float), y.to_numpy(dtype=float))
            label = f"node {int(name)}" if key == "node" else str(name)
            ax.plot(tt, yy, label=label, lw=.9)
        ax.set_ylabel(title, fontsize=8)
        ax.legend(fontsize=7, ncol=4)
        ax.grid(alpha=.3)
    axes[-1, 0].set_xlabel("time (s)")
    fig.tight_layout()
    fig.savefig(fig_dir / f"resources_{run.id}.png", dpi=140)
    plt.close(fig)


# ---------------------------------------------------------------- output


def md_table(df: pd.DataFrame, floats=3) -> str:
    if df.empty:
        return "(no data)\n"
    def f(v):
        return f"{v:.{floats}g}" if isinstance(v, (float, np.floating)) else str(v)
    head = "| " + " | ".join(df.columns) + " |\n|" + "---|" * len(df.columns) + "\n"
    return head + "".join("| " + " | ".join(f(v) for v in r) + " |\n" for r in df.itertuples(index=False))


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("runs", type=Path, help="directory of run directories")
    ap.add_argument("--out", type=Path, default=Path("results/summary"))
    ap.add_argument("--window-ms", type=int, default=500, help="window for failover detection")
    ap.add_argument("--timelines", type=int, default=3, help="failure timeline figures to draw")
    a = ap.parse_args(argv)

    runs = load_runs(a.runs)
    if not runs:
        print(f"no runs found under {a.runs}", file=sys.stderr)
        return 1
    tab, fig = a.out / "tables", a.out / "figures"
    tab.mkdir(parents=True, exist_ok=True)
    fig.mkdir(parents=True, exist_ok=True)

    per_run = pd.DataFrame([run_latency(r) for r in runs])
    agg = aggregate(per_run)
    fails = pd.DataFrame([f for r in runs for f in failures(r, w=a.window_ms / 1000)])
    fair = pd.DataFrame([x for x in (fairness(r) for r in runs) if x])
    cnt = pd.DataFrame([counters(r) for r in runs])
    for name, df in [("per_run", per_run), ("aggregate", agg), ("failures", fails),
                     ("fairness", fair), ("counters", cnt)]:
        df.to_csv(tab / f"{name}.csv", index=False)

    plot_load(agg, fig)
    drawn = 0
    for r in runs:
        plot_resources(r, fig)
        if drawn < a.timelines and (r.ev.kind == "inject_start").any():
            plot_timeline(r, fig)
            drawn += 1

    md = ["# Phase 1 results summary\n", f"{len(runs)} runs, {int(per_run.n_ok.sum())} measured operations.\n"]
    md += ["\n## Latency and throughput (mean over seeds, 95% CI half-width)\n", md_table(agg)]
    if len(fails):
        g = fails.groupby("label")[["failover_s", "view_removal_s", "recovery_s", "peak_p99_ms", "retries_during"]]
        md += ["\n## Injected failures (mean over injections)\n", md_table(g.mean().reset_index())]
    if len(fair):
        md += ["\n## Load distribution\n", md_table(fair.groupby("label").mean(numeric_only=True).reset_index())]
    md += ["\n## Counters\n", md_table(cnt.groupby("label").mean(numeric_only=True).reset_index())]
    (a.out / "summary.md").write_text("".join(md))
    print(f"wrote {a.out}/summary.md, {len(runs)} runs")
    return 0


if __name__ == "__main__":
    sys.exit(main())
