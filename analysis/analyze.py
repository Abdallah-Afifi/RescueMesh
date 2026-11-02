#!/usr/bin/env python3
"""Turn raw event logs into the tables and plots of the Phase 1 report.

    python analysis/analyze.py results/raw --out results/summary

Needs Python 3.11+ and the packages in requirements.txt.

Input: a directory of run directories, or of one directory per sweep (each
sweep run into its own --out, e.g. results/raw/<sweep>/<run>). Each run
directory holds the
`*.jsonl` event logs of every process (schema: crates/proto/src/event.rs),
the client machines' `client<m>-summary.json` and, if the experiment runner
made it, a `meta.json`:

    {"label": "mwbe-load400", "series": "mwbe_cached", "x": 400,
     "x_name": "offered load (ops/s)", "seed": 3, "mode": "open",
     "concurrent_users": 600, ...}

Without meta.json the seed is read from a run id ending in `-s<seed>` and
the label is the rest of the id. A run's sweep is its directory below the
root when nested, else meta.json's `sweep`, else the run id's first word;
configurations are (sweep, label), so sweeps never mix. Directories whose
name starts with `_` (the runner's `_failed/`) are skipped.

Every number in the output is computed from the logs only. Requests are
classified by the phase in which they started (warm-up is discarded). All
timestamps are corrected by each machine's logged clock offset (offset is
positive when the machine's clock is ahead; corrected = local - offset).

Outputs (under --out):
    tables/per_run.csv            one row per run: latency, throughput, errors
    tables/aggregate.csv          per configuration, mean and 95% CI over seeds
    tables/failures.csv           one row per injected failure (failover, view removal, recovery)
    tables/injection_phases.csv   per injection: latency/throughput before, during, after the silence
    tables/elections.csv          one row per coordinator election (durations, trigger)
    tables/suspicions.csv         per run: false-suspicion and wrongful-removal rates
    tables/imbalance.csv          per load-knob episode: the loaded node's share of plans
    tables/fairness.csv           per-run load distribution and Jain's index
    tables/counters.csv           election / view / claim / re-plan / message counters per run
    tables/health.csv             per run: dropped events, core-loop lag, client CPU, uncorrected clocks
    tables/machines.csv           every machine: cores, CPU model, memory, OS, clock source and offset
    tables/overhead.csv           per node: heartbeat and protocol message/byte rates, per role
    tables/*_by_config.csv        per (sweep, label): averaged within a seed, then mean and 95% CI over seeds
                                  (failures, phases, elections, imbalance, counters, fairness,
                                  overhead, service time); suspicions pooled over node-hours
    figures/*.png                 load curves per sweep, throughput vs clients, failure timelines
                                  (every injection of one run per configuration), load imbalance
                                  (one run per configuration), resources
    summary.md                    the aggregate tables in Markdown, with data-quality warnings
"""

from __future__ import annotations

import argparse
import gc
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
NODES = (1, 2, 3)
MSG_ROLES = ["msgs_tx_election", "msgs_tx_replication", "msgs_tx_claim", "msgs_tx_commit", "msgs_tx_other"]
# Thresholds for the data-quality warnings (item: overload or missing data).
CORE_LAG_WARN_MS = 100.0
CLIENT_CPU_WARN_PCT = 80.0
UNRESOLVED_WARN_FRAC = 0.001   # 0.1% of the measured requests
BACKLOG_WARN = 256             # client requests waiting at a node (the node drops at 4096)


# ---------------------------------------------------------------- loading


class Run:
    """One run: `ev` holds the events the analysis reads (narrow, typed
    columns), `rs` the once-a-second resource samples, `kind_counts` how many
    lines of each kind the logs had (including the kinds that are skipped)."""

    def __init__(self, path: Path, ev: pd.DataFrame, meta: dict, t0_ns: int = 0, summaries=None,
                 rs: pd.DataFrame | None = None, kind_counts: dict | None = None, group: str = ""):
        self.path, self.ev, self.meta = path, ev, meta
        # A run under <root>/<group>/<run> (one --out per sweep) is "<group>/<run>".
        self.id = f"{group}/{path.name}" if group else path.name
        self.fid = self.id.replace("/", "__")  # for file names
        # The sweep: its directory when runs are nested, else meta.json, else
        # the run id up to its first "-" (run ids are <sweep>-<label>-s<seed>).
        self.sweep = group or meta.get("sweep") or path.name.split("-", 1)[0]
        self.t0_ns = t0_ns  # wall-clock origin of `t` (ns), for converting meta times
        self.summaries = summaries or []  # client<m>-summary.json contents
        self.rs = rs if rs is not None else pd.DataFrame(
            {c: pd.Series(dtype=float) for c in ["t", "node", *RS_COLS]} |
            {c: pd.Series(dtype=object) for c in ["machine", "file", "scope"]})
        self.kind_counts = kind_counts or {}

    @property
    def label(self):
        return self.meta["label"]

    @property
    def seed(self):
        return self.meta["seed"]

    def ident(self) -> dict:
        """The columns that identify a run in every table."""
        return {"run": self.id, "sweep": self.sweep, "label": self.label, "seed": self.seed}


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
    if meta.get("x") is None:
        meta["x"] = float("nan")
    lg = meta.get("loadgen", {}) or {}
    meta.setdefault("mode", lg.get("mode", "unknown"))
    if "concurrent_users" not in meta and "users" in lg:
        meta["concurrent_users"] = int(lg["users"]) * int(lg.get("processes", 4)) * int(meta.get("client_machines", 1))
    meta.setdefault("concurrent_users", float("nan"))
    return meta


def ensure_cols(df: pd.DataFrame, cols) -> pd.DataFrame:
    for c in cols:
        if c not in df.columns:
            df[c] = np.nan
    return df


# What is read from the logs. Everything else (ranked, reply, propose,
# plan_start, req_recv, machine_info, ...: well over half of all lines) is
# counted and skipped before JSON parsing, only these columns are kept, and
# repeated strings become categoricals. Resource samples, whose many counters
# would widen every row, go to a frame of their own.
EV_KINDS = frozenset({
    "client_send", "client_reply", "client_retry", "plan_end", "claim", "commit", "replan", "busy",
    "duplicate_suppressed", "step_down", "suspect", "view_change", "election_start", "election_won",
    "inject_start", "inject_end", "inject_refused", "active", "clock_offset", "load_knob", "machine_info",
    "sync_start"})
EV_CAT = ["kind", "machine", "file", "phase", "outcome", "source", "when", "cpu_model", "os"]
EV_F32 = ["node", "attempt", "epoch", "victim", "k", "view", "peer", "threads", "duty_permille", "cores"]
EV_F64 = ["latency_us", "service_us", "duration_ms", "offset_ns", "mem_bytes", "last_seq", "commit", "applied"]
EV_OBJ = ["req", "active", "on", "by_operator"]
REQ_KINDS = frozenset({"client_retry", "claim"})  # the only kinds whose request id is used
RS_COLS = ["cpu_permille", "rss_bytes", "net_rx_bytes", "net_tx_bytes", "queue_len", "mem_used_bytes",
           "heartbeats_tx", "udp_tx_bytes", "peer_msgs_tx", "peer_retransmits", "peer_bare_acks",
           "client_overload_dropped", "client_backlog",
           "events_dropped", "core_lag_ms_max", "dropped_client", *MSG_ROLES]


def _kind_of(line: str) -> str | None:
    """The `kind` of a JSON event line without parsing it."""
    i = line.find('"kind"')
    if i < 0:
        return None
    j = line.find('"', line.find(":", i + 6) + 1)
    k = line.find('"', j + 1)
    return line[j + 1:k] if j >= 0 and k > j else None


def _read_logs(path: Path):
    """Column lists for events and resource samples, from every *.jsonl."""
    ev = {c: [] for c in ["ts_ns", *EV_CAT, *EV_F32, *EV_F64, *EV_OBJ]}
    rs = {c: [] for c in ["ts_ns", "machine", "node", "file", "scope", *RS_COLS]}
    counts: dict[str, int] = {}
    intern: dict = {}  # one string object per distinct value, not one per line
    # Requests started in the measured window, and requests that got a final
    # reply (ok or gave_up; busy and error are per attempt and retried). Kept
    # as sets of ids while reading only: the difference is the unresolved count.
    started: set = set()
    finished: set = set()

    def iv(x):
        return intern.setdefault(x, x) if isinstance(x, str) else x

    for f in sorted(path.glob("*.jsonl")):
        stem = iv(f.stem)
        with f.open() as fh:
            for line in fh:
                kind = _kind_of(line)
                if kind is None:
                    continue
                counts[kind] = counts.get(kind, 0) + 1
                is_rs = kind == "resource_sample"
                if not is_rs and kind not in EV_KINDS:
                    continue
                try:
                    d = json.loads(line)
                except json.JSONDecodeError:
                    continue  # a line cut off by a killed process
                ts = d.get("ts_ns")
                if ts is None:
                    continue
                if kind == "client_send":
                    if d.get("attempt") == 0 and d.get("phase") == "measure" and d.get("req"):
                        started.add(d["req"])
                elif kind == "client_reply" and d.get("outcome") in ("ok", "gave_up") and d.get("req"):
                    finished.add(d["req"])
                if is_rs:
                    rs["ts_ns"].append(ts)
                    rs["machine"].append(iv(d.get("machine")))
                    rs["node"].append(d.get("node"))
                    rs["file"].append(stem)
                    rs["scope"].append(iv(d.get("scope")))
                    for c in RS_COLS:
                        rs[c].append(d.get(c))
                    continue
                ev["ts_ns"].append(ts)
                ev["kind"].append(iv(kind))
                ev["file"].append(stem)
                for c in ("machine", "phase", "outcome", "source", "when", "cpu_model", "os"):
                    ev[c].append(iv(d.get(c)))
                for c in EV_F32:
                    ev[c].append(d.get(c))
                for c in EV_F64:
                    ev[c].append(d.get(c))
                ev["req"].append(d.get("req") if kind in REQ_KINDS else None)
                ev["active"].append(d.get("active") if kind == "view_change" else None)
                ev["on"].append(d.get("on"))
                ev["by_operator"].append(d.get("by_operator"))
    counts["_measure_started"] = len(started)
    counts["_unresolved"] = len(started - finished)
    return ev, rs, counts


def _frame(cols: dict, cats, f32, f64) -> pd.DataFrame:
    out = {"ts_ns": np.asarray(cols.pop("ts_ns"), dtype=np.int64)}
    for c in list(cols):
        v = cols.pop(c)
        if c in cats:
            out[c] = pd.Categorical(v)
        elif c in f32 or c in f64:
            x = pd.to_numeric(pd.Series(v, dtype=object), errors="coerce")
            out[c] = x.astype("float32" if c in f32 else "float64")
        else:
            out[c] = pd.Series(v, dtype=object)
    return pd.DataFrame(out)


def load_run(path: Path, group: str = "") -> Run | None:
    evc, rsc, counts = _read_logs(path)
    if not evc["ts_ns"] and not rsc["ts_ns"]:
        return None
    ev = _frame(evc, set(EV_CAT), set(EV_F32), set(EV_F64))
    rs = _frame(rsc, {"machine", "file", "scope"}, {"node"}, set(RS_COLS))
    # Clock correction: mean of the offsets logged by each machine. A machine
    # without a usable offset stays uncorrected; health() warns about it.
    co = ev[(ev.kind == "clock_offset") & ev.offset_ns.notna()]
    offs = co.offset_ns.groupby(co.machine.astype(str)).mean()
    big = np.iinfo(np.int64).max
    t0 = int(min(ev.ts_ns.min() if len(ev) else big, rs.ts_ns.min() if len(rs) else big))
    for df in (ev, rs):
        off = df.machine.astype(str).map(offs).fillna(0.0).to_numpy(dtype=float)
        df["t"] = (df.ts_ns.to_numpy(dtype=np.int64) - t0 - np.round(off).astype(np.int64)) / 1e9
    summaries = []
    for f in sorted(path.glob("client*-summary.json")):
        try:
            summaries.append(json.loads(f.read_text()))
        except (json.JSONDecodeError, OSError):
            pass
    return Run(path, ev, parse_meta(path), t0, summaries, rs, counts, group)


def run_dirs(root: Path) -> list[tuple[Path, str]]:
    """(run directory, group) under root. A directory with *.jsonl files is a
    run; any other directory is searched (one --out per sweep gives
    <root>/<sweep>/<run>), and its path below root becomes the run's group.
    `_failed/` and other `_` directories are skipped."""
    out = []

    def walk(d: Path, group: str):
        for p in sorted(x for x in d.iterdir() if x.is_dir() and not x.name.startswith("_")):
            if any(p.glob("*.jsonl")):
                out.append((p, group))
            else:
                walk(p, f"{group}/{p.name}" if group else p.name)

    walk(root, "")
    return out


def iter_runs(root: Path):
    """Runs one at a time, so only one run's events are in memory. The
    previous run is released before the next is loaded (the generator's own
    reference would otherwise keep two alive at the peak)."""
    for d, group in run_dirs(root):
        r = load_run(d, group)
        if r is not None:
            yield r
        del r
        gc.collect()


def load_runs(root: Path) -> list[Run]:
    """All runs at once: for tests and small directories (main() uses iter_runs)."""
    return list(iter_runs(root))


def num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def load_span(run: Run) -> tuple[float, float]:
    """(first, last) client send of any phase: when load was offered at all."""
    s = run.ev[run.ev.kind == "client_send"]
    if s.empty:
        t = np.concatenate([run.ev.t[run.ev.node.notna()].to_numpy(), run.rs.t[run.rs.node.notna()].to_numpy()])
        return (float(t.min()), float(t.max())) if len(t) else (0.0, 0.0)
    return float(s.t.min()), float(s.t.max())


def measure_span(run: Run) -> tuple[float, float]:
    """(first, last) first-attempt send in the measured window; the node log span without clients."""
    s = run.ev[(run.ev.kind == "client_send") & (run.ev.phase == "measure")]
    if len(s) > 1:
        return float(s.t.min()), float(s.t.max())
    return load_span(run)


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
        **run.ident(), "series": run.meta["series"],
        "x": run.meta["x"], "x_name": run.meta["x_name"], "mode": run.meta["mode"],
        "concurrent_users": run.meta["concurrent_users"], "seed": run.seed,
        "n_ok": len(ok), "n_gave_up": len(gu), "n_busy_replies": len(busy),
        # Started in the measured window but never finished (no ok, no gave_up
        # reply): past saturation, or the drain was too short. Throughput and the
        # latency percentiles are over ok replies only, so they EXCLUDE these.
        "n_started": run.kind_counts.get("_measure_started", float("nan")),
        "unresolved": run.kind_counts.get("_unresolved", float("nan")),
        "window_s": span,
        "throughput": len(ok) / span if span and span > 0 else float("nan"),
        "mean_ms": float(lat.mean()) if len(lat) else float("nan"),
        "max_ms": float(lat.max()) if len(lat) else float("nan"),
        "retried_frac": float((ok.attempt > 0).mean()) if len(ok) else float("nan"),
        "gave_up_frac": len(gu) / max(len(ok) + len(gu), 1),
    }
    for name, p in PCTS.items():
        out[f"{name}_ms"] = pct(lat, p)
    st_ = out["n_started"]
    out["unresolved_frac"] = out["unresolved"] / st_ if st_ == st_ and st_ else float("nan")
    # Planner service time (the cost knob's effect), plans finished in the measured window.
    lo, hi = measure_span(run)
    pe = ev[(ev.kind == "plan_end") & (ev.t >= lo) & (ev.t <= hi)].service_us.to_numpy(dtype=float)
    pe = pe[~np.isnan(pe)]
    out.update(plans=len(pe), service_mean_us=float(pe.mean()) if len(pe) else float("nan"),
               service_p50_us=pct(pe, 50), service_p99_us=pct(pe, 99))
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


METRICS = ["throughput", "mean_ms", "p50_ms", "p95_ms", "p99_ms", "retried_frac", "gave_up_frac", "unresolved_frac",
           "service_mean_us", "service_p50_us", "service_p99_us"]


def aggregate(per_run: pd.DataFrame) -> pd.DataFrame:
    rows = []
    if "sweep" not in per_run:
        per_run = per_run.assign(sweep="")
    for (sweep, series, label), g in per_run.groupby(["sweep", "series", "label"], sort=False):
        row = {"sweep": sweep, "series": series, "label": label, "x": g.x.iloc[0], "seeds": len(g)}
        for c in ("x_name", "mode", "concurrent_users"):
            if c in g:
                row[c] = g[c].iloc[0]
        for m in METRICS:
            if m not in g:
                continue
            mean, half, _ = ci95(g[m])
            row[m], row[m + "_ci95"] = mean, half
        rows.append(row)
    return pd.DataFrame(rows)


# ---------------------------------------------------------------- injections


def window_p99(t: np.ndarray, lat: np.ndarray, lo: float, hi: float, w: float) -> np.ndarray:
    """p99 of latency in consecutive windows of w seconds over [lo, hi); NaN if empty."""
    return window_stats(t, lat, lo, hi, w)[0]


def window_stats(t: np.ndarray, lat: np.ndarray, lo: float, hi: float, w: float):
    """(p99, count) of latency in consecutive windows of w seconds over [lo, hi)."""
    n = max(int(round((hi - lo) / w)), 0)
    idx = np.floor((t - lo) / w).astype(int)
    p99, cnt = np.full(n, np.nan), np.zeros(n, dtype=int)
    for i in range(n):
        sel = lat[idx == i]
        cnt[i] = len(sel)
        if len(sel):
            p99[i] = np.percentile(sel, 99)
    return p99, cnt


def first_by_k(df: pd.DataFrame, victim_col="node") -> pd.DataFrame:
    """One row per injection k, preferring the victim's own event."""
    if df.empty or "k" not in df or df.k.isna().all():
        return df.iloc[0:0]  # no usable injection numbers: nothing to match
    rows = []
    for k, g in df.groupby("k"):
        mine = g[g.node == g.victim] if "victim" in g and g.victim.notna().any() else g.iloc[0:0]
        rows.append((mine if len(mine) else g).sort_values("t").iloc[0])
    return pd.DataFrame(rows)


def injections(run: Run, min_window_s: float = 0.5) -> pd.DataFrame:
    """One row per injected silence, sorted by start: k, victim, t0, dur, t_end, in_window.

    t_end is the victim's `inject_end` (matched by k), else t0 + duration.
    `in_window` is false for a silence that starts outside the measured
    window or less than `min_window_s` before its end (during warm-up, or
    after the load stopped, as automatic injection often does): there is
    nothing to measure, so failures(), injection_phases() and the timelines
    skip it. Every injection, measured or not, still bounds its neighbours'
    baseline and search windows."""
    ev = run.ev
    starts = ev[ev.kind == "inject_start"]
    if starts.empty:
        return pd.DataFrame(columns=["k", "victim", "t0", "dur", "t_end", "by_operator", "in_window"])
    starts = first_by_k(starts)
    ends = ev[ev.kind == "inject_end"]
    ends = first_by_k(ends) if len(ends) else ends
    rows = []
    for _, s in starts.iterrows():
        t0, dur = float(s.t), float(s.duration_ms) / 1000.0
        e = ends[ends.k == s.k] if len(ends) else ends
        t_end = float(e.t.iloc[0]) if len(e) else t0 + dur
        rows.append({"k": s.k, "victim": s.victim, "t0": t0, "dur": dur, "t_end": t_end,
                     "by_operator": s.get("by_operator", np.nan)})
    out = pd.DataFrame(rows).sort_values("t0").reset_index(drop=True)
    lo, hi = measure_span(run)
    out["in_window"] = (out.t0 >= lo) & (out.t0 <= hi - min_window_s)
    return out


def ok_replies(run: Run, measure_only=False) -> tuple[np.ndarray, np.ndarray]:
    ev = run.ev
    ok = ev[(ev.kind == "client_reply") & (ev.outcome == "ok")]
    if measure_only:
        ok = ok[ok.phase == "measure"]
    return ok.t.to_numpy(dtype=float), ok.latency_us.to_numpy(dtype=float) / 1000.0


def failures(run: Run, w=0.5, band_mult=1.5, band_abs_ms=5.0, band_q=95.0, hold_s=2.0,
             baseline_s=10.0, settle_s=5.0, onset_max_s=5.0, post_s=30.0, min_per_window=20,
             starve_frac=0.25, spike_mult=2.0) -> list[dict]:
    """Per injection: the p99 excursion and how long until it is back in band.

    Definition (refines DL-044 and DL-049; why: docs/workstream-e.md):

    * Windows. Completions (`client_reply` ok, by completion time) are cut
      into windows of `w` seconds, widened to a multiple of `w` so that the
      baseline expects at least `min_per_window` completions per window. A
      window's p99 over fewer samples is just its maximum, which made single
      slow replies look like excursions at low load.
    * Baseline. The windows in the `baseline_s` before the silence, but never
      before the load started or before `settle_s` after the previous
      injection ended. b = median window p99. Band = max(band_mult x b,
      b + band_abs_ms, the band_q-th percentile of the baseline windows' p99):
      the band includes the noise the system shows when nothing is wrong.
    * A window is in band if its p99 <= band and it has at least starve_frac x
      the baseline's mean completions (an empty or starved window, where
      service stalled, is a breach, as in DL-044).
    * Excursion. The first breach within `onset_max_s` of the silence that is
      confirmed: starved, or p99 > spike_mult x band, or followed by a second
      breach. A single slightly-high window is noise. No confirmed breach in
      that interval: `no_excursion`, failover_s = 0.
    * Recovery. failover_s = from the start of the silence to the start of
      the first run of `hold_s` seconds of in-band windows after the onset.
    * Horizon. The search ends at the next injection's start, the end of the
      offered load, or `post_s` after the silence ends, whichever is first, so
      a later injection or an unrelated spike is never matched. No recovery
      before it: failover_s is NaN and `censored` is true.

    Only injections inside the measured window are reported (`injections`).
    `failover_lb_s` is failover_s, or the horizon for a censored one (a lower
    bound), so averages do not silently drop the worst cases.

    Also per injection: time to the first suspicion of the victim
    (`detect_s`), to the committed VIEW removal (`view_removal_s`), from the
    end of the silence to the victim's `active` (`recovery_s`), the peak p99,
    and the number of requests that needed a client retry during the
    failover (`retries_during`; during the first window if no excursion).
    """
    inj = injections(run)
    if inj.empty:
        return []
    ev = run.ev
    t_ok, lat_ok = ok_replies(run)
    load_lo, load_hi = load_span(run)
    views = view_table(run)
    won = ev[ev.kind == "election_won"].sort_values("t")
    out = []
    for i, s in inj.iterrows():
        if not s.in_window:
            continue
        t0, victim, dur, t_end = s.t0, s.victim, s.dur, s.t_end
        coord = won[won.t <= t0].node.iloc[-1] if (won.t <= t0).any() else np.nan
        nxt = inj.t0.iloc[i + 1] if i + 1 < len(inj) else math.inf
        prev_end = inj.t_end.iloc[i - 1] if i > 0 else -math.inf
        row = {**run.ident(), "k": s.k,
               "victim": victim, "duration_s": dur, "t0": t0, "t_end": t_end,
               "victim_role": ("unknown" if coord != coord else "coordinator" if coord == victim else "follower"),
               "retries_during": float("nan"), "censored": False}
        b_lo = max(t0 - baseline_s, prev_end + settle_s, load_lo)
        hi = min(t0 + dur + post_s, nxt, load_hi)
        row["horizon_s"] = hi - t0
        in_base = (t_ok >= b_lo) & (t_ok < t0)
        rate = in_base.sum() / (t0 - b_lo) if t0 > b_lo else 0.0
        we = w
        if rate > 0:
            we = min(w * math.ceil(min_per_window / (rate * w)), 4 * w)
        row["window_s"] = we
        nb = int((t0 - b_lo) // we)
        base, bcnt = window_stats(t_ok, lat_ok, t0 - nb * we, t0, we) if nb > 0 else (np.array([]), np.array([]))
        base_ok = base[~np.isnan(base)]
        if len(base_ok) >= 3:
            b = float(np.median(base_ok))
            band = max(b * band_mult, b + band_abs_ms, float(np.percentile(base_ok, band_q)))
            min_cnt = max(1.0, starve_frac * float(bcnt.mean()))
            post, pcnt = window_stats(t_ok, lat_ok, t0, t0 + max(int((hi - t0) // we), 0) * we, we)
            starved = pcnt < min_cnt
            good = ~starved & ~np.isnan(post) & (post <= band)
            bad = ~good
            hold = max(1, int(round(hold_s / we)))
            n_on = min(len(post), int(math.ceil(onset_max_s / we)))
            onset = None
            for j in range(n_on):
                if not bad[j]:
                    continue
                spike = not np.isnan(post[j]) and post[j] > spike_mult * band
                if starved[j] or spike or (j + 1 < len(post) and bad[j + 1]):
                    onset = j
                    break
            if onset is None:
                k_ok, row["no_excursion"] = 0, True
            else:
                row["no_excursion"] = False
                k_ok = next((j for j in range(onset + 1, len(post) - hold + 1) if good[j:j + hold].all()), None)
                row["censored"] = k_ok is None
            upto = len(post) if k_ok is None else max(k_ok, 1)
            seen = post[:upto]
            row.update(baseline_p99_ms=b, band_ms=band, min_count=min_cnt,
                       onset_s=(onset * we) if onset is not None else float("nan"),
                       peak_p99_ms=float(np.nanmax(seen)) if np.isfinite(seen).any() else float("nan"),
                       failover_s=(k_ok * we) if k_ok is not None else float("nan"))
            span = (k_ok * we + we) if k_ok is not None else hi - t0
            rt = ev[(ev.kind == "client_retry") & (ev.t >= t0) & (ev.t < t0 + span)]
            # Requests that needed a retry (protocol Section 10), not retry events.
            row["retries_during"] = int(rt.req.nunique()) if "req" in rt else len(rt)
        sus = ev[(ev.kind == "suspect") & (num(ev.peer) == victim) & (ev.t >= t0) & (ev.t < hi)]
        if len(sus):
            row["detect_s"] = float(sus.t.min() - t0)
        for _, v in views[views.t >= t0].iterrows():
            if victim not in v.active:
                row["view_removal_s"] = float(v.t - t0)
                break
        act = ev[(ev.kind == "active") & (ev.node == victim) & (ev.t >= t_end) & (ev.t < max(nxt, t_end + 1))]
        if len(act):
            row["recovery_s"] = float(act.t.min() - t_end)
        row.update(convergence(run, victim, t_end, float(act.t.min()) if len(act) else math.nan))
        f = row.get("failover_s", float("nan"))
        row["failover_lb_s"] = row["horizon_s"] if row["censored"] else f
        out.append(row)
    return out


def convergence(run: Run, victim, t_end: float, t_active: float) -> dict:
    """How the victim rejoined: catch-up from its `sync_start` (its log
    position at waking) to `active` (applied position when re-admitted), and
    whether it did any work in between. A node must not plan or claim before
    it has converged (protocol recovery rule); `worked_before_active` counts
    the `plan_end` and `claim` events it logged in that interval."""
    ev = run.ev
    mine = ev[(ev.node == victim) & (ev.t >= t_end)]
    out = {"sync_s": math.nan, "catch_up_entries": math.nan, "worked_before_active": 0}
    sync = mine[mine.kind == "sync_start"]
    if len(sync) and t_active == t_active:
        s0 = sync.sort_values("t").iloc[0]
        out["sync_s"] = t_active - float(s0.t)
        act = mine[(mine.kind == "active") & (mine.t == t_active)]
        if len(act) and pd.notna(act.applied.iloc[0]) and pd.notna(s0.last_seq):
            out["catch_up_entries"] = float(act.applied.iloc[0] - s0.last_seq)
    if t_active == t_active:
        out["worked_before_active"] = int((mine.kind.isin(["plan_end", "claim"]) & (mine.t < t_active)).sum())
    return out


def injection_phases(run: Run, pre_s=10.0, horizon_s=30.0, settle_s=5.0, min_s=0.5) -> list[dict]:
    """Per injection, measured-window requests (by completion time) before the
    silence, during it, and after it until the next injection or `horizon_s`.

    `before` starts at most `pre_s` earlier and never before `settle_s` after
    the previous injection ended, so it is a clean baseline. Only injections
    inside the measured window count, every phase is clipped to it, and a
    phase shorter than `min_s` is left out."""
    inj = injections(run)
    if inj.empty:
        return []
    ev = run.ev
    ok = ev[(ev.kind == "client_reply") & (ev.outcome == "ok") & (ev.phase == "measure")]
    gu = ev[(ev.kind == "client_reply") & (ev.outcome == "gave_up") & (ev.phase == "measure")]
    lo_m, hi_m = measure_span(run)
    out = []
    for i, s in inj.iterrows():
        if not s.in_window:
            continue
        prev_end = inj.t_end.iloc[i - 1] if i > 0 else -math.inf
        nxt = inj.t0.iloc[i + 1] if i + 1 < len(inj) else math.inf
        spans = {"before": (max(s.t0 - pre_s, prev_end + settle_s, lo_m), s.t0),
                 "during": (s.t0, min(s.t_end, hi_m)),
                 "after": (s.t_end, min(nxt, s.t_end + horizon_s, hi_m))}
        for phase, (a, b) in spans.items():
            if b - a < min_s:
                continue  # nothing measured in it (e.g. the load ended during the silence)
            sel = ok[(ok.t >= a) & (ok.t < b)]
            lat = sel.latency_us.to_numpy(dtype=float) / 1000.0
            dur = b - a
            out.append({**run.ident(), "k": s.k, "victim": s.victim,
                        "phase": phase, "start_s": a - s.t0, "end_s": b - s.t0, "n_ok": len(sel),
                        "throughput": len(sel) / dur if dur > 0 else float("nan"),
                        "mean_ms": float(lat.mean()) if len(lat) else float("nan"),
                        "p50_ms": pct(lat, 50), "p99_ms": pct(lat, 99),
                        "retried_frac": float((sel.attempt > 0).mean()) if len(sel) else float("nan"),
                        "gave_up": int(((gu.t >= a) & (gu.t < b)).sum())})
    return out


# ---------------------------------------------------------------- membership, elections


def view_table(run: Run) -> pd.DataFrame:
    """One row per distinct view number: when it was first applied (by any
    node) and its active set. Views are counted once, not once per node."""
    vc = run.ev[(run.ev.kind == "view_change") & run.ev.view.notna()]
    if vc.empty:
        return pd.DataFrame(columns=["view", "t", "active"])
    rows = []
    for v, g in vc.groupby("view"):
        g = g.sort_values("t")
        a = g.active.iloc[0]
        rows.append({"view": int(v), "t": float(g.t.iloc[0]), "active": list(a) if isinstance(a, list) else []})
    return pd.DataFrame(rows).sort_values("view").reset_index(drop=True)


def elections(run: Run, max_election_s=10.0, trigger_s=10.0) -> list[dict]:
    """One row per coordinator election, i.e. per distinct epoch won.

    * Start: the earliest `election_start` with phase "pre_vote", by any node,
      in the `max_election_s` before the win and after the previous win (the
      earliest start of any phase if the log has no phases).
    * duration_s: from that start to `election_won`.
    * Trigger: the previous coordinator's failure. from_inject_s is from its
      `inject_start` (the silence began), from_suspect_s from the first
      `suspect` of it by a peer, both looked for in the `trigger_s` before
      the start. The first election of a run has no previous coordinator and
      is marked `startup`.
    """
    ev = run.ev
    won = ev[ev.kind == "election_won"].sort_values("t")
    if won.empty:
        return []
    won = won.groupby(num(won.epoch), dropna=False).head(1)  # one winner per epoch
    starts = ev[ev.kind == "election_start"]
    pre = starts[starts.phase == "pre_vote"]
    if pre.empty:
        pre = starts
    inj = ev[ev.kind == "inject_start"]
    sus = ev[ev.kind == "suspect"]
    out, prev_t, prev_coord = [], -math.inf, None
    for _, w in won.iterrows():
        cand = pre[(pre.t <= w.t) & (pre.t > max(prev_t, w.t - max_election_s))]
        t_start = float(cand.t.min()) if len(cand) else float("nan")
        row = {**run.ident(), "epoch": w.epoch, "winner": w.node,
               "t_won": float(w.t), "t_start": t_start, "startup": prev_coord is None,
               "duration_s": float(w.t - t_start) if len(cand) else float("nan"),
               "pre_vote_starts": len(cand), "prev_coordinator": prev_coord}
        ref = t_start if len(cand) else float(w.t)
        if prev_coord is not None:
            lo = max(prev_t, ref - trigger_s)
            i = inj[(num(inj.victim) == prev_coord) & (inj.t >= lo) & (inj.t <= ref)]
            s = sus[(num(sus.peer) == prev_coord) & (sus.t >= lo) & (sus.t <= ref)]
            row["from_inject_s"] = float(w.t - i.t.min()) if len(i) else float("nan")
            row["from_suspect_s"] = float(w.t - s.t.min()) if len(s) else float("nan")
        out.append(row)
        prev_t, prev_coord = float(w.t), w.node
    return out


def counter_total(rs: pd.DataFrame, col: str) -> float:
    """Sum over nodes of the increase of a cumulative counter (resets ignored)."""
    total, seen = 0.0, False
    for _, g in rs.groupby("node"):
        y = num(g.sort_values("t")[col]).dropna().to_numpy()
        if len(y):
            seen = True
            d = np.diff(y)
            total += float(d[d > 0].sum()) + 0.0
    return total if seen else float("nan")


def node_samples(run: Run) -> pd.DataFrame:
    rs = run.rs[run.rs.node.notna()]
    if "scope" in rs and rs.scope.notna().any():
        rs = rs[(rs.scope == "node") | rs.scope.isna()]
    return rs


def counters(run: Run) -> dict:
    ev = run.ev
    n = lambda k: int((ev.kind == k).sum())  # noqa: E731
    row = run.ident()
    for k in ["step_down", "suspect", "replan", "duplicate_suppressed", "busy", "commit"]:
        row[k] = n(k)
    inj = injections(run)
    row["injections"] = int(inj.in_window.sum()) if len(inj) else 0
    row["injections_outside_window"] = int((~inj.in_window).sum()) if len(inj) else 0
    vt = view_table(run)
    row["views"] = len(vt)
    row["view_changes"] = max(len(vt) - 1, 0)  # distinct views after the first one
    el = pd.DataFrame(elections(run))
    starts = ev[ev.kind == "election_start"]
    row["election_attempts"] = int((starts.phase == "pre_vote").sum()) if starts.phase.notna().any() else len(starts)
    row["elections"] = len(el)
    later = el[~el.startup] if len(el) else el
    row["elections_after_startup"] = len(later)
    lo, hi = measure_span(run)
    minutes = (hi - lo) / 60 if hi > lo else float("nan")
    if len(el):
        in_win = el[(el.t_won >= lo) & (el.t_won <= hi)]
        row["elections_per_min"] = len(in_win) / minutes if minutes == minutes else float("nan")
        if len(later):
            row["election_mean_s"] = float(later.duration_s.mean())
            row["election_max_s"] = float(later.duration_s.max())
            for c in ("from_suspect_s", "from_inject_s"):
                if c in later and later[c].notna().any():
                    row[c[:-2] + "_mean_s"] = float(later[c].mean())
                    row[c[:-2] + "_max_s"] = float(later[c].max())
        start = el[el.startup]
        if len(start):
            row["startup_election_s"] = float(start.duration_s.iloc[0])
    rs = node_samples(run)
    if len(rs) and rs[MSG_ROLES].notna().any().any():
        span = float(rs.t.max() - rs.t.min()) or float("nan")
        ok = int(((ev.kind == "client_reply") & (ev.outcome == "ok")).sum())
        for c in MSG_ROLES:
            row[c] = counter_total(rs, c)
        row["election_msgs_per_election"] = row["msgs_tx_election"] / len(el) if len(el) else float("nan")
        row["election_msgs_per_s"] = row["msgs_tx_election"] / span
        if ok:
            for c in ("msgs_tx_claim", "msgs_tx_commit", "msgs_tx_replication"):
                row[c.replace("msgs_tx_", "") + "_msgs_per_req"] = row[c] / ok
    cl = ev[ev.kind == "claim"]
    if len(cl):
        per = cl.groupby("req").size()
        row["claims_per_req"] = float(per.mean())
        row["competing_claim_rate"] = float((per > 1).mean())
    if n("commit"):
        row["replan_rate"] = n("replan") / n("commit")
    return row


def suspicions(run: Run, pre_s=5.0, grace_s=2.0, tol_s=0.5) -> dict:
    """False suspicions and wrongful view removals.

    A `suspect` of peer P at time t is justified if P was silenced then: an
    injection of P with t0 - tol_s <= t <= max(t0 + pre_s, t_end + grace_s),
    i.e. an `inject_start` of P in the preceding `pre_s`, or during P's
    silence, plus `grace_s` for the detector and `tol_s` for clock error.
    Every other suspicion is false. Rates are per node-hour of logged node
    time. A view that drops a node that was not silenced is a wrongful removal.
    """
    ev = run.ev
    inj = injections(run)

    def silenced(p, t) -> bool:
        x = inj[num(inj.victim) == p]
        return bool(((x.t0 - tol_s <= t) & (t <= np.maximum(x.t0 + pre_s, x.t_end + grace_s))).any())

    sus = ev[(ev.kind == "suspect") & ev.peer.notna()]
    false = [r for _, r in sus.iterrows() if not silenced(float(r.peer), float(r.t))]
    nodes = pd.concat([ev.loc[ev.node.notna(), ["node", "t"]], run.rs.loc[run.rs.node.notna(), ["node", "t"]]])
    hours = sum(float(g.t.max() - g.t.min()) for _, g in nodes.groupby("node")) / 3600
    vt = view_table(run)
    removals, wrongful = 0, 0
    for i in range(1, len(vt)):
        gone = set(vt.active.iloc[i - 1]) - set(vt.active.iloc[i])
        for p in gone:
            removals += 1
            wrongful += not silenced(p, float(vt.t.iloc[i]))
    return {**run.ident(), "node_hours": hours,
            "suspects": len(sus), "false_suspects": len(false),
            "false_suspects_per_node_hour": len(false) / hours if hours > 0 else float("nan"),
            "false_suspect_detail": "; ".join(f"n{int(r.node)}->n{int(r.peer)}@{r.t:.1f}s" for r in false[:10]),
            "view_removals": removals, "wrongful_removals": wrongful,
            "wrongful_removals_per_node_hour": wrongful / hours if hours > 0 else float("nan")}


# ---------------------------------------------------------------- fairness, imbalance, health


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
    busy = pe.groupby("node").service_us.sum().reindex(list(NODES)).fillna(0.0)
    cnt = pe.groupby("node").size().reindex(list(NODES)).fillna(0)
    row = {**run.ident(), "series": run.meta["series"], "jain_busy": jain(busy), "jain_count": jain(cnt)}
    for n in NODES:
        row[f"busy_share_n{n}"] = float(busy[n] / busy.sum()) if busy.sum() else float("nan")
    # Against capacity (plan 1.4, protocol Section 10): busy time per core.
    # Jain over busy/cores is 1 when every node is equally busy relative to
    # its capacity, whatever the machines' sizes.
    cores = node_cores(run).reindex(list(NODES))
    if cores.notna().all() and (cores > 0).all():
        row["jain_busy_per_core"] = jain(busy / cores)
        for n in NODES:
            row[f"capacity_share_n{n}"] = float(cores[n] / cores.sum())
    return row


def node_cores(run: Run) -> pd.Series:
    """Cores of each cloud node's machine, from its machine_info event."""
    mi = run.ev[(run.ev.kind == "machine_info") & run.ev.node.notna()]
    return mi.groupby("node").cores.first() if len(mi) else pd.Series(dtype=float)


def machines(run: Run) -> list[dict]:
    """Hardware and clock source of every machine of the run (A7)."""
    ev = run.ev
    mi = ev[ev.kind == "machine_info"]
    co = ev[ev.kind == "clock_offset"]
    names = sorted(set(ev.machine.dropna().astype(str)) | set(run.rs.machine.dropna().astype(str)))
    out = []
    for m in names:
        i = mi[mi.machine.astype(str) == m]
        c = co[co.machine.astype(str) == m]
        first = i.iloc[0] if len(i) else None
        out.append({**run.ident(), "machine": m,
                    "cores": float(first.cores) if first is not None else math.nan,
                    "cpu_model": str(first.cpu_model) if first is not None else "",
                    "mem_gb": float(first.mem_bytes) / 1e9 if first is not None and pd.notna(first.mem_bytes) else math.nan,
                    "os": str(first.os) if first is not None else "",
                    "clock_source": ",".join(sorted(set(c.source.dropna().astype(str)))),
                    "clock_offset_ms_mean": float(c.offset_ns.mean() / 1e6) if c.offset_ns.notna().any() else math.nan,
                    "clock_samples": int(c.offset_ns.notna().sum())})
    return out


def overhead(run: Run) -> list[dict]:
    """Per node: heartbeat and protocol traffic rates from the cumulative
    counters in its resource samples (A5)."""
    rs = node_samples(run)
    out = []
    for node, g in rs.groupby("node"):
        span = float(g.t.max() - g.t.min())
        if span <= 0:
            continue
        row = {**run.ident(), "node": int(node), "span_s": span}
        for col, name in [("heartbeats_tx", "heartbeats_per_s"), ("udp_tx_bytes", "heartbeat_bytes_per_s"),
                          ("net_tx_bytes", "net_tx_bytes_per_s"), ("net_rx_bytes", "net_rx_bytes_per_s"),
                          ("peer_msgs_tx", "peer_msgs_per_s"), ("peer_retransmits", "retransmits_per_s"),
                          ("peer_bare_acks", "bare_acks_per_s"), *[(c, c + "_per_s") for c in MSG_ROLES]]:
            if col in g and g[col].notna().any():
                row[name] = counter_total(g, col) / span
        out.append(row)
    return out


def knob_episodes(run: Run) -> pd.DataFrame:
    """Load-knob on/off periods: node, threads, t_on, t_off. From `load_knob`
    events; without them, from the runner's actions in meta.json."""
    ev = run.ev
    kn = ev[ev.kind == "load_knob"].sort_values("t")
    rows = []
    if len(kn):
        for node, g in kn.groupby("node"):
            cur = None
            for _, r in g.iterrows():
                on = bool(r.on) if pd.notna(r.on) else True
                if on and cur is None:
                    cur = {"node": int(node), "threads": r.threads, "duty_permille": r.duty_permille, "t_on": float(r.t),
                           "t_off": float(r.t + r.duration_ms / 1000) if pd.notna(r.duration_ms) and r.duration_ms else math.nan}
                elif not on and cur is not None:
                    cur["t_off"] = float(r.t)
                    rows.append(cur)
                    cur = None
            if cur is not None:
                rows.append(cur)
    else:
        acts = [a for a in run.meta.get("actions", []) if a.get("type") == "load_knob" and a.get("rc") == 0
                and "sent_ms_client0" in a]
        offs = [a for a in run.meta.get("actions", []) if a.get("type") == "load_knob_off" and a.get("rc") == 0]
        for a in acts:
            t_on = (a["sent_ms_client0"] * 1e6 - run.t0_ns) / 1e9
            off = [(o["sent_ms_client0"] * 1e6 - run.t0_ns) / 1e9 for o in offs
                   if o.get("node") == a.get("node") and "sent_ms_client0" in o]
            t_off = min([x for x in off if x > t_on], default=t_on + a["duration_ms"] / 1000
                        if a.get("duration_ms") else math.nan)
            rows.append({"node": a["node"], "threads": a.get("threads"), "duty_permille": a.get("duty"),
                         "t_on": t_on, "t_off": t_off})
    return pd.DataFrame(rows, columns=["node", "threads", "duty_permille", "t_on", "t_off"])


def imbalance(run: Run, span_s=20.0) -> list[dict]:
    """Per load-knob episode: the loaded node's share of completed plans in
    the `span_s` before it, while it is on, and in the `span_s` after it."""
    eps = knob_episodes(run)
    pe = run.ev[run.ev.kind == "plan_end"]
    if eps.empty or pe.empty:
        return []
    start, end = load_span(run)
    out = []
    for _, e in eps.iterrows():
        t_off = e.t_off if e.t_off == e.t_off else end
        row = {**run.ident(), "series": run.meta["series"], "node": e.node, "threads": e.threads, "t_on": e.t_on, "t_off": t_off}
        # Clipped to the offered load, so plans/s is not diluted by idle time.
        for name, (a, b) in {"before": (max(e.t_on - span_s, start), e.t_on),
                             "during": (max(e.t_on, start), min(t_off, end)),
                             "after": (t_off, min(t_off + span_s, end))}.items():
            sel = pe[(pe.t >= a) & (pe.t < b)]
            row[f"share_{name}"] = float((sel.node == e.node).mean()) if len(sel) else float("nan")
            row[f"plans_per_s_{name}"] = len(sel) / (b - a) if b > a else float("nan")
        out.append(row)
    return out


def health(run: Run) -> tuple[dict, list[str]]:
    """Dropped events, core-loop lag and client-dropped arrivals, with warnings."""
    ev, warn = run.ev, []
    row = run.ident()
    rs = node_samples(run)
    nd = {}
    for node, g in rs.groupby("node"):
        d = num(g.events_dropped).max()
        if d == d:
            nd[int(node)] = int(d)
        lag = num(g.core_lag_ms_max)
        if lag.notna().any():
            row[f"core_lag_max_ms_n{int(node)}"] = float(lag.max())
            row[f"core_lag_p99_ms_n{int(node)}"] = float(lag.quantile(0.99))
            if lag.max() > CORE_LAG_WARN_MS:
                over = int((lag > CORE_LAG_WARN_MS).sum())
                what = ("the node was overloaded" if over > 1 else
                        "a single late tick: overload, or a stall of the whole machine (sleep, suspend, swap)")
                warn.append(f"{run.id}: node {int(node)} core-loop lag up to {lag.max():.0f} ms "
                            f"({over} sample(s) over {CORE_LAG_WARN_MS:.0f} ms): {what}")
    row["node_events_dropped"] = sum(nd.values()) if nd else float("nan")
    for n, d in nd.items():
        if d > 0:
            warn.append(f"{run.id}: node {n} dropped {d} events: its log is incomplete")
    cd = [s.get("events_dropped", 0) for s in run.summaries]
    crs = run.rs[run.rs.node.isna()]
    proc_drop = num(crs.groupby("file").events_dropped.max() if len(crs) else pd.Series(dtype=float)).sum()
    row["client_events_dropped"] = int(sum(cd)) if cd else (int(proc_drop) if proc_drop == proc_drop else float("nan"))
    if row["client_events_dropped"] == row["client_events_dropped"] and row["client_events_dropped"] > 0:
        warn.append(f"{run.id}: clients dropped {row['client_events_dropped']} events: client logs are incomplete")
    dc = num(crs.groupby("file").dropped_client.max()).sum() if len(crs) and crs.dropped_client.notna().any() else 0
    row["client_dropped_arrivals"] = int(dc)
    if dc > 0:
        warn.append(f"{run.id}: {int(dc)} open-loop arrivals dropped at the client (max_inflight): "
                    "the client machine limited the load (DL-043)")
    lo, hi = measure_span(run)
    started, unres = run.kind_counts.get("_measure_started", 0), run.kind_counts.get("_unresolved", 0)
    row["unresolved"] = unres
    row["unresolved_frac"] = unres / started if started else float("nan")
    if started and unres / started > UNRESOLVED_WARN_FRAC:
        warn.append(f"{run.id}: {unres} of {started} requests ({100 * unres / started:.1f}%) started in the measured "
                    "window never got a final reply (no ok, no gave_up before the drain ended). Throughput and "
                    "latency percentiles EXCLUDE them, so they flatter this run: it was past saturation, or the "
                    "drain was too short")
    # Overload signals other than core lag (which can stay low while the node collapses).
    ev = run.ev
    busy = int(((ev.kind == "client_reply") & (ev.outcome == "busy") & (ev.t >= lo) & (ev.t <= hi)).sum())
    row["busy_replies"] = busy
    if busy:
        warn.append(f"{run.id}: {busy} Busy replies in the measured window: a planning queue was full (overload)")
    nrs = node_samples(run)
    for node, g in nrs.groupby("node"):
        if "client_overload_dropped" in g and g.client_overload_dropped.notna().any():
            dropped = counter_total(g, "client_overload_dropped")
            row[f"overload_dropped_n{int(node)}"] = dropped
            if dropped > 0:
                warn.append(f"{run.id}: node {int(node)} dropped {dropped:.0f} client requests because its client "
                            "backlog was full (overload)")
        if "client_backlog" in g and g.client_backlog.notna().any():
            gw = g[(g.t >= lo) & (g.t <= hi)].client_backlog.dropna()
            if len(gw):
                row[f"client_backlog_p50_n{int(node)}"] = float(gw.median())
                high = float((gw >= BACKLOG_WARN).mean())
                if high > 0.25:
                    warn.append(f"{run.id}: node {int(node)} had a client backlog of {BACKLOG_WARN} or more in "
                                f"{100 * high:.0f}% of the measured window's samples (median {gw.median():.0f}): "
                                "requests were waiting to be handled (overload)")
    cm = run.rs[(run.rs.scope.astype(str) == "machine") & (run.rs.t >= lo) & (run.rs.t <= hi)]
    worst = 0.0
    for m, g in cm.groupby(cm.machine.astype(str)):
        avg = float(g.cpu_permille.mean()) / 10 if g.cpu_permille.notna().any() else math.nan
        row[f"client_cpu_pct_{m}"] = avg
        if avg == avg:
            worst = max(worst, avg)
            if avg > CLIENT_CPU_WARN_PCT:
                warn.append(f"{run.id}: client machine {m} averaged {avg:.0f}% CPU in the measured window: "
                            "the client side may be the bottleneck, not the cloud (plan 1.4)")
    row["client_cpu_pct_max"] = worst if len(cm) else math.nan
    sd = [s.get("send_dropped", 0) for s in run.summaries]
    row["client_send_dropped"] = int(sum(sd)) if sd else float("nan")
    bad = clock_problems(run)
    row["clock_uncorrected"] = ",".join(sorted(bad))
    if bad:
        warn.append(f"{run.id}: no usable clock offset from {', '.join(f'{m} ({why})' for m, why in sorted(bad.items()))}: "
                    "their timestamps are not corrected, so cross-machine timelines (failover, detection, "
                    "elections) carry their clock error. Harmless if every process ran on one machine; on the "
                    "lab machines check chrony (or timesyncd) and that chronyc is on the PATH of a "
                    "non-interactive ssh shell.")
    return row, warn


def clock_problems(run: Run) -> dict[str, str]:
    """Machines whose `clock_offset` events never gave a usable offset:
    source `unavailable` or `*_unparsed`, a missing offset, or none logged."""
    ev = run.ev
    co = ev[ev.kind == "clock_offset"]
    machines = set(ev.machine.dropna().astype(str)) | set(run.rs.machine.dropna().astype(str))
    out = {}
    for m in sorted(machines):
        g = co[co.machine.astype(str) == m]
        if g.offset_ns.notna().any():
            continue
        srcs = sorted(set(g.source.dropna().astype(str)))
        out[m] = "/".join(srcs) if srcs else "no clock_offset event"
    return out


# ---------------------------------------------------------------- plots


def _slug(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "_", str(s)).strip("_").lower() or "x"


PCT_STYLE = {"p50_ms": ":", "p95_ms": "--", "p99_ms": "-"}


def _errbar(ax, x, y, ci, log: bool, **kw):
    """errorbar whose lower whisker stays above 0 on a log axis and at or
    above 0 on a linear one (throughput and latency cannot be negative)."""
    ci = np.nan_to_num(np.asarray(ci, dtype=float))
    y = np.asarray(y, dtype=float)
    lower = np.minimum(ci, y * 0.9) if log else np.minimum(ci, np.maximum(y, 0))
    return ax.errorbar(x, y, yerr=[lower, ci], capsize=2, **kw)


def plot_load(agg: pd.DataFrame, fig_dir: Path) -> list[Path]:
    """Throughput and latency against each sweep's x, one figure per (sweep,
    x_name), one colour per series (line style: p50 dotted, p95 dashed, p99
    solid). Runs without an x (e.g. imbalance) are left out: they have
    their own figures. A single figure is load_curves.png."""
    if agg.empty:
        return []
    agg = agg.copy()
    if "x_name" not in agg:
        agg["x_name"] = "load level"
    if "sweep" not in agg:
        agg["sweep"] = ""
    agg["x_name"] = agg.x_name.fillna("load level")
    agg = agg[pd.to_numeric(agg.x, errors="coerce").notna()]
    groups = list(agg.groupby(["sweep", "x_name"], sort=False))
    paths = []
    for (sweep, xname), sub in groups:
        fig, ax = plt.subplots(1, 2, figsize=(11, 4))
        colours = plt.rcParams["axes.prop_cycle"].by_key()["color"]
        for j, (series, g) in enumerate(sub.groupby("series", sort=False)):
            g = g.sort_values("x")
            c = colours[j % len(colours)]
            _errbar(ax[0], g.x, g.throughput, g.throughput_ci95, False, marker="o", color=c, label=series)
            for col, ls in PCT_STYLE.items():
                _errbar(ax[1], g.x, g[col], g[col + "_ci95"], True, ls=ls, marker=".", color=c,
                        label=f"{series} {col[:3]}")
        ax[0].set_ylabel("throughput (ok replies / s; unresolved excluded)")
        ax[1].set_ylabel("latency of ok replies (ms): p50 dotted, p95 dashed, p99 solid")
        ax[1].set_yscale("log")
        for a_ in ax:
            a_.set_xlabel(xname)
            a_.grid(alpha=.3)
        ax[0].legend(fontsize=8)
        ax[1].legend(fontsize=6, ncol=3)
        fig.suptitle(f"{sweep + ': ' if sweep else ''}throughput and latency vs {xname} (mean over seeds, 95% CI)")
        fig.tight_layout()
        name = "load_curves.png" if len(groups) == 1 else f"load_curves_{_slug(sweep)}_{_slug(xname)}.png"
        fig.savefig(fig_dir / name, dpi=140)
        plt.close(fig)
        paths.append(fig_dir / name)
    return paths


def plot_clients(agg: pd.DataFrame, fig_dir: Path) -> list[Path]:
    """Closed-loop throughput (and p50/p99) against concurrent users =
    users x processes x client machines, from meta.json; one figure per
    sweep, one colour per series."""
    if agg.empty or "mode" not in agg or "concurrent_users" not in agg:
        return []
    cl = agg[(agg["mode"] == "closed") & agg.concurrent_users.notna()].copy()
    if cl.empty:
        return []
    if "sweep" not in cl:
        cl["sweep"] = ""
    sweeps = list(cl.groupby("sweep", sort=False))
    paths = []
    colours = plt.rcParams["axes.prop_cycle"].by_key()["color"]
    for sweep, sub in sweeps:
        fig, ax = plt.subplots(1, 2, figsize=(11, 4))
        for j, (series, g) in enumerate(sub.groupby("series", sort=False)):
            g = g.sort_values("concurrent_users")
            c = colours[j % len(colours)]
            _errbar(ax[0], g.concurrent_users, g.throughput, g.throughput_ci95, False, marker="o", color=c,
                    label=series)
            for col in ("p50_ms", "p99_ms"):
                _errbar(ax[1], g.concurrent_users, g[col], g[col + "_ci95"], True, ls=PCT_STYLE[col], marker=".",
                        color=c, label=f"{series} {col[:3]}")
        ax[0].set_ylabel("throughput (ok replies / s; unresolved excluded)")
        ax[1].set_ylabel("latency of ok replies (ms): p50 dotted, p99 solid")
        ax[1].set_yscale("log")
        for a_ in ax:
            a_.set_xlabel("concurrent users (users x processes x client machines), closed loop")
            a_.grid(alpha=.3)
            a_.legend(fontsize=7)
        fig.suptitle(f"{sweep + ': ' if sweep else ''}throughput as concurrent clients increase "
                     "(mean over seeds, 95% CI)")
        fig.tight_layout()
        name = "throughput_vs_clients.png" if len(sweeps) == 1 else f"throughput_vs_clients_{_slug(sweep)}.png"
        fig.savefig(fig_dir / name, dpi=140)
        plt.close(fig)
        paths.append(fig_dir / name)
    return paths


def plot_timeline(run: Run, fig_dir: Path, w=None, pre=10.0, post=30.0, k=None) -> list[Path]:
    """One figure per injection (or only injection `k`): completions per second
    and windowed p99 around the silence, cut at the next injection and at the
    end of the load. The p99 windows are the failover metric's own (aligned
    on the silence, same width unless `w` is given), with its in-band limit."""
    ev = run.ev
    inj = injections(run)
    if inj.empty:
        return []
    t, lat = ok_replies(run)
    views = view_table(run)
    sus = ev[ev.kind == "suspect"]
    won = ev[ev.kind == "election_won"]
    fo = {f["k"]: f for f in failures(run)}
    load_hi = load_span(run)[1]
    paths = []
    for i, s in inj.iterrows():
        if (k is not None and s.k != k) or not s.in_window:
            continue
        t0, dur = s.t0, s.dur
        f = fo.get(s.k)
        ww = w or (f or {}).get("window_s") or 0.5
        nxt = inj.t0.iloc[i + 1] if i + 1 < len(inj) else math.inf
        prev = inj.t_end.iloc[i - 1] if i > 0 else -math.inf
        lo = t0 - ww * math.floor((t0 - max(t0 - pre, prev)) / ww)
        hi = min(s.t_end + post, nxt, load_hi)
        n = max(int(math.ceil((hi - lo) / ww)), 1)
        hi = lo + n * ww
        w_ = ww
        p99 = window_p99(t, lat, lo, hi, w_)
        thr = np.histogram(t, bins=n, range=(lo, hi))[0] / w_
        xs = lo + w_ * (np.arange(n) + .5) - t0
        fig, ax = plt.subplots(2, 1, figsize=(9, 5), sharex=True)
        ax[0].plot(xs, thr)
        ax[0].set_ylabel("completions / s")
        ax[1].plot(xs, p99)
        ax[1].set_ylabel(f"p99 latency per {w_:g} s (ms)")
        ax[1].set_yscale("log")
        ax[1].set_xlabel("seconds since the injected silence began")
        marks = [(0, "silence starts", "r"), (s.t_end - t0, "silence ends", "g")]
        sv = sus[(num(sus.peer) == s.victim) & (sus.t >= t0) & (sus.t < hi)]
        if len(sv):
            marks.append((float(sv.t.min() - t0), "suspected", "m"))
        rem = views[(views.t >= t0) & (views.t < hi)]
        rem = rem[[s.victim not in a for a in rem.active]]
        if len(rem):
            marks.append((float(rem.t.iloc[0] - t0), "view removal", "k"))
        wn = won[(won.t >= t0) & (won.t < hi)]
        if len(wn):
            marks.append((float(wn.t.min() - t0), "new coordinator", "b"))
        act = ev[(ev.kind == "active") & (ev.node == s.victim) & (ev.t >= s.t_end) & (ev.t < hi)]
        if len(act):
            marks.append((float(act.t.min() - t0), "victim active", "c"))
        if f is not None and f.get("failover_s") == f.get("failover_s") and not f.get("no_excursion", True):
            marks.append((f["failover_s"], f"back in band ({f['failover_s']:.2f} s)", "orange"))
        for a in ax:
            for x, label, c in marks:
                a.axvline(x, color=c, ls="--", lw=1, label=label if a is ax[0] else None)
            a.grid(alpha=.3)
        if f is not None and f.get("band_ms") == f.get("band_ms") and "band_ms" in f:
            ax[1].axhline(f["band_ms"], color="grey", ls=":", lw=1)
            ax[1].text(xs[0], f["band_ms"], " in-band limit", color="grey", fontsize=7, va="bottom")
        ax[0].legend(fontsize=7, loc="upper right", ncol=2)
        kk = int(s.k) if pd.notna(s.k) else i
        vic = int(s.victim) if pd.notna(s.victim) else "?"
        fig.suptitle(f"Failure timeline: {run.id}, injection k={kk}, victim n{vic}, {dur:g} s")
        fig.tight_layout()
        p = fig_dir / f"timeline_{run.fid}_k{kk}.png"
        fig.savefig(p, dpi=140)
        plt.close(fig)
        paths.append(p)
    return paths


def plot_imbalance(run: Run, fig_dir: Path, bin_s=1.0) -> Path | None:
    """Each node's share of completed plans per second, with the load knob's
    on/off times, and each node's CPU and queue length: the evidence for the
    load-imbalance demo."""
    ev = run.ev
    pe = ev[ev.kind == "plan_end"]
    if pe.empty:
        return None
    eps = knob_episodes(run)
    rs = node_samples(run)
    lo, hi = float(pe.t.min()), float(pe.t.max())
    n = max(int(math.ceil((hi - lo) / bin_s)), 1)
    edges = lo + bin_s * np.arange(n + 1)
    counts = np.array([np.histogram(pe[pe.node == nd].t, bins=edges)[0] for nd in NODES], dtype=float)
    tot = counts.sum(axis=0)
    with np.errstate(invalid="ignore", divide="ignore"):
        share = np.where(tot > 0, counts / tot, np.nan)
    xs = edges[:-1] + bin_s / 2
    panels = 3 if len(rs) else 1
    fig, axes = plt.subplots(panels + 1, 1, figsize=(10, 2.4 * (panels + 1)), sharex=True)
    axes = np.atleast_1d(axes)
    for j, nd in enumerate(NODES):
        axes[0].plot(xs, counts[j] / bin_s, lw=.9, label=f"node {nd}")
        axes[1].plot(xs, share[j], lw=.9, label=f"node {nd}")
    axes[0].set_ylabel("plans completed / s", fontsize=8)
    axes[1].set_ylabel("share of plans", fontsize=8)
    axes[1].set_ylim(0, 1)
    if panels == 3:
        for nd, g in rs.groupby("node"):
            for ax, col in ((axes[2], "cpu_permille"), (axes[3], "queue_len")):
                gg = g[num(g[col]).notna()]
                if gg.empty:
                    continue
                tt, yy = break_gaps(gg.t.to_numpy(dtype=float), num(gg[col]).to_numpy(dtype=float))
                ax.plot(tt, yy, lw=.9, label=f"node {int(nd)}")
        axes[2].set_ylabel("node CPU (‰)", fontsize=8)
        axes[3].set_ylabel("queue length", fontsize=8)
    for _, e in eps.iterrows():
        for ax in axes:
            ax.axvspan(e.t_on, e.t_off if e.t_off == e.t_off else hi, color="orange", alpha=.15)
            ax.axvline(e.t_on, color="r", ls="--", lw=1)
            if e.t_off == e.t_off:
                ax.axvline(e.t_off, color="g", ls="--", lw=1)
        axes[0].text(e.t_on, axes[0].get_ylim()[1], f" knob on n{int(e.node)} ({int(e.threads) if e.threads == e.threads else '?'} threads)",
                     color="r", fontsize=7, va="top")
    for ax in axes:
        ax.grid(alpha=.3)
        ax.legend(fontsize=7, ncol=3, loc="upper right")
    axes[-1].set_xlabel("time (s)")
    fig.suptitle(f"Load imbalance: {run.id}" + ("" if len(eps) else " (no load-knob events)"))
    fig.tight_layout()
    p = fig_dir / f"imbalance_{run.fid}.png"
    fig.savefig(p, dpi=140)
    plt.close(fig)
    return p


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
    """CPU, memory, network, queue and core-loop lag traces per client machine and cloud node."""
    rs = run.rs.copy()
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
        ("cloud node core-loop lag, max per s (ms)", nodes, "core_lag_ms_max", "node", None),
    ]
    panels = [p for p in panels if len(p[1]) and pd.to_numeric(p[1][p[2]], errors="coerce").notna().any()]
    if not panels:
        return
    fig, axes = plt.subplots(len(panels), 1, figsize=(9, 2.3 * len(panels)), sharex=True, squeeze=False)
    for ax, (title, df, col, key, how) in zip(axes[:, 0], panels):
        for name, g in df.groupby(key):
            g = g[pd.to_numeric(g[col], errors="coerce").notna()]  # other sample kinds lack the column
            if g.empty:
                continue
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
    lo, hi = measure_span(run)
    inj = injections(run)
    for ax in axes[:, 0]:
        ax.axvspan(lo, hi, color="green", alpha=.06, lw=0)
        for _, s in inj.iterrows():
            ax.axvspan(s.t0, s.t_end, color="red", alpha=.15, lw=0)
    axes[0, 0].set_title("green: measured window; red: injected silences", fontsize=8)
    axes[-1, 0].set_xlabel("time (s)")
    fig.tight_layout()
    fig.savefig(fig_dir / f"resources_{run.fid}.png", dpi=140)
    plt.close(fig)


# ---------------------------------------------------------------- output


def md_table(df: pd.DataFrame, floats=3) -> str:
    if df.empty:
        return "(no data)\n"

    def f(v):
        return f"{v:.{floats}g}" if isinstance(v, (float, np.floating)) else str(v)
    head = "| " + " | ".join(map(str, df.columns)) + " |\n|" + "---|" * len(df.columns) + "\n"
    return head + "".join("| " + " | ".join(f(v) for v in r) + " |\n" for r in df.itertuples(index=False))


def _with_sweep(df: pd.DataFrame) -> pd.DataFrame:
    return df if "sweep" in df or df.empty else df.assign(sweep="")


def by_label(df: pd.DataFrame, how: str) -> pd.DataFrame:
    """Numeric columns per (sweep, label) (mean or max over runs), without the seed."""
    if df.empty:
        return df
    g = _with_sweep(df).drop(columns=[c for c in ("seed",) if c in df]).groupby(["sweep", "label"], sort=False)
    return getattr(g, how)(numeric_only=True).reset_index()


def seed_ci(df: pd.DataFrame, keys: list[str], metrics: list[str]) -> pd.DataFrame:
    """Per configuration: each metric averaged within a seed first, then the
    mean and 95% Student-t half-width over seeds (DL-044: runs, not
    injections or requests, are the independent samples). `seeds` is the
    number of seeds with data."""
    metrics = [m for m in metrics if m in df]
    if df.empty or not metrics:
        return pd.DataFrame()
    df = _with_sweep(df)
    if keys[0] != "sweep":
        keys = ["sweep", *keys]
    per_seed = df.groupby([*keys, "seed"], sort=False, dropna=False)[metrics].mean().reset_index()
    rows = []
    for kv, g in per_seed.groupby(keys, sort=False, dropna=False):
        row = dict(zip(keys, kv if isinstance(kv, tuple) else (kv,)))
        row["seeds"] = int(g.seed.nunique())
        for m in metrics:
            mean, half, _ = ci95(g[m].astype(float))
            row[m], row[m + "_ci95"] = mean, half
        rows.append(row)
    return pd.DataFrame(rows)


def failures_by_config(fails: pd.DataFrame) -> pd.DataFrame:
    """Failure measures per (label, victim role) with CIs over seeds.
    failover_s here is `failover_lb_s`: a censored failover counts as its
    horizon (a lower bound) instead of dropping out; `censored` says how many."""
    if fails.empty:
        return pd.DataFrame()
    f = _with_sweep(fails.copy())
    f["failover_s"] = f["failover_lb_s"] if "failover_lb_s" in f else f.get("failover_s")
    t = seed_ci(f, ["label", "victim_role"], ["failover_s", "detect_s", "view_removal_s", "recovery_s",
                                              "sync_s", "catch_up_entries", "peak_p99_ms", "retries_during"])
    keys = ["sweep", "label", "victim_role"]
    g = f.groupby(keys, sort=False)
    counts = pd.DataFrame({"injections": g.size(), "censored": g.censored.sum().astype(int),
                           "no_baseline": g.failover_s.apply(lambda x: int(x.isna().sum()))}).reset_index()
    if "worked_before_active" in f:
        counts["worked_before_active"] = g.worked_before_active.sum().astype(int).to_numpy()
    return t.merge(counts, on=keys, how="left")


def suspicion_rates(susp: pd.DataFrame) -> pd.DataFrame:
    """Counts and node-hours summed over seeds, then divided (a pooled rate)."""
    if susp.empty:
        return pd.DataFrame()
    g = _with_sweep(susp).groupby(["sweep", "label"], sort=False)[[
        "node_hours", "suspects", "false_suspects", "view_removals", "wrongful_removals"]].sum().reset_index()
    g["false_suspects_per_node_hour"] = g.false_suspects / g.node_hours.where(g.node_hours > 0)
    g["wrongful_removals_per_node_hour"] = g.wrongful_removals / g.node_hours.where(g.node_hours > 0)
    return g


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("runs", type=Path, help="directory of run directories, or of one directory per sweep")
    ap.add_argument("--out", type=Path, default=Path("results/summary"))
    ap.add_argument("--window-ms", type=int, default=500, help="window for failover detection")
    ap.add_argument("--hold-s", type=float, default=2.0, help="in-band time that counts as recovered")
    ap.add_argument("--onset-max-s", type=float, default=5.0,
                    help="an excursion must start this soon after the silence to be its failover")
    ap.add_argument("--timelines", type=int, default=1,
                    help="runs per configuration (sweep and label) whose failure timelines are drawn, all injections")
    ap.add_argument("--imbalance", type=int, default=1,
                    help="runs per configuration with a load knob whose imbalance figure is drawn")
    a = ap.parse_args(argv)

    if not a.runs.is_dir():
        print(f"no runs found under {a.runs}", file=sys.stderr)
        return 1
    tab, fig = a.out / "tables", a.out / "figures"
    tab.mkdir(parents=True, exist_ok=True)
    fig.mkdir(parents=True, exist_ok=True)

    # One run at a time: compute its rows and figures, then drop its events,
    # so memory does not grow with the number of runs.
    names = ["per_run", "failures", "injection_phases", "elections", "suspicions", "imbalance", "fairness",
             "counters", "health", "machines", "overhead"]
    rows: dict[str, list] = {n: [] for n in names}
    warnings: list[str] = []
    n_runs = 0
    # Figures are chosen per configuration (sweep, label), not the first runs
    # in alphabetical order: every failure target and every policy of a knob
    # sweep gets its figure.
    drawn: dict = {}
    drawn_imb: dict = {}
    for r in iter_runs(a.runs):
        n_runs += 1
        rows["per_run"].append(run_latency(r))
        rows["failures"] += failures(r, w=a.window_ms / 1000, hold_s=a.hold_s, onset_max_s=a.onset_max_s)
        rows["injection_phases"] += injection_phases(r)
        rows["elections"] += elections(r)
        rows["suspicions"].append(suspicions(r))
        rows["imbalance"] += imbalance(r)
        fr = fairness(r)
        if fr:
            rows["fairness"].append(fr)
        rows["counters"].append(counters(r))
        rows["machines"] += machines(r)
        rows["overhead"] += overhead(r)
        h, ws = health(r)
        rows["health"].append(h)
        warnings += ws
        early = [f for f in rows["failures"] if f["run"] == r.id and f.get("worked_before_active", 0) > 0]
        for f in early:
            warnings.append(f"{r.id}: victim n{int(f['victim'])} logged {f['worked_before_active']} plan_end/claim "
                            f"event(s) between waking (k={int(f['k'])}) and its `active`: it worked before converging")
        plot_resources(r, fig)
        key = (r.sweep, r.label)
        if drawn.get(key, 0) < a.timelines and plot_timeline(r, fig):
            drawn[key] = drawn.get(key, 0) + 1
        if drawn_imb.get(key, 0) < a.imbalance and not knob_episodes(r).empty:
            plot_imbalance(r, fig)
            drawn_imb[key] = drawn_imb.get(key, 0) + 1
        del r  # iter_runs frees it before loading the next
    if not n_runs:
        print(f"no runs found under {a.runs}", file=sys.stderr)
        return 1

    t = {n: pd.DataFrame(v) for n, v in rows.items()}
    per_run, fails, phases, elec = t["per_run"], t["failures"], t["injection_phases"], t["elections"]
    susp, imb, fair, cnt, hlth = t["suspicions"], t["imbalance"], t["fairness"], t["counters"], t["health"]
    agg = aggregate(per_run)
    t["aggregate"] = agg
    t["failures_by_config"] = failures_by_config(fails)
    t["phases_by_config"] = seed_ci(phases, ["label", "phase"], ["throughput", "mean_ms", "p50_ms", "p99_ms"])
    later = elec[~elec.startup.astype(bool)] if len(elec) else elec
    t["elections_by_config"] = seed_ci(later, ["label"], ["duration_s", "from_suspect_s", "from_inject_s"])
    t["imbalance_by_config"] = seed_ci(imb, ["label", "node"], ["share_before", "share_during", "share_after"])
    t["counters_by_config"] = seed_ci(cnt, ["label"], [
        "elections_per_min", "election_mean_s", "election_max_s", "elections_after_startup", "view_changes",
        "election_msgs_per_election", "election_msgs_per_s", "claim_msgs_per_req", "commit_msgs_per_req",
        "competing_claim_rate", "replan_rate", "injections"])
    t["fairness_by_config"] = seed_ci(fair, ["label"], ["jain_busy", "jain_busy_per_core", "jain_count",
                                                         "busy_share_n1", "busy_share_n2", "busy_share_n3",
                                                         "capacity_share_n1", "capacity_share_n2",
                                                         "capacity_share_n3"])
    ov = t["overhead"]
    t["overhead_by_config"] = seed_ci(ov, ["label", "node"], [c for c in ov.columns if c.endswith("_per_s")]) \
        if len(ov) else pd.DataFrame()
    t["service_by_config"] = seed_ci(per_run, ["label"], ["plans", "service_mean_us", "service_p50_us",
                                                         "service_p99_us"])
    t["suspicions_by_config"] = suspicion_rates(susp)
    for name, df in t.items():
        df.to_csv(tab / f"{name}.csv", index=False)

    plot_load(agg, fig)
    plot_clients(agg, fig)

    md = ["# Phase 1 results summary\n", f"{n_runs} runs, {int(per_run.n_ok.sum())} measured operations. "
          "Every `_ci95` is the 95% t-interval half-width over seeds; values are averaged within a seed first.\n"]
    if warnings:
        md += ["\n## Data-quality warnings\n", "".join(f"- {w}\n" for w in warnings)]
    md += ["\n## Latency and throughput\n",
           "Throughput and latency are over requests with an `ok` reply. Requests that never got a final "
           "reply (`unresolved_frac`) are EXCLUDED from both, so check that column: above 0.1% the run "
           "was past saturation and these numbers flatter it.\n\n", md_table(agg)]
    if len(t["failures_by_config"]):
        md += ["\n## Injected failures (only silences inside the measured window; failover definition in "
               "analyze.failures; a censored failover counts as its horizon, a lower bound)\n",
               md_table(t["failures_by_config"])]
    if len(t["phases_by_config"]):
        md += ["\n## Before / during / after each silence\n", md_table(t["phases_by_config"])]
    if len(t["elections_by_config"]):
        md += ["\n## Coordinator elections after start-up\n", md_table(t["elections_by_config"])]
    if len(t["suspicions_by_config"]):
        md += ["\n## False suspicions and wrongful removals (pooled: total count / total node-hours)\n",
               md_table(t["suspicions_by_config"])]
    if len(t["imbalance_by_config"]):
        md += ["\n## Load imbalance: the loaded node's share of plans\n", md_table(t["imbalance_by_config"])]
    if len(t["fairness_by_config"]):
        md += ["\n## Load distribution (jain_busy_per_core: fairness against capacity)\n",
               md_table(t["fairness_by_config"])]
    if len(t["service_by_config"]):
        md += ["\n## Planner service time (plans finished in the measured window)\n",
               md_table(t["service_by_config"])]
    if len(t["overhead_by_config"]):
        md += ["\n## Heartbeat and protocol overhead per node (rates over the whole run)\n",
               md_table(t["overhead_by_config"])]
    if len(t["counters_by_config"]):
        md += ["\n## Counters\n", md_table(t["counters_by_config"])]
    md += ["\n## Health (worst run: events dropped, core-loop lag)\n", md_table(by_label(hlth, "max"))]
    (a.out / "summary.md").write_text("".join(md))
    for w in warnings:
        print("WARNING:", w, file=sys.stderr)
    print(f"wrote {a.out}/summary.md, {n_runs} runs")
    return 0


if __name__ == "__main__":
    sys.exit(main())
