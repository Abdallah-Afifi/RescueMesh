"""Tests for lab_run.py: the shared timetable and the collection of logs.

Run: python -m pytest experiments -q
"""

import json
import sys
import tomllib
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import lab_run  # noqa: E402

REPO = Path(__file__).resolve().parent.parent
LOCAL = tomllib.loads((REPO / "config/local.toml").read_text())
SMOKE = REPO / "experiments/sweeps/smoke.toml"
FAILURE = REPO / "experiments/sweeps/failure.toml"


def test_the_timetable_is_the_same_wherever_it_is_computed():
    a = lab_run.timetable([SMOKE, FAILURE], LOCAL, 2_000_000_000, "1,2", None)
    b = lab_run.timetable([SMOKE, FAILURE], LOCAL, 2_000_000_000, "1,2", None)
    assert lab_run.timetable_id(a) == lab_run.timetable_id(b)
    c = lab_run.timetable([SMOKE, FAILURE], LOCAL, 2_000_000_060, "1,2", None)
    assert lab_run.timetable_id(a) != lab_run.timetable_id(c), "the start time is part of the id"


def test_slots_follow_each_other_without_overlap():
    slots = lab_run.timetable([SMOKE, FAILURE], LOCAL, 2_000_000_000, "1,2", None)
    for s in slots:
        p = s["plan"]
        assert s["start"] < s["lg_start"] < s["load_at"] < s["drain_end"] < s["stop"]
        assert s["drain_end"] - s["load_at"] == p["warmup"] + p["duration"] + p["drain"]
    for a, b in zip(slots, slots[1:]):
        assert b["start"] >= a["stop"] + lab_run.GAP_S, "the next run's nodes start after the last stopped"
    ids = [s["plan"]["run_id"] for s in slots]
    assert len(ids) == len(set(ids))


def test_start_times_parse():
    assert lab_run.parse_start("2000000000") == 2_000_000_000
    assert lab_run.parse_start("2026-11-03 18:30") > 0


def test_a_placeholder_address_is_a_problem():
    cfg = tomllib.loads((REPO / "config/lab.toml").read_text())
    probs = lab_run.role_problems("client1", cfg)
    assert probs and "IPv4" in probs[0]
    assert lab_run.role_problems("cloud1", LOCAL) == []  # 127.0.0.1 is this machine


def _fake_run(d: Path, run_id: str, procs: int, client1_record: dict):
    r = d / run_id
    r.mkdir(parents=True)
    for i in (1, 2, 3):
        (r / f"cloud{i}-node.jsonl").write_text('{"kind":"x"}\n')
        (r / f"client{i}-frontend.jsonl").write_text('{"kind":"x"}\n')
        (r / f"client{i}-summary.json").write_text("{}")
        for p in range(procs):
            (r / f"client{i}-{p}.jsonl").write_text('{"kind":"x"}\n')
    (r / "client1-lab.json").write_text(json.dumps(client1_record))


def test_collect_writes_meta_and_parks_incomplete_runs(tmp_path, monkeypatch):
    monkeypatch.chdir(REPO)
    start = 2_000_000_000
    slots = lab_run.timetable([SMOKE], LOCAL, start, "1,2", None)
    assert len(slots) == 2
    src = tmp_path / "logs"
    procs = int(slots[0]["plan"]["lg"]["processes"])
    actions = [{"type": "failure", "at_s": 4, "rc": 0}, {"type": "load_knob", "at_s": 9, "rc": 0},
               {"type": "failure", "at_s": 15, "rc": 0}]
    _fake_run(src, slots[0]["plan"]["run_id"], procs, {"ready": True, "actions": actions})
    # The second run: cluster not ready, so it must not be reported.
    _fake_run(src, slots[1]["plan"]["run_id"], procs, {"ready": False, "ready_why": "no coordinator",
                                                       "actions": actions})
    out = tmp_path / "raw"
    rc = lab_run.main(["collect", "--config", "config/local.toml", "--start", str(start), "--seeds", "1,2",
                       "--from", str(src), "--out", str(out), str(SMOKE)])
    assert rc == 2
    good = out / "smoke" / slots[0]["plan"]["run_id"]
    meta = json.loads((good / "meta.json").read_text())
    assert meta["sweep"] == "smoke" and meta["seed"] == 1 and meta["client_machines"] == 3
    assert meta["start_at_ms"] == int(slots[0]["load_at"] * 1000)
    assert len(meta["failure_injections"]) == 2
    assert not (out / "smoke" / slots[1]["plan"]["run_id"]).exists()
    failed = list((out / "smoke" / "_failed").iterdir())
    assert len(failed) == 1 and "not ready" in (failed[0] / "failure.txt").read_text()
