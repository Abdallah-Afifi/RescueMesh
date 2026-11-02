# RescueMesh

Term project for CSCE 4411 (Fall 2026, Prof. Amr El-Kadi): a simulated smart-campus emergency-response system built as a three-node peer cloud plus peer-to-peer clients, written in Rust. It is an educational system and is not for real emergency use.

The project is graded on distributed-systems design, measurement and documentation: election, load balancing, fault tolerance, replicated state, discovery, and offline reconciliation.

## Status

| Phase | Due | State |
|---|---|---|
| 0. Election algorithms survey | 4 Oct 2026 | Submitted. Revised after review (version 2). |
| 1. Cloud processing, load balancing, fault tolerance | 4 Nov 2026, 8 PM | Implemented and integrated in `rm-node`; reviewed and fixed before deployment (design log DL-066 to DL-073). Next: runs on the six machines, see `docs/deployment.md`. |
| 2. Discovery, P2P, offline reconciliation | 5 Dec 2026, 8 PM | Not started. |

## Where things are

| Path | What it is |
|---|---|
| `docs/spec/RescueMesh_Term_Project_v2.pdf` | The project description from the instructor. |
| `docs/protocol-v2.md` | The cloud protocol we implement in Phase 1. Start here. |
| `config/` | Run configuration: `cluster.example.toml` for the six machines, `local.toml` for one machine. |
| `docs/deployment.md` | Setting up and running the six machines, the demo and the experiments. |
| `docs/phase1-plan.md` | Phase 1 requirements, who owns what, interfaces, schedule. |
| `docs/design-log.md` | Every design decision, the alternatives, and the reason. Add an entry whenever a decision is made or changed. |
| `docs/phase0/104_survey_v1_submitted.pdf` | The survey exactly as submitted. Do not edit. |
| `docs/phase0/104_survey_v2.docx` | The corrected survey, for reuse in the Phase 1 and final reports. |
| `docs/phase0/survey-errata.md` | What changed between the two survey versions, with sources. |
| `docs/phase0/figures/` | The request-flow figure used in the survey and the script that draws it. |

## Building

Install Rust with [rustup](https://rustup.rs). The compiler version is pinned in `rust-toolchain.toml`, and rustup installs it on first use.

```sh
cargo test --workspace                                  # all tests
cargo clippy --workspace --all-targets -- -D warnings   # lint, as CI does
cargo run -p node -- --config config/local.toml --id 1  # one cloud node
```

| Crate | Owner | Purpose |
|---|---|---|
| `crates/proto` | shared | Types, messages, traits, config, event schema. Changes need a design-log entry. |
| `crates/replication` | A | Replicated log, coordinator election, view, rejoin, fault injection. Fully tested (see design log DL-037). |
| `crates/incident` | D | Incident service: data model, deterministic planner (priority, Hungarian assignment, responder list, route), replicated state (DL-020, DL-056 to DL-063, DL-071, DL-072) |
| `crates/selection` | C | Executor selection: MWBE score, baseline policies, request table, claims, load knob (see design log DL-038). |
| `crates/node` | A–D | Core task `NodeTask` (replication + selection + planning pool, DL-052); runtime, transport, heartbeats and φ-accrual failure detector (B, DL-053–055, `docs/workstream-b.md`); the planning pool (D, DL-060); the cloud node binary `rm-node` |
| `crates/client` | E | Client middleware library: multicast, retries, RTT probes, event-log writer, `testkit` stand-in cloud |
| `crates/loadgen` | E | Workload generator and frontend `rm-loadgen`; stand-in cloud `rm-stub` |
| `analysis/` | E | `analyze.py`: logs to tables and plots |
| `experiments/` | E | `run_sweep.py`: configurations x seeds on the six machines |
| `crates/ctl` | A, B | Operator tool `rm-ctl` (status, targeted silence, load knob) |

## Working agreements

These come from the project description.

- Keep the design log current. Decisions that changed, with reasons, are part of the final report.
- Keep a clear commit history; every member should be able to explain the whole architecture.
- Define message formats, timeouts, retry policy and state ownership in `docs/protocol-v2.md` before implementing them.
- Build the workload generator and the fault-injection control before the features they test.
- State in the report which functions come from libraries and which protocols we wrote ourselves.

## Team

Hanna Mostafa, Joudy El Gayar, Youssef Badawy, Sara Faheem, Abdallah Afifi.
