//! Client middleware and measurement support (workstream E,
//! `docs/protocol-v2.md` Sections 5.3 and 5.5).
//!
//! - [`middleware`]: `Middleware` and `Client`, with `Client::submit(Op)`:
//!   multicast to all three nodes, idempotent retries with exponential
//!   backoff and jitter, adaptive timeout, `ackedUpTo`.
//! - [`net`]: the TCP connections and RTT probes behind it.
//! - [`eventlog`]: the JSON-lines writer every process uses for the event log.
//! - [`hist`]: latency histogram that can be summarized and merged.
//! - [`rng`]: seeded random numbers for repeatable workloads.
//! - [`testkit`]: a stand-in cloud for tests and for developing the load
//!   generator before the real nodes exist.

pub mod eventlog;
pub mod hist;
pub mod middleware;
pub mod net;
pub mod rng;
pub mod testkit;

pub use eventlog::EventLog;
pub use middleware::{
    AckTracker, Client, Completed, Middleware, MiddlewareStats, Phase, RetryPolicy, SubmitError,
    fresh_incarnation, now_ms, op_name,
};
pub use net::{ClientNet, NetConfig, NetStats};
