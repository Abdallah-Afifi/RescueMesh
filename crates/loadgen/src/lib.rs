//! Workload generator and client-machine frontend (workstream E).
//!
//! On each client machine `rm-loadgen run` (the frontend) starts K worker
//! processes; each worker runs M simulated users as asynchronous tasks
//! (design log DL-018). Every worker writes its own event log, and the
//! frontend merges the workers' summaries.

pub mod frontend;
pub mod params;
pub mod sampler;
pub mod summary;
pub mod worker;
pub mod workload;
