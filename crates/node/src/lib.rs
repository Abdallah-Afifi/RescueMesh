//! The cloud node.
//!
//! - [`task::NodeTask`]: the core task that owns all protocol state (design
//!   log DL-017, DL-052; workstream A).
//! - [`runtime`]: runs the core task on its own thread and connects it to
//!   the transport, the heartbeat thread, the planning pool and the event
//!   log (workstream B, DL-053, DL-054).
//! - [`net`]: TCP transport for peers, clients and the operator tool, with
//!   the fault-injection silence switch.
//! - [`link`]: application-level acknowledgment and retransmission.
//! - [`heartbeat`] and [`fd`]: UDP heartbeats on a dedicated thread and the
//!   φ-accrual failure detector.
//! - [`dev`]: a placeholder planner and pool until workstream D's lands.

pub mod dev;
pub mod fd;
pub mod heartbeat;
pub mod link;
pub mod net;
pub mod runtime;
pub mod task;

pub use task::{NodeTask, Out, PlanJob, PlanPool};
