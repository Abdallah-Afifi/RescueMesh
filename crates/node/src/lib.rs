//! The cloud node. [`task::NodeTask`] is the core task that owns all protocol
//! state on one node (design log DL-017); the `rm-node` binary will run it on
//! the async runtime with the transport (workstream B) and the planning pool
//! (workstream D).

pub mod task;

pub use task::{NodeTask, Out, PlanJob, PlanPool};
