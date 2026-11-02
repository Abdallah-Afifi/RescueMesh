//! The incident processing service (workstream D): the committed state of
//! incidents, resources and assignments ([`State`], a
//! [`proto::StateMachine`]), and the deterministic response planner
//! ([`IncidentPlanner`], a [`proto::Planner`]) over the synthetic campus map.
//! The planning worker pool that runs the planner is `node::workers`.
//!
//! Design decisions: DL-020, DL-056 to DL-059 in `docs/design-log.md`.

pub mod graph;
pub mod hungarian;
pub mod planner;
pub mod state;

pub use planner::IncidentPlanner;
pub use state::State;
