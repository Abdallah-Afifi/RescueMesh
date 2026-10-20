//! Workstream A: the replicated log and everything that orders it.
//!
//! [`Core`] implements, for one cloud node, the parts of `docs/protocol-v2.md`
//! that keep the three replicas consistent:
//!
//! - the commit path at the coordinator: duplicate check, resource-version
//!   check, sequencing, replication and commit (Section 5.3, steps 6–9);
//! - the executor side of proposing: routing to the coordinator, retrying and
//!   reporting the outcome;
//! - coordinator election with pre-vote, epoch fencing, Bully-ordered
//!   candidacy and a self-fencing lease (Section 7);
//! - the membership view and the node's ACTIVE / SYNCING status (Section 6);
//! - rejoin after a silence (Section 9);
//! - scheduled and operator-requested fault injection (Section 8).
//!
//! The core performs no I/O and reads no clock. The node runtime feeds it
//! inputs with the current monotonic time in milliseconds and carries out the
//! [`Action`]s it returns. That makes every behaviour reproducible and lets
//! the tests in `tests/` run the whole protocol under a simulated network.
//!
//! # Driving the core from the node runtime
//!
//! One task owns the [`Core`] (design log DL-017). It calls:
//!
//! | When | Call |
//! |---|---|
//! | every 5 ms or so | [`Core::tick`] |
//! | a `NodeMsg` arrives from a peer | [`Core::on_message`] |
//! | the failure detector changes its opinion of a peer | [`Core::set_suspected`] |
//! | selection's executor has a planned result | [`Core::propose`] |
//! | the operator tool sends `SILENCE` | [`Core::silence`] |
//!
//! and carries out every returned [`Action`] in order:
//!
//! | Action | Runtime does |
//! |---|---|
//! | `Send` | `Transport::send` |
//! | `Apply` | `StateMachine::apply`; `View` entries also go to selection |
//! | `ProposeDone` | hands the outcome to the executor: reply to the client on `Committed`/`Duplicate`, plan again on `Stale` |
//! | `SetSilenced` | `Transport::set_silenced` |
//! | `Event` | writes an event-log line |
//!
//! Selection may claim requests only while [`Core::is_active`] is true and
//! ranks the members of [`Core::view`]. Heartbeats report [`Core::epoch`],
//! [`Core::coordinator`], [`Core::view`] and [`Core::commit_seq`].
//!
//! A plan that changes a resource must include a `PutResource` write for it,
//! and the state machine must report a resource's version as the `seq` of the
//! last entry that wrote it; the coordinator's version check relies on both.

mod core;
pub mod rng;

pub use crate::core::{Action, Core, CoreCfg, InjectSchedule, Role, Status};
