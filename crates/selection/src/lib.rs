//! Workstream C: choosing which cloud node executes each request.
//!
//! This crate implements Section 5 of `docs/protocol-v2.md`:
//!
//! - [`score`]: the MWBE load score (Section 5.1), in integer thousandths so
//!   that every node computes the identical value (design log DL-023);
//! - [`rankers`]: MWBE and the baselines of Section 10 behind the
//!   [`proto::Ranker`] trait, chosen by [`proto::config::RankerKind`];
//! - [`selector`]: the request table (Section 5.2) and the claim logic
//!   (Section 5.3 steps 2–5, Section 5.4 and Section 5.5);
//! - [`knob`]: the artificial CPU load used by the load-imbalance demo.
//!
//! Like the replication core, the [`Selector`] performs no I/O and reads no
//! clock. The node runtime passes the current monotonic time in milliseconds
//! and carries out the [`Action`]s it returns, so every behaviour can be
//! tested without a network.
//!
//! # Driving the selector from the node runtime
//!
//! The core task (design log DL-017) owns one [`Selector`] next to the
//! replication core and calls:
//!
//! | When | Call |
//! |---|---|
//! | every 5 ms or so | [`Selector::tick`] |
//! | a client `Req` arrives | [`Selector::on_request`] |
//! | a peer's `Claim` arrives | [`Selector::on_claim`] |
//! | a heartbeat arrives, or this node sends its own | [`Selector::on_heartbeat`] |
//! | the core applies a `View` entry, or `Core::is_active` changes | [`Selector::set_view`] |
//! | the core applies a `Result` entry | [`Selector::on_committed`] |
//! | the measured peer round-trip time changes | [`Selector::set_peer_rtt_ms`] |
//!
//! and carries out every returned [`Action`]:
//!
//! | Action | Runtime does |
//! |---|---|
//! | `Ranked` | writes the `ranked` event (`rank`, `policy`) |
//! | `Claim` | `Transport::broadcast(NodeMsg::Claim { .. })` and writes the `claim` event |
//! | `Execute` | queues the request on the planning pool; the result goes to `Core::propose` |
//! | `Abort` | drops the request from the planning queue if it has not been proposed yet |
//! | `Reply` | sends the stored result to the client (a retry of a committed request) |
//!
//! After each call the runtime may pass [`Selector::rank1_hint`] to
//! `Core::set_rank1_hint`, so the operator can silence the node currently
//! ranked first.
//!
//! The executor replies to the client itself when the core reports the
//! proposal committed (protocol Section 5.3, step 9); the selector only
//! answers retries.

pub mod knob;
pub mod rankers;
pub mod score;
pub mod selector;

pub use rankers::make_ranker;
pub use selector::{Action, RowState, Selector, Stats};
