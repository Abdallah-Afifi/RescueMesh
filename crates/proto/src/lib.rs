//! Shared contract between all RescueMesh components.
//!
//! Everything that crosses a module or machine boundary is defined here:
//! identifiers, domain types, log entries, wire messages, the traits each
//! module implements, the configuration file and the event-log schema.
//! The protocol these types serve is specified in `docs/protocol-v2.md`;
//! section numbers in comments refer to that document.
//!
//! This crate holds no protocol logic. Changing a type here changes the
//! contract for everyone, so changes need a design-log entry.

pub mod api;
pub mod codec;
pub mod config;
pub mod domain;
pub mod entry;
pub mod event;
pub mod hash;
pub mod ids;
pub mod msg;

pub use api::*;
pub use domain::*;
pub use entry::*;
pub use ids::*;
pub use msg::*;
