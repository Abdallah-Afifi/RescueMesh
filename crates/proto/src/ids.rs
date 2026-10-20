//! Identifiers. All are small `Copy` values with a total order, so they can be
//! used as `BTreeMap` keys; iteration order must never depend on hashing.

use serde::{Deserialize, Serialize};
use std::fmt;

/// A cloud node. Valid values are 1, 2 and 3.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct NodeId(pub u8);

impl NodeId {
    pub const ALL: [NodeId; 3] = [NodeId(1), NodeId(2), NodeId(3)];

    pub fn is_valid(self) -> bool {
        (1..=3).contains(&self.0)
    }

    /// Zero-based index for arrays of per-node values.
    pub fn index(self) -> usize {
        usize::from(self.0) - 1
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "n{}", self.0)
    }
}

/// One simulated user: which client machine, which OS process on it, and
/// which user task inside that process.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ClientId {
    pub machine: u16,
    pub process: u16,
    pub user: u32,
}

impl fmt::Display for ClientId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.machine, self.process, self.user)
    }
}

/// Identifier of one state-changing operation (spec p. 3). Retries reuse it,
/// which is what makes them idempotent. `incarnation` is drawn at random when
/// the client process starts, so a restarted client never reuses an ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ReqId {
    pub client: ClientId,
    pub incarnation: u32,
    pub seq: u64,
}

impl fmt::Display for ReqId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{:08x}/{}", self.client, self.incarnation, self.seq)
    }
}

/// An incident is identified by the request that reported it.
pub type IncidentId = ReqId;
/// A resource offer is identified by the request that created it.
pub type ResourceId = ReqId;

macro_rules! counter {
    ($(#[$doc:meta])* $name:ident) => {
        $(#[$doc])*
        #[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        pub struct $name(pub u64);

        impl $name {
            pub fn next(self) -> Self {
                Self(self.0 + 1)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(f)
            }
        }
    };
}

counter!(
    /// Coordinator term. Increases with every election attempt (Section 7).
    Epoch
);
counter!(
    /// Membership view number (Section 6).
    ViewNo
);
counter!(
    /// Position in the replicated log. 0 means "before the first entry".
    Seq
);
counter!(
    /// Version of a resource: the `Seq` of the last committed entry that wrote it.
    Version
);

impl From<Seq> for Version {
    fn from(s: Seq) -> Self {
        Version(s.0)
    }
}

/// Identifies one node-to-node message for application-level acknowledgment.
/// `incarnation` changes when the sending process restarts; `n` increases by
/// one per message sent to a given peer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct MsgId {
    pub incarnation: u32,
    pub n: u64,
}
