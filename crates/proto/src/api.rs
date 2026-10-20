//! Traits each module implements, and the data passed between them.
//!
//! | Trait          | Implemented by        | Used by                 |
//! |----------------|-----------------------|-------------------------|
//! | [`Planner`]    | incident service (D)  | executor (C), node core |
//! | [`StateMachine`] | incident service (D) | replication core (A)   |
//! | [`Ranker`]     | selection (C)         | node core               |
//! | [`LoadSource`] | incident service (D)  | heartbeats (B), C       |
//! | [`Transport`]  | runtime (B)           | A, C                    |

use crate::domain::{Assignment, Incident, OpResult, Request, ResourceOffer, Write};
use crate::entry::{Entry, View};
use crate::ids::{IncidentId, NodeId, ResourceId, Seq, Version};
use crate::msg::{ClientReq, LoadSample, NodeMsg};
use std::collections::BTreeMap;
use std::sync::Arc;

/// Committed state at one log position. `BTreeMap` everywhere so iteration
/// order is identical on every machine.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Snapshot {
    pub applied: Seq,
    pub incidents: BTreeMap<IncidentId, Incident>,
    pub resources: BTreeMap<ResourceId, (ResourceOffer, Version)>,
    pub assignments: BTreeMap<(IncidentId, ResourceId), Assignment>,
}

/// What the planner returns for one request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanOutput {
    /// Every resource the computation looked at, with the version it saw.
    pub read_set: Vec<(ResourceId, Version)>,
    pub writes: Vec<Write>,
    pub result: OpResult,
}

/// Computes the response to a write request.
///
/// Must be a pure function of its arguments: no clock, no randomness, no I/O,
/// no hash-map iteration, no floating point in anything that affects the
/// output. The same snapshot and request give the same output on every node.
pub trait Planner: Send + Sync {
    fn plan(&self, snapshot: &Snapshot, request: &Request) -> PlanOutput;
}

/// Applies committed entries. Owned by the node's core task; only that task
/// calls `apply`.
pub trait StateMachine: Send {
    /// Entries arrive in `seq` order with no gaps.
    fn apply(&mut self, entry: &Entry);
    /// Cheap: returns a shared, immutable copy for planning threads.
    fn snapshot(&self) -> Arc<Snapshot>;
    fn version_of(&self, resource: ResourceId) -> Option<Version>;
    fn applied(&self) -> Seq;
    /// Answers a read-only request from applied state.
    fn query(&self, request: &Request) -> OpResult;
}

/// What the ranking node knows about one peer when ranking.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PeerScore {
    pub node: NodeId,
    pub load: LoadSample,
    /// Claims seen from this node since its last heartbeat (Section 5.1).
    pub claims_since_heartbeat: u32,
    /// Fraction of this node's recent heartbeats that were missed, 0..=1000.
    pub missed_heartbeats_permille: u16,
}

/// Ranks the members of the view for one request, best first.
///
/// Must be a pure function of its arguments so that every node computes the
/// same ranking from the same inputs. Policies that look random (random,
/// two choices) draw from [`crate::hash`] of the request, never from an RNG;
/// round-robin derives its position from the request ID, not from a counter.
pub trait Ranker: Send + Sync {
    fn name(&self) -> &'static str;
    fn rank(&self, req: &ClientReq, view: &View, scores: &[PeerScore]) -> Vec<NodeId>;
}

/// Current load of this node, for heartbeats and for ranking itself.
pub trait LoadSource: Send + Sync {
    fn sample(&self) -> LoadSample;
}

/// Node-to-node messaging. Implementations queue and return immediately;
/// incoming messages are delivered on a channel created with the transport.
/// Acknowledgment and retransmission happen inside (see [`crate::msg`]).
pub trait Transport: Send + Sync {
    fn send(&self, to: NodeId, body: NodeMsg);
    fn broadcast(&self, body: NodeMsg);
    /// Fault injection: while silenced, drop everything in and out,
    /// including client traffic and heartbeats (Section 8).
    fn set_silenced(&self, silenced: bool);
    fn is_silenced(&self) -> bool;
}
