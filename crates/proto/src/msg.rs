//! Wire messages (Sections 5–9).
//!
//! Three channels:
//! - client ↔ node over TCP: [`ClientMsg`];
//! - node ↔ node over TCP: [`Envelope`] carrying a [`NodeMsg`];
//! - node → node heartbeats over UDP: [`Heartbeat`].
//!
//! **Acknowledgment.** Every node-to-node envelope has a per-peer `msg_id`
//! that increases by one per message, and carries `ack_up_to`: the highest
//! `msg_id.n` received in order from the destination's current incarnation.
//! A sender keeps each message until it is covered by `ack_up_to`, and
//! retransmits after `timeouts.retransmit_ms` until then or until the peer
//! leaves the view. If there is nothing else to send, a bare [`NodeMsg::Ack`]
//! goes out after `timeouts.ack_delay_ms`. TCP success alone is never treated
//! as delivery, because a silenced node drops messages inside the process.
//! Receivers discard any `msg_id` they have already seen.

use crate::domain::{OpResult, Request, Write};
use crate::entry::Entry;
use crate::ids::{Epoch, MsgId, NodeId, ReqId, ResourceId, Seq, Version, ViewNo};
use serde::{Deserialize, Serialize};

/// Bumped on any incompatible change to these types. Receivers drop frames
/// with a different version and log an error.
pub const PROTOCOL_VERSION: u16 = 1;

// ---------------------------------------------------------------- client ↔ node

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClientMsg {
    Req(ClientReq),
    Reply(ClientReply),
}

/// Sent by the client middleware to all three nodes (Section 5.3, step 1).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientReq {
    pub version: u16,
    pub req: Request,
    /// The client's latest measured round-trip time to each node, indexed by
    /// [`NodeId::index`]. `None` until measured.
    pub rtt_us: [Option<u32>; 3],
    /// Highest `seq` of this client incarnation whose reply has been received.
    /// Nodes may forget request-table rows at or below it.
    pub acked_up_to: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientReply {
    pub req: ReqId,
    pub from: NodeId,
    pub body: ReplyBody,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplyBody {
    Ok(OpResult),
    /// The planning queue is full; the client backs off and retries.
    Busy,
    Error(String),
}

// ---------------------------------------------------------------- node ↔ node

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Envelope {
    pub version: u16,
    pub from: NodeId,
    pub msg_id: MsgId,
    pub ack_up_to: u64,
    pub body: NodeMsg,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeMsg {
    /// Carries only the envelope's `ack_up_to`.
    Ack,

    // Executor selection (Section 5.3, steps 3–5)
    Claim {
        req: ReqId,
        view: ViewNo,
        key: ClaimKey,
    },

    // Commit path (Section 5.3, steps 6–9)
    Propose(Proposal),
    ProposeReply {
        req: ReqId,
        outcome: ProposeOutcome,
    },
    Append {
        epoch: Epoch,
        prev_seq: Seq,
        prev_epoch: Epoch,
        entries: Vec<Entry>,
        commit: Seq,
    },
    AppendAck {
        epoch: Epoch,
        /// False when the follower's log did not match at `prev_seq`.
        ok: bool,
        /// Highest seq the follower now holds that matches the coordinator.
        match_seq: Seq,
    },

    // Coordinator election (Section 7)
    PreVote {
        next_epoch: Epoch,
        last_epoch: Epoch,
        last_seq: Seq,
    },
    PreVoteReply {
        next_epoch: Epoch,
        granted: bool,
    },
    Election {
        epoch: Epoch,
        last_epoch: Epoch,
        last_seq: Seq,
    },
    Vote {
        epoch: Epoch,
        granted: bool,
    },
    Coordinator {
        epoch: Epoch,
    },

    // Rejoin (Section 9)
    Join {
        last_epoch: Epoch,
        last_seq: Seq,
    },
    JoinData {
        entries: Vec<Entry>,
        commit: Seq,
    },

    // Operator (Section 8); forwarded to the coordinator by whichever node receives it
    Silence {
        target: SilenceTarget,
        duration_ms: u64,
    },
}

/// Ranking key carried by a claim. Lower wins; the derived `Ord` compares
/// fields in declaration order, so `node` only decides exact ties.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ClaimKey {
    /// Quantized score in steps of `score.quantum_permille`.
    pub score_step: u16,
    /// [`crate::hash::tie_break`] of the request and the claiming node.
    pub tie: u64,
    pub node: NodeId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Proposal {
    pub req: ReqId,
    pub epoch: Epoch,
    pub view: ViewNo,
    pub executor: NodeId,
    /// Version of every resource the plan read (invariant I3).
    pub read_set: Vec<(ResourceId, Version)>,
    pub writes: Vec<Write>,
    pub result: OpResult,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProposeOutcome {
    Committed(Seq),
    /// The request already has a committed result; this is it (invariant I1).
    Duplicate(OpResult),
    /// A resource in the read set changed; plan again from a fresh snapshot.
    Stale {
        current: Seq,
    },
    NotCoordinator {
        hint: Option<NodeId>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SilenceTarget {
    Node(NodeId),
    Coordinator,
    /// The node currently ranked first for new requests.
    CurrentRank1,
}

// ---------------------------------------------------------------- heartbeats (UDP)

/// Sent every `timeouts.heartbeat_ms` from a dedicated thread (Section 6).
/// Must fit in [`crate::codec::MAX_DATAGRAM`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Heartbeat {
    pub version: u16,
    pub from: NodeId,
    pub incarnation: u32,
    /// Increases by one per heartbeat, so receivers can count missed ones.
    pub n: u64,
    pub epoch: Epoch,
    pub coordinator: Option<NodeId>,
    pub view: ViewNo,
    pub commit: Seq,
    pub applied: Seq,
    pub load: LoadSample,
}

/// Load metrics for the score (Section 5.1). Integers so that every node
/// quantizes identically.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LoadSample {
    pub queue_len: u32,
    /// CPU utilization in thousandths, 0..=1000.
    pub cpu_permille: u16,
    /// Moving average of recent planning time, in microseconds.
    pub service_ewma_us: u32,
}
