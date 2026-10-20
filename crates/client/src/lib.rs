//! Client middleware (workstream E, `docs/protocol-v2.md` Section 5.3 and 5.5).
//!
//! To provide: `submit(Op) -> Result<ClientReply>` that sends the request to
//! all three nodes, measures round-trip time per node, retries with the same
//! `ReqId` using exponential backoff with jitter, and reports `acked_up_to`.
