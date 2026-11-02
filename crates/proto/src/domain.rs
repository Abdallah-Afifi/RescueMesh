//! The incident, resource and assignment model (spec pp. 2–3), the operations
//! clients can request, and what they get back. All data is synthetic.
//!
//! Times are milliseconds since the Unix epoch as stamped by the client that
//! sent the request. The planner treats the request's timestamp as "now", so
//! a plan never depends on the executing node's clock.

use crate::ids::{ClientId, IncidentId, ReqId, ResourceId};
use serde::{Deserialize, Serialize};

/// A logical campus zone, a vertex of the synthetic campus graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ZoneId(pub u16);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Category {
    BlockedRoad,
    PowerOutage,
    LabIncident,
    Flooding,
    NetworkOutage,
    MedicalAssistance,
    TechnicalAssistance,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum IncidentStatus {
    Open,
    Assigned,
    Resolved,
    Expired,
    /// A conflict that could not be resolved automatically (spec p. 3).
    Reconciling,
}

/// Severity from 1 (minor) to 5 (critical).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Severity(pub u8);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Incident {
    pub id: IncidentId,
    pub owner: ClientId,
    pub category: Category,
    pub zone: ZoneId,
    pub severity: Severity,
    pub reported_at_ms: u64,
    pub status: IncidentStatus,
    pub ttl_ms: u64,
    /// Optional small test attachment, represented by metadata only.
    pub attachment: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum ResourceKind {
    Medic,
    Technician,
    SecurityOfficer,
    Vehicle,
    Generator,
    FirstAidKit,
    Volunteer,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceOffer {
    pub id: ResourceId,
    pub owner: ClientId,
    pub kind: ResourceKind,
    pub capacity: u32,
    pub available: u32,
    pub zone: ZoneId,
    pub expires_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Assignment {
    pub incident: IncidentId,
    pub resource: ResourceId,
    pub units: u32,
    pub created_at_ms: u64,
    pub expires_at_ms: u64,
}

/// What a client asks for. Every variant except `Query` changes state and
/// therefore goes through claim, propose and commit (Section 5.3).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Op {
    /// Report an incident; the executor computes a response plan.
    ReportIncident {
        category: Category,
        zone: ZoneId,
        severity: Severity,
        ttl_ms: u64,
        attachment: Option<String>,
    },
    OfferResource {
        kind: ResourceKind,
        capacity: u32,
        zone: ZoneId,
        lease_ms: u64,
    },
    UpdateResource {
        resource: ResourceId,
        available: u32,
        extend_lease_ms: Option<u64>,
    },
    /// Read-only. Answered by any node from its applied state, which may be
    /// slightly behind the coordinator's.
    Query(Query),
}

impl Op {
    pub fn is_write(&self) -> bool {
        !matches!(self, Op::Query(_))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Query {
    Incident(IncidentId),
    IncidentsInZone(ZoneId),
    Resource(ResourceId),
}

/// One client request as every module sees it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Request {
    pub id: ReqId,
    pub sent_at_ms: u64,
    pub op: Op,
}

/// A response plan (spec p. 3).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Plan {
    pub incident: IncidentId,
    /// Higher is more urgent. Integer so every machine computes the same value.
    pub priority: i64,
    pub assignments: Vec<Assignment>,
    /// Recommended sequence of zones for the first assigned responder.
    pub route: Vec<ZoneId>,
    /// Prioritized responder list (spec p. 3): capable resources, best first,
    /// ranked by the total dispatch cost if the incident were given each one.
    /// The substitutes if the assigned responder cannot go (DL-059).
    pub responders: Vec<ResourceId>,
}

/// A state change carried by a committed `Result` entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Write {
    PutIncident(Incident),
    PutResource(ResourceOffer),
    PutAssignment(Assignment),
}

/// What the client receives.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum OpResult {
    Planned(Plan),
    Accepted,
    Incident(Option<Incident>),
    Incidents(Vec<Incident>),
    Resource(Option<ResourceOffer>),
    /// The operation was understood but cannot be applied, for example an
    /// update to a resource that does not exist.
    Refused(String),
}
