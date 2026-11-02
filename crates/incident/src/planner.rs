//! The deterministic planner (design log DL-020, DL-053, DL-054).
//!
//! A report is planned as a fresh dispatch of the most urgent waiting
//! incidents, the new one always among them:
//!
//! 1. **Priority** of each open incident: severity, plus how long it has
//!    waited. Integers only.
//! 2. **Assignment**: the Hungarian algorithm matches the top `top_k`
//!    incidents to available resources. A pair's cost is the walking distance,
//!    a penalty for a less suitable kind of responder, and a penalty per unit
//!    the resource is short. Each incident may also stay unassigned, at a cost
//!    that grows with its priority, so when two incidents want one resource
//!    the more urgent one gets it. `top_k` is the cost knob: the work grows
//!    with about `top_k² · (resources + top_k)`.
//! 3. **Route**: the shortest path from the assigned responder to the
//!    incident.
//! 4. **Prioritized responder list**: for up to `top_k` capable responders,
//!    the whole assignment is solved again with the new incident forced onto
//!    that responder, and responders are ranked by the resulting total cost.
//!    This is the step that makes planning cost real CPU (DL-056): about
//!    `top_k` further solves, so the knob's effect is roughly cubic.
//!
//! Every incident the solution assigns is written, with its resource, so an
//! incident that found no help when it was reported is picked up by a later
//! plan. The read set is exactly the incidents and resources written.

use crate::graph::Campus;
use crate::hungarian;
use proto::{
    Assignment, Category, Incident, IncidentStatus, Op, OpResult, Plan, PlanOutput, Planner,
    Request, ResourceId, ResourceKind, ResourceOffer, Severity, Snapshot, Version, Write,
};

/// Priority points per severity level (1–5).
pub const W_SEVERITY: i64 = 1_000;
/// Priority points per second waited, up to [`AGE_CAP_S`]. Ten minutes of
/// waiting is worth 1.2 severity levels.
pub const W_AGE: i64 = 2;
pub const AGE_CAP_S: u64 = 600;
/// Cost of a responder kind that is the `i`-th choice for the category.
pub const W_KIND: i64 = 20;
/// Cost per unit the resource is short of what the incident needs.
pub const W_SHORT: i64 = 30;
/// Cost of leaving an incident unassigned, on top of its priority. Larger
/// than any real pair's cost, so an incident is helped whenever a capable
/// resource is free.
pub const UNASSIGNED: i64 = 100_000;
/// A pairing that must never be chosen.
const NEVER: i64 = 1 << 40;

/// Responder kinds that can handle a category, best first.
pub fn capable(category: Category) -> &'static [ResourceKind] {
    use Category as C;
    use ResourceKind as K;
    match category {
        C::BlockedRoad => &[K::Vehicle, K::SecurityOfficer, K::Volunteer],
        C::PowerOutage => &[K::Generator, K::Technician],
        C::LabIncident => &[K::SecurityOfficer, K::Medic, K::FirstAidKit],
        C::Flooding => &[K::Volunteer, K::Vehicle, K::Technician],
        C::NetworkOutage => &[K::Technician],
        C::MedicalAssistance => &[K::Medic, K::FirstAidKit, K::Volunteer],
        C::TechnicalAssistance => &[K::Technician, K::Volunteer],
    }
}

/// Units an incident needs: 1 for severity 1–2, 2 for 3–4, 3 for 5.
pub fn units_needed(s: Severity) -> u32 {
    1 + u32::from(s.0.saturating_sub(1)) / 2
}

/// Priority of `i` at time `now_ms`. Higher is more urgent.
pub fn priority(i: &Incident, now_ms: u64) -> i64 {
    let waited_s = now_ms.saturating_sub(i.reported_at_ms) / 1_000;
    W_SEVERITY * i64::from(i.severity.0) + W_AGE * waited_s.min(AGE_CAP_S) as i64
}

fn pair_cost(campus: &Campus, i: &Incident, r: &ResourceOffer) -> i64 {
    let Some(rank) = capable(i.category).iter().position(|&k| k == r.kind) else {
        return NEVER;
    };
    let short = units_needed(i.severity).saturating_sub(r.available);
    i64::from(campus.distance(r.zone, i.zone)) + W_KIND * rank as i64 + W_SHORT * i64::from(short)
}

fn refused(why: &str) -> PlanOutput {
    PlanOutput {
        read_set: vec![],
        writes: vec![],
        result: OpResult::Refused(why.to_owned()),
    }
}

/// The planner every node runs. Built once from the configuration, so every
/// node holds the same campus map and cost knob.
#[derive(Debug, Clone)]
pub struct IncidentPlanner {
    campus: Campus,
    top_k: usize,
}

impl IncidentPlanner {
    pub fn new(zones: u16, top_k: u32) -> IncidentPlanner {
        IncidentPlanner {
            campus: Campus::new(zones),
            top_k: (top_k as usize).max(1),
        }
    }

    pub fn from_config(cfg: &proto::config::Config) -> IncidentPlanner {
        IncidentPlanner::new(cfg.planner.zones, cfg.planner.top_k)
    }

    pub fn campus(&self) -> &Campus {
        &self.campus
    }

    fn report(&self, snap: &Snapshot, req: &Request, new: Incident) -> PlanOutput {
        let now = req.sent_at_ms;
        // Rows: the new incident, then the most urgent other open incidents.
        let mut waiting: Vec<(&Incident, Version)> = snap
            .incidents
            .values()
            .filter(|(i, _)| {
                i.id != new.id
                    && i.status == IncidentStatus::Open
                    && i.reported_at_ms.saturating_add(i.ttl_ms) > now
            })
            .map(|(i, v)| (i, *v))
            .collect();
        waiting.sort_by_key(|(i, _)| (std::cmp::Reverse(priority(i, now)), i.id));
        waiting.truncate(self.top_k - 1);
        let rows: Vec<(&Incident, Version)> =
            std::iter::once((&new, Version(0))).chain(waiting).collect();

        // Columns: resources that can help at least one row, then one
        // "unassigned" column per row.
        let resources: Vec<(&ResourceOffer, Version)> = snap
            .resources
            .values()
            .filter(|(r, _)| {
                r.available > 0
                    && r.expires_at_ms > now
                    && rows
                        .iter()
                        .any(|(i, _)| capable(i.category).contains(&r.kind))
            })
            .map(|(r, v)| (r, *v))
            .collect();
        let (n, m) = (rows.len(), resources.len());
        let mut cost: Vec<Vec<i64>> = rows
            .iter()
            .enumerate()
            .map(|(a, (i, _))| {
                let mut row: Vec<i64> = resources
                    .iter()
                    .map(|(r, _)| pair_cost(&self.campus, i, r))
                    .collect();
                row.extend((0..n).map(|b| {
                    if a == b {
                        UNASSIGNED + priority(i, now)
                    } else {
                        NEVER
                    }
                }));
                row
            })
            .collect();
        let chosen = hungarian::assign(&cost);
        let responders = self.rank_responders(&mut cost, &resources, chosen[0]);

        let mut read_set = Vec::new();
        let mut writes = Vec::new();
        let mut mine = Vec::new();
        let mut route = vec![new.zone];
        for (a, ((incident, iv), &col)) in rows.iter().zip(&chosen).enumerate() {
            let mut incident = (*incident).clone();
            let pair = (col < m && cost[a][col] < NEVER).then(|| resources[col]);
            if let Some((r, rv)) = pair {
                let units = units_needed(incident.severity).min(r.available);
                let assignment = Assignment {
                    incident: incident.id,
                    resource: r.id,
                    units,
                    created_at_ms: now,
                    expires_at_ms: incident.reported_at_ms.saturating_add(incident.ttl_ms),
                };
                let mut r = r.clone();
                r.available -= units;
                incident.status = IncidentStatus::Assigned;
                read_set.push((r.id, rv));
                if a == 0 {
                    route = self.campus.route(r.zone, incident.zone);
                    mine.push(assignment.clone());
                }
                writes.push(Write::PutAssignment(assignment));
                writes.push(Write::PutResource(r));
            } else if a > 0 {
                continue; // still waiting, nothing to write
            }
            read_set.push((incident.id, *iv));
            writes.push(Write::PutIncident(incident));
        }
        PlanOutput {
            read_set,
            writes,
            result: OpResult::Planned(Plan {
                incident: new.id,
                priority: priority(&new, now),
                assignments: mine,
                route,
                responders,
            }),
        }
    }
}

impl IncidentPlanner {
    /// Capable responders for row 0, best first by the total cost of the
    /// whole assignment when row 0 is forced onto each. Ties go to the
    /// responder the main solve chose, then to the lower ID. Row 0 of `cost`
    /// is restored before returning.
    fn rank_responders(
        &self,
        cost: &mut [Vec<i64>],
        resources: &[(&ResourceOffer, Version)],
        chosen: usize,
    ) -> Vec<ResourceId> {
        let original = cost[0].clone();
        let mut candidates: Vec<usize> = (0..resources.len())
            .filter(|&j| original[j] < NEVER)
            .collect();
        candidates.sort_by_key(|&j| (original[j], resources[j].0.id));
        candidates.truncate(self.top_k);
        let mut ranked: Vec<(i64, bool, ResourceId)> = candidates
            .into_iter()
            .map(|j| {
                cost[0].fill(NEVER);
                cost[0][j] = original[j];
                let total: i64 = hungarian::assign(cost)
                    .iter()
                    .enumerate()
                    .map(|(a, &c)| cost[a][c])
                    .sum();
                (total, j != chosen, resources[j].0.id)
            })
            .collect();
        cost[0] = original;
        ranked.sort();
        ranked.into_iter().map(|(_, _, id)| id).collect()
    }
}

impl Planner for IncidentPlanner {
    fn plan(&self, snap: &Snapshot, req: &Request) -> PlanOutput {
        let now = req.sent_at_ms;
        let owner = req.id.client;
        match &req.op {
            Op::ReportIncident {
                category,
                zone,
                severity,
                ttl_ms,
                attachment,
            } => {
                if !self.campus.contains(*zone) {
                    return refused("unknown zone");
                }
                if !(1..=5).contains(&severity.0) || *ttl_ms == 0 {
                    return refused("severity must be 1-5 and ttl positive");
                }
                let new = Incident {
                    id: req.id,
                    owner,
                    category: *category,
                    zone: *zone,
                    severity: *severity,
                    reported_at_ms: now,
                    status: IncidentStatus::Open,
                    ttl_ms: *ttl_ms,
                    attachment: attachment.clone(),
                };
                self.report(snap, req, new)
            }
            Op::OfferResource {
                kind,
                capacity,
                zone,
                lease_ms,
            } => {
                if !self.campus.contains(*zone) {
                    return refused("unknown zone");
                }
                if *capacity == 0 || *lease_ms == 0 {
                    return refused("capacity and lease must be positive");
                }
                PlanOutput {
                    read_set: vec![],
                    writes: vec![Write::PutResource(ResourceOffer {
                        id: req.id,
                        owner,
                        kind: *kind,
                        capacity: *capacity,
                        available: *capacity,
                        zone: *zone,
                        expires_at_ms: now.saturating_add(*lease_ms),
                    })],
                    result: OpResult::Accepted,
                }
            }
            Op::UpdateResource {
                resource,
                available,
                extend_lease_ms,
            } => {
                let Some((r, v)) = snap.resources.get(resource) else {
                    return refused("unknown resource");
                };
                if r.owner != owner {
                    return refused("only the owner may update a resource");
                }
                let mut r = r.clone();
                r.available = (*available).min(r.capacity);
                if let Some(ext) = extend_lease_ms {
                    r.expires_at_ms = r.expires_at_ms.max(now).saturating_add(*ext);
                }
                PlanOutput {
                    read_set: vec![(r.id, *v)],
                    writes: vec![Write::PutResource(r)],
                    result: OpResult::Accepted,
                }
            }
            Op::Query(_) => refused("queries are answered from applied state"),
        }
    }
}
