//! The synthetic operation mix. Everything here is seeded: user `u` of
//! process `p` on machine `m` draws the same sequence of operations in every
//! run with the same seed (spec p. 8, repeatable workloads). All data is
//! synthetic.

use client::rng::{Rng, seed_from};
use proto::{Category, ClientId, Op, Query, ReqId, ResourceKind, Severity, ZoneId};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Mix {
    pub report: u32,
    pub offer: u32,
    pub update: u32,
    pub query: u32,
}

impl Mix {
    /// Parses `report=60,offer=20,update=15,query=5`. Missing names are zero.
    pub fn parse(s: &str) -> Result<Mix, String> {
        let mut m = Mix {
            report: 0,
            offer: 0,
            update: 0,
            query: 0,
        };
        for part in s.split(',').map(str::trim).filter(|p| !p.is_empty()) {
            let (name, value) = part
                .split_once('=')
                .ok_or_else(|| format!("mix entry '{part}' is not name=weight"))?;
            let w: u32 = value
                .trim()
                .parse()
                .map_err(|_| format!("mix weight '{value}' is not a number"))?;
            match name.trim() {
                "report" => m.report = w,
                "offer" => m.offer = w,
                "update" => m.update = w,
                "query" => m.query = w,
                other => return Err(format!("unknown mix entry '{other}'")),
            }
        }
        if m.total() == 0 {
            return Err("mix weights sum to zero".into());
        }
        Ok(m)
    }

    fn total(&self) -> u64 {
        u64::from(self.report)
            + u64::from(self.offer)
            + u64::from(self.update)
            + u64::from(self.query)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Report,
    Offer,
    Update,
    Query,
}

const CATEGORIES: [Category; 7] = [
    Category::BlockedRoad,
    Category::PowerOutage,
    Category::LabIncident,
    Category::Flooding,
    Category::NetworkOutage,
    Category::MedicalAssistance,
    Category::TechnicalAssistance,
];

const RESOURCES: [ResourceKind; 7] = [
    ResourceKind::Medic,
    ResourceKind::Technician,
    ResourceKind::SecurityOfficer,
    ResourceKind::Vehicle,
    ResourceKind::Generator,
    ResourceKind::FirstAidKit,
    ResourceKind::Volunteer,
];

/// Severity 1..=5 with 1 the most common and 5 the rarest.
const SEVERITY_WEIGHTS: [u64; 5] = [30, 25, 20, 15, 10];

const REMEMBER: usize = 32;
/// How long a reported incident stays open, and so how long the units given
/// to it are held. Shorter than the resource lease, so held units come back
/// while their resource is still offered; with 10 minutes, unserved
/// incidents piled up for 10 minutes and every plan and snapshot copy grew
/// with them (DL-075).
const INCIDENT_TTL_MS: u64 = 120_000;
const LEASE_MS: u64 = 300_000;

/// Draws operations for one simulated user.
pub struct UserGen {
    rng: Rng,
    zones: u16,
    mix: Mix,
    /// Resources this user created and was told about, for later updates.
    resources: Vec<ReqId>,
    incidents: Vec<ReqId>,
    reported: u64,
}

impl UserGen {
    pub fn new(seed: u64, user: ClientId, zones: u16, mix: Mix) -> UserGen {
        UserGen {
            rng: Rng::new(seed_from(&[
                seed,
                u64::from(user.machine),
                u64::from(user.process),
                u64::from(user.user),
            ])),
            zones: zones.max(1),
            mix,
            resources: Vec::new(),
            incidents: Vec::new(),
            reported: 0,
        }
    }

    fn zone(&mut self) -> ZoneId {
        ZoneId(self.rng.below(u64::from(self.zones)) as u16)
    }

    fn pick(&mut self, from: &[ReqId]) -> Option<ReqId> {
        (!from.is_empty()).then(|| from[self.rng.below(from.len() as u64) as usize])
    }

    fn offer(&mut self) -> Op {
        Op::OfferResource {
            kind: RESOURCES[self.rng.below(RESOURCES.len() as u64) as usize],
            capacity: 1 + self.rng.below(8) as u32,
            zone: self.zone(),
            lease_ms: LEASE_MS,
        }
    }

    fn report(&mut self) -> Op {
        let mut r = self.rng.below(SEVERITY_WEIGHTS.iter().sum());
        let mut severity = 1u8;
        for (i, w) in SEVERITY_WEIGHTS.iter().enumerate() {
            if r < *w {
                severity = i as u8 + 1;
                break;
            }
            r -= w;
        }
        self.reported += 1;
        let attachment =
            (self.rng.below(20) == 0).then(|| format!("synthetic-photo-{}", self.reported));
        Op::ReportIncident {
            category: CATEGORIES[self.rng.below(CATEGORIES.len() as u64) as usize],
            zone: self.zone(),
            severity: Severity(severity),
            ttl_ms: INCIDENT_TTL_MS,
            attachment,
        }
    }

    pub fn next_op(&mut self) -> (Op, Kind) {
        let mut r = self.rng.below(self.mix.total());
        let m = self.mix;
        if r < u64::from(m.report) {
            return (self.report(), Kind::Report);
        }
        r -= u64::from(m.report);
        if r < u64::from(m.offer) {
            return (self.offer(), Kind::Offer);
        }
        r -= u64::from(m.offer);
        if r < u64::from(m.update) {
            let known = self.resources.clone();
            return match self.pick(&known) {
                Some(resource) => (
                    Op::UpdateResource {
                        resource,
                        available: self.rng.below(9) as u32,
                        extend_lease_ms: (self.rng.below(4) == 0).then_some(LEASE_MS),
                    },
                    Kind::Update,
                ),
                // Nothing to update yet: create something instead.
                None => (self.offer(), Kind::Offer),
            };
        }
        let incidents = self.incidents.clone();
        let resources = self.resources.clone();
        let q = match self.rng.below(3) {
            0 => self.pick(&incidents).map(Query::Incident),
            1 => self.pick(&resources).map(Query::Resource),
            _ => None,
        }
        .unwrap_or_else(|| Query::IncidentsInZone(self.zone()));
        (Op::Query(q), Kind::Query)
    }

    /// Records that request `id` of this kind completed, so later operations
    /// can refer to what it created. An incident or resource is identified by
    /// the request that created it (DL-021).
    pub fn note(&mut self, kind: Kind, id: ReqId) {
        let list = match kind {
            Kind::Report => &mut self.incidents,
            Kind::Offer => &mut self.resources,
            _ => return,
        };
        if list.len() == REMEMBER {
            list.remove(0);
        }
        list.push(id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn uid(u: u32) -> ClientId {
        ClientId {
            machine: 1,
            process: 2,
            user: u,
        }
    }

    fn rid(seq: u64) -> ReqId {
        ReqId {
            client: uid(1),
            incarnation: 1,
            seq,
        }
    }

    #[test]
    fn mix_parsing() {
        let m = Mix::parse("report=1, query = 3").unwrap();
        assert_eq!((m.report, m.offer, m.update, m.query), (1, 0, 0, 3));
        assert!(Mix::parse("report=x").is_err());
        assert!(Mix::parse("fly=3").is_err());
        assert!(Mix::parse("report").is_err());
        assert!(Mix::parse("").is_err());
    }

    #[test]
    fn same_seed_same_operations_different_user_different_operations() {
        let mix = Mix::parse("report=60,offer=20,update=15,query=5").unwrap();
        let draw = |seed, user| {
            let mut g = UserGen::new(seed, uid(user), 30, mix);
            (0..50).map(|_| g.next_op().0).collect::<Vec<_>>()
        };
        assert_eq!(draw(7, 1), draw(7, 1));
        assert_ne!(draw(7, 1), draw(8, 1));
        assert_ne!(draw(7, 1), draw(7, 2));
    }

    #[test]
    fn frequencies_follow_the_mix_and_values_are_in_range() {
        let mix = Mix::parse("report=60,offer=20,update=15,query=5").unwrap();
        let mut g = UserGen::new(1, uid(1), 30, mix);
        g.note(Kind::Offer, rid(1));
        let mut counts = [0u32; 4];
        let n = 40_000;
        for _ in 0..n {
            let (op, kind) = g.next_op();
            counts[kind as usize] += 1;
            match op {
                Op::ReportIncident { zone, severity, .. } => {
                    assert!(zone.0 < 30);
                    assert!((1..=5).contains(&severity.0));
                }
                Op::OfferResource { zone, capacity, .. } => {
                    assert!(zone.0 < 30 && (1..=8).contains(&capacity));
                }
                Op::UpdateResource { resource, .. } => assert_eq!(resource, rid(1)),
                Op::Query(_) => {}
            }
        }
        let share = |c: u32| f64::from(c) / f64::from(n);
        assert!((share(counts[0]) - 0.60).abs() < 0.02, "{counts:?}");
        assert!((share(counts[3]) - 0.05).abs() < 0.01, "{counts:?}");
    }

    #[test]
    fn update_without_a_known_resource_becomes_an_offer() {
        let mut g = UserGen::new(1, uid(1), 30, Mix::parse("update=1").unwrap());
        assert_eq!(g.next_op().1, Kind::Offer);
        g.note(Kind::Offer, rid(5));
        assert_eq!(g.next_op().1, Kind::Update);
    }

    #[test]
    fn remembered_ids_are_bounded() {
        let mut g = UserGen::new(1, uid(1), 30, Mix::parse("query=1").unwrap());
        for s in 0..1000 {
            g.note(Kind::Report, rid(s));
        }
        assert_eq!(g.incidents.len(), REMEMBER);
        assert_eq!(*g.incidents.last().unwrap(), rid(999));
    }
}
