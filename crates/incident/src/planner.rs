//! The deterministic planner (design log DL-020, DL-056, DL-057, DL-059).
//!
//! A report is planned as a fresh dispatch of the most urgent waiting
//! incidents, the new one always among them:
//!
//! 1. **Priority** of each open incident: severity, plus how long it has
//!    waited. Integers only.
//! 2. **Assignment**: the Hungarian algorithm matches the top `top_k`
//!    incidents (the rows) to resources (the columns). A pair's cost is the
//!    walking distance, a penalty for a less suitable kind of responder, and a
//!    penalty per unit the resource is short. Each incident may also stay
//!    unassigned, at a cost that grows with its priority, so when two
//!    incidents want one resource the more urgent one gets it.
//! 3. **Route**: the shortest path from the assigned responder to the
//!    incident.
//! 4. **Prioritized responder list**: for up to `top_k` capable responders,
//!    the whole assignment is solved again with the new incident forced onto
//!    that responder, and responders are ranked by the resulting total cost.
//!    This is the step that makes planning cost real CPU (DL-059). All of
//!    them are ranked, but a plan keeps only the best [`RESPONDERS_KEPT`]:
//!    every committed plan is stored on every node, and the tail of a
//!    100-long list is never used.
//!
//! Only the new incident is written, with its resource if it gets one. The
//! waiting incidents take part in the solve, so the new one does not take a
//! responder a more urgent incident should have, but they are dispatched
//! only when capacity appears: by offers and updates (DL-062), and by the
//! first report after units come back from expired holds (see
//! [Holds](#holds)). The waiting rows are the most urgent incidents some
//! free resource can serve: one nothing can serve would only take its
//! unassigned column and push out one that could be served. The read set is
//! exactly the incidents and resources written.
//!
//! ## Which resources become columns
//!
//! Putting every live resource into the matrix made a plan's time grow with
//! the number of resources: 0.95 s per plan at 30 000 resources. Instead each
//! waiting incident brings its [`COLUMNS_PER_ROW`] cheapest capable resources
//! (ties by ID), and the new incident brings its `top_k` cheapest, which are
//! also the responder candidates of step 4. The matrix then has at most
//! about `(COLUMNS_PER_ROW + 1) · top_k` resource columns, however many
//! resources there are.
//!
//! Why the answer stays optimal. An incident needs a resource outside its
//! own few cheapest only if other incidents have taken every one of those:
//! were one of them free, moving the incident onto it would cost no more.
//! With `n` rows, keeping each row's `n` cheapest is therefore always enough,
//! since the other `n − 1` cannot take all `n`. Keeping `c` per row, with `c`
//! much smaller than `n`, is enough unless `c` other incidents take exactly a
//! row's `c` cheapest responders. That happens when responders are scarce,
//! and also when incidents in one zone bring the same equally cheap
//! resources (ties go to the lower ID, so they all bring the same ones). So
//! the answer is checked rather than trusted. The solver returns each row's
//! dual price `u_i` ([`hungarian::Solution`]); a column left out of the
//! matrix could lower the total only if its cost for some row were below
//! that row's `u_i`. Every column a row left out costs it at least its
//! `c`-th cheapest, `t_i`. So when `u_i ≤ t_i` for every row, the restricted
//! answer is optimal for the full matrix as well; otherwise the failing rows
//! bring twice as many columns and the problem is solved again. The forced
//! solves of step 4 are checked the same way. A plan therefore has the same
//! total cost, and the same responder ranking, as with every resource as a
//! column; it can differ only in the choice between equally cheap answers.
//! Measured at `top_k` 100, the main problem took 3 to 5 solves and the
//! forced solves almost never needed another: 103 to 113 solves where 101
//! would do, on a matrix of under 300 resource columns at 30 000 resources.
//!
//! Finding a row's cheapest resources does not scan them all. A pair's cost
//! depends only on the resource's kind, zone and free units (1, 2, or 3 and
//! more, since no incident needs more than 3), so resources are grouped by
//! those, and a row visits the groups in order of cost, from its own zone
//! outwards, until it has enough. Building the groups, and choosing the rows
//! among the open incidents, is the one pass a plan makes over everything:
//! about 0.1 ms at 30 000 resources and 10 000 open incidents.
//!
//! ## Cost knob
//!
//! `top_k` sets the work: up to `top_k + 1` solves of a matrix of `top_k`
//! rows and at most about `(COLUMNS_PER_ROW + 1) · top_k` resource columns.
//! One solve is cubic in the worst case, so a plan is quartic in the worst
//! case. How close it comes depends on contention. Where waiting incidents
//! rarely want the new one's responders, most pairs are incompatible and
//! most augmenting paths are short: `top_k` 10 to 100 costs about 100 times
//! the time, roughly quadratic (DL-059). Where every waiting incident
//! competes for the same responders, as in a stress run, it costs about 800
//! times, and 200 costs another 14 times: roughly cubic, steeper at the top
//! (`service_time_against_top_k`). Either way the time depends on `top_k`
//! and on contention, not on the number of resources
//! (`service_time_against_resource_count`).
//!
//! ## Holds
//!
//! An assignment holds units of its resource until it expires, with its
//! incident's TTL ([`proto::Hold`]). Units of an expired hold count as free
//! ([`free_units`]), and every plan that writes a resource first moves them
//! back into `available` ([`release_expired`]). Units return only inside a
//! plan's writes, never when an entry is applied, because the coordinator
//! checks only the versions of what a plan writes: a resource changed behind
//! a plan's back would let a stale plan overwrite it unnoticed (I3).
//!
//! Units that come back this way have no offer or update to dispatch them
//! to the incidents already waiting. Left alone, the most urgent waiting
//! incident would claim the resource in every later solve without ever being
//! written, and the resource would sit idle. So a report's plan dispatches,
//! as an offer would, writing each resource and the incidents it serves:
//!
//! - each resource its solve gives to a waiting row, first to that row, so
//!   the dispatch keeps the optimal matching (a resource handed to the most
//!   urgent incident instead could leave another incident, which only that
//!   resource can serve, waiting while a resource it cannot use sits idle),
//!   then any units left to the most urgent capable waiting incidents;
//! - each resource with units back from expired holds, at most `top_k` of
//!   them, if some capable incident waits outside the rows (or `top_k` is 1
//!   and there are none);
//! - the new incident's own resource, if units came back to it, for what
//!   the new incident leaves.
//!
//! Afterwards those resources have no units a waiting incident can use, so
//! the next plans leave them alone and concurrent reports collide over them
//! once.
//!
//! When a hold has expired is decided by [`plan_time`]: the request's
//! timestamp, kept within [`CLOCK_SLACK_MS`] of the committed clients'
//! clock. A client whose clock is a minute fast would otherwise release a
//! live hold and take its units: a plan the read-set check must accept,
//! since nothing it read had changed, yet two incidents would then hold one
//! unit by everyone else's clock. A client whose clock is slow would serve
//! incidents everyone else sees as expired. With the bound, a client's
//! clock moves these decisions by at most 5 s. The committed clock lags
//! real time by about half the span of the entries it is the median of, so
//! when there are few requests a hold is released up to that lag, less 5 s,
//! late: the safe direction.

use crate::graph::Campus;
use crate::hungarian;
use proto::{
    Assignment, Category, Hold, Incident, IncidentId, IncidentStatus, Op, OpResult, Plan,
    PlanOutput, Planner, Request, ResourceId, ResourceKind, ResourceOffer, Severity, Snapshot,
    Version, Write, ZoneId,
};
use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet};

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
/// How many of its cheapest resources each waiting incident brings to the
/// matrix at first. Three covers a row whose two cheapest are taken by more
/// urgent incidents; a row that needs more gets more (see the module docs).
pub const COLUMNS_PER_ROW: usize = 3;
/// Length of the responder list kept in a plan.
pub const RESPONDERS_KEPT: usize = 10;
/// How far a request's own timestamp may stray from the committed clients'
/// clock when a plan judges what has expired ([`plan_time`]).
pub const CLOCK_SLACK_MS: u64 = 5_000;
/// A pairing that must never be chosen.
const NEVER: i64 = 1 << 40;
/// Number of [`ResourceKind`]s; `Volunteer` is the last.
const KINDS: usize = ResourceKind::Volunteer as usize + 1;
/// Free units are grouped as 1, 2, and 3 or more.
const UNIT_CLASSES: u32 = 3;

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

/// When `i` expires, and with it any assignment to it.
pub fn expiry(i: &Incident) -> u64 {
    i.reported_at_ms.saturating_add(i.ttl_ms)
}

/// The time a plan judges expiry by: the request's timestamp, kept within
/// [`CLOCK_SLACK_MS`] of the committed clients' clock
/// (`Snapshot::clients_now_ms`). Holds are released, and incidents and
/// leases count as expired, by this time, so a client whose clock is fast
/// cannot take units still held, and one whose clock is slow cannot serve
/// incidents everyone else sees as expired. Before any committed timestamp
/// the request's own is used.
pub fn plan_time(snap: &Snapshot, sent_at_ms: u64) -> u64 {
    let committed = snap.clients_now_ms;
    if committed == 0 {
        return sent_at_ms;
    }
    sent_at_ms.clamp(
        committed.saturating_sub(CLOCK_SLACK_MS),
        committed.saturating_add(CLOCK_SLACK_MS),
    )
}

/// Units of `r` free at `now`: its available units plus those of every hold
/// that has expired by then.
pub fn free_units(r: &ResourceOffer, now: u64) -> u32 {
    let expired: u32 = r
        .holds
        .iter()
        .filter(|h| h.expires_at_ms <= now)
        .map(|h| h.units)
        .sum();
    r.available + expired
}

/// `r` as it stands at `now`: the units of expired holds moved back into
/// `available`, capacity unchanged. Applied to every resource a plan writes.
pub fn release_expired(mut r: ResourceOffer, now: u64) -> ResourceOffer {
    r.available = free_units(&r, now);
    r.holds.retain(|h| h.expires_at_ms > now);
    r
}

fn pair_cost(campus: &Campus, i: &Incident, r: &ResourceOffer, free: u32) -> i64 {
    let Some(rank) = capable(i.category).iter().position(|&k| k == r.kind) else {
        return NEVER;
    };
    let short = units_needed(i.severity).saturating_sub(free);
    i64::from(campus.distance(r.zone, i.zone)) + W_KIND * rank as i64 + W_SHORT * i64::from(short)
}

/// The `k` most urgent of `v`, most urgent first, ties to the lower ID.
/// Selects before sorting, so it costs O(|v|) rather than O(|v| log |v|).
fn most_urgent(mut v: Vec<(&Incident, Version)>, k: usize, now: u64) -> Vec<(&Incident, Version)> {
    let key = |(i, _): &(&Incident, Version)| (Reverse(priority(i, now)), i.id);
    if k == 0 {
        return Vec::new();
    }
    if v.len() > k {
        v.select_nth_unstable_by_key(k - 1, key);
        v.truncate(k);
    }
    // The keys hold the ID, so they are unique and the order is fixed.
    v.sort_unstable_by_key(key);
    v
}

fn refused(why: &str) -> PlanOutput {
    PlanOutput {
        read_set: vec![],
        writes: vec![],
        result: OpResult::Refused(why.to_owned()),
    }
}

/// A request's two times. `sent` is the client's own timestamp, which the
/// plan records (an incident's report time, an assignment's creation time)
/// and the committed clock is made of; `now` is [`plan_time`], which the
/// plan judges expiry and waiting time by.
#[derive(Debug, Clone, Copy)]
struct Times {
    sent: u64,
    now: u64,
}

/// A resource that can take an assignment at the plan's time.
struct Usable<'a> {
    offer: &'a ResourceOffer,
    version: Version,
    /// [`free_units`] at the plan's time; at least 1.
    free: u32,
}

/// The resources a plan may assign, in ID order, grouped by kind, zone and
/// free units so that a row's cheapest are found without scanning them all.
struct Pool<'a> {
    res: Vec<Usable<'a>>,
    /// Indices into `res`, in ID order, one list per group.
    groups: Vec<Vec<usize>>,
    /// Resources per (kind, unit class), to skip empty cursors at once.
    counts: [usize; KINDS * UNIT_CLASSES as usize],
    /// Indices into `res`, in ID order, of the resources with units back
    /// from expired holds: capacity nobody has dispatched yet.
    returned: Vec<usize>,
    zones: usize,
}

impl<'a> Pool<'a> {
    /// Resources whose lease runs past `now` and that have a unit free at
    /// `now`.
    fn new(campus: &Campus, snap: &'a Snapshot, now: u64) -> Pool<'a> {
        let zones = usize::from(campus.zones());
        let mut pool = Pool {
            res: Vec::new(),
            groups: vec![Vec::new(); KINDS * zones * UNIT_CLASSES as usize],
            counts: [0; KINDS * UNIT_CLASSES as usize],
            returned: Vec::new(),
            zones,
        };
        for (r, v) in snap.resources.values() {
            if r.expires_at_ms <= now || !campus.contains(r.zone) {
                continue;
            }
            let free = free_units(r, now);
            if free == 0 {
                continue;
            }
            if free > r.available {
                pool.returned.push(pool.res.len());
            }
            let class = free.min(UNIT_CLASSES);
            let g = pool.group(r.kind as usize, r.zone, class);
            pool.groups[g].push(pool.res.len());
            pool.counts[Pool::count_of(r.kind as usize, class)] += 1;
            pool.res.push(Usable {
                offer: r,
                version: *v,
                free,
            });
        }
        pool
    }

    /// Whether some resource in the pool can serve `category`.
    fn serves(&self, category: Category) -> bool {
        capable(category).iter().any(|&k| {
            (1..=UNIT_CLASSES).any(|class| self.counts[Pool::count_of(k as usize, class)] > 0)
        })
    }

    fn group(&self, kind: usize, zone: ZoneId, class: u32) -> usize {
        (kind * self.zones + usize::from(zone.0)) * UNIT_CLASSES as usize + class as usize - 1
    }

    fn count_of(kind: usize, class: u32) -> usize {
        kind * UNIT_CLASSES as usize + class as usize - 1
    }
}

/// A row's cheapest usable resources.
struct Cheapest {
    /// Indices into the pool, cheapest first, ties to the lower ID.
    cols: Vec<usize>,
    /// If some capable resource was left out: the cost of the dearest one
    /// kept. Every one left out costs the row at least this much.
    cut_at: Option<i64>,
}

/// The assignment problem of one report: the rows, the columns each row
/// brought, and the cost matrix over the union of those columns plus one
/// "unassigned" column per row.
struct Problem<'a> {
    planner: &'a IncidentPlanner,
    pool: &'a Pool<'a>,
    rows: &'a [(&'a Incident, Version)],
    now: u64,
    /// How many columns each row asks for.
    want: Vec<usize>,
    lists: Vec<Cheapest>,
    /// Pool indices, ascending, so in resource ID order.
    cols: Vec<usize>,
    cost: Vec<Vec<i64>>,
}

impl<'a> Problem<'a> {
    fn new(
        planner: &'a IncidentPlanner,
        pool: &'a Pool<'a>,
        rows: &'a [(&'a Incident, Version)],
        now: u64,
    ) -> Problem<'a> {
        // Row 0, the new incident, brings its responder candidates too.
        let mut want = vec![planner.columns_per_row; rows.len()];
        want[0] = want[0].max(planner.top_k);
        let lists = rows
            .iter()
            .zip(&want)
            .map(|((i, _), &c)| planner.cheapest(pool, i, c))
            .collect();
        let mut p = Problem {
            planner,
            pool,
            rows,
            now,
            want,
            lists,
            cols: Vec::new(),
            cost: Vec::new(),
        };
        p.build();
        p
    }

    fn build(&mut self) {
        let union: BTreeSet<usize> = self
            .lists
            .iter()
            .flat_map(|l| l.cols.iter().copied())
            .collect();
        self.cols = union.into_iter().collect();
        let n = self.rows.len();
        let (campus, pool, now) = (&self.planner.campus, self.pool, self.now);
        self.cost = self
            .rows
            .iter()
            .enumerate()
            .map(|(a, (i, _))| {
                let mut row: Vec<i64> = self
                    .cols
                    .iter()
                    .map(|&j| pair_cost(campus, i, pool.res[j].offer, pool.res[j].free))
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
    }

    /// Solves the problem, with row 0 forced onto pool resource `forced` if
    /// given. Returns each row's resource (a pool index, `None` if
    /// unassigned) and the total cost, with the total as the full problem
    /// with every usable resource as a column would give it: until the dual
    /// check of the module docs passes, failing rows bring twice as many
    /// columns and the problem is solved again. When forced, row 0 is exempt
    /// from the check, since it may use only `forced` anyway.
    fn solve(&mut self, forced: Option<usize>) -> (Vec<Option<usize>>, i64) {
        loop {
            let saved = forced.map(|j| {
                let at = self
                    .cols
                    .binary_search(&j)
                    .expect("row 0's candidates are columns");
                let original = self.cost[0].clone();
                self.cost[0].fill(NEVER);
                self.cost[0][at] = original[at];
                original
            });
            let s = hungarian::solve(&self.cost);
            let total: i64 = s
                .cols
                .iter()
                .enumerate()
                .map(|(a, &c)| self.cost[a][c])
                .sum();
            if let Some(original) = saved {
                self.cost[0] = original;
            }
            let failing: Vec<usize> = (usize::from(forced.is_some())..self.rows.len())
                .filter(|&a| self.lists[a].cut_at.is_some_and(|t| s.row_price[a] > t))
                .collect();
            if failing.is_empty() {
                let given = s
                    .cols
                    .iter()
                    .enumerate()
                    .map(|(a, &c)| {
                        (c < self.cols.len() && self.cost[a][c] < NEVER).then(|| self.cols[c])
                    })
                    .collect();
                return (given, total);
            }
            for a in failing {
                self.want[a] = self.want[a].saturating_mul(2);
                self.lists[a] = self
                    .planner
                    .cheapest(self.pool, self.rows[a].0, self.want[a]);
            }
            self.build();
        }
    }
}

/// The planner every node runs. Built once from the configuration, so every
/// node holds the same campus map and cost knob.
#[derive(Debug, Clone)]
pub struct IncidentPlanner {
    campus: Campus,
    top_k: usize,
    columns_per_row: usize,
    /// For each zone, every zone as `(distance, zone)`, nearest first, ties
    /// to the lower zone number.
    near: Vec<Vec<(u32, ZoneId)>>,
}

impl IncidentPlanner {
    pub fn new(zones: u16, top_k: u32) -> IncidentPlanner {
        let campus = Campus::new(zones);
        let near = (0..zones)
            .map(|from| {
                let mut v: Vec<(u32, ZoneId)> = (0..zones)
                    .map(|to| (campus.distance(ZoneId(from), ZoneId(to)), ZoneId(to)))
                    .collect();
                v.sort_unstable();
                v
            })
            .collect();
        IncidentPlanner {
            campus,
            top_k: (top_k as usize).max(1),
            columns_per_row: COLUMNS_PER_ROW,
            near,
        }
    }

    pub fn from_config(cfg: &proto::config::Config) -> IncidentPlanner {
        IncidentPlanner::new(cfg.planner.zones, cfg.planner.top_k)
    }

    /// The same planner with each waiting incident bringing `c` columns at
    /// first instead of [`COLUMNS_PER_ROW`]. `usize::MAX` puts every usable
    /// resource in the matrix, as before columns were bounded; the tests use
    /// it to check that bounding changes no answer. Plans differ only in
    /// effort, but every node should still use the same value.
    pub fn with_columns_per_row(mut self, c: usize) -> IncidentPlanner {
        self.columns_per_row = c.max(1);
        self
    }

    pub fn campus(&self) -> &Campus {
        &self.campus
    }

    /// The `c` cheapest resources in `pool` for incident `i`, cheapest first,
    /// ties to the lower ID. Visits the groups in order of cost: one cursor
    /// per (kind, unit class) walks the zones nearest first, and along a
    /// cursor the cost grows with distance only. Stops once `c` are found and
    /// no unvisited group can cost as little as the dearest of them.
    fn cheapest(&self, pool: &Pool, i: &Incident, c: usize) -> Cheapest {
        let need = units_needed(i.severity);
        let near = &self.near[usize::from(i.zone.0)];
        // (fixed part of the cost, kind, unit class, next position in `near`)
        let mut cursors: Vec<(i64, usize, u32, usize)> = Vec::new();
        for (rank, &kind) in capable(i.category).iter().enumerate() {
            for class in 1..=UNIT_CLASSES {
                if pool.counts[Pool::count_of(kind as usize, class)] > 0 {
                    let fixed =
                        W_KIND * rank as i64 + W_SHORT * i64::from(need.saturating_sub(class));
                    cursors.push((fixed, kind as usize, class, 0));
                }
            }
        }
        let mut found: Vec<(i64, ResourceId, usize)> = Vec::new();
        let mut bound: Option<i64> = None;
        let mut cut = false;
        loop {
            let next = cursors
                .iter()
                .enumerate()
                .filter(|(_, c)| c.3 < near.len())
                .map(|(k, c)| (c.0 + i64::from(near[c.3].0), k))
                .min();
            let Some((cost, k)) = next else {
                break;
            };
            if bound.is_some_and(|b| cost > b) {
                cut = true;
                break;
            }
            let (_, kind, class, at) = cursors[k];
            cursors[k].3 += 1;
            let group = &pool.groups[pool.group(kind, near[at].1, class)];
            // Every member of a group costs the same, so only its first `c`
            // by ID can be among the `c` cheapest.
            cut |= group.len() > c;
            found.extend(
                group
                    .iter()
                    .take(c)
                    .map(|&x| (cost, pool.res[x].offer.id, x)),
            );
            if bound.is_none() && found.len() >= c {
                bound = Some(cost);
            }
        }
        found.sort_unstable();
        if found.len() > c {
            found.truncate(c);
            cut = true;
        }
        Cheapest {
            cut_at: if cut { found.last().map(|f| f.0) } else { None },
            cols: found.into_iter().map(|f| f.2).collect(),
        }
    }

    /// The rows of a report's assignment: the new incident, then the most
    /// urgent other open incidents that some resource in `pool` can serve,
    /// `top_k` in all. An incident nothing can serve would only take its
    /// unassigned column, and would push out one that can be served.
    fn rows<'a>(
        &self,
        snap: &'a Snapshot,
        new: &'a Incident,
        pool: &Pool,
        now: u64,
    ) -> Vec<(&'a Incident, Version)> {
        let waiting: Vec<(&Incident, Version)> = snap
            .incidents
            .values()
            .filter(|(i, _)| {
                i.id != new.id
                    && i.status == IncidentStatus::Open
                    && expiry(i) > now
                    && pool.serves(i.category)
            })
            .map(|(i, v)| (i, *v))
            .collect();
        std::iter::once((new, Version(0)))
            .chain(most_urgent(waiting, self.top_k - 1, now))
            .collect()
    }

    /// The resources a plan at `now` may assign.
    fn pool<'a>(&self, snap: &'a Snapshot, now: u64) -> Pool<'a> {
        Pool::new(&self.campus, snap, now)
    }

    fn report(&self, snap: &Snapshot, t: Times, new: Incident) -> PlanOutput {
        let now = t.now;
        if expiry(&new) <= now {
            // Already expired by the committed clients' clock (its client's
            // clock is slow): recorded, but not served.
            return PlanOutput {
                read_set: vec![(new.id, Version(0))],
                result: OpResult::Planned(Plan {
                    incident: new.id,
                    priority: priority(&new, now),
                    assignments: vec![],
                    route: vec![new.zone],
                    responders: vec![],
                }),
                writes: vec![Write::PutIncident(new)],
            };
        }
        let pool = self.pool(snap, now);
        let rows = self.rows(snap, &new, &pool, now);
        let mut problem = Problem::new(self, &pool, &rows, now);
        let (given, _) = problem.solve(None);
        let chosen = given[0];
        let responders = self.rank_responders(&mut problem, chosen);

        // Row 0, the new incident, is written. The waiting incidents take part
        // in the solve, so the new one does not take a responder a more urgent
        // incident should have, but they are not reassigned here: every
        // concurrent report would then write the same waiting incidents and
        // all but one would be rejected as stale (DL-062). The exception is
        // capacity nobody has dispatched (module docs, Holds).
        let mut read_set = Vec::new();
        let mut writes = Vec::new();
        // Resources the solve gives to waiting rows, each with its row. The
        // paired rows are reserved, so each resource serves its own row first
        // and the dispatch keeps the solve's matching.
        let paired: BTreeMap<usize, usize> = given
            .iter()
            .enumerate()
            .skip(1)
            .filter_map(|(a, g)| g.map(|j| (j, a)))
            .collect();
        let mut taken: BTreeSet<IncidentId> = paired.values().map(|&a| rows[a].0.id).collect();
        // Waiting incidents are ranked once per kind, not once per resource,
        // with room for every one this plan could skip or serve.
        let mut room = [self.top_k + taken.len(); KINDS];
        for &j in paired.keys().chain(&pool.returned).chain(&chosen) {
            room[pool.res[j].offer.kind as usize] += pool.res[j].free as usize;
        }
        let mut ranked = BTreeMap::new();
        for (&j, &a) in &paired {
            let u = &pool.res[j];
            let waiting = self.ranked(&mut ranked, snap, now, u.offer.kind, &room);
            read_set.push((u.offer.id, u.version));
            let r = release_expired(u.offer.clone(), now);
            let first = Some(rows[a]);
            self.dispatch_waiting(t, r, first, waiting, &mut taken, &mut read_set, &mut writes);
        }
        // Units back from expired holds that no row was paired with, which
        // happens when the incidents that can use them are not among the rows
        // (or `top_k` is 1): dispatched to the most urgent capable waiting
        // incidents, at most `top_k` resources per plan.
        let mut extra = 0;
        for &j in &pool.returned {
            if extra == self.top_k {
                break;
            }
            if paired.contains_key(&j) || chosen == Some(j) {
                continue;
            }
            let u = &pool.res[j];
            let waiting = self.ranked(&mut ranked, snap, now, u.offer.kind, &room);
            if waiting.iter().all(|(i, _)| taken.contains(&i.id)) {
                continue;
            }
            extra += 1;
            read_set.push((u.offer.id, u.version));
            let r = release_expired(u.offer.clone(), now);
            self.dispatch_waiting(t, r, None, waiting, &mut taken, &mut read_set, &mut writes);
        }
        let mut mine = Vec::new();
        let mut route = vec![new.zone];
        let mut incident = new.clone();
        if let Some(j) = chosen {
            let u = &pool.res[j];
            let mut r = release_expired(u.offer.clone(), now);
            let units = units_needed(incident.severity).min(r.available);
            let assignment = Assignment {
                incident: incident.id,
                resource: r.id,
                units,
                created_at_ms: t.sent,
                expires_at_ms: expiry(&incident),
            };
            r.available -= units;
            r.holds.push(Hold {
                incident: incident.id,
                units,
                expires_at_ms: assignment.expires_at_ms,
            });
            incident.status = IncidentStatus::Assigned;
            read_set.push((r.id, u.version));
            route = self.campus.route(r.zone, incident.zone);
            mine.push(assignment.clone());
            writes.push(Write::PutAssignment(assignment));
            if pool.returned.binary_search(&j).is_ok() {
                // Its returned units may be more than the new incident needs.
                let waiting = self.ranked(&mut ranked, snap, now, r.kind, &room);
                self.dispatch_waiting(t, r, None, waiting, &mut taken, &mut read_set, &mut writes);
            } else {
                writes.push(Write::PutResource(r));
            }
        }
        read_set.push((incident.id, Version(0)));
        writes.push(Write::PutIncident(incident));
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

    /// The open incidents still live at `now` that a `kind` can serve, at
    /// most `k` of them, most urgent first.
    fn waiting_for<'a>(
        &self,
        snap: &'a Snapshot,
        now: u64,
        kind: ResourceKind,
        k: usize,
    ) -> Vec<(&'a Incident, Version)> {
        let waiting = snap
            .incidents
            .values()
            .filter(|(i, _)| {
                i.status == IncidentStatus::Open
                    && expiry(i) > now
                    && capable(i.category).contains(&kind)
            })
            .map(|(i, v)| (i, *v))
            .collect();
        most_urgent(waiting, k, now)
    }

    /// [`IncidentPlanner::waiting_for`] `kind`, at most `room[kind]` of
    /// them, computed once per plan and kept in `cache`.
    fn ranked<'m, 'a>(
        &self,
        cache: &'m mut BTreeMap<usize, Vec<(&'a Incident, Version)>>,
        snap: &'a Snapshot,
        now: u64,
        kind: ResourceKind,
        room: &[usize; KINDS],
    ) -> &'m [(&'a Incident, Version)] {
        let k = kind as usize;
        cache
            .entry(k)
            .or_insert_with(|| self.waiting_for(snap, now, kind, room[k]))
    }

    /// [`IncidentPlanner::dispatch_waiting`] for an offer or update: `r` is
    /// the only resource the plan dispatches.
    fn dispatch_alone(
        &self,
        snap: &Snapshot,
        t: Times,
        r: ResourceOffer,
        read_set: &mut Vec<(ResourceId, Version)>,
        writes: &mut Vec<Write>,
    ) {
        let waiting = if r.expires_at_ms > t.now && r.available > 0 {
            self.waiting_for(snap, t.now, r.kind, self.top_k)
        } else {
            Vec::new()
        };
        let mut taken = BTreeSet::new();
        self.dispatch_waiting(t, r, None, &waiting, &mut taken, read_set, writes);
    }

    /// Gives `r`'s free units to waiting incidents: `first` if given (the
    /// row the solve paired `r` with), then the most urgent of `waiting`
    /// (candidates it can serve, most urgent first) not yet `taken` by this
    /// plan, at most `top_k` of those. The ones served are added to `taken`.
    /// Called when capacity appears: a new offer, an update that frees units,
    /// or units nobody has dispatched (see `report`). `r` must already have
    /// its expired holds released. Writes only `r` and the incidents it is
    /// assigned to, so it rarely collides with concurrent plans (DL-062).
    #[allow(clippy::too_many_arguments)]
    fn dispatch_waiting(
        &self,
        t: Times,
        mut r: ResourceOffer,
        first: Option<(&Incident, Version)>,
        waiting: &[(&Incident, Version)],
        taken: &mut BTreeSet<IncidentId>,
        read_set: &mut Vec<(ResourceId, Version)>,
        writes: &mut Vec<Write>,
    ) {
        if r.expires_at_ms > t.now && r.available > 0 {
            let fresh: Vec<(&Incident, Version)> = first
                .into_iter()
                .chain(
                    waiting
                        .iter()
                        .filter(|(i, _)| !taken.contains(&i.id))
                        .take(self.top_k)
                        .copied(),
                )
                .collect();
            for (i, iv) in fresh {
                if r.available == 0 {
                    break;
                }
                taken.insert(i.id);
                let units = units_needed(i.severity).min(r.available);
                r.available -= units;
                r.holds.push(Hold {
                    incident: i.id,
                    units,
                    expires_at_ms: expiry(i),
                });
                let mut incident = i.clone();
                incident.status = IncidentStatus::Assigned;
                writes.push(Write::PutAssignment(Assignment {
                    incident: i.id,
                    resource: r.id,
                    units,
                    created_at_ms: t.sent,
                    expires_at_ms: expiry(i),
                }));
                read_set.push((i.id, iv));
                writes.push(Write::PutIncident(incident));
            }
        }
        writes.push(Write::PutResource(r));
    }

    /// Row 0's candidates, its `top_k` cheapest resources, best first by the
    /// total cost of the whole assignment when row 0 is forced onto each.
    /// Ties go to the responder the main solve chose, then to the lower ID.
    /// All candidates are ranked; only the best [`RESPONDERS_KEPT`] are
    /// returned.
    fn rank_responders(&self, problem: &mut Problem, chosen: Option<usize>) -> Vec<ResourceId> {
        let candidates: Vec<usize> = problem.lists[0]
            .cols
            .iter()
            .take(self.top_k)
            .copied()
            .collect();
        let mut ranked: Vec<(i64, bool, ResourceId)> = candidates
            .into_iter()
            .map(|j| {
                let (_, total) = problem.solve(Some(j));
                (total, Some(j) != chosen, problem.pool.res[j].offer.id)
            })
            .collect();
        ranked.sort_unstable();
        ranked.truncate(RESPONDERS_KEPT);
        ranked.into_iter().map(|(_, _, id)| id).collect()
    }
}

impl Planner for IncidentPlanner {
    fn plan(&self, snap: &Snapshot, req: &Request) -> PlanOutput {
        let t = Times {
            sent: req.sent_at_ms,
            now: plan_time(snap, req.sent_at_ms),
        };
        let now = t.now;
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
                    reported_at_ms: t.sent,
                    status: IncidentStatus::Open,
                    ttl_ms: *ttl_ms,
                    attachment: attachment.clone(),
                };
                self.report(snap, t, new)
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
                let offer = ResourceOffer {
                    id: req.id,
                    owner,
                    kind: *kind,
                    capacity: *capacity,
                    available: *capacity,
                    zone: *zone,
                    expires_at_ms: now.saturating_add(*lease_ms),
                    holds: Vec::new(),
                };
                // A new resource has no version to check; the incidents it is
                // given to do.
                let (mut read_set, mut writes) = (Vec::new(), Vec::new());
                self.dispatch_alone(snap, t, offer, &mut read_set, &mut writes);
                PlanOutput {
                    read_set,
                    writes,
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
                // Expired holds give their units back first.
                let mut r = release_expired(r.clone(), now);
                // `available` is how many units the owner now offers in total,
                // held ones included. Units still held stay held and cannot be
                // withdrawn, so capacity never drops below them and
                // `capacity == available + held` always holds; a unit can never
                // be given out twice (DL-062).
                let held: u32 = r.holds.iter().map(|h| h.units).sum();
                r.capacity = (*available).max(held);
                r.available = r.capacity - held;
                if let Some(ext) = extend_lease_ms {
                    r.expires_at_ms = r.expires_at_ms.max(now).saturating_add(*ext);
                }
                let mut read_set = vec![(r.id, *v)];
                let mut writes = Vec::new();
                self.dispatch_alone(snap, t, r, &mut read_set, &mut writes);
                PlanOutput {
                    read_set,
                    writes,
                    result: OpResult::Accepted,
                }
            }
            Op::Query(_) => refused("queries are answered from applied state"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proto::{ClientId, ReqId};

    fn id(user: u32, seq: u64) -> ReqId {
        ReqId {
            client: ClientId {
                machine: 1,
                process: 1,
                user,
            },
            incarnation: 1,
            seq,
        }
    }

    const KIND_LIST: [ResourceKind; KINDS] = [
        ResourceKind::Medic,
        ResourceKind::Technician,
        ResourceKind::SecurityOfficer,
        ResourceKind::Vehicle,
        ResourceKind::Generator,
        ResourceKind::FirstAidKit,
        ResourceKind::Volunteer,
    ];
    const CATEGORY_LIST: [Category; 7] = [
        Category::BlockedRoad,
        Category::PowerOutage,
        Category::LabIncident,
        Category::Flooding,
        Category::NetworkOutage,
        Category::MedicalAssistance,
        Category::TechnicalAssistance,
    ];

    /// A random small campus at time 50 000, with resources of few kinds in
    /// few zones, so incidents compete and costs often tie. Some resources
    /// have holds, about half of them expired by then, and some have an
    /// expired lease. Returns the snapshot, a new incident and a `top_k`.
    fn random_campus(seed: u64) -> (Snapshot, Incident, u32) {
        let mut x = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
        let mut next = move |n: u64| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x % n
        };
        let mut snap = Snapshot::default();
        for u in 0..(5 + next(40)) as u32 {
            let capacity = 1 + next(3) as u32;
            let mut holds = Vec::new();
            let mut held = 0;
            while held < capacity && next(3) == 0 {
                holds.push(Hold {
                    incident: id(9_000 + u, held.into()),
                    units: 1,
                    expires_at_ms: 40_000 + next(20_000),
                });
                held += 1;
            }
            let r = ResourceOffer {
                id: id(u, 1),
                owner: id(u, 1).client,
                kind: KIND_LIST[next(4) as usize],
                capacity,
                available: capacity - held,
                zone: ZoneId(next(6) as u16),
                expires_at_ms: if next(10) == 0 { 10_000 } else { 300_000 },
                holds,
            };
            snap.resources.insert(r.id, (r, Version(u.into())));
        }
        let mut incident = |user: u32, reported_at_ms: u64| Incident {
            id: id(user, 1),
            owner: id(user, 1).client,
            category: CATEGORY_LIST[next(7) as usize],
            zone: ZoneId(next(6) as u16),
            severity: Severity(1 + next(5) as u8),
            reported_at_ms,
            status: IncidentStatus::Open,
            ttl_ms: 600_000,
            attachment: None,
        };
        for u in 0..30 {
            let i = incident(1_000 + u, u64::from(u) * 1_500);
            snap.incidents.insert(i.id, (i, Version(1)));
        }
        let new = incident(5_000, 50_000);
        (snap, new, 2 + (seed % 12) as u32)
    }

    /// The main solve's total, and each responder candidate's forced total.
    fn totals(
        p: &IncidentPlanner,
        snap: &Snapshot,
        new: &Incident,
    ) -> (i64, Vec<(ResourceId, i64)>) {
        let now = new.reported_at_ms;
        let pool = p.pool(snap, now);
        let rows = p.rows(snap, new, &pool, now);
        let mut problem = Problem::new(p, &pool, &rows, now);
        let (_, main) = problem.solve(None);
        let candidates: Vec<usize> = problem.lists[0]
            .cols
            .iter()
            .take(p.top_k)
            .copied()
            .collect();
        let mut forced: Vec<(ResourceId, i64)> = candidates
            .into_iter()
            .map(|j| (pool.res[j].offer.id, problem.solve(Some(j)).1))
            .collect();
        forced.sort_unstable();
        (main, forced)
    }

    #[test]
    fn bounding_the_columns_changes_no_total_cost() {
        for seed in 0..400 {
            let (snap, new, top_k) = random_campus(seed);
            let every = IncidentPlanner::new(30, top_k).with_columns_per_row(usize::MAX);
            let want = totals(&every, &snap, &new);
            for c in [1, 2, COLUMNS_PER_ROW] {
                let bounded = every.clone().with_columns_per_row(c);
                assert_eq!(totals(&bounded, &snap, &new), want, "seed {seed}, c {c}");
            }
        }
    }

    #[test]
    fn a_rows_cheapest_are_the_cheapest_by_cost_then_id() {
        for seed in 0..200 {
            let (snap, new, top_k) = random_campus(seed);
            let p = IncidentPlanner::new(30, top_k);
            let now = new.reported_at_ms;
            let pool = p.pool(&snap, now);
            let rows = p.rows(&snap, &new, &pool, now);
            for (i, _) in &rows {
                let mut all: Vec<(i64, ResourceId, usize)> = (0..pool.res.len())
                    .map(|j| {
                        let u = &pool.res[j];
                        (pair_cost(&p.campus, i, u.offer, u.free), u.offer.id, j)
                    })
                    .filter(|e| e.0 < NEVER)
                    .collect();
                all.sort_unstable();
                for c in [1, 2, 3, 5, 100] {
                    let got = p.cheapest(&pool, i, c);
                    let want: Vec<usize> = all.iter().take(c).map(|e| e.2).collect();
                    assert_eq!(got.cols, want, "seed {seed}, c {c}");
                    // When some were left out, `cut_at` bounds their cost.
                    if all.len() > c {
                        let t = got.cut_at.expect("some were left out");
                        assert!(all[c..].iter().all(|e| e.0 >= t), "seed {seed}");
                    }
                }
            }
        }
    }

    #[test]
    fn expired_holds_count_as_free_and_are_released_on_write() {
        let hold = |user, units, expires_at_ms| Hold {
            incident: id(user, 1),
            units,
            expires_at_ms,
        };
        let r = ResourceOffer {
            id: id(1, 1),
            owner: id(1, 1).client,
            kind: ResourceKind::Medic,
            capacity: 6,
            available: 1,
            zone: ZoneId(0),
            expires_at_ms: 1_000_000,
            holds: vec![hold(2, 2, 100), hold(3, 3, 200)],
        };
        assert_eq!(free_units(&r, 99), 1);
        assert_eq!(free_units(&r, 100), 3);
        assert_eq!(free_units(&r, 200), 6);
        let r = release_expired(r, 150);
        assert_eq!((r.capacity, r.available), (6, 3));
        assert_eq!(r.holds, vec![hold(3, 3, 200)]);
    }
}
