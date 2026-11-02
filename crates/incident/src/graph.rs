//! The synthetic campus map (design log DL-020): a weighted graph of zones,
//! with every shortest distance and route computed once at start-up.
//!
//! Built from the zone count alone, so every node holds the identical map.
//! Weights are walking minutes, 1–9, drawn from a pinned hash.

use proto::ZoneId;
use proto::hash::fnv1a;
use std::cmp::Reverse;
use std::collections::BinaryHeap;

/// Distances and routes between every pair of zones.
#[derive(Debug, Clone)]
pub struct Campus {
    /// `dist[a][b]`: length of the shortest path from `a` to `b`.
    dist: Vec<Vec<u32>>,
    /// `prev[a][b]`: the zone before `b` on the shortest path from `a`.
    prev: Vec<Vec<u16>>,
}

fn weight(a: u16, b: u16) -> u32 {
    let (lo, hi) = (a.min(b), a.max(b));
    let mut bytes = [0u8; 4];
    bytes[..2].copy_from_slice(&lo.to_le_bytes());
    bytes[2..].copy_from_slice(&hi.to_le_bytes());
    1 + (fnv1a(&bytes) % 9) as u32
}

impl Campus {
    /// A ring of `zones` zones, so every zone is reachable, plus a chord from
    /// each zone to the one five steps on, so paths are not all the long way
    /// round. Panics for fewer than 2 zones (`Config::validate` refuses them).
    pub fn new(zones: u16) -> Campus {
        assert!(zones >= 2, "the campus needs at least 2 zones");
        let n = usize::from(zones);
        let mut adj: Vec<Vec<(u16, u32)>> = vec![Vec::new(); n];
        let mut link = |a: u16, b: u16| {
            if a != b && !adj[usize::from(a)].iter().any(|&(x, _)| x == b) {
                let w = weight(a, b);
                adj[usize::from(a)].push((b, w));
                adj[usize::from(b)].push((a, w));
            }
        };
        for z in 0..zones {
            link(z, (z + 1) % zones);
            if zones > 5 {
                link(z, (z + 5) % zones);
            }
        }
        let (dist, prev) = (0..zones).map(|s| dijkstra(&adj, s)).unzip();
        Campus { dist, prev }
    }

    pub fn zones(&self) -> u16 {
        self.dist.len() as u16
    }

    pub fn contains(&self, z: ZoneId) -> bool {
        z.0 < self.zones()
    }

    pub fn distance(&self, a: ZoneId, b: ZoneId) -> u32 {
        self.dist[usize::from(a.0)][usize::from(b.0)]
    }

    /// Zones from `from` to `to` inclusive, along a shortest path.
    pub fn route(&self, from: ZoneId, to: ZoneId) -> Vec<ZoneId> {
        let row = &self.prev[usize::from(from.0)];
        let mut path = vec![to];
        let mut at = to.0;
        while at != from.0 {
            at = row[usize::from(at)];
            path.push(ZoneId(at));
        }
        path.reverse();
        path
    }
}

/// Shortest paths from `src`. Ties go to the lower zone number, so the routes
/// are the same on every machine.
fn dijkstra(adj: &[Vec<(u16, u32)>], src: u16) -> (Vec<u32>, Vec<u16>) {
    let n = adj.len();
    let mut dist = vec![u32::MAX; n];
    let mut prev = vec![src; n];
    let mut heap = BinaryHeap::new();
    dist[usize::from(src)] = 0;
    heap.push(Reverse((0u32, src)));
    while let Some(Reverse((d, u))) = heap.pop() {
        if d > dist[usize::from(u)] {
            continue;
        }
        for &(v, w) in &adj[usize::from(u)] {
            let nd = d + w;
            let (vi, best) = (usize::from(v), &mut dist[usize::from(v)]);
            if nd < *best || (nd == *best && u < prev[vi]) {
                *best = nd;
                prev[vi] = u;
                heap.push(Reverse((nd, v)));
            }
        }
    }
    (dist, prev)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distances_are_symmetric_finite_and_obey_the_triangle_inequality() {
        let c = Campus::new(30);
        for a in 0..30 {
            assert_eq!(c.distance(ZoneId(a), ZoneId(a)), 0);
            for b in 0..30 {
                let d = c.distance(ZoneId(a), ZoneId(b));
                assert!(d < u32::MAX);
                assert_eq!(d, c.distance(ZoneId(b), ZoneId(a)));
                for m in 0..30 {
                    assert!(
                        d <= c.distance(ZoneId(a), ZoneId(m)) + c.distance(ZoneId(m), ZoneId(b))
                    );
                }
            }
        }
    }

    #[test]
    fn a_route_starts_and_ends_right_and_has_the_shortest_length() {
        let c = Campus::new(30);
        for (a, b) in [(0, 0), (0, 17), (29, 3), (12, 13)] {
            let r = c.route(ZoneId(a), ZoneId(b));
            assert_eq!((r[0], *r.last().unwrap()), (ZoneId(a), ZoneId(b)));
            let len: u32 = r.windows(2).map(|w| weight(w[0].0, w[1].0)).sum();
            assert_eq!(len, c.distance(ZoneId(a), ZoneId(b)));
        }
    }

    #[test]
    fn the_map_is_the_same_every_time_it_is_built() {
        let (a, b) = (Campus::new(30), Campus::new(30));
        assert_eq!((a.dist, a.prev), (b.dist, b.prev));
    }
}
