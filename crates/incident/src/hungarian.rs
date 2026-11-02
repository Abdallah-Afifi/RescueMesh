//! Minimum-cost assignment of rows to distinct columns: the Hungarian
//! algorithm with potentials, O(rows² · columns). Integer arithmetic only, and
//! columns are scanned in index order with strict comparisons, so equal inputs
//! give equal answers on every machine (DL-023).

/// An optimal assignment and the dual prices that prove it optimal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Solution {
    /// The column given to each row.
    pub cols: Vec<usize>,
    /// `row_price[i]`: the row's dual potential `u_i`. With column potentials
    /// `v_j ≤ 0`, and `v_j = 0` for every column no row took, the solver keeps
    /// `u_i + v_j ≤ cost[i][j]` everywhere. So a column left out of the
    /// matrix, which would have `v_j = 0`, could not improve the answer if
    /// `u_i ≤ cost[i][j]` for every row: that is the planner's check that
    /// leaving columns out lost nothing.
    pub row_price: Vec<i64>,
}

/// For an `n × m` cost matrix with `n ≤ m`, returns the column given to each
/// row, minimising the total cost. Costs must stay well inside `i64`.
pub fn assign(cost: &[Vec<i64>]) -> Vec<usize> {
    solve(cost).cols
}

/// [`assign`], with the row potentials of the optimal dual.
pub fn solve(cost: &[Vec<i64>]) -> Solution {
    let n = cost.len();
    if n == 0 {
        return Solution {
            cols: Vec::new(),
            row_price: Vec::new(),
        };
    }
    let m = cost[0].len();
    assert!(n <= m, "need at least as many columns as rows");
    const INF: i64 = i64::MAX / 4;
    // 1-based, as in the textbook formulation; index 0 is a sentinel.
    let mut u = vec![0i64; n + 1];
    let mut v = vec![0i64; m + 1];
    let mut row_of = vec![0usize; m + 1];
    let mut way = vec![0usize; m + 1];
    // Reset for each row rather than reallocated.
    let mut minv = vec![INF; m + 1];
    let mut used = vec![false; m + 1];
    for i in 1..=n {
        row_of[0] = i;
        let mut j0 = 0;
        minv.fill(INF);
        used.fill(false);
        loop {
            used[j0] = true;
            let i0 = row_of[j0];
            let mut delta = INF;
            let mut j1 = 0;
            for j in 1..=m {
                if !used[j] {
                    let cur = cost[i0 - 1][j - 1] - u[i0] - v[j];
                    if cur < minv[j] {
                        minv[j] = cur;
                        way[j] = j0;
                    }
                    if minv[j] < delta {
                        delta = minv[j];
                        j1 = j;
                    }
                }
            }
            for j in 0..=m {
                if used[j] {
                    u[row_of[j]] += delta;
                    v[j] -= delta;
                } else {
                    minv[j] -= delta;
                }
            }
            j0 = j1;
            if row_of[j0] == 0 {
                break;
            }
        }
        loop {
            let j1 = way[j0];
            row_of[j0] = row_of[j1];
            j0 = j1;
            if j0 == 0 {
                break;
            }
        }
    }
    let mut col = vec![0usize; n];
    for j in 1..=m {
        if row_of[j] != 0 {
            col[row_of[j] - 1] = j - 1;
        }
    }
    Solution {
        cols: col,
        row_price: u[1..].to_vec(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn total(cost: &[Vec<i64>], cols: &[usize]) -> i64 {
        cols.iter().enumerate().map(|(i, &j)| cost[i][j]).sum()
    }

    /// Best total over every way of giving `n` rows distinct columns.
    fn brute(cost: &[Vec<i64>], row: usize, used: &mut Vec<bool>) -> i64 {
        if row == cost.len() {
            return 0;
        }
        let mut best = i64::MAX;
        for j in 0..used.len() {
            if !used[j] {
                used[j] = true;
                best = best.min(cost[row][j] + brute(cost, row + 1, used));
                used[j] = false;
            }
        }
        best
    }

    #[test]
    fn matches_brute_force_on_many_small_matrices() {
        let mut x: u64 = 7;
        let mut next = || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        for _ in 0..500 {
            let n = 1 + (next() % 5) as usize;
            let m = n + (next() % 3) as usize;
            let cost: Vec<Vec<i64>> = (0..n)
                .map(|_| (0..m).map(|_| (next() % 40) as i64 - 10).collect())
                .collect();
            let cols = assign(&cost);
            let mut seen = cols.clone();
            seen.sort_unstable();
            seen.dedup();
            assert_eq!(seen.len(), n, "columns must be distinct: {cost:?}");
            assert_eq!(
                total(&cost, &cols),
                brute(&cost, 0, &mut vec![false; m]),
                "{cost:?}"
            );
        }
    }

    #[test]
    fn row_prices_are_a_feasible_dual_that_proves_the_answer_optimal() {
        let mut x: u64 = 11;
        let mut next = || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        for _ in 0..300 {
            let n = 1 + (next() % 6) as usize;
            let m = n + (next() % 4) as usize;
            let cost: Vec<Vec<i64>> = (0..n)
                .map(|_| (0..m).map(|_| (next() % 50) as i64).collect())
                .collect();
            let s = solve(&cost);
            // Column prices from the tight assigned pairs; columns nobody
            // took are priced 0.
            let mut v = vec![0i64; m];
            for (i, &j) in s.cols.iter().enumerate() {
                v[j] = cost[i][j] - s.row_price[i];
                assert!(v[j] <= 0, "{cost:?}");
            }
            for i in 0..n {
                for j in 0..m {
                    assert!(s.row_price[i] + v[j] <= cost[i][j], "{cost:?}");
                }
            }
            // Strong duality: the prices add up to the optimal total.
            let dual: i64 = s.row_price.iter().sum::<i64>() + v.iter().sum::<i64>();
            assert_eq!(dual, total(&cost, &s.cols), "{cost:?}");
        }
    }

    #[test]
    fn same_input_same_answer_even_with_ties() {
        let cost = vec![vec![5; 6]; 4];
        assert_eq!(assign(&cost), assign(&cost));
        assert!(assign(&[]).is_empty());
    }
}
