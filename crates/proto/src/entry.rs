//! Replicated log entries (Section 3) and the membership view (Section 6).

use crate::domain::{OpResult, Write};
use crate::ids::{Epoch, NodeId, ReqId, Seq, ViewNo};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entry {
    pub seq: Seq,
    /// Epoch of the coordinator that appended the entry. Log matching compares
    /// `(seq, epoch)` pairs (Section 7, step 7).
    pub epoch: Epoch,
    pub kind: EntryKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EntryKind {
    /// The committed outcome of one client request. At most one per `req`
    /// (invariant I1).
    Result {
        req: ReqId,
        executor: NodeId,
        writes: Vec<Write>,
        result: OpResult,
    },
    /// A membership change. The new coordinator's first entry is always one.
    View(View),
    /// A scheduled or operator-requested silence (Section 8).
    Inject {
        /// Injection counter; the schedule is derived from `(seed, k)`.
        k: u64,
        victim: NodeId,
        duration_ms: u64,
        by_operator: bool,
    },
    /// Appended by a new coordinator when it has nothing else to commit, so
    /// that entries from earlier epochs become committed.
    Noop,
}

/// The agreed set of active nodes. Deserialization goes through
/// [`View::new`], so a view received from the network is always sorted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(from = "ViewWire")]
pub struct View {
    pub no: ViewNo,
    /// Sorted, without duplicates. Use [`View::new`] to keep it that way.
    active: Vec<NodeId>,
}

#[derive(Deserialize)]
struct ViewWire {
    no: ViewNo,
    active: Vec<NodeId>,
}

impl From<ViewWire> for View {
    fn from(w: ViewWire) -> View {
        View::new(w.no, w.active)
    }
}

impl View {
    pub fn new(no: ViewNo, mut active: Vec<NodeId>) -> Self {
        active.sort();
        active.dedup();
        View { no, active }
    }

    pub fn active(&self) -> &[NodeId] {
        &self.active
    }

    pub fn contains(&self, n: NodeId) -> bool {
        self.active.binary_search(&n).is_ok()
    }

    pub fn without(&self, n: NodeId) -> View {
        View::new(
            self.no.next(),
            self.active.iter().copied().filter(|&m| m != n).collect(),
        )
    }

    pub fn with(&self, n: NodeId) -> View {
        let mut a = self.active.clone();
        a.push(n);
        View::new(self.no.next(), a)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn view_stays_sorted_and_unique() {
        let v = View::new(ViewNo(1), vec![NodeId(3), NodeId(1), NodeId(3)]);
        assert_eq!(v.active(), &[NodeId(1), NodeId(3)]);
        let v2 = v.with(NodeId(2));
        assert_eq!(v2.active(), &[NodeId(1), NodeId(2), NodeId(3)]);
        assert_eq!(v2.no, ViewNo(2));
        assert!(!v2.without(NodeId(2)).contains(NodeId(2)));
    }

    #[test]
    fn unsorted_view_from_the_wire_is_normalized() {
        let v: View = serde_json::from_str(r#"{"no":4,"active":[3,1,3]}"#).unwrap();
        assert_eq!(v.active(), &[NodeId(1), NodeId(3)]);
    }
}
