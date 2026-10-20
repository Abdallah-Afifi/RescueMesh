//! Single-node tests of individual protocol rules, driven with hand-written
//! messages. The cluster tests cannot always reach these cases, because other
//! safeguards (pre-vote, the view entry each coordinator appends first) keep
//! the dangerous interleavings away; these tests check each rule on its own.

mod common;

use common::{Params, core_cfg, request_id};
use proto::{Entry, EntryKind, Epoch, NodeId, NodeMsg, OpResult, Seq};
use replication::{Action, Core, Role};

const N1: NodeId = NodeId(1);
const N2: NodeId = NodeId(2);
const N3: NodeId = NodeId(3);

fn result_entry(seq: u64, epoch: u64, n: u64) -> Entry {
    Entry {
        seq: Seq(seq),
        epoch: Epoch(epoch),
        kind: EntryKind::Result {
            req: request_id(n),
            executor: N3,
            writes: vec![],
            result: OpResult::Accepted,
        },
    }
}

fn sent(actions: &[Action], to: NodeId) -> Vec<NodeMsg> {
    actions
        .iter()
        .filter_map(|a| match a {
            Action::Send { to: t, msg } if *t == to => Some(msg.clone()),
            _ => None,
        })
        .collect()
}

/// A follower of n3 in epoch 1 holding one uncommitted entry.
fn follower_with_one_entry(id: NodeId) -> Core {
    let mut c = Core::new(id, core_cfg(&Params::calm(0)));
    let append = NodeMsg::Append {
        epoch: Epoch(1),
        prev_seq: Seq(0),
        prev_epoch: Epoch(0),
        entries: vec![result_entry(1, 1, 1)],
        commit: Seq(0),
    };
    c.on_message(10, N3, append);
    assert_eq!(
        (c.last_seq(), c.commit_seq(), c.coordinator()),
        (Seq(1), Seq(0), Some(N3))
    );
    c
}

/// Lose n3 and let `c` win epoch 2 with n2's votes.
fn win_epoch_2(c: &mut Core) {
    c.set_suspected(20, N3, true);
    let mut t = 20;
    loop {
        t += 1;
        let actions = c.tick(t);
        if sent(&actions, N2)
            .iter()
            .any(|m| matches!(m, NodeMsg::PreVote { .. }))
        {
            break;
        }
        assert!(t < 5_000, "never started a pre-vote");
    }
    let a = c.on_message(
        t,
        N2,
        NodeMsg::PreVoteReply {
            next_epoch: Epoch(2),
            granted: true,
        },
    );
    assert!(sent(&a, N2).iter().any(|m| matches!(
        m,
        NodeMsg::Election {
            epoch: Epoch(2),
            ..
        }
    )));
    c.on_message(
        t,
        N2,
        NodeMsg::Vote {
            epoch: Epoch(2),
            granted: true,
        },
    );
    assert_eq!(c.role(), Role::Coordinator);
    assert_eq!(c.epoch(), Epoch(2));
}

#[test]
fn an_entry_from_an_earlier_epoch_is_not_committed_by_counting_replicas() {
    let mut c = follower_with_one_entry(N1);
    win_epoch_2(&mut c);
    // The new coordinator appended its view entry at seq 2 in epoch 2.
    assert!(matches!(
        c.entry(Seq(2)).map(|e| (&e.kind, e.epoch)),
        Some((EntryKind::View(_), Epoch(2)))
    ));
    // n2 now holds seq 1 (epoch 1) but not seq 2: a majority holds seq 1,
    // yet it must not count as committed (Raft, Figure 8).
    c.on_message(
        100,
        N2,
        NodeMsg::AppendAck {
            epoch: Epoch(2),
            ok: true,
            match_seq: Seq(1),
        },
    );
    assert_eq!(c.commit_seq(), Seq(0));
    // Once the current-epoch entry is on a majority, both commit.
    c.on_message(
        101,
        N2,
        NodeMsg::AppendAck {
            epoch: Epoch(2),
            ok: true,
            match_seq: Seq(2),
        },
    );
    assert_eq!(c.commit_seq(), Seq(2));
}

#[test]
fn votes_go_only_to_candidates_whose_log_is_at_least_as_current() {
    let mut c = follower_with_one_entry(N2);
    c.set_suspected(20, N3, true);
    // n1 has an empty log: refused.
    let a = c.on_message(
        30,
        N1,
        NodeMsg::Election {
            epoch: Epoch(2),
            last_epoch: Epoch(0),
            last_seq: Seq(0),
        },
    );
    assert_eq!(
        sent(&a, N1),
        vec![NodeMsg::Vote {
            epoch: Epoch(2),
            granted: false
        }]
    );
    // Same last entry: granted.
    let a = c.on_message(
        31,
        N1,
        NodeMsg::Election {
            epoch: Epoch(3),
            last_epoch: Epoch(1),
            last_seq: Seq(1),
        },
    );
    assert_eq!(
        sent(&a, N1),
        vec![NodeMsg::Vote {
            epoch: Epoch(3),
            granted: true
        }]
    );
    // One vote per epoch.
    let a = c.on_message(
        32,
        N3,
        NodeMsg::Election {
            epoch: Epoch(3),
            last_epoch: Epoch(1),
            last_seq: Seq(1),
        },
    );
    assert_eq!(
        sent(&a, N3),
        vec![NodeMsg::Vote {
            epoch: Epoch(3),
            granted: false
        }]
    );
}

#[test]
fn pre_votes_go_only_to_current_candidates_when_the_coordinator_is_lost() {
    let mut c = follower_with_one_entry(N2);
    // Coordinator still alive: refused, whatever the candidate's log.
    let a = c.on_message(
        20,
        N1,
        NodeMsg::PreVote {
            next_epoch: Epoch(2),
            last_epoch: Epoch(1),
            last_seq: Seq(1),
        },
    );
    assert_eq!(
        sent(&a, N1),
        vec![NodeMsg::PreVoteReply {
            next_epoch: Epoch(2),
            granted: false
        }]
    );
    c.set_suspected(30, N3, true);
    // Lost, but the candidate is behind: refused.
    let a = c.on_message(
        31,
        N1,
        NodeMsg::PreVote {
            next_epoch: Epoch(2),
            last_epoch: Epoch(0),
            last_seq: Seq(0),
        },
    );
    assert_eq!(
        sent(&a, N1),
        vec![NodeMsg::PreVoteReply {
            next_epoch: Epoch(2),
            granted: false
        }]
    );
    // Lost and current: granted. A pre-vote changes no state.
    let a = c.on_message(
        32,
        N1,
        NodeMsg::PreVote {
            next_epoch: Epoch(2),
            last_epoch: Epoch(1),
            last_seq: Seq(1),
        },
    );
    assert_eq!(
        sent(&a, N1),
        vec![NodeMsg::PreVoteReply {
            next_epoch: Epoch(2),
            granted: true
        }]
    );
    assert_eq!(c.epoch(), Epoch(1));
}

#[test]
fn a_follower_with_a_live_coordinator_ignores_candidates() {
    let mut c = follower_with_one_entry(N2);
    let a = c.on_message(
        20,
        N1,
        NodeMsg::Election {
            epoch: Epoch(5),
            last_epoch: Epoch(1),
            last_seq: Seq(1),
        },
    );
    assert_eq!(
        sent(&a, N1),
        vec![NodeMsg::Vote {
            epoch: Epoch(5),
            granted: false
        }]
    );
    assert_eq!(c.epoch(), Epoch(1), "the epoch must not be disturbed");
    assert_eq!(c.coordinator(), Some(N3));
}

#[test]
fn an_append_from_an_older_epoch_is_fenced() {
    let mut c = follower_with_one_entry(N2);
    c.set_suspected(20, N3, true);
    c.on_message(
        30,
        N1,
        NodeMsg::Election {
            epoch: Epoch(2),
            last_epoch: Epoch(1),
            last_seq: Seq(1),
        },
    );
    assert_eq!(c.epoch(), Epoch(2));
    // The old coordinator returns and sends with epoch 1.
    let stale = NodeMsg::Append {
        epoch: Epoch(1),
        prev_seq: Seq(1),
        prev_epoch: Epoch(1),
        entries: vec![result_entry(2, 1, 2)],
        commit: Seq(2),
    };
    let a = c.on_message(40, N3, stale);
    assert_eq!(
        sent(&a, N3),
        vec![NodeMsg::AppendAck {
            epoch: Epoch(2),
            ok: false,
            match_seq: Seq(0)
        }]
    );
    assert_eq!((c.last_seq(), c.commit_seq()), (Seq(1), Seq(0)));
}

#[test]
fn a_conflicting_uncommitted_suffix_is_replaced() {
    let mut c = follower_with_one_entry(N2);
    let fresh = NodeMsg::Append {
        epoch: Epoch(2),
        prev_seq: Seq(0),
        prev_epoch: Epoch(0),
        entries: vec![result_entry(1, 2, 9)],
        commit: Seq(1),
    };
    let a = c.on_message(50, N1, fresh);
    assert_eq!(
        sent(&a, N1),
        vec![NodeMsg::AppendAck {
            epoch: Epoch(2),
            ok: true,
            match_seq: Seq(1)
        }]
    );
    assert_eq!(c.entry(Seq(1)).unwrap().epoch, Epoch(2));
    assert_eq!(c.commit_seq(), Seq(1));
}
