use std::time::{Duration, Instant};

use crate::{PersistentStorage, Request, State, TimeRange};
use crate::msg::{AppendEntries, VoteReceived};
use crate::state_machine::RaftSM;
use crate::tests::{TestCommand, TestSender};
use crate::tests::storage::in_mem::InMemStorage;

#[test]
// TODO - Move that test to proptest to check that the logic is working regardless of how many
// entries that we got, to see that the correct last index and term are always correct.
fn test_follower_is_moving_to_candidate_on_timeout() {
    let node_id = 0;
    let seeds = (1usize..=2).collect::<Vec<_>>();
    let time_range = TimeRange::new(150, 300);
    let sender = TestSender::new();
    let storage = InMemStorage::empty();

    let mut sm = RaftSM::<usize, TestCommand>::new(node_id, &time_range, seeds.clone(), None);
    let election_timeout = sm.election_timeout;
    let term = sm.term;
    let new_time = Instant::now() + election_timeout;
    let last_entry = storage.last_entry_or_default();

    assert_eq!(State::Follower, sm.state);

    sm.handle_tick(&time_range, &storage, &sender, new_time);

    assert_eq!(State::Candidate, sm.state);
    assert_ne!(election_timeout, sm.election_timeout);
    assert_eq!(term + 1, sm.term);
    assert_eq!(new_time, sm.time);
    assert_eq!(Some(node_id), sm.voted_for);

    let mut reqs = sender.take();

    reqs.sort_by(|a, b| a.target.cmp(&b.target));

    assert!(!reqs.is_empty());
    assert_eq!(reqs.len(), seeds.len());

    for (seed, req) in seeds.into_iter().zip(reqs.into_iter()) {
        assert_eq!(seed, req.target);

        if let Request::RequestVote(args) = req.request {
            assert_eq!(node_id, args.candidate_id);
            assert_eq!(sm.term, args.term);
            assert_eq!(last_entry.index, args.last_log_index);
            assert_eq!(last_entry.term, args.last_log_term);

            continue;
        }

        panic!("We expected to only deal with vote requests");
    }
}

#[test]
// TODO - Move that test to proptest to check that the logic is working regardless of how many
// entries that we got, to see that the correct prev index and term are always correct.
fn test_move_to_leader_when_garnered_enough_votes() {
    let node_id = 0;
    let seeds = (1usize..=2).collect::<Vec<_>>();
    let time_range = TimeRange::new(150, 300);
    let sender = TestSender::new();
    let storage = InMemStorage::empty();

    let mut sm = RaftSM::<usize, TestCommand>::new(node_id, &time_range, seeds.clone(), None);
    let election_timeout = sm.election_timeout;
    let new_time = Instant::now() + election_timeout;

    sm.handle_tick(&time_range, &storage, &sender, new_time);

    // We clear the vote requests
    sender.take();

    sm.handle_vote_received(
        &time_range,
        &storage,
        &sender,
        new_time + Duration::from_millis(10),
        VoteReceived {
            node_id: 1,
            term: sm.term,
            granted: true,
        },
    );

    assert_eq!(State::Leader, sm.state);

    // We check that as the leader of the cluster, the node send append entries RPC as soon as
    // possible.
    let mut reqs = sender.take();
    reqs.sort_by(|a, b| a.target.cmp(&b.target));

    assert!(!reqs.is_empty());
    assert_eq!(seeds.len(), reqs.len());

    let last_entry = storage.last_entry_or_default();
    for (seed, req) in seeds.into_iter().zip(reqs.into_iter()) {
        assert_eq!(seed, req.target);

        if let Request::AppendEntries(args) = req.request {
            assert_eq!(args.leader_id, node_id);
            assert_eq!(args.leader_commit, sm.commit_index);
            assert_eq!(args.term, sm.term);
            assert_eq!(args.prev_log_index, last_entry.index);
            assert_eq!(args.prev_log_term, last_entry.term);

            continue;
        }

        panic!("We expected to only deal with append entries requests");
    }
}

#[test]
// TODO - Move that test to proptest to check that the logic is working regardless of how many
// entries that we got.
fn move_from_candidate_to_follower_when_leader_show_up_empty_log() {
    let node_id = 0;
    let seeds = (1usize..=2).collect::<Vec<_>>();
    let time_range = TimeRange::new(150, 300);
    let sender = TestSender::new();
    let mut storage = InMemStorage::empty();

    let mut sm = RaftSM::<usize, TestCommand>::new(node_id, &time_range, seeds.clone(), None);
    sm.handle_tick(
        &time_range,
        &storage,
        &sender,
        Instant::now() + sm.election_timeout,
    );

    assert_eq!(State::Candidate, sm.state);
    sender.take();

    let new_time = Instant::now() + Duration::from_millis(10);
    let term = sm.term;
    sm.handle_append_entries(
        &sender,
        &mut storage,
        new_time,
        AppendEntries {
            term: sm.term,
            leader_id: 1,
            prev_log_index: 0,
            prev_log_term: 0,
            leader_commit: 0,
            entries: vec![],
        },
    );

    let mut reqs = sender.take();

    assert_eq!(1, reqs.len());

    let req = reqs.pop().unwrap();

    assert_eq!(1, req.target);

    let args = if let Request::EntriesReplicated(args) = req.request {
        args
    } else {
        panic!("We expected entries replicated msg");
    };

    assert_eq!(new_time, sm.time);
    assert_eq!(State::Follower, sm.state);
    assert_eq!(term, sm.term);
    assert_eq!(term, args.term);
    assert_eq!(node_id, args.node_id);
    assert!(args.success);
}

#[test]
// TODO - Move that test to proptest to check that the logic is working regardless of how many
// entries that we got.
fn move_from_leader_to_follower_if_better_leader_is_showing_up() {
    let node_id = 0;
    let seeds = (1usize..=2).collect::<Vec<_>>();
    let time_range = TimeRange::new(150, 300);
    let sender = TestSender::new();
    let mut storage = InMemStorage::empty();

    let mut sm = RaftSM::<usize, TestCommand>::new(node_id, &time_range, seeds.clone(), None);
    let election_timeout = sm.election_timeout;
    let new_time = Instant::now() + election_timeout;

    sm.handle_tick(&time_range, &storage, &sender, new_time);

    // We clear the vote requests
    sender.take();

    sm.handle_vote_received(
        &time_range,
        &storage,
        &sender,
        new_time + Duration::from_millis(10),
        VoteReceived {
            node_id: 1,
            term: sm.term,
            granted: true,
        },
    );

    assert_eq!(State::Leader, sm.state);

    let new_term = sm.term + 1;
    let new_time = sm.time + Duration::from_millis(10);
    sm.handle_append_entries(
        &sender,
        &mut storage,
        new_time,
        AppendEntries {
            term: new_term,
            leader_id: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            leader_commit: 0,
            entries: vec![],
        },
    );

    assert_eq!(State::Follower, sm.state);
    assert_eq!(new_term, sm.term);
    assert_eq!(new_time, sm.time);
}
