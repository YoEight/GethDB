use std::time::{Duration, Instant};

use crate::{PersistentStorage, Request, State, TimeRange};
use crate::msg::VoteReceived;
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

    let mut reqs = sender.take();
    reqs.sort_by(|a, b| a.target.cmp(&b.target));

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
