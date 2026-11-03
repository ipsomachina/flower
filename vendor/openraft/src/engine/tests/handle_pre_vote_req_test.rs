//! Flower: pre-votes (see `Config::pre_vote`) are granted only where a vote
//! would be, and change nothing on the voter; a node refused by a voter
//! holding a greater vote adopts it, uncommitted.

use std::sync::Arc;
use std::time::Duration;

use maplit::btreeset;
use pretty_assertions::assert_eq;

use crate::core::ServerState;
use crate::engine::testing::UTConfig;
use crate::engine::Command;
use crate::engine::Engine;
use crate::engine::LogIdList;
use crate::raft::VoteRequest;
use crate::raft::VoteResponse;
use crate::testing::log_id;
use crate::utime::UTime;
use crate::EffectiveMembership;
use crate::LogId;
use crate::Membership;
use crate::TokioInstant;
use crate::Vote;

fn m012() -> Membership<u64, ()> {
    Membership::<u64, ()>::new(vec![btreeset! {0,1,2}], None)
}

/// Node 0, following node 1 in term 2 with the log to (2, 1, 3), last
/// heard from `ago`; the default leader lease is 150 ms.
fn follower(ago: Duration) -> Engine<UTConfig> {
    member(0, Vote::new_committed(2, 1), log_id(2, 1, 3), ago)
}

/// Node `id` holding `vote`, set `ago`, with the log to `last`.
fn member(id: u64, vote: Vote<u64>, last: LogId<u64>, ago: Duration) -> Engine<UTConfig> {
    let mut eng = Engine::default();
    eng.state.enable_validation(false); // Disable validation for incomplete state

    eng.config.id = id;
    eng.state.vote = UTime::new(TokioInstant::now() - ago, vote);
    eng.state.log_ids = LogIdList::new(vec![last]);
    eng.state
        .membership_state
        .set_effective(Arc::new(EffectiveMembership::new(Some(log_id(1, 1, 1)), m012())));
    eng.vote_handler().update_internal_server_state();
    eng.output.take_commands();

    eng
}

/// What a pre-vote request must leave as it was.
fn assert_unchanged(eng: &mut Engine<UTConfig>, heard: Option<TokioInstant>) {
    assert_eq!(Vote::new_committed(2, 1), *eng.state.vote_ref());
    assert_eq!(heard, eng.state.vote_last_modified());
    assert_eq!(ServerState::Follower, eng.state.server_state);
    assert!(eng.leader.is_none());
    assert!(eng.candidate_ref().is_none());
    assert_eq!(0, eng.output.take_commands().len());
}

#[test]
fn test_handle_pre_vote_req_granted_after_the_lease_changes_nothing() -> anyhow::Result<()> {
    let mut eng = follower(Duration::from_millis(300));
    let heard = eng.state.vote_last_modified();

    let resp = eng.handle_vote_req(VoteRequest::new_pre_vote(Vote::new(3, 2), Some(log_id(2, 1, 3))));

    assert_eq!(
        VoteResponse::new(Vote::new_committed(2, 1), Some(log_id(2, 1, 3)), true),
        resp
    );
    assert_unchanged(&mut eng, heard);

    // The real vote that follows is granted and saved.
    let resp = eng.handle_vote_req(VoteRequest::new(Vote::new(3, 2), Some(log_id(2, 1, 3))));
    assert_eq!(VoteResponse::new(Vote::new(3, 2), Some(log_id(2, 1, 3)), true), resp);
    assert_eq!(Vote::new(3, 2), *eng.state.vote_ref());

    Ok(())
}

#[test]
fn test_handle_pre_vote_req_refused_within_the_lease_unless_released() -> anyhow::Result<()> {
    let mut eng = follower(Duration::ZERO);
    let heard = eng.state.vote_last_modified();
    let req = VoteRequest::new_pre_vote(Vote::new(3, 2), Some(log_id(2, 1, 3)));

    let resp = eng.handle_vote_req(req.clone());

    assert!(!resp.vote_granted);
    assert_unchanged(&mut eng, heard);

    // A transfer released the leader's lease: the target's pre-vote passes.
    assert!(eng.release_leader_lease(&Vote::new_committed(2, 1)));
    let resp = eng.handle_vote_req(req);

    assert!(resp.vote_granted);
    assert_unchanged(&mut eng, heard);

    Ok(())
}

#[test]
fn test_handle_pre_vote_req_refused_for_a_shorter_log() -> anyhow::Result<()> {
    let mut eng = follower(Duration::from_millis(300));
    let heard = eng.state.vote_last_modified();

    for last_log_id in [None, Some(log_id(1, 1, 3)), Some(log_id(2, 1, 2))] {
        let resp = eng.handle_vote_req(VoteRequest::new_pre_vote(Vote::new(3, 2), last_log_id));

        assert_eq!(
            VoteResponse::new(Vote::new_committed(2, 1), Some(log_id(2, 1, 3)), false),
            resp,
            "{last_log_id:?}"
        );
        assert_unchanged(&mut eng, heard);
    }

    Ok(())
}

#[test]
fn test_handle_pre_vote_req_refused_for_a_smaller_vote() -> anyhow::Result<()> {
    let mut eng = follower(Duration::from_millis(300));
    let heard = eng.state.vote_last_modified();

    let resp = eng.handle_vote_req(VoteRequest::new_pre_vote(Vote::new(1, 2), Some(log_id(2, 1, 3))));

    assert!(!resp.vote_granted);
    assert_unchanged(&mut eng, heard);

    Ok(())
}

#[test]
fn test_handle_pre_vote_req_refused_while_leading() -> anyhow::Result<()> {
    let mut eng = follower(Duration::from_millis(300));
    eng.testing_new_leader();

    let resp = eng.handle_vote_req(VoteRequest::new_pre_vote(Vote::new(3, 2), Some(log_id(2, 1, 3))));

    assert!(!resp.vote_granted);
    assert_eq!(Vote::new_committed(2, 1), *eng.state.vote_ref());
    assert!(eng.leader.is_some());
    assert_eq!(0, eng.output.take_commands().len());

    Ok(())
}

#[test]
fn test_handle_pre_vote_refused_adopts_a_greater_vote_uncommitted() -> anyhow::Result<()> {
    let mut eng = follower(Duration::from_millis(300));

    // Voter 2 refused holding its vote in term 3, which it campaigned for.
    eng.handle_pre_vote_refused(Vote::new(3, 2));

    assert_eq!(Vote::new(3, 2), *eng.state.vote_ref());
    assert_eq!(ServerState::Follower, eng.state.server_state);
    assert_eq!(
        vec![Command::SaveVote { vote: Vote::new(3, 2) }],
        eng.output.take_commands()
    );

    // A leader's vote is adopted uncommitted: this node has not heard from
    // that leader, whose appends then supersede it.
    eng.handle_pre_vote_refused(Vote::new_committed(4, 2));

    assert_eq!(Vote::new(4, 2), *eng.state.vote_ref());
    assert_eq!(
        vec![Command::SaveVote { vote: Vote::new(4, 2) }],
        eng.output.take_commands()
    );

    // A smaller or equal vote changes nothing.
    for vote in [Vote::new(3, 2), Vote::new(4, 1), Vote::new(4, 2)] {
        eng.handle_pre_vote_refused(vote);
        assert_eq!(Vote::new(4, 2), *eng.state.vote_ref());
        assert!(eng.output.take_commands().is_empty());
    }

    Ok(())
}

/// The member with the longest log asks for its own term + 1 and is
/// refused by a voter that voted in that term already, whose own pre-vote
/// for the term after it refuses for its shorter log. Adopting the
/// refusal's vote, the first asks for the term after it next, and is
/// granted.
#[test]
fn test_a_pre_vote_refused_by_a_higher_vote_asks_for_the_term_after_it() -> anyhow::Result<()> {
    let long = Duration::from_millis(300);
    // Node 0 followed node 1 in term 2 and holds its last entry; node 2
    // campaigned in term 3 without an answer, its log one entry short.
    let mut ahead = follower(long);
    let mut campaigner = member(2, Vote::new(3, 2), log_id(2, 1, 2), long);

    let refused = campaigner.handle_vote_req(VoteRequest::new_pre_vote(Vote::new(3, 0), Some(log_id(2, 1, 3))));
    assert!(!refused.vote_granted);
    assert_eq!(Vote::new(3, 2), refused.vote);
    let theirs = ahead.handle_vote_req(VoteRequest::new_pre_vote(Vote::new(4, 2), Some(log_id(2, 1, 2))));
    assert!(!theirs.vote_granted);

    ahead.handle_pre_vote_refused(refused.vote);

    let next = Vote::new(ahead.state.vote_ref().leader_id().get_term() + 1, 0);
    assert_eq!(Vote::new(4, 0), next);
    let resp = campaigner.handle_vote_req(VoteRequest::new_pre_vote(next, Some(log_id(2, 1, 3))));
    assert!(resp.vote_granted);
    let resp = campaigner.handle_vote_req(VoteRequest::new(next, Some(log_id(2, 1, 3))));
    assert!(resp.vote_granted);
    assert_eq!(next, *campaigner.state.vote_ref());

    Ok(())
}
