from typing import Dict, List

import raftlog
import raftmessage
import raftstate
from test_raftlog import paper_log, logs_by_identifier


def init_raft_state(
    log: List[raftlog.LogEntry],
    role: raftstate.Role,
    current_term: int,
    identifier: int = 0,
) -> raftstate.RaftState:
    raft_state = raftstate.RaftState(identifier)
    raft_state.identifier = identifier
    raft_state.log = log
    raft_state.current_term = current_term

    messages = raft_state.handle_role_change(role)

    return raft_state


def init_raft_states(
    leader_log: List[raftlog.LogEntry], follower_log: List[raftlog.LogEntry]
):
    leader_state = init_raft_state(leader_log, raftstate.Role.LEADER, 6)
    follower_state = init_raft_state(follower_log, raftstate.Role.FOLLOWER, 6)

    return leader_state, follower_state


def test_update_next_index(paper_log: List[raftlog.LogEntry]) -> None:
    leader_state = init_raft_state(
        paper_log,
        raftstate.Role.LEADER,
        6,
    )
    assert leader_state.next_index[1] == 10

    leader_state.update_next_index(1, 1)
    assert leader_state.next_index[1] == 11


def test_update_commit_index(paper_log: List[raftlog.LogEntry]) -> None:
    leader_state = init_raft_state(paper_log, raftstate.Role.LEADER, 6)
    assert leader_state.commit_index == -1

    leader_state.match_index = {0: 9, 1: 9, 2: None}
    leader_state.update_commit_index()
    assert leader_state.commit_index == 9


def test_create_append_entries_arguments(paper_log: List[raftlog.LogEntry]) -> None:
    leader_state = init_raft_state(paper_log, raftstate.Role.LEADER, 6)

    (
        previous_index,
        previous_term,
        entries,
        commit_index,
    ) = leader_state.create_append_entries_arguments(1)

    assert previous_index == 9
    assert previous_term == 6
    assert entries == []
    assert commit_index == -1


def test_handle_append_entries_request(
    logs_by_identifier: Dict[str, List[raftlog.LogEntry]]
) -> None:
    # Figure 7a
    follower_state = init_raft_state(
        logs_by_identifier["a"], raftstate.Role.FOLLOWER, 6
    )

    response, _ = follower_state.handle_append_entries_request(
        0, 1, 8, 6, [raftlog.LogEntry(6, "9")], -1
    )
    assert isinstance(response[0], raftmessage.AppendEntryResponse)
    assert response[0].success
    assert response[0].entries_length == 1
    assert response[0].properties["pre_length"] == 9
    assert response[0].properties["post_length"] == 10

    response, _ = follower_state.handle_append_entries_request(
        0, 1, 10, 6, [raftlog.LogEntry(6, "11")], -1
    )
    assert isinstance(response[0], raftmessage.AppendEntryResponse)
    assert not response[0].success
    assert response[0].entries_length == 1
    assert response[0].properties["pre_length"] == 10
    assert response[0].properties["post_length"] == 10


def test_handle_append_entries_response(paper_log: List[raftlog.LogEntry]) -> None:
    # Figure 7
    leader_state = init_raft_state(paper_log, raftstate.Role.LEADER, 6)

    response, _ = leader_state.handle_append_entries_response(
        1, 0, False, 0, {"pre_length": 9, "post_length": 9}
    )

    assert isinstance(response[0], raftmessage.AppendEntryRequest)
    assert response[0].previous_index == 8
    assert response[0].previous_term == 6
    assert response[0].entries == [raftlog.LogEntry(6, "9")]

    response, _ = leader_state.handle_append_entries_response(
        1, 0, True, 1, {"pre_length": 9, "post_length": 9}
    )
    assert len(response) == 0


def test_handle_leader_heartbeat(paper_log: List[raftlog.LogEntry]) -> None:
    # Figure 7
    leader_state = init_raft_state(paper_log, raftstate.Role.LEADER, 6)

    response, _ = leader_state.handle_leader_heartbeat(0, 0, [1])

    assert isinstance(response[0], raftmessage.AppendEntryRequest)
    assert response[0].previous_index == 9
    assert response[0].previous_term == 6
    assert len(response[0].entries) == 0


def test_handle_message_a(
    paper_log: List[raftlog.LogEntry],
    logs_by_identifier: Dict[str, List[raftlog.LogEntry]],
) -> None:
    # Figure 7a
    leader_state, follower_state = init_raft_states(paper_log, logs_by_identifier["a"])

    request, _ = leader_state.handle_message(raftmessage.UpdateFollowers(0, 0, [1]))
    response, _ = follower_state.handle_message(request[0])

    assert not response[0].success
    assert response[0].entries_length == 0
    assert response[0].properties["pre_length"] == 9
    assert response[0].properties["post_length"] == 9
    assert leader_state.next_index[1] == 10

    request, _ = leader_state.handle_message(response[0])
    response, _ = follower_state.handle_message(request[0])

    assert response[0].success
    assert response[0].entries_length == 1
    assert response[0].properties["pre_length"] == 9
    assert response[0].properties["post_length"] == 10
    assert leader_state.next_index[1] == 9

    assert len(leader_state.handle_message(response[0])[0]) == 0
    assert leader_state.next_index[1] == 10


def test_handle_message_b(
    paper_log: List[raftlog.LogEntry],
    logs_by_identifier: Dict[str, List[raftlog.LogEntry]],
) -> None:
    # Figure 7b
    leader_state, follower_state = init_raft_states(paper_log, logs_by_identifier["b"])

    request, _ = leader_state.handle_message(raftmessage.UpdateFollowers(0, 0, [1]))

    for i in range(6):
        response, _ = follower_state.handle_message(request[0])

        assert not response[0].success
        assert response[0].entries_length == i
        assert response[0].properties["pre_length"] == 4
        assert response[0].properties["post_length"] == 4
        assert leader_state.next_index[1] == 10 - i

        request, _ = leader_state.handle_message(response[0])

    response, _ = follower_state.handle_message(request[0])
    assert response[0].success
    assert response[0].entries_length == 6
    assert response[0].properties["pre_length"] == 4
    assert response[0].properties["post_length"] == 10
    assert leader_state.next_index[1] == 4

    assert len(leader_state.handle_message(response[0])[0]) == 0
    assert leader_state.next_index[1] == 10


def test_handle_message_c(
    paper_log: List[raftlog.LogEntry],
    logs_by_identifier: Dict[str, List[raftlog.LogEntry]],
) -> None:
    # Figure 7c
    leader_state, follower_state = init_raft_states(paper_log, logs_by_identifier["c"])

    request, _ = leader_state.handle_message(raftmessage.UpdateFollowers(0, 0, [1]))
    response, _ = follower_state.handle_message(request[0])

    assert response[0].success
    assert response[0].entries_length == 0
    assert response[0].properties["pre_length"] == 11
    assert response[0].properties["post_length"] == 11
    assert leader_state.next_index[1] == 10

    assert len(leader_state.handle_message(response[0])[0]) == 0
    assert leader_state.next_index[1] == 10


def test_handle_message_d(
    paper_log: List[raftlog.LogEntry],
    logs_by_identifier: Dict[str, List[raftlog.LogEntry]],
) -> None:
    # Figure 7d
    leader_state, follower_state = init_raft_states(paper_log, logs_by_identifier["d"])

    request, _ = leader_state.handle_message(raftmessage.UpdateFollowers(0, 0, [1]))
    response, _ = follower_state.handle_message(request[0])

    assert response[0].success
    assert response[0].entries_length == 0
    assert response[0].properties["pre_length"] == 12
    assert response[0].properties["post_length"] == 12
    assert leader_state.next_index[1] == 10

    assert len(leader_state.handle_message(response[0])[0]) == 0
    assert leader_state.next_index[1] == 10


def test_handle_message_e(
    paper_log: List[raftlog.LogEntry],
    logs_by_identifier: Dict[str, List[raftlog.LogEntry]],
) -> None:
    # Figure 7e
    leader_state, follower_state = init_raft_states(paper_log, logs_by_identifier["e"])

    request, _ = leader_state.handle_message(raftmessage.UpdateFollowers(0, 0, [1]))

    for i in range(5):
        response, _ = follower_state.handle_message(request[0])

        assert not response[0].success
        assert response[0].entries_length == i
        assert response[0].properties["pre_length"] == 7
        assert response[0].properties["post_length"] == 7
        assert leader_state.next_index[1] == 10 - i

        request, _ = leader_state.handle_message(response[0])

    response, _ = follower_state.handle_message(request[0])
    assert response[0].success
    assert response[0].entries_length == 5
    assert response[0].properties["pre_length"] == 7
    assert response[0].properties["post_length"] == 10
    assert leader_state.next_index[1] == 5

    assert len(leader_state.handle_message(response[0])[0]) == 0
    assert leader_state.next_index[1] == 10


def test_handle_message_f(
    paper_log: List[raftlog.LogEntry],
    logs_by_identifier: Dict[str, List[raftlog.LogEntry]],
) -> None:
    # Figure 7f
    leader_state, follower_state = init_raft_states(paper_log, logs_by_identifier["f"])

    request, _ = leader_state.handle_message(raftmessage.UpdateFollowers(0, 0, [1]))

    for i in range(7):
        response, _ = follower_state.handle_message(request[0])

        assert not response[0].success
        assert response[0].entries_length == i
        assert response[0].properties["pre_length"] == 11
        assert response[0].properties["post_length"] == 11
        assert leader_state.next_index[1] is 10 - i

        request, _ = leader_state.handle_message(response[0])

    response, _ = follower_state.handle_message(request[0])
    assert response[0].success
    assert response[0].entries_length == 7
    assert response[0].properties["pre_length"] == 11
    assert response[0].properties["post_length"] == 10
    assert leader_state.next_index[1] == 3

    assert len(leader_state.handle_message(response[0])[0]) == 0
    assert leader_state.next_index[1] == 10


def test_consensus(
    paper_log: List[raftlog.LogEntry],
    logs_by_identifier: Dict[str, List[raftlog.LogEntry]],
) -> None:
    leader_state = init_raft_state(paper_log, raftstate.Role.LEADER, 6)
    follower_a_state = init_raft_state(
        logs_by_identifier["a"], raftstate.Role.FOLLOWER, 6
    )
    follower_b_state = init_raft_state(
        logs_by_identifier["b"], raftstate.Role.FOLLOWER, 6
    )

    request, _ = leader_state.handle_leader_heartbeat(0, 0, [1, 2])
    assert leader_state.next_index == {0: 10, 1: 10, 2: 10}
    assert leader_state.match_index == {0: 9, 1: None, 2: None}
    assert leader_state.commit_index == -1

    response_a, _ = follower_a_state.handle_message(request[0])
    request_a, _ = leader_state.handle_message(response_a[0])
    assert leader_state.next_index == {0: 10, 1: 9, 2: 10}
    assert leader_state.match_index == {0: 9, 1: None, 2: None}
    assert leader_state.commit_index == -1

    response_a, _ = follower_a_state.handle_message(request_a[0])
    leader_state.handle_message(response_a[0])
    assert leader_state.next_index == {0: 10, 1: 10, 2: 10}
    assert leader_state.match_index == {0: 9, 1: 9, 2: None}
    assert leader_state.commit_index == 9

    response_b, _ = follower_b_state.handle_message(request[1])

    for i in range(6):
        request_b, _ = leader_state.handle_message(response_b[0])

        assert leader_state.next_index == {0: 10, 1: 10, 2: 9 - i}
        assert leader_state.match_index == {0: 9, 1: 9, 2: None}
        assert leader_state.commit_index == 9

        response_b, _ = follower_b_state.handle_message(request_b[0])

    leader_state.handle_message(response_b[0])
    assert leader_state.next_index == {0: 10, 1: 10, 2: 10}
    assert leader_state.match_index == {0: 9, 1: 9, 2: 9}
    assert leader_state.commit_index == 9

    request, _ = leader_state.handle_leader_heartbeat(0, 0, [1, 2])
    follower_a_state.handle_message(request[0])
    follower_b_state.handle_message(request[1])
    assert follower_a_state.commit_index == 9
    assert follower_b_state.commit_index == 9


def test_handle_vote_request(
    logs_by_identifier: Dict[str, List[raftlog.LogEntry]]
) -> None:
    """
    Set up with candidate state as per Figure 7c.
    """
    # TODO: Carve out become_candidate to own test.
    candidate_state = init_raft_state(
        logs_by_identifier["c"], raftstate.Role.FOLLOWER, 6, identifier=0
    )
    assert candidate_state.role == raftstate.Role.FOLLOWER
    assert candidate_state.current_term == 6
    assert candidate_state.voted_for == {0: None, 1: None, 2: None}

    request = candidate_state.become_candidate()
    assert candidate_state.role == raftstate.Role.CANDIDATE
    assert candidate_state.current_term == 7
    assert candidate_state.voted_for == {0: 0, 1: None, 2: None}

    # Figure 7a
    follower_a_state = init_raft_state(
        logs_by_identifier["a"],
        raftstate.Role.FOLLOWER,
        6,
    )

    # Initial vote request.
    response, _ = follower_a_state.handle_message(request[0])
    assert isinstance(response[0], raftmessage.RequestVoteResponse)
    assert response[0].success
    assert response[0].current_term == 7

    # Resend of vote request returns True.
    response, _ = follower_a_state.handle_message(request[0])
    assert isinstance(response[0], raftmessage.RequestVoteResponse)
    assert response[0].success
    assert response[0].current_term == 7

    # Vote request from another candidate returns False.
    response, _ = follower_a_state.handle_request_vote_request(2, 1, 7, 10, 6)
    assert isinstance(response[0], raftmessage.RequestVoteResponse)
    assert not response[0].success
    assert response[0].current_term == 7

    # Figure 7d
    follower_d_state = init_raft_state(
        logs_by_identifier["d"],
        raftstate.Role.FOLLOWER,
        6,
    )

    # Voter has longer log than candidate.
    response, _ = follower_d_state.handle_message(request[1])
    assert isinstance(response[0], raftmessage.RequestVoteResponse)
    assert not response[0].success
    assert response[0].current_term == 7

    follower_d_state.log.pop()

    # Voter has most recent entry having higher term.
    response, _ = follower_d_state.handle_message(request[1])
    assert isinstance(response[0], raftmessage.RequestVoteResponse)
    assert not response[0].success
    assert response[0].current_term == 7

    follower_d_state.log.pop()

    # Vote can now succeed.
    response, _ = follower_d_state.handle_message(request[1])
    assert isinstance(response[0], raftmessage.RequestVoteResponse)
    assert response[0].success
    assert response[0].current_term == 7


def test_handle_vote_response(
    paper_log: List[raftlog.LogEntry],
    logs_by_identifier: Dict[str, List[raftlog.LogEntry]],
) -> None:
    """
    Set up with candidate state as per Figure 7c.
    """
    candidate_state = init_raft_state(
        logs_by_identifier["c"], raftstate.Role.FOLLOWER, 6, identifier=0
    )
    request = candidate_state.become_candidate()

    # Figure 7d
    follower_d_state = init_raft_state(
        logs_by_identifier["d"], raftstate.Role.FOLLOWER, 6, identifier=1
    )

    response, _ = follower_d_state.handle_message(request[0])
    assert isinstance(response[0], raftmessage.RequestVoteResponse)
    assert not response[0].success
    assert response[0].current_term == 7
    assert follower_d_state.voted_for == {0: None, 1: None, 2: None}

    request_pre, change_role = candidate_state.handle_message(response[0])
    request_post = candidate_state.handle_role_change(change_role)
    assert candidate_state.role == raftstate.Role.CANDIDATE
    assert candidate_state.current_term == 7
    assert candidate_state.voted_for == {0: 0, 1: None, 2: None}
    assert len(request_pre) == 0
    assert len(request_post) == 0

    # Figure 7a
    follower_a_state = init_raft_state(
        logs_by_identifier["a"], raftstate.Role.FOLLOWER, 6, identifier=2
    )

    response, _ = follower_a_state.handle_message(request[1])
    assert isinstance(response[0], raftmessage.RequestVoteResponse)
    assert response[0].success
    assert response[0].current_term == 7
    assert follower_a_state.voted_for == {0: None, 1: None, 2: 0}

    request_pre, change_role = candidate_state.handle_message(response[0])
    request_post = candidate_state.handle_role_change(change_role)
    assert candidate_state.role == raftstate.Role.LEADER
    assert candidate_state.current_term == 7
    assert candidate_state.voted_for == {0: 0, 1: None, 2: 0}
    assert len(request_pre) == 0
    assert len(request_post) == 2

    assert isinstance(request_post[0], raftmessage.AppendEntryRequest)
    assert request_post[0].target == 1

    assert isinstance(request_post[1], raftmessage.AppendEntryRequest)
    assert request_post[1].target == 2
