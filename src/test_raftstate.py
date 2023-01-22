from typing import Dict, List, Optional

import raftlog
import raftmessage
import raftstate
from test_raftlog import paper_log, logs_by_identifier


def init_raft_state(
    log: List[raftlog.LogEntry],
    role: raftstate.Role,
    current_term: int,
    next_index: Dict[int, Optional[int]],
) -> raftstate.RaftState:
    raft_state = raftstate.RaftState()
    raft_state.log = log
    raft_state.role = role
    raft_state.current_term = current_term
    raft_state.next_index = next_index
    return raft_state


def init_raft_states(
    leader_log: List[raftlog.LogEntry], follower_log: List[raftlog.LogEntry]
):
    leader_state = init_raft_state(
        leader_log, raftstate.Role.LEADER, 6, {0: 10, 1: None, 2: None}
    )
    follower_state = init_raft_state(follower_log, raftstate.Role.FOLLOWER, 6, {})

    return leader_state, follower_state


def test_get_next_index(paper_log: List[raftlog.LogEntry]) -> None:
    leader_state = init_raft_state(
        paper_log, raftstate.Role.LEADER, 6, {0: 10, 1: None, 2: None}
    )

    assert leader_state.get_next_index(0) == 10
    assert leader_state.get_next_index(1) == 10
    assert leader_state.get_next_index(2) == 10


def test_update_next_index(paper_log: List[raftlog.LogEntry]) -> None:
    leader_state = init_raft_state(
        paper_log, raftstate.Role.LEADER, 6, {0: 10, 1: None, 2: None}
    )

    assert leader_state.next_index[1] is None

    # Next index updates from None to previous_index + 1.
    leader_state.update_next_index(1, 0, 9)
    assert leader_state.next_index[1] == 10

    # The argument for previous_index is ignored if current value is not None.
    leader_state.update_next_index(1, 1, 1000)
    assert leader_state.next_index[1] == 11


def test_update_commit_index(paper_log: List[raftlog.LogEntry]) -> None:
    leader_state = init_raft_state(
        paper_log, raftstate.Role.LEADER, 6, {0: 10, 1: None, 2: None}
    )
    assert leader_state.commit_index == -1

    leader_state.update_commit_index()
    assert leader_state.commit_index == -1

    leader_state.next_index = {0: 10, 1: 1, 2: None}
    leader_state.update_commit_index()
    assert leader_state.commit_index == 0

    leader_state.next_index = {0: 10, 1: 10, 2: None}
    leader_state.update_commit_index()
    assert leader_state.commit_index == 9


def test_create_append_entries_arguments(paper_log: List[raftlog.LogEntry]) -> None:
    leader_state = init_raft_state(
        paper_log, raftstate.Role.LEADER, 6, {0: 10, 1: None, 2: None}
    )

    (
        previous_index,
        previous_term,
        entries,
        commit_index,
    ) = leader_state.create_append_entries_arguments(1, None)

    assert previous_index == 9
    assert previous_term == 6
    assert entries == []
    assert commit_index == -1

    (
        previous_index,
        previous_term,
        entries,
        commit_index,
    ) = leader_state.create_append_entries_arguments(1, 8)

    assert previous_index == 8
    assert previous_term == 6
    assert entries == [raftlog.LogEntry(6, "9")]
    assert commit_index == -1


def test_handle_append_entries_request(
    logs_by_identifier: Dict[str, List[raftlog.LogEntry]]
) -> None:
    # Figure 7a
    follower_state = init_raft_state(
        logs_by_identifier["a"], raftstate.Role.FOLLOWER, 6, {}
    )

    response = follower_state.handle_append_entries_request(
        0, 1, 8, 6, [raftlog.LogEntry(6, "9")], -1
    )[0]
    assert isinstance(response, raftmessage.AppendEntryResponse)
    assert response.success
    assert response.entries_length == 1
    assert response.properties["pre_length"] == 9
    assert response.properties["post_length"] == 10

    response = follower_state.handle_append_entries_request(
        0, 1, 10, 6, [raftlog.LogEntry(6, "11")], -1
    )[0]
    assert isinstance(response, raftmessage.AppendEntryResponse)
    assert not response.success
    assert response.entries_length == 1
    assert response.properties["pre_length"] == 10
    assert response.properties["post_length"] == 10


def test_handle_append_entries_response(paper_log: List[raftlog.LogEntry]) -> None:
    # Figure 7
    leader_state = init_raft_state(
        paper_log, raftstate.Role.LEADER, 6, {0: 10, 1: None, 2: None}
    )

    response = leader_state.handle_append_entries_response(
        1, 0, False, 9, 0, {"pre_length": 9, "post_length": 9}
    )

    assert isinstance(response[0], raftmessage.AppendEntryRequest)
    assert response[0].previous_index == 8
    assert response[0].previous_term == 6
    assert response[0].entries == [raftlog.LogEntry(6, "9")]

    response = leader_state.handle_append_entries_response(
        1, 0, True, 7, 1, {"pre_length": 9, "post_length": 9}
    )
    assert len(response) == 0


def test_handle_leader_heartbeat(paper_log: List[raftlog.LogEntry]) -> None:
    # Figure 7
    leader_state = init_raft_state(
        paper_log, raftstate.Role.LEADER, 6, {0: 10, 1: None, 2: None}
    )

    response = leader_state.handle_leader_heartbeat(0, 0, [1])[0]

    assert isinstance(response, raftmessage.AppendEntryRequest)
    assert response.previous_index == 9
    assert response.previous_term == 6
    assert len(response.entries) == 0


def test_handle_message_a(
    paper_log: List[raftlog.LogEntry],
    logs_by_identifier: Dict[str, List[raftlog.LogEntry]],
) -> None:
    # Figure 7a
    leader_state, follower_state = init_raft_states(paper_log, logs_by_identifier["a"])

    request = leader_state.handle_message(raftmessage.UpdateFollowers(0, 0, [1]))[0]
    response = follower_state.handle_message(request)[0]

    assert not response.success
    assert response.previous_index == 9
    assert response.entries_length == 0
    assert response.properties["pre_length"] == 9
    assert response.properties["post_length"] == 9
    assert leader_state.next_index[1] is None

    request = leader_state.handle_message(response)[0]
    response = follower_state.handle_message(request)[0]

    assert response.success
    assert response.previous_index == 8
    assert response.entries_length == 1
    assert response.properties["pre_length"] == 9
    assert response.properties["post_length"] == 10
    assert leader_state.next_index[1] is None

    assert len(leader_state.handle_message(response)) == 0
    assert leader_state.next_index[1] == 10


def test_handle_message_b(
    paper_log: List[raftlog.LogEntry],
    logs_by_identifier: Dict[str, List[raftlog.LogEntry]],
) -> None:
    # Figure 7b
    leader_state, follower_state = init_raft_states(paper_log, logs_by_identifier["b"])

    request = leader_state.handle_message(raftmessage.UpdateFollowers(0, 0, [1]))[0]

    for i in range(6):
        response = follower_state.handle_message(request)[0]

        assert not response.success
        assert response.previous_index == 9 - i
        assert response.entries_length == i
        assert response.properties["pre_length"] == 4
        assert response.properties["post_length"] == 4
        assert leader_state.next_index[1] is None

        request = leader_state.handle_message(response)[0]

    response = follower_state.handle_message(request)[0]
    assert response.success
    assert response.entries_length == 6
    assert response.properties["pre_length"] == 4
    assert response.properties["post_length"] == 10
    assert leader_state.next_index[1] is None

    assert len(leader_state.handle_message(response)) == 0
    assert leader_state.next_index[1] == 10


def test_handle_message_c(
    paper_log: List[raftlog.LogEntry],
    logs_by_identifier: Dict[str, List[raftlog.LogEntry]],
) -> None:
    # Figure 7c
    leader_state, follower_state = init_raft_states(paper_log, logs_by_identifier["c"])

    request = leader_state.handle_message(raftmessage.UpdateFollowers(0, 0, [1]))[0]
    response = follower_state.handle_message(request)[0]

    assert response.success
    assert response.previous_index == 9
    assert response.entries_length == 0
    assert response.properties["pre_length"] == 11
    assert response.properties["post_length"] == 11
    assert leader_state.next_index[1] is None

    assert len(leader_state.handle_message(response)) == 0
    assert leader_state.next_index[1] == 10


def test_handle_message_d(
    paper_log: List[raftlog.LogEntry],
    logs_by_identifier: Dict[str, List[raftlog.LogEntry]],
) -> None:
    # Figure 7d
    leader_state, follower_state = init_raft_states(paper_log, logs_by_identifier["d"])

    request = leader_state.handle_message(raftmessage.UpdateFollowers(0, 0, [1]))[0]
    response = follower_state.handle_message(request)[0]

    assert response.success
    assert response.previous_index == 9
    assert response.entries_length == 0
    assert response.properties["pre_length"] == 12
    assert response.properties["post_length"] == 12
    assert leader_state.next_index[1] is None

    assert len(leader_state.handle_message(response)) == 0
    assert leader_state.next_index[1] == 10


def test_handle_message_e(
    paper_log: List[raftlog.LogEntry],
    logs_by_identifier: Dict[str, List[raftlog.LogEntry]],
) -> None:
    # Figure 7e
    leader_state, follower_state = init_raft_states(paper_log, logs_by_identifier["e"])

    request = leader_state.handle_message(raftmessage.UpdateFollowers(0, 0, [1]))[0]

    for i in range(5):
        response = follower_state.handle_message(request)[0]

        assert not response.success
        assert response.previous_index == 9 - i
        assert response.entries_length == i
        assert response.properties["pre_length"] == 7
        assert response.properties["post_length"] == 7
        assert leader_state.next_index[1] is None

        request = leader_state.handle_message(response)[0]

    response = follower_state.handle_message(request)[0]
    assert response.success
    assert response.previous_index == 4
    assert response.entries_length == 5
    assert response.properties["pre_length"] == 7
    assert response.properties["post_length"] == 10
    assert leader_state.next_index[1] is None

    assert len(leader_state.handle_message(response)) == 0
    assert leader_state.next_index[1] == 10


def test_handle_message_f(
    paper_log: List[raftlog.LogEntry],
    logs_by_identifier: Dict[str, List[raftlog.LogEntry]],
) -> None:
    # Figure 7f
    leader_state, follower_state = init_raft_states(paper_log, logs_by_identifier["f"])

    request = leader_state.handle_message(raftmessage.UpdateFollowers(0, 0, [1]))[0]

    for i in range(7):
        response = follower_state.handle_message(request)[0]

        assert not response.success
        assert response.previous_index == 9 - i
        assert response.entries_length == i
        assert response.properties["pre_length"] == 11
        assert response.properties["post_length"] == 11
        assert leader_state.next_index[1] is None

        request = leader_state.handle_message(response)[0]

    response = follower_state.handle_message(request)[0]
    assert response.success
    assert response.previous_index == 2
    assert response.entries_length == 7
    assert response.properties["pre_length"] == 11
    assert response.properties["post_length"] == 10
    assert leader_state.next_index[1] is None

    assert len(leader_state.handle_message(response)) == 0
    assert leader_state.next_index[1] == 10


def test_consensus(
    paper_log: List[raftlog.LogEntry],
    logs_by_identifier: Dict[str, List[raftlog.LogEntry]],
) -> None:
    leader_state = init_raft_state(
        paper_log, raftstate.Role.LEADER, 6, {0: 10, 1: None, 2: None}
    )
    follower_a_state = init_raft_state(
        logs_by_identifier["a"], raftstate.Role.FOLLOWER, 6, {}
    )
    follower_b_state = init_raft_state(
        logs_by_identifier["b"], raftstate.Role.FOLLOWER, 6, {}
    )

    request = leader_state.handle_leader_heartbeat(0, 0, [1, 2])
    assert leader_state.commit_index == -1
    assert leader_state.next_index == {0: 10, 1: None, 2: None}

    response_a = follower_a_state.handle_message(request[0])[0]
    request_a = leader_state.handle_message(response_a)[0]
    assert leader_state.commit_index == -1
    assert leader_state.next_index == {0: 10, 1: None, 2: None}

    response_a = follower_a_state.handle_message(request_a)[0]
    leader_state.handle_message(response_a)
    assert leader_state.commit_index == 9
    assert leader_state.next_index == {0: 10, 1: 10, 2: None}

    response_b = follower_b_state.handle_message(request[1])[0]

    for i in range(6):
        request_b = leader_state.handle_message(response_b)[0]

        assert leader_state.commit_index == 9
        assert leader_state.next_index == {0: 10, 1: 10, 2: None}

        response_b = follower_b_state.handle_message(request_b)[0]

    leader_state.handle_message(response_b)
    assert leader_state.commit_index == 9
    assert leader_state.next_index == {0: 10, 1: 10, 2: 10}

    request = leader_state.handle_leader_heartbeat(0, 0, [1, 2])
    follower_a_state.handle_message(request[0])[0]
    follower_b_state.handle_message(request[1])[0]
    assert follower_a_state.commit_index == 9
    assert follower_b_state.commit_index == 9


def test_handle_vote_request(
    paper_log: List[raftlog.LogEntry],
    logs_by_identifier: Dict[str, List[raftlog.LogEntry]],
) -> None:
    """
    Set up with candidate state as per Figure 7c.
    """
    # Figure 7a
    follower_a_state = init_raft_state(
        logs_by_identifier["a"], raftstate.Role.FOLLOWER, 6, {}
    )

    # Initial vote request.
    response = follower_a_state.handle_request_vote_request(0, 1, 7, 10, 6)[0]
    assert isinstance(response, raftmessage.RequestVoteResponse)
    assert response.success
    assert response.current_term == 7

    # Resend of vote request returns True.
    response = follower_a_state.handle_request_vote_request(0, 1, 7, 10, 6)[0]
    assert isinstance(response, raftmessage.RequestVoteResponse)
    assert response.success
    assert response.current_term == 7

    # Vote request from another candidate returns False.
    response = follower_a_state.handle_request_vote_request(2, 1, 7, 10, 6)[0]
    assert isinstance(response, raftmessage.RequestVoteResponse)
    assert not response.success
    assert response.current_term == 7

    # Figure 7d
    follower_d_state = init_raft_state(
        logs_by_identifier["d"], raftstate.Role.FOLLOWER, 6, {}
    )

    # Voter has longer log than candidate.
    response = follower_d_state.handle_request_vote_request(0, 1, 7, 10, 6)[0]
    assert isinstance(response, raftmessage.RequestVoteResponse)
    assert not response.success
    assert response.current_term == 7

    follower_d_state.log.pop()

    # Voter has most recent entry having higher term.
    response = follower_d_state.handle_request_vote_request(0, 1, 7, 10, 6)[0]
    assert isinstance(response, raftmessage.RequestVoteResponse)
    assert not response.success
    assert response.current_term == 7

    follower_d_state.log.pop()

    # Vote can now succeed.
    response = follower_d_state.handle_request_vote_request(0, 1, 7, 10, 6)[0]
    assert isinstance(response, raftmessage.RequestVoteResponse)
    assert response.success
    assert response.current_term == 7
