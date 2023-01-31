from typing import Dict, List, Optional, Tuple

import raftlog
import raftmessage
import raftstate
import raftrole
from test_raftlog import paper_log, logs_by_identifier


def init_raft_state(
    identifier: int,
    log: List[raftlog.LogEntry],
    role: raftrole.Role,
    current_term: int,
) -> Tuple[raftstate.RaftState, List[raftmessage.Message]]:
    state = raftstate.RaftState(identifier)
    state.identifier = identifier
    state.log = log
    state.current_term = current_term
    state.role = raftrole.Role.FOLLOWER

    if role in {raftrole.Role.CANDIDATE, raftrole.Role.LEADER}:
        raftstate.change_role_from_follower_to_candidate(state, state.current_term - 1)
        messages, _ = state.handle_candidate_solicitation(
            state.identifier, state.identifier
        )

        if role == raftrole.Role.LEADER:
            raftstate.change_role_from_candidate_to_leader(state)
            messages, _ = state.handle_leader_heartbeat(
                state.identifier, state.identifier
            )

    else:
        messages = []

    return state, messages


def init_raft_states(
    leader_log: List[raftlog.LogEntry],
    follower_a_log: List[raftlog.LogEntry],
    follower_b_log: Optional[List[raftlog.LogEntry]],
):
    leader_state, messages = init_raft_state(0, leader_log, raftrole.Role.LEADER, 6)
    follower_a_state, _ = init_raft_state(1, follower_a_log, raftrole.Role.FOLLOWER, 6)

    if follower_b_log is not None:
        follower_b_state, _ = init_raft_state(
            2, follower_b_log, raftrole.Role.FOLLOWER, 6
        )
    else:
        follower_b_state = None

    return leader_state, follower_a_state, follower_b_state, messages


def init_commit_states(
    logs: List[List[raftlog.LogEntry]],
    experimental_mode: bool,
    config: Dict[int, Tuple[str, int]],
    commit_index: int,
) -> Tuple[
    raftstate.RaftState,
    raftstate.RaftState,
    raftstate.RaftState,
    raftstate.RaftState,
    raftstate.RaftState,
]:

    state_1, _ = init_raft_state(1, logs[0], raftrole.Role.FOLLOWER, 2)
    state_2, _ = init_raft_state(2, logs[1], raftrole.Role.FOLLOWER, 2)
    state_3, _ = init_raft_state(3, logs[2], raftrole.Role.FOLLOWER, 2)
    state_4, _ = init_raft_state(4, logs[3], raftrole.Role.FOLLOWER, 2)
    state_5, _ = init_raft_state(5, logs[4], raftrole.Role.FOLLOWER, 2)

    for state in [state_1, state_2, state_3, state_4, state_5]:
        state.experimental_mode = experimental_mode
        state.config = config
        state.commit_index = commit_index

    return state_1, state_2, state_3, state_4, state_5


def test_update_indexes(paper_log: List[raftlog.LogEntry]) -> None:
    leader_state, _ = init_raft_state(
        0,
        paper_log,
        raftrole.Role.LEADER,
        7,
    )

    assert leader_state.next_index is not None
    assert leader_state.next_index == {0: 10, 1: 10, 2: 10}
    assert leader_state.match_index == {0: 9, 1: None, 2: None}
    assert leader_state.commit_index == -1

    leader_state.update_indexes(1)
    assert leader_state.next_index == {0: 10, 1: 10, 2: 10}
    assert leader_state.match_index == {0: 9, 1: 9, 2: None}
    assert leader_state.commit_index == -1

    (
        non_null_match_index_count,
        potential_commit_index,
    ) = leader_state.get_index_metrics()
    assert non_null_match_index_count == 2
    assert potential_commit_index == 9

    leader_state.handle_client_log_append(0, 0, "7")
    assert leader_state.next_index == {0: 11, 1: 10, 2: 10}
    assert leader_state.match_index == {0: 10, 1: 9, 2: None}
    assert leader_state.commit_index == -1

    leader_state.update_indexes(1)
    assert leader_state.next_index == {0: 11, 1: 11, 2: 10}
    assert leader_state.match_index == {0: 10, 1: 10, 2: None}
    assert leader_state.commit_index == 10

    (
        non_null_match_index_count,
        potential_commit_index,
    ) = leader_state.get_index_metrics()
    assert non_null_match_index_count == 2
    assert potential_commit_index == 10


def test_create_append_entries_arguments(paper_log: List[raftlog.LogEntry]) -> None:
    leader_state, _ = init_raft_state(0, paper_log, raftrole.Role.LEADER, 6)

    (
        current_term,
        previous_index,
        previous_term,
        entries,
        commit_index,
    ) = leader_state.create_append_entries_arguments(1)

    assert current_term == 6
    assert previous_index == 9
    assert previous_term == 6
    assert entries == []
    assert commit_index == -1


def test_handle_append_entries_request(
    logs_by_identifier: Dict[str, List[raftlog.LogEntry]]
) -> None:
    # Figure 7a
    follower_state, _ = init_raft_state(
        0, logs_by_identifier["a"], raftrole.Role.FOLLOWER, 6
    )

    response, _ = follower_state.handle_append_entries_request(
        0, 1, 6, 8, 6, [raftlog.LogEntry(6, "6")], -1
    )
    assert isinstance(response[0], raftmessage.AppendEntryResponse)
    assert response[0].success
    assert response[0].entries_length == 1

    response, _ = follower_state.handle_append_entries_request(
        0, 1, 6, 10, 6, [raftlog.LogEntry(6, "6")], -1
    )
    assert isinstance(response[0], raftmessage.AppendEntryResponse)
    assert not response[0].success
    assert response[0].entries_length == 1


def test_handle_append_entries_response(paper_log: List[raftlog.LogEntry]) -> None:
    # Figure 7
    leader_state, _ = init_raft_state(0, paper_log, raftrole.Role.LEADER, 6)

    response, _ = leader_state.handle_append_entries_response(1, 0, 6, False, 0)
    assert isinstance(response[0], raftmessage.AppendEntryRequest)
    assert response[0].previous_index == 8
    assert response[0].previous_term == 6
    assert response[0].entries == [raftlog.LogEntry(6, "6")]

    response, _ = leader_state.handle_append_entries_response(1, 0, 6, True, 1)
    assert len(response) == 0


def test_handle_leader_heartbeat(paper_log: List[raftlog.LogEntry]) -> None:
    # Figure 7
    leader_state, messages = init_raft_state(0, paper_log, raftrole.Role.LEADER, 6)

    assert isinstance(messages[0], raftmessage.AppendEntryRequest)
    assert messages[0].previous_index == 9
    assert messages[0].previous_term == 6
    assert len(messages[0].entries) == 0


def test_handle_message_a(
    paper_log: List[raftlog.LogEntry],
    logs_by_identifier: Dict[str, List[raftlog.LogEntry]],
) -> None:
    # Figure 7a
    leader_state, follower_state, _, request = init_raft_states(
        paper_log, logs_by_identifier["a"], None
    )

    response, _ = follower_state.handle_message(request[0])
    assert not response[0].success
    assert response[0].entries_length == 0
    assert leader_state.next_index[1] == 10

    request, _ = leader_state.handle_message(response[0])
    response, _ = follower_state.handle_message(request[0])
    assert response[0].success
    assert response[0].entries_length == 1
    assert leader_state.next_index[1] == 9

    assert len(leader_state.handle_message(response[0])[0]) == 0
    assert leader_state.next_index[1] == 10


def test_handle_message_b(
    paper_log: List[raftlog.LogEntry],
    logs_by_identifier: Dict[str, List[raftlog.LogEntry]],
) -> None:
    # Figure 7b
    leader_state, follower_state, _, request = init_raft_states(
        paper_log, logs_by_identifier["b"], None
    )

    for i in range(6):
        response, _ = follower_state.handle_message(request[0])

        assert not response[0].success
        assert response[0].entries_length == i
        assert leader_state.next_index[1] == 10 - i

        request, _ = leader_state.handle_message(response[0])

    response, _ = follower_state.handle_message(request[0])
    assert response[0].success
    assert response[0].entries_length == 6
    assert leader_state.next_index[1] == 4

    assert len(leader_state.handle_message(response[0])[0]) == 0
    assert leader_state.next_index[1] == 10


def test_handle_message_c(
    paper_log: List[raftlog.LogEntry],
    logs_by_identifier: Dict[str, List[raftlog.LogEntry]],
) -> None:
    # Figure 7c
    leader_state, follower_state, _, request = init_raft_states(
        paper_log, logs_by_identifier["c"], None
    )

    response, _ = follower_state.handle_message(request[0])
    assert response[0].success
    assert response[0].entries_length == 0
    assert leader_state.next_index[1] == 10

    assert len(leader_state.handle_message(response[0])[0]) == 0
    assert leader_state.next_index[1] == 10


def test_handle_message_d(
    paper_log: List[raftlog.LogEntry],
    logs_by_identifier: Dict[str, List[raftlog.LogEntry]],
) -> None:
    # Figure 7d
    leader_state, follower_state, _, request = init_raft_states(
        paper_log, logs_by_identifier["d"], None
    )

    response, _ = follower_state.handle_message(request[0])
    assert response[0].success
    assert response[0].entries_length == 0
    assert leader_state.next_index[1] == 10

    assert len(leader_state.handle_message(response[0])[0]) == 0
    assert leader_state.next_index[1] == 10


def test_handle_message_e(
    paper_log: List[raftlog.LogEntry],
    logs_by_identifier: Dict[str, List[raftlog.LogEntry]],
) -> None:
    # Figure 7e
    leader_state, follower_state, _, request = init_raft_states(
        paper_log, logs_by_identifier["e"], None
    )

    request, _ = leader_state.handle_message(raftmessage.UpdateFollowers(0, 0, [1]))

    for i in range(5):
        response, _ = follower_state.handle_message(request[0])

        assert not response[0].success
        assert response[0].entries_length == i
        assert leader_state.next_index[1] == 10 - i

        request, _ = leader_state.handle_message(response[0])

    response, _ = follower_state.handle_message(request[0])
    assert response[0].success
    assert response[0].entries_length == 5
    assert leader_state.next_index[1] == 5

    assert len(leader_state.handle_message(response[0])[0]) == 0
    assert leader_state.next_index[1] == 10


def test_handle_message_f(
    paper_log: List[raftlog.LogEntry],
    logs_by_identifier: Dict[str, List[raftlog.LogEntry]],
) -> None:
    # Figure 7f
    leader_state, follower_state, _, request = init_raft_states(
        paper_log, logs_by_identifier["f"], None
    )

    for i in range(7):
        response, _ = follower_state.handle_message(request[0])

        assert not response[0].success
        assert response[0].entries_length == i
        assert leader_state.next_index[1] is 10 - i

        request, _ = leader_state.handle_message(response[0])

    response, _ = follower_state.handle_message(request[0])
    assert response[0].success
    assert response[0].entries_length == 7
    assert leader_state.next_index[1] == 3

    assert len(leader_state.handle_message(response[0])[0]) == 0
    assert leader_state.next_index[1] == 10


def test_consensus(
    paper_log: List[raftlog.LogEntry],
    logs_by_identifier: Dict[str, List[raftlog.LogEntry]],
) -> None:
    leader_state, follower_a_state, follower_b_state, request = init_raft_states(
        paper_log, logs_by_identifier["a"], logs_by_identifier["b"]
    )

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
    candidate_state, _ = init_raft_state(
        0, logs_by_identifier["c"], raftrole.Role.FOLLOWER, 6
    )
    assert candidate_state.role == raftrole.Role.FOLLOWER
    assert candidate_state.current_term == 6
    assert candidate_state.voted_for is None

    raftstate.change_role_from_follower_to_candidate(candidate_state)
    request, _ = candidate_state.handle_candidate_solicitation(0, 0)
    assert candidate_state.role == raftrole.Role.CANDIDATE
    assert candidate_state.current_term == 7
    assert candidate_state.voted_for == 0

    # Figure 7a
    follower_a_state, _ = init_raft_state(
        1,
        logs_by_identifier["a"],
        raftrole.Role.FOLLOWER,
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
    follower_d_state, _ = init_raft_state(
        2,
        logs_by_identifier["d"],
        raftrole.Role.FOLLOWER,
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
    candidate_state, request = init_raft_state(
        0, logs_by_identifier["c"], raftrole.Role.CANDIDATE, 7
    )

    # Figure 7d
    follower_d_state, _ = init_raft_state(
        1, logs_by_identifier["d"], raftrole.Role.FOLLOWER, 6
    )

    response, _ = follower_d_state.handle_message(request[0])
    assert isinstance(response[0], raftmessage.RequestVoteResponse)
    assert not response[0].success
    assert response[0].current_term == 7
    assert follower_d_state.voted_for is None

    _, change_role = candidate_state.handle_message(response[0])
    assert change_role is None
    assert candidate_state.role == raftrole.Role.CANDIDATE
    assert candidate_state.current_term == 7
    assert candidate_state.voted_for == 0

    # Figure 7a
    follower_a_state, _ = init_raft_state(
        2, logs_by_identifier["a"], raftrole.Role.FOLLOWER, 6
    )

    response, _ = follower_a_state.handle_message(request[1])
    assert isinstance(response[0], raftmessage.RequestVoteResponse)
    assert response[0].success
    assert response[0].current_term == 7
    assert follower_a_state.voted_for == 0

    _, change_role = candidate_state.handle_message(response[0])
    assert change_role == (raftrole.Role.CANDIDATE, raftrole.Role.LEADER)
    assert candidate_state.role == raftrole.Role.LEADER
    assert candidate_state.current_term == 7
    assert candidate_state.voted_for == 0


def test_commit_with_requirement() -> None:
    logs: List[List[raftlog.LogEntry]] = [
        [raftlog.LogEntry(1, "1"), raftlog.LogEntry(2, "2")],
        [raftlog.LogEntry(1, "1"), raftlog.LogEntry(2, "2")],
        [raftlog.LogEntry(1, "1")],
        [raftlog.LogEntry(1, "1")],
        [raftlog.LogEntry(1, "1")],
    ]
    config: Dict[int, Tuple[str, int]] = {(i + 1): ("", i) for i in range(5)}
    experimental_mode: bool = False

    # (a) starting state
    state_1, state_2, state_3, state_4, state_5 = init_commit_states(
        logs, experimental_mode, config, 0
    )

    # (b) elect 5 as leader and append entry
    raftstate.change_role_from_follower_to_candidate(state_5)
    raftstate.change_role_from_candidate_to_leader(state_5)
    state_5.handle_client_log_append(5, 5, "3")
    state_5.handle_client_log_append(5, 5, "3")
    state_5.handle_client_log_append(5, 5, "3")

    # (c) election with candidate 1 with no winner in a split network, then
    # elect 1 as leader and append entry
    raftstate.change_role_from_follower_to_candidate(state_1)
    state_1.current_term += 1
    raftstate.change_role_from_candidate_to_leader(state_1)

    for _ in range(2):
        messages, _ = state_1.handle_leader_heartbeat(1, 1)

        for i, state in enumerate([state_2, state_3, state_4, state_5]):
            if i in [2, 3]:
                continue

            request = [messages[i]]

            while len(request) > 0:
                response, _ = state.handle_message(request[0])
                request, _ = state_1.handle_message(response[0])

    assert len(state_2.log) == 2
    assert state_2.log[1] == raftlog.LogEntry(2, "2")
    assert state_2.commit_index == 0

    prior_log = state_2.log.copy()
    prior_commit_index = state_2.commit_index

    # (d) crash 5, election with candidate 5 with no winner in a split network,
    # then elect 5 as leader
    raftstate.change_role_from_leader_to_follower(state_5)
    raftstate.change_role_from_follower_to_candidate(state_5)
    state_5.current_term += 1
    raftstate.change_role_from_candidate_to_leader(state_5)

    for _ in range(3):
        messages, _ = state_5.handle_leader_heartbeat(1, 1)

        for i, state in enumerate([state_1, state_2, state_3, state_4]):
            request = [messages[i]]

            while len(request) > 0:
                response, _ = state.handle_message(request[0])
                request, _ = state_5.handle_message(response[0])

    assert len(state_2.log) == 4
    assert state_2.log[1] == raftlog.LogEntry(3, "3")
    assert state_2.commit_index == 0

    assert prior_log[prior_commit_index] == state_2.log[prior_commit_index]


def test_commit_without_requirement() -> None:
    logs: List[List[raftlog.LogEntry]] = [
        [raftlog.LogEntry(1, "1"), raftlog.LogEntry(2, "2")],
        [raftlog.LogEntry(1, "1"), raftlog.LogEntry(2, "2")],
        [raftlog.LogEntry(1, "1")],
        [raftlog.LogEntry(1, "1")],
        [raftlog.LogEntry(1, "1")],
    ]
    config: Dict[int, Tuple[str, int]] = {(i + 1): ("", i) for i in range(5)}
    experimental_mode: bool = True

    # (a) starting state
    state_1, state_2, state_3, state_4, state_5 = init_commit_states(
        logs, experimental_mode, config, 0
    )

    # (b) elect 5 as leader and append entry
    raftstate.change_role_from_follower_to_candidate(state_5)
    raftstate.change_role_from_candidate_to_leader(state_5)
    state_5.handle_client_log_append(5, 5, "3")
    state_5.handle_client_log_append(5, 5, "3")
    state_5.handle_client_log_append(5, 5, "3")

    # (c) election with candidate 1 with no winner in a split network, then
    # elect 1 as leader and append entry
    raftstate.change_role_from_follower_to_candidate(state_1)
    state_1.current_term += 1
    raftstate.change_role_from_candidate_to_leader(state_1)

    for _ in range(2):
        messages, _ = state_1.handle_leader_heartbeat(1, 1)

        for i, state in enumerate([state_2, state_3, state_4, state_5]):
            if i in [2, 3]:
                continue

            request = [messages[i]]

            while len(request) > 0:
                response, _ = state.handle_message(request[0])
                request, _ = state_1.handle_message(response[0])

    assert len(state_2.log) == 2
    assert state_2.log[1] == raftlog.LogEntry(2, "2")
    assert state_2.commit_index == 1

    prior_log = state_2.log.copy()
    prior_commit_index = state_2.commit_index

    # (d) crash 5, election with candidate 5 with no winner in a split network,
    # then elect 5 as leader
    raftstate.change_role_from_leader_to_follower(state_5)
    raftstate.change_role_from_follower_to_candidate(state_5)
    state_5.current_term += 1
    raftstate.change_role_from_candidate_to_leader(state_5)

    for _ in range(2):
        messages, _ = state_5.handle_leader_heartbeat(1, 1)

        for i, state in enumerate([state_1, state_2, state_3, state_4]):
            request = [messages[i]]

            while len(request) > 0:
                response, _ = state.handle_message(request[0])
                request, _ = state_5.handle_message(response[0])

    assert len(state_2.log) == 4
    assert state_2.log[1] == raftlog.LogEntry(3, "3")
    assert state_2.commit_index == 3

    assert prior_log[prior_commit_index] != state_2.log[prior_commit_index]
