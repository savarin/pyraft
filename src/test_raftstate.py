import raftlog
import raftmessage
import raftstate
from test_raftlog import paper_log, logs_by_identifier


def init_raft_state(
    log, current_term, current_state, next_index
) -> raftstate.RaftState:
    raft_state = raftstate.RaftState()
    raft_state.log = log
    raft_state.current_term = current_term
    raft_state.current_state = current_state
    raft_state.next_index = next_index
    return raft_state


def init_raft_states(leader_log, follower_log):
    leader_state = init_raft_state(
        leader_log, 6, raftstate.StateEnum.LEADER, {0: 10, 1: None, 2: None}
    )
    follower_state = init_raft_state(follower_log, 6, raftstate.StateEnum.FOLLOWER, {})

    return leader_state, follower_state


def test_get_next_index(paper_log) -> None:
    leader_state = init_raft_state(
        paper_log, 6, raftstate.StateEnum.LEADER, {0: 10, 1: None, 2: None}
    )

    assert leader_state.get_next_index(0) == 10
    assert leader_state.get_next_index(1) == 10
    assert leader_state.get_next_index(2) == 10


def test_update_next_index(paper_log) -> None:
    leader_state = init_raft_state(
        paper_log, 6, raftstate.StateEnum.LEADER, {0: 10, 1: None, 2: None}
    )

    assert leader_state.next_index[1] is None

    # Next index updates from None to previous_index + 1.
    leader_state.update_next_index(1, 0, 9)
    assert leader_state.next_index[1] == 10

    # The argument for previous_index is ignored if current value is not None.
    leader_state.update_next_index(1, 1, 1000)
    assert leader_state.next_index[1] == 11


def test_update_commit_index(paper_log) -> None:
    leader_state = init_raft_state(
        paper_log, 6, raftstate.StateEnum.LEADER, {0: 10, 1: None, 2: None}
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


def test_create_append_entries_arguments(paper_log) -> None:
    leader_state = init_raft_state(
        paper_log, 6, raftstate.StateEnum.LEADER, {0: 10, 1: None, 2: None}
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


def test_handle_append_entries_request(logs_by_identifier) -> None:
    # Figure 7a
    follower_state = init_raft_state(
        logs_by_identifier["a"], 6, raftstate.StateEnum.FOLLOWER, {}
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


def test_handle_append_entries_response(paper_log) -> None:
    # Figure 7
    leader_state = init_raft_state(
        paper_log, 6, raftstate.StateEnum.LEADER, {0: 10, 1: None, 2: None}
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


def test_handle_leader_heartbeat(paper_log) -> None:
    # Figure 7
    leader_state = init_raft_state(
        paper_log, 6, raftstate.StateEnum.LEADER, {0: 10, 1: None, 2: None}
    )

    response = leader_state.handle_leader_heartbeat(0, 0, [1])[0]

    assert isinstance(response, raftmessage.AppendEntryRequest)
    assert response.previous_index == 9
    assert response.previous_term == 6
    assert len(response.entries) == 0


def test_handle_message_a(paper_log, logs_by_identifier) -> None:
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


def test_handle_message_b(paper_log, logs_by_identifier) -> None:
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


def test_handle_message_c(paper_log, logs_by_identifier) -> None:
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


def test_handle_message_d(paper_log, logs_by_identifier) -> None:
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


def test_handle_message_e(paper_log, logs_by_identifier) -> None:
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


def test_handle_message_f(paper_log, logs_by_identifier) -> None:
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


def test_consensus(paper_log, logs_by_identifier) -> None:
    leader_state = init_raft_state(
        paper_log, 6, raftstate.StateEnum.LEADER, {0: 10, 1: None, 2: None}
    )
    follower_a_state = init_raft_state(
        logs_by_identifier["a"], 6, raftstate.StateEnum.FOLLOWER, {}
    )
    follower_b_state = init_raft_state(
        logs_by_identifier["b"], 6, raftstate.StateEnum.FOLLOWER, {}
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
