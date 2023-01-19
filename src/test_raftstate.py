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
    leader_state = init_raft_state(leader_log, 6, raftstate.StateEnum.LEADER, {1: 9})
    follower_state = init_raft_state(
        follower_log, 6, raftstate.StateEnum.FOLLOWER, None
    )

    return leader_state, follower_state


def test_handle_append_entries_request(logs_by_identifier) -> None:
    # Figure 7a
    follower_state = init_raft_state(
        logs_by_identifier["a"], 6, raftstate.StateEnum.FOLLOWER, None
    )

    response = follower_state.handle_append_entries_request(
        0, 1, 8, 6, [raftlog.LogEntry(6, "9")]
    )[0]
    assert isinstance(response, raftmessage.AppendEntryResponse)
    assert response.success
    assert response.properties["pre_length"] == 9
    assert response.properties["post_length"] == 10
    assert response.properties["entries_length"] == 1

    response = follower_state.handle_append_entries_request(
        0, 1, 10, 6, [raftlog.LogEntry(6, "11")]
    )[0]
    assert isinstance(response, raftmessage.AppendEntryResponse)
    assert not response.success
    assert response.properties["pre_length"] == 10
    assert response.properties["post_length"] == 10
    assert response.properties["entries_length"] == 1


def test_handle_append_entries_response(logs_by_identifier) -> None:
    # Figure 7a
    leader_state = init_raft_state(
        logs_by_identifier["a"], 6, raftstate.StateEnum.LEADER, {1: 9}
    )

    response = leader_state.handle_append_entries_response(
        1, 0, False, {"pre_length": 9, "post_length": 9, "entries_length": 0}
    )

    assert isinstance(response[0], raftmessage.AppendEntryRequest)
    assert response[0].previous_index == 7
    assert response[0].previous_term == 6
    assert response[0].entries == [raftlog.LogEntry(6, "8")]

    response = leader_state.handle_append_entries_response(
        1, 0, True, {"pre_length": 9, "post_length": 9, "entries_length": 1}
    )
    assert len(response) == 0


def test_handle_leader_heartbeat(logs_by_identifier) -> None:
    # Figure 7a
    leader_state = init_raft_state(
        logs_by_identifier["a"], 6, raftstate.StateEnum.LEADER, {1: 9}
    )

    response = leader_state.handle_leader_heartbeat(0, 0, [1])[0]

    assert isinstance(response, raftmessage.AppendEntryRequest)
    assert response.previous_index == 8
    assert response.previous_term == 6
    assert len(response.entries) == 0


def test_handle_message_a(paper_log, logs_by_identifier) -> None:
    # Figure 7a
    leader_state, follower_state = init_raft_states(paper_log, logs_by_identifier["a"])

    request = leader_state.handle_message(raftmessage.UpdateFollowers(0, 0, [1]))[0]
    response = follower_state.handle_message(request)[0]
    assert response.success
    assert response.properties["pre_length"] == 9
    assert response.properties["post_length"] == 10
    assert response.properties["entries_length"] == 1
    assert leader_state.next_index == {1: 9}

    assert len(leader_state.handle_message(response)) == 0
    assert leader_state.next_index == {1: 10}


def test_handle_message_b(paper_log, logs_by_identifier) -> None:
    # Figure 7b
    leader_state, follower_state = init_raft_states(paper_log, logs_by_identifier["b"])

    request = leader_state.handle_message(raftmessage.UpdateFollowers(0, 0, [1]))[0]

    for i in range(5):
        response = follower_state.handle_message(request)[0]

        assert not response.success
        assert response.properties["pre_length"] == 4
        assert response.properties["post_length"] == 4
        assert response.properties["entries_length"] == i + 1
        assert leader_state.next_index == {1: 9 - i}

        request = leader_state.handle_message(response)[0]

    response = follower_state.handle_message(request)[0]
    assert response.success
    assert response.properties["pre_length"] == 4
    assert response.properties["post_length"] == 10
    assert response.properties["entries_length"] == 6
    assert leader_state.next_index == {1: 4}

    assert len(leader_state.handle_message(response)) == 0
    assert leader_state.next_index == {1: 10}


def test_handle_message_c(paper_log, logs_by_identifier) -> None:
    # Figure 7c
    leader_state, follower_state = init_raft_states(paper_log, logs_by_identifier["c"])

    request = leader_state.handle_message(raftmessage.UpdateFollowers(0, 0, [1]))[0]
    response = follower_state.handle_message(request)[0]
    assert response.success
    assert response.properties["pre_length"] == 11
    assert response.properties["post_length"] == 11
    assert response.properties["entries_length"] == 1
    assert leader_state.next_index == {1: 9}

    assert len(leader_state.handle_message(response)) == 0
    assert leader_state.next_index == {1: 10}


def test_handle_message_d(paper_log, logs_by_identifier) -> None:
    # Figure 7d
    leader_state, follower_state = init_raft_states(paper_log, logs_by_identifier["d"])

    request = leader_state.handle_message(raftmessage.UpdateFollowers(0, 0, [1]))[0]
    response = follower_state.handle_message(request)[0]
    assert response.success
    assert response.properties["pre_length"] == 12
    assert response.properties["post_length"] == 12
    assert response.properties["entries_length"] == 1
    assert leader_state.next_index == {1: 9}

    assert len(leader_state.handle_message(response)) == 0
    assert leader_state.next_index == {1: 10}


def test_handle_message_e(paper_log, logs_by_identifier) -> None:
    # Figure 7e
    leader_state, follower_state = init_raft_states(paper_log, logs_by_identifier["e"])

    request = leader_state.handle_message(raftmessage.UpdateFollowers(0, 0, [1]))[0]

    for i in range(4):
        response = follower_state.handle_message(request)[0]

        assert not response.success
        assert response.properties["pre_length"] == 7
        assert response.properties["post_length"] == 7
        assert response.properties["entries_length"] == i + 1
        assert leader_state.next_index == {1: 9 - i}

        request = leader_state.handle_message(response)[0]

    response = follower_state.handle_message(request)[0]
    assert response.success
    assert response.properties["pre_length"] == 7
    assert response.properties["post_length"] == 10
    assert response.properties["entries_length"] == 5
    assert leader_state.next_index == {1: 5}

    assert len(leader_state.handle_message(response)) == 0
    assert leader_state.next_index == {1: 10}


def test_handle_message_f(paper_log, logs_by_identifier) -> None:
    # Figure 7f
    leader_state, follower_state = init_raft_states(paper_log, logs_by_identifier["f"])

    request = leader_state.handle_message(raftmessage.UpdateFollowers(0, 0, [1]))[0]

    for i in range(6):
        response = follower_state.handle_message(request)[0]

        assert not response.success
        assert response.properties["pre_length"] == 11
        assert response.properties["post_length"] == 11
        assert response.properties["entries_length"] == i + 1
        assert leader_state.next_index == {1: 9 - i}

        request = leader_state.handle_message(response)[0]

    response = follower_state.handle_message(request)[0]
    assert response.success
    assert response.properties["pre_length"] == 11
    assert response.properties["post_length"] == 10
    assert response.properties["entries_length"] == 7
    assert leader_state.next_index == {1: 3}

    assert len(leader_state.handle_message(response)) == 0
    assert leader_state.next_index == {1: 10}
