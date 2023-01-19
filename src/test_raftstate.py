import pytest

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


@pytest.fixture
def init_callback(logs_by_identifier):
    def closure(identifier, current_term):
        follower_state = init_raft_state(
            logs_by_identifier[identifier],
            current_term,
            raftstate.StateEnum.FOLLOWER,
            None,
        )
        return follower_state.handle_append_entries

    return closure


def test_handle_append_entries(logs_by_identifier) -> None:
    # Figure 7a
    follower_state = init_raft_state(
        logs_by_identifier["a"], 6, raftstate.StateEnum.FOLLOWER, None
    )

    response = follower_state.handle_append_entries(
        0, 1, 8, 6, [raftlog.LogEntry(6, "9")]
    )
    assert response.success
    assert response.properties["pre_length"] == 9
    assert response.properties["post_length"] == 10
    assert response.properties["entries_length"] == 1


def test_handle_append_entries_response(paper_log, init_callback) -> None:
    # Figure 7a
    leader_state = init_raft_state(paper_log, 6, raftstate.StateEnum.LEADER, 10)
    callback = init_callback("a", 6)

    response = leader_state.handle_append_entries_response(1, 0, False, None, callback)
    assert response.success
    assert response.properties["pre_length"] == 9
    assert response.properties["post_length"] == 10
    assert response.properties["entries_length"] == 1
    assert leader_state.next_index == 9

    assert leader_state.handle_append_entries_response(
        1, 0, response.success, response.properties, callback
    )[0]
    assert leader_state.next_index == 10

    # Figure 7b
    leader_state = init_raft_state(paper_log, 6, raftstate.StateEnum.LEADER, 10)
    callback = init_callback("b", 6)
    response = raftmessage.AppendEntryResponse(1, 0, False, {})

    for i in range(5):
        response = leader_state.handle_append_entries_response(
            1, 0, response.success, response.properties, callback
        )
        assert not response.success
        assert response.properties["pre_length"] == 4
        assert response.properties["post_length"] == 4
        assert response.properties["entries_length"] == i + 1
        assert leader_state.next_index == 9 - i

    response = leader_state.handle_append_entries_response(1, 0, False, None, callback)
    assert response.success
    assert response.properties["pre_length"] == 4
    assert response.properties["post_length"] == 10
    assert response.properties["entries_length"] == 6
    assert leader_state.next_index == 4

    assert leader_state.handle_append_entries_response(
        1, 0, response.success, response.properties, callback
    )[0]
    assert leader_state.next_index == 10

    # Figure 7c
    leader_state = init_raft_state(paper_log, 6, raftstate.StateEnum.LEADER, 10)
    callback = init_callback("c", 6)

    response = leader_state.handle_append_entries_response(1, 0, False, None, callback)
    assert response.success
    assert response.properties["pre_length"] == 11
    assert response.properties["post_length"] == 11
    assert response.properties["entries_length"] == 1
    assert leader_state.next_index == 9

    assert leader_state.handle_append_entries_response(
        1, 0, response.success, response.properties, callback
    )[0]
    assert leader_state.next_index == 10

    # Figure 7d
    leader_state = init_raft_state(paper_log, 6, raftstate.StateEnum.LEADER, 10)
    callback = init_callback("d", 6)

    response = leader_state.handle_append_entries_response(1, 0, False, None, callback)
    assert response.success
    assert response.properties["pre_length"] == 12
    assert response.properties["post_length"] == 12
    assert response.properties["entries_length"] == 1
    assert leader_state.next_index == 9

    assert leader_state.handle_append_entries_response(
        1, 0, response.success, response.properties, callback
    )[0]
    assert leader_state.next_index == 10

    # Figure 7e
    leader_state = init_raft_state(paper_log, 6, raftstate.StateEnum.LEADER, 10)
    callback = init_callback("e", 6)
    response = raftmessage.AppendEntryResponse(1, 0, False, {})

    for i in range(4):
        response = leader_state.handle_append_entries_response(
            1, 0, response.success, response.properties, callback
        )
        assert not response.success
        assert response.properties["pre_length"] == 7
        assert response.properties["post_length"] == 7
        assert response.properties["entries_length"] == i + 1
        assert leader_state.next_index == 9 - i

    response = leader_state.handle_append_entries_response(
        1, 0, response.success, response.properties, callback
    )
    assert response.success
    assert response.properties["pre_length"] == 7
    assert response.properties["post_length"] == 10
    assert response.properties["entries_length"] == 5
    assert leader_state.next_index == 5

    assert leader_state.handle_append_entries_response(
        1, 0, response.success, response.properties, callback
    )[0]
    assert leader_state.next_index == 10

    # Figure 7f
    leader_state = init_raft_state(paper_log, 6, raftstate.StateEnum.LEADER, 10)
    callback = init_callback("f", 6)
    response = raftmessage.AppendEntryResponse(1, 0, False, {})

    for i in range(6):
        response = leader_state.handle_append_entries_response(
            1, 0, response.success, response.properties, callback
        )
        assert not response.success
        assert response.properties["pre_length"] == 11
        assert response.properties["post_length"] == 11
        assert response.properties["entries_length"] == i + 1
        assert leader_state.next_index == 9 - i

    response = leader_state.handle_append_entries_response(
        1, 0, response.success, response.properties, callback
    )
    assert response.success
    assert response.properties["pre_length"] == 11
    assert response.properties["post_length"] == 10
    assert response.properties["entries_length"] == 7
    assert leader_state.next_index == 3

    assert leader_state.handle_append_entries_response(
        1, 0, response.success, response.properties, callback
    )[0]
    assert leader_state.next_index == 10


# TODO: Fix callback
# def test_handle_leader_heartbeat(logs_by_identifier, init_callback) -> None:
#     # Figure 7a
#     leader_state = init_raft_state(
#         logs_by_identifier["a"], 6, raftstate.StateEnum.LEADER, 9
#     )
#     callback = init_callback("a", 6)

#     response = leader_state.handle_leader_heartbeat(callback)
#     assert response.success
#     assert response.properties["pre_length"] == 9
#     assert response.properties["post_length"] == 9
#     assert response.properties["entries_length"] == 0
