import pytest

import raftserver
from test_raftlog import paper_log, logs_by_identifier


def init_raft_state(
    log, current_term, current_state, next_index
) -> raftserver.RaftState:
    raft_state = raftserver.RaftState()
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
            raftserver.StateEnum.FOLLOWER,
            None,
        )
        return follower_state.handle_append_entries

    return closure


def test_handle_append_entries_response(paper_log, init_callback) -> None:
    # Figure 7a
    leader_state = init_raft_state(paper_log, 6, raftserver.StateEnum.LEADER, 10)
    callback = init_callback("a", 6)

    response, pre, post = leader_state.handle_append_entries_response(False, callback)
    assert response
    assert pre == 9
    assert post == 10
    assert leader_state.next_index == 10

    # Figure 7b
    leader_state = init_raft_state(paper_log, 6, raftserver.StateEnum.LEADER, 10)
    callback = init_callback("b", 6)

    for i in range(5):
        response, pre, post = leader_state.handle_append_entries_response(
            False, callback
        )
        assert not response
        assert pre == 4
        assert post == 4
        assert leader_state.next_index == 9 - i

    response, pre, post = leader_state.handle_append_entries_response(False, callback)
    assert response
    assert pre == 4
    assert post == 10
    assert leader_state.next_index == 10

    # Figure 7c
    leader_state = init_raft_state(paper_log, 6, raftserver.StateEnum.LEADER, 10)
    callback = init_callback("c", 6)

    response, pre, post = leader_state.handle_append_entries_response(False, callback)
    assert response
    assert pre == 11
    assert post == 11
    assert leader_state.next_index == 10

    # Figure 7d
    leader_state = init_raft_state(paper_log, 6, raftserver.StateEnum.LEADER, 10)
    callback = init_callback("d", 6)

    response, pre, post = leader_state.handle_append_entries_response(False, callback)
    assert response
    assert pre == 12
    assert post == 12
    assert leader_state.next_index == 10

    # Figure 7e
    leader_state = init_raft_state(paper_log, 6, raftserver.StateEnum.LEADER, 10)
    callback = init_callback("e", 6)

    for i in range(4):
        response, pre, post = leader_state.handle_append_entries_response(
            False, callback
        )
        assert not response
        assert pre == 7
        assert post == 7
        assert leader_state.next_index == 9 - i

    response, pre, post = leader_state.handle_append_entries_response(False, callback)
    assert response
    assert pre == 7
    assert post == 10
    assert leader_state.next_index == 10

    # Figure 7e
    leader_state = init_raft_state(paper_log, 6, raftserver.StateEnum.LEADER, 10)
    callback = init_callback("f", 6)

    for i in range(6):
        response, pre, post = leader_state.handle_append_entries_response(
            False, callback
        )
        assert not response
        assert pre == 11
        assert post == 11
        assert leader_state.next_index == 9 - i

    response, pre, post = leader_state.handle_append_entries_response(False, callback)
    assert response
    assert pre == 11
    assert post == 10
    assert leader_state.next_index == 10
