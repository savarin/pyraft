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
    leader_state = init_raft_state(paper_log, 6, raftserver.StateEnum.LEADER, 9)
    assert leader_state.handle_append_entries_response(init_callback("a", 6))
    assert leader_state.next_index == 9

    leader_state = init_raft_state(paper_log, 6, raftserver.StateEnum.LEADER, 9)
    assert leader_state.handle_append_entries_response(init_callback("b", 6))
    assert leader_state.next_index == 4

    leader_state = init_raft_state(paper_log, 6, raftserver.StateEnum.LEADER, 9)
    assert leader_state.handle_append_entries_response(init_callback("c", 6))
    assert leader_state.next_index == 9

    leader_state = init_raft_state(paper_log, 6, raftserver.StateEnum.LEADER, 9)
    assert leader_state.handle_append_entries_response(init_callback("d", 6))
    assert leader_state.next_index == 9

    leader_state = init_raft_state(paper_log, 6, raftserver.StateEnum.LEADER, 9)
    assert leader_state.handle_append_entries_response(init_callback("e", 6))
    assert leader_state.next_index == 5

    leader_state = init_raft_state(paper_log, 6, raftserver.StateEnum.LEADER, 9)
    assert leader_state.handle_append_entries_response(init_callback("f", 6))
    assert leader_state.next_index == 3
