import pytest

import raftserver
from test_raftlog import paper_log


def init_raft_state(
    log, current_term, current_state, next_index
) -> raftserver.RaftState:
    raft_state = raftserver.RaftState()
    raft_state.log = log
    raft_state.current_term = current_term
    raft_state.current_state = current_state
    raft_state.next_index = next_index
    return raft_state


def test_handle_append_entries_response(paper_log) -> None:
    log_1 = paper_log.copy()
    log_1.pop()
    leader_state = init_raft_state(paper_log.copy(), 6, raftserver.StateEnum.LEADER, 9)
    follower_state = init_raft_state(log_1, 6, raftserver.StateEnum.FOLLOWER, None)
    callback = follower_state.handle_append_entries

    assert leader_state.handle_append_entries_response(callback)
    assert leader_state.next_index == 9
