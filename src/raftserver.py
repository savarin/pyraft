from typing import List, Optional
import dataclasses
import enum

import raftlog


class StateEnum(enum.Enum):
    LEADER = "LEADER"
    FOLLOWER = "FOLLOWER"


@dataclasses.dataclass
class RaftState:
    def __post_init__(self) -> None:
        self.log: List[raftlog.LogEntry] = []
        self.current_term: int = 1
        self.current_state: StateEnum = StateEnum.FOLLOWER
        self.next_index: Optional[int] = None

    def change_state(self, state_enum: StateEnum):
        self.current_state = state_enum

    def create_append_entries_arguments(self):
        if self.next_index == -1:
            raise Exception("Invalid follower state.")

        previous_index = self.next_index - 1
        previous_term = self.log[previous_index].term
        return previous_index, previous_term, self.log[self.next_index :]

    def handle_client_log_append(self, item: str):
        # Client adds a log entry (received by leader).
        self.log.append(raftlog.LogEntry(self.current_term, item))

    def handle_append_entries(
        self,
        previous_index: int,
        previous_term: int,
        entries: List[raftlog.LogEntry],
    ):
        # Update to the log (received by a follower).
        pre_length = len(self.log)
        response = raftlog.append_entries(
            self.log, previous_index, previous_term, entries
        )

        return response, pre_length, len(self.log)

    def handle_append_entries_response(self, response, callback):
        # Follower response (received by leader).
        if response:
            return True, None, None

        self.next_index -= 1
        arguments = self.create_append_entries_arguments()
        response, pre_length, post_length = callback(*arguments)

        if response:
            _, _, entries = arguments
            self.next_index += len(entries)

        return response, pre_length, post_length

    def handle_leader_heartbeat(self):
        # Leader heartbeat. Send AppendEntries to all followers.
        pass
