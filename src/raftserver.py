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

    def generate_append_entries_arguments(self):
        while True:
            if self.next_index == 0:
                raise StopIteration("Invalid follower state.")

            previous_index = self.next_index - 1
            previous_term = self.log[previous_index].term
            yield previous_index, previous_term, self.log[self.next_index :]

            self.next_index = previous_index

    def handle_client_log_append(self, item: str):
        self.log.append(raftlog.LogEntry(self.current_term, item))

    def handle_append_entries(
        self,
        previous_index: int,
        previous_term: int,
        entries: List[raftlog.LogEntry],
    ):
        return raftlog.append_entries(self.log, previous_index, previous_term, entries)

    def handle_append_entries_response(self, callback):
        for arguments in self.generate_append_entries_arguments():
            if callback(*arguments):
                return self.next_index

        return None

    def handle_leader_heartbeat(self):
        pass
