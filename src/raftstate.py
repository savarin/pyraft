"""
Layer around the core append_entries operation to keep state of the log and
handle well-defined events.
"""
from typing import List
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
        self.next_index: int = 0

    def change_state(self, state_enum: StateEnum):
        self.current_state = state_enum

    def create_append_entries_arguments(self):
        if self.next_index == -1:
            raise Exception("Invalid follower state.")

        previous_index = self.next_index - 1
        previous_term = self.log[previous_index].term
        return previous_index, previous_term, self.log[self.next_index :]

    def handle_append_entries(
        self,
        previous_index: int,
        previous_term: int,
        entries: List[raftlog.LogEntry],
    ):
        """
        Update to the log (received by a follower.
        """

        pre_length = len(self.log)
        response = raftlog.append_entries(
            self.log, previous_index, previous_term, entries
        )

        properties = {
            "pre_length": pre_length,
            "post_length": len(self.log),
            "entries_length": len(entries),
        }

        return response, properties

    def handle_client_log_append(self, item: str):
        """
        Client adds a log entry (received by leader).
        """
        self.log.append(raftlog.LogEntry(self.current_term, item))

    def handle_append_entries_response(self, response, properties, callback):
        """
        Follower response (received by leader).
        """
        if response:
            self.next_index += properties["entries_length"]
            return True, properties

        self.next_index -= 1
        arguments = self.create_append_entries_arguments()
        return callback(*arguments)

    def handle_leader_heartbeat(self, callback):
        """
        Leader heartbeat. Send AppendEntries to all followers.
        """
        arguments = self.create_append_entries_arguments()
        return callback(*arguments)
