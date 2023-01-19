"""
Layer around the core append_entries operation to keep state of the log and
handle well-defined events.
"""
from typing import List
import dataclasses
import enum

import raftlog
import raftmessage


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

    def handle_append_entries_request(
        self,
        source: int,
        target: int,
        previous_index: int,
        previous_term: int,
        entries: List[raftlog.LogEntry],
    ):
        """
        Update to the log (received by a follower.
        """

        pre_length = len(self.log)
        success = raftlog.append_entries(
            self.log, previous_index, previous_term, entries
        )

        properties = {
            "pre_length": pre_length,
            "post_length": len(self.log),
            "entries_length": len(entries),
        }

        return [raftmessage.AppendEntryResponse(target, source, success, properties)]

    def handle_client_log_append(self, item: str):
        """
        Client adds a log entry (received by leader).
        """
        self.log.append(raftlog.LogEntry(self.current_term, item))

    def handle_append_entries_response(self, source, target, success, properties):
        """
        Follower response (received by leader).
        """
        if success:
            self.next_index += properties["entries_length"]
            return None

        self.next_index -= 1
        previous_index, previous_term, entries = self.create_append_entries_arguments()

        return [
            raftmessage.AppendEntryRequest(
                source, target, previous_index, previous_term, entries
            )
        ]

    def handle_leader_heartbeat(self, source, target, followers):
        """
        Leader heartbeat. Send AppendEntries to all followers.
        """
        previous_index, previous_term, entries = self.create_append_entries_arguments()
        messages = []

        for follower in followers:
            message = raftmessage.AppendEntryRequest(
                source, follower, previous_index, previous_term, entries
            )
            messages.append(message)

        return messages

    def handle_text(self, source, target, text):
        print(text)
        return None

    def handle_message(self, message):
        match message:
            case raftmessage.AppendEntryRequest():
                return self.handle_append_entries_request(**vars(message))

            case raftmessage.AppendEntryResponse():
                return self.handle_append_entries_response(**vars(message))

            case raftmessage.UpdateFollowers():
                return self.handle_leader_heartbeat(**vars(message))

            case raftmessage.Text():
                return self.handle_text(**vars(message))

            case _:
                raise Exception("Exhaustive switch error on message type.")
