"""
Layer around the core append_entries operation to keep state of the log and
handle well-defined events.
"""
from typing import Dict, List, Tuple
import dataclasses
import enum

import raftconfig
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
        self.next_index: Dict[int, int] = {
            identifier: 0 for identifier in raftconfig.ADDRESS_BY_IDENTIFIER
        }

    def change_state(self, state_enum: StateEnum) -> None:
        self.current_state = state_enum

    def create_append_entries_arguments(
        self, target: int
    ) -> Tuple[int, int, List[raftlog.LogEntry]]:
        if self.next_index[target] == -1:
            raise Exception("Invalid follower state.")

        previous_index = self.next_index[target] - 1
        previous_term = self.log[previous_index].term
        return previous_index, previous_term, self.log[self.next_index[target] :]

    def handle_append_entries_request(
        self,
        source: int,
        target: int,
        previous_index: int,
        previous_term: int,
        entries: List[raftlog.LogEntry],
    ) -> List[raftmessage.Message]:
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

    def handle_client_log_append(self, item: str) -> None:
        """
        Client adds a log entry (received by leader).
        """
        self.log.append(raftlog.LogEntry(self.current_term, item))

    def handle_append_entries_response(
        self, source: int, target: int, success: bool, properties: Dict[str, int]
    ) -> List[raftmessage.Message]:
        """
        Follower response (received by leader).
        """
        if success:
            self.next_index[source] += properties["entries_length"]
            return []

        self.next_index[source] -= 1
        return [
            raftmessage.AppendEntryRequest(
                target, source, *self.create_append_entries_arguments(source)
            )
        ]

    def handle_leader_heartbeat(
        self, source: int, target: int, followers: List[int]
    ) -> List[raftmessage.Message]:
        """
        Leader heartbeat. Send AppendEntries to all followers.
        """
        messages: List[raftmessage.Message] = []

        for follower in followers:
            message = raftmessage.AppendEntryRequest(
                source, follower, *self.create_append_entries_arguments(follower)
            )
            messages.append(message)

        return messages

    def handle_text(
        self, source: int, target: int, text: str
    ) -> List[raftmessage.Message]:
        if text == "expose":
            print(f"\n+ {str(self.log)}\n{target} > ", end="")

        else:
            print(f"\n{source} > {target} {text}\n{target} > ", end="")

        return []

    def handle_message(self, message: raftmessage.Message) -> List[raftmessage.Message]:
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
