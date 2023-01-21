"""
Layer around the core append_entries operation to keep state of the log and
handle well-defined events.
"""
from typing import Dict, List, Optional, Tuple
import dataclasses
import enum

import raftconfig
import raftlog
import raftmessage


class NotLeader(Exception):
    pass


class NotFollower(Exception):
    pass


class Role(enum.Enum):
    LEADER = "LEADER"
    CANDIDATE = "CANDIDATE"
    FOLLOWER = "FOLLOWER"


@dataclasses.dataclass
class RaftState:
    def __post_init__(self) -> None:
        self.log: List[raftlog.LogEntry] = []
        self.role: Role = Role.FOLLOWER
        self.current_term: int = -1
        self.commit_index: int = -1
        self.next_index: Dict[int, Optional[int]] = {
            identifier: None for identifier in raftconfig.ADDRESS_BY_IDENTIFIER
        }

    def change_state(self, state_enum: Role) -> None:
        self.role = state_enum

    def get_next_index(self, target: int) -> int:
        next_index = self.next_index[target]

        if next_index is None:
            return len(self.log)

        assert next_index is not None
        return next_index

    def update_next_index(
        self, target: int, entries_length: int, previous_index: int
    ) -> None:
        next_index = self.next_index[target]

        if next_index is None:
            self.next_index[target] = previous_index + 1 + entries_length

        else:
            self.next_index[target] = next_index + entries_length

    def update_commit_index(self) -> None:
        next_index_values = sorted(
            [item for item in self.next_index.values() if item is not None]
        )
        majority_count = len(self.next_index) // 2 + 1
        null_count = len(self.next_index) - len(next_index_values)

        # Require at least majority of next_index to be non-null.
        if len(next_index_values) < majority_count:
            return None

        self.commit_index = next_index_values[majority_count - 1 - null_count] - 1

    def create_append_entries_arguments(
        self,
        target: int,
        previous_index: Optional[int],
    ) -> Tuple[int, int, List[raftlog.LogEntry], int]:
        if previous_index is None:
            next_index = self.get_next_index(target)
            previous_index = next_index - 1

        else:
            next_index = previous_index + 1

        previous_term = self.log[previous_index].term if previous_index >= 0 else -1

        return (
            previous_index,
            previous_term,
            self.log[next_index:],
            self.commit_index,
        )

    def handle_client_log_append(
        self, source: int, target: int, item: str
    ) -> List[raftmessage.Message]:
        """
        Client adds a log entry (received by leader).
        """
        if self.role != Role.LEADER:
            raise NotLeader("Require leader role for client log append.")

        self.log.append(raftlog.LogEntry(self.current_term, item))
        self.next_index[target] = len(self.log)

        return []

    def handle_append_entries_request(
        self,
        source: int,
        target: int,
        previous_index: int,
        previous_term: int,
        entries: List[raftlog.LogEntry],
        commit_index: int,
    ) -> List[raftmessage.Message]:
        """
        Update to the log (received by a follower).
        """
        # TODO: When exception raised, return message to leader.
        if self.role != Role.FOLLOWER:
            raise NotFollower("Require follower role for append entries request.")

        self.commit_index = commit_index

        pre_length = len(self.log)
        success = raftlog.append_entries(
            self.log, previous_index, previous_term, entries
        )

        properties = {
            "pre_length": pre_length,
            "post_length": len(self.log),
        }

        return [
            raftmessage.AppendEntryResponse(
                target, source, success, previous_index, len(entries), properties
            )
        ]

    def handle_append_entries_response(
        self,
        source: int,
        target: int,
        success: bool,
        previous_index: int,
        entries_length: int,
        properties: Dict[str, int],
    ) -> List[raftmessage.Message]:
        """
        Follower response (received by leader).
        """
        if self.role != Role.LEADER:
            raise NotLeader("Require leader role for append entries response.")

        if success:
            self.update_next_index(source, entries_length, previous_index)
            self.update_commit_index()
            return []

        previous_index -= 1
        _, previous_term, entries, commit_index = self.create_append_entries_arguments(
            source, previous_index
        )

        return [
            raftmessage.AppendEntryRequest(
                target, source, previous_index, previous_term, entries, commit_index
            )
        ]

    def handle_leader_heartbeat(
        self, source: int, target: int, followers: List[int]
    ) -> List[raftmessage.Message]:
        """
        Leader heartbeat. Send AppendEntries to all followers.
        """
        if self.role != Role.LEADER:
            raise NotLeader("Require leader role for leader heartbeat.")

        messages: List[raftmessage.Message] = []

        for follower in followers:
            message = raftmessage.AppendEntryRequest(
                source, follower, *self.create_append_entries_arguments(follower, None)
            )
            messages.append(message)

        return messages

    def handle_text(
        self, source: int, target: int, text: str
    ) -> List[raftmessage.Message]:
        """
        Simplify testing with custom commands passed by messages. Have ability
        to expose and modify state, but not send messages so update is not
        implemented.
        """
        if text.startswith("expose"):
            text = f"+ {str(self.commit_index)} {str(self.log)}"

        elif text.startswith("append"):
            try:
                for item in text.replace("append ", "").split():
                    self.handle_client_log_append(target, target, item)

            except Exception as e:
                text = f"Exception: {e}"

        else:
            text = f"{source} > {target} {text}"

        print(f"\n{text}\n{target} > ", end="")
        return []

    def handle_message(self, message: raftmessage.Message) -> List[raftmessage.Message]:
        # TODO: Ensure committed entries are not rewritten.
        match message:
            case raftmessage.ClientLogAppend():
                return self.handle_client_log_append(**vars(message))

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
