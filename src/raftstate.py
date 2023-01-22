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
    identifier: int

    def __post_init__(self) -> None:
        self.log: List[raftlog.LogEntry] = []
        self.role: Role = Role.FOLLOWER
        self.current_term: int = -1
        self.commit_index: int = -1
        self.next_index: Dict[int, Optional[int]] = {
            identifier: None for identifier in raftconfig.ADDRESS_BY_IDENTIFIER
        }
        self.voted_for: Dict[int, Optional[int]] = {
            identifier: None for identifier in raftconfig.ADDRESS_BY_IDENTIFIER
        }

    def has_won_election(self):
        votes = len(
            [
                identifier
                for identifier in self.voted_for
                if identifier == self.identifier
            ]
        )

        return votes >= (len(self.voted_for) // 2)

    def create_heartbeat_messages(self) -> List[raftmessage.Message]:
        followers = list(raftconfig.ADDRESS_BY_IDENTIFIER.keys())
        followers.remove(self.identifier)

        return self.handle_leader_heartbeat(
            self.identifier, self.identifier, followers
        )[0]

    def become_leader(self) -> List[raftmessage.Message]:
        self.role = Role.LEADER
        return self.create_heartbeat_messages()

    def become_candidate(self) -> List[raftmessage.Message]:
        self.role = Role.CANDIDATE
        self.current_term += 1
        self.voted_for[self.identifier] = self.identifier
        return []

    def become_follower(self) -> List[raftmessage.Message]:
        self.role = Role.FOLLOWER
        return []

    def handle_role_change(
        self, role_change: Optional[Role]
    ) -> List[raftmessage.Message]:
        match role_change:
            case Role.LEADER:
                return self.become_leader()

            case Role.CANDIDATE:
                return self.become_candidate()

            case Role.FOLLOWER:
                return self.become_follower()

            case None:
                return []

            case _:
                raise Exception(
                    f"Exhaustive switch error on role change to {role_change}."
                )

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
    ) -> Tuple[List[raftmessage.Message], Optional[Role]]:
        """
        Client adds a log entry (received by leader).
        """
        if self.role != Role.LEADER:
            raise NotLeader("Require leader role for client log append.")

        self.log.append(raftlog.LogEntry(self.current_term, item))
        self.next_index[target] = len(self.log)

        return [], None

    def handle_append_entries_request(
        self,
        source: int,
        target: int,
        previous_index: int,
        previous_term: int,
        entries: List[raftlog.LogEntry],
        commit_index: int,
    ) -> Tuple[List[raftmessage.Message], Optional[Role]]:
        """
        Update to the log (received by a follower).
        """
        # TODO: Review edge cases where role not follower because have role with
        # different term, and if role change is needed.
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
        ], None

    def handle_append_entries_response(
        self,
        source: int,
        target: int,
        success: bool,
        previous_index: int,
        entries_length: int,
        properties: Dict[str, int],
    ) -> Tuple[List[raftmessage.Message], Optional[Role]]:
        """
        Follower response (received by leader).
        """
        if self.role != Role.LEADER:
            raise NotLeader("Require leader role for append entries response.")

        if success:
            self.update_next_index(source, entries_length, previous_index)
            self.update_commit_index()
            return [], None

        previous_index -= 1
        _, previous_term, entries, commit_index = self.create_append_entries_arguments(
            source, previous_index
        )

        return [
            raftmessage.AppendEntryRequest(
                target, source, previous_index, previous_term, entries, commit_index
            )
        ], None

    def handle_leader_heartbeat(
        self, source: int, target: int, followers: List[int]
    ) -> Tuple[List[raftmessage.Message], Optional[Role]]:
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

        return messages, None

    def handle_request_vote_request(
        self,
        source: int,
        target: int,
        term: int,
        last_log_index: int,
        last_log_term: int,
    ) -> Tuple[List[raftmessage.Message], Optional[Role]]:
        role_change = None

        if term > self.current_term:
            self.current_term = term
            role_change = Role.FOLLOWER

        # Require candidate have higher term.
        if term < self.current_term:
            success = False

        # Require candidate have last entry having at least the same term.
        elif last_log_term < self.log[-1].term:
            success = False

        # Require candidate have at least same log length.
        elif last_log_index < len(self.log) - 1:
            success = False

        else:
            assert term == self.current_term

            # Require vote not already cast to a different candidate.
            if (
                self.voted_for[self.identifier] is not None
                and self.voted_for[self.identifier] != source
            ):
                success = False

            else:
                # If vote not cast, cast vote.
                if self.voted_for[self.identifier] is None:
                    self.voted_for[self.identifier] = source

                success = True

        return [
            raftmessage.RequestVoteResponse(target, source, success, self.current_term)
        ], role_change

    def handle_request_vote_response(
        self,
        source: int,
        target: int,
        success: bool,
        current_term: int,
    ) -> Tuple[List[raftmessage.Message], Optional[Role]]:
        messages: List[raftmessage.Message] = []
        role_change = None

        if success:
            self.voted_for[source] = target

            if self.has_won_election():
                role_change = Role.LEADER

        elif current_term > self.current_term:
            role_change = Role.FOLLOWER

        return messages, role_change

    def handle_text(
        self, source: int, target: int, text: str
    ) -> Tuple[List[raftmessage.Message], Optional[Role]]:
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
        return [], None

    def handle_message(
        self, message: raftmessage.Message
    ) -> Tuple[List[raftmessage.Message], Optional[Role]]:
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

            case raftmessage.RequestVoteRequest():
                return self.handle_request_vote_request(**vars(message))

            case raftmessage.RequestVoteResponse():
                return self.handle_request_vote_response(**vars(message))

            case raftmessage.Text():
                return self.handle_text(**vars(message))

            case _:
                raise Exception(
                    "Exhaustive switch error on message type with message {message}."
                )
