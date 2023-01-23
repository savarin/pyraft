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
        self.next_index: Dict[int, int] = {
            identifier: len(self.log) for identifier in raftconfig.ADDRESS_BY_IDENTIFIER
        }
        self.match_index: Dict[int, Optional[int]] = {
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

    def create_leader_heartbeats(
        self, followers: List[int]
    ) -> List[raftmessage.Message]:
        messages: List[raftmessage.Message] = []

        for follower in followers:
            message = raftmessage.AppendEntryRequest(
                self.identifier,
                follower,
                *self.create_append_entries_arguments(follower),
            )
            messages.append(message)

        return messages

    def create_vote_requests(self, followers: List[int]) -> List[raftmessage.Message]:
        messages: List[raftmessage.Message] = []

        previous_term = self.log[-1].term if len(self.log) >= 0 else -1

        for follower in followers:
            message = raftmessage.RequestVoteRequest(
                self.identifier,
                follower,
                self.current_term,
                len(self.log) - 1,
                previous_term,
            )
            messages.append(message)

        return messages

    def become_leader(self) -> List[raftmessage.Message]:
        self.role = Role.LEADER
        self.next_index = {identifier: len(self.log) for identifier in self.next_index}
        self.match_index = {identifier: None for identifier in self.next_index}
        self.match_index[self.identifier] = len(self.log) - 1

        followers = list(raftconfig.ADDRESS_BY_IDENTIFIER.keys())
        followers.remove(self.identifier)

        return self.create_leader_heartbeats(followers)

    def become_candidate(self) -> List[raftmessage.Message]:
        self.role = Role.CANDIDATE
        self.current_term += 1
        self.voted_for[self.identifier] = self.identifier

        followers = list(raftconfig.ADDRESS_BY_IDENTIFIER.keys())
        followers.remove(self.identifier)

        return self.create_vote_requests(followers)

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

    def update_next_index(self, target: int, entries_length: int) -> None:
        next_index = self.next_index[target]
        self.next_index[target] = next_index + entries_length

    def update_match_index(self, target: int) -> None:
        self.match_index[target] = self.next_index[target] - 1

    def update_commit_index(self) -> None:
        match_index_values = sorted(
            [item for item in self.match_index.values() if item is not None]
        )
        majority_count = len(self.next_index) // 2 + 1
        null_count = len(self.next_index) - len(match_index_values)

        # Require at least majority of next_index to be non-null.
        if len(match_index_values) < majority_count:
            return None

        commit_index = match_index_values[majority_count - 1 - null_count]

        if self.log[commit_index].term == self.current_term:
            self.commit_index = commit_index

    def create_append_entries_arguments(
        self, target: int
    ) -> Tuple[int, int, List[raftlog.LogEntry], int]:
        next_index = self.next_index[target]
        previous_index = next_index - 1

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

        pre_length = len(self.log)
        success = raftlog.append_entries(
            self.log, previous_index, previous_term, entries
        )

        if commit_index > self.commit_index:
            self.commit_index = min(commit_index, len(self.log) - 1)

        properties = {
            "pre_length": pre_length,
            "post_length": len(self.log),
        }

        return [
            raftmessage.AppendEntryResponse(
                target, source, success, len(entries), properties
            )
        ], None

    def handle_append_entries_response(
        self,
        source: int,
        target: int,
        success: bool,
        entries_length: int,
        properties: Dict[str, int],
    ) -> Tuple[List[raftmessage.Message], Optional[Role]]:
        """
        Follower response (received by leader).
        """
        if self.role != Role.LEADER:
            raise NotLeader("Require leader role for append entries response.")

        if success:
            self.update_next_index(source, entries_length)
            self.update_match_index(source)
            self.update_commit_index()
            return [], None

        self.next_index[source] -= 1

        return [
            raftmessage.AppendEntryRequest(
                target,
                source,
                *self.create_append_entries_arguments(source),
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

        return self.create_leader_heartbeats(followers), None

    def handle_request_vote_request(
        self,
        source: int,
        target: int,
        current_term: int,
        last_log_index: int,
        last_log_term: int,
    ) -> Tuple[List[raftmessage.Message], Optional[Role]]:
        role_change = None

        if current_term > self.current_term:
            self.current_term = current_term
            role_change = Role.FOLLOWER

        # Require candidate have higher term.
        if current_term < self.current_term:
            success = False

        # Require candidate have last entry having at least the same term.
        elif last_log_term < self.log[-1].term:
            success = False

        # Require candidate have at least same log length.
        elif last_log_index < len(self.log) - 1:
            success = False

        else:
            assert current_term == self.current_term

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
