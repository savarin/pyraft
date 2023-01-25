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


class NotCandidate(Exception):
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

        self.reset_attributes(-1, True, True)

    def reset_attributes(
        self, term: int, reset_commit_index: bool, reset_voted_for: bool
    ) -> None:
        """
        Certain attributes need to be reset on state changes. Here we take the
        conservative approach of resets even though it might be updated later,
        such as when a leader is elected.

        - commit_index is reset when moving to follower since this might have
          moved when acting as a leader but has not been communicated.
        - next_index is reset even though initialized when elected leader and
          only used as a leader.
        - match_index is reset even though initialized when elected leader and
          only used as a leader.
        - voted_for is reset when moving to follower to ensure stale votes are
          not counted.
        """
        if reset_commit_index:
            self.commit_index = -1

        if reset_voted_for:
            self.voted_for: Dict[int, Optional[int]] = {
                identifier: None for identifier in raftconfig.ADDRESS_BY_IDENTIFIER
            }

        self.current_term = term
        self.next_index: Dict[int, Optional[int]] = {
            identifier: len(self.log) for identifier in raftconfig.ADDRESS_BY_IDENTIFIER
        }
        self.match_index: Dict[int, Optional[int]] = {
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
        self.reset_attributes(self.current_term, False, False)
        self.match_index[self.identifier] = len(self.log) - 1

        followers = list(raftconfig.ADDRESS_BY_IDENTIFIER.keys())
        followers.remove(self.identifier)

        return self.create_leader_heartbeats(followers)

    def become_candidate(self) -> List[raftmessage.Message]:
        self.role = Role.CANDIDATE
        self.reset_attributes(self.current_term + 1, False, True)
        self.voted_for[self.identifier] = self.identifier

        followers = list(raftconfig.ADDRESS_BY_IDENTIFIER.keys())
        followers.remove(self.identifier)

        return self.create_vote_requests(followers)

    def become_follower(self, term: int) -> List[raftmessage.Message]:
        self.role = Role.FOLLOWER
        self.reset_attributes(term, True, True)
        return []

    # TODO: Carve out state change logic into separate method.
    def handle_role_change(
        self,
        role_change: Optional[Role],
        term: Optional[int] = None,
    ) -> List[raftmessage.Message]:
        match role_change:
            case Role.LEADER:
                return self.become_leader()

            case Role.CANDIDATE:
                return self.become_candidate()

            case Role.FOLLOWER:
                term = term or self.current_term
                return self.become_follower(term)

            case None:
                return []

            case _:
                raise Exception(
                    f"Exhaustive switch error on role change to {role_change}."
                )

    def update_next_index(self, target: int, entries_length: int) -> None:
        next_index = self.next_index[target]
        assert next_index is not None
        self.next_index[target] = next_index + entries_length

    def update_match_index(self, target: int) -> None:
        next_index = self.next_index[target]
        assert next_index is not None
        self.match_index[target] = next_index - 1

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
    ) -> Tuple[int, int, int, List[raftlog.LogEntry], int]:
        next_index = self.next_index[target]
        assert next_index is not None
        previous_index = next_index - 1

        previous_term = self.log[previous_index].term if previous_index >= 0 else -1

        return (
            self.current_term,
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
        current_term: int,
        previous_index: int,
        previous_term: int,
        entries: List[raftlog.LogEntry],
        commit_index: int,
    ) -> Tuple[List[raftmessage.Message], Optional[Role]]:
        """
        Update to the log (received by a follower).
        """
        # Handle role change in method to enable processing of request.
        if current_term > self.current_term:
            self.become_follower(current_term)

        # If candidate and discover current leader, then become follower.
        if self.role == Role.CANDIDATE and current_term == self.current_term:
            self.become_follower(current_term)

        # If candidate and have higher term, reject request. If leader and have
        # the same term or higher, reject the request.
        if self.role != Role.FOLLOWER:
            return [
                raftmessage.AppendEntryResponse(
                    target, source, self.current_term, False, len(entries), {}
                )
            ], None

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
                target, source, self.current_term, success, len(entries), properties
            )
        ], None

    def handle_append_entries_response(
        self,
        source: int,
        target: int,
        current_term: int,
        success: bool,
        entries_length: int,
        properties: Dict[str, int],
    ) -> Tuple[List[raftmessage.Message], Optional[Role]]:
        """
        Follower response (received by leader).
        """
        if self.role != Role.LEADER:
            raise NotLeader("Require leader role for append entries response.")

        if current_term > self.current_term:
            self.current_term = current_term
            return [], Role.FOLLOWER

        if success:
            self.update_next_index(source, entries_length)
            self.update_match_index(source)
            self.update_commit_index()
            return [], None

        next_index = self.next_index[source]
        assert next_index is not None
        self.next_index[source] = next_index - 1

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
        if current_term > self.current_term:
            self.become_follower(current_term)

        # If candidate or leader with same term or higher term, can simply
        # reject vote request.
        if self.role != Role.FOLLOWER:
            return [
                raftmessage.RequestVoteResponse(
                    target, source, False, self.current_term
                )
            ], None

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
        ], None

    def handle_request_vote_response(
        self,
        source: int,
        target: int,
        success: bool,
        current_term: int,
    ) -> Tuple[List[raftmessage.Message], Optional[Role]]:
        if self.role != Role.CANDIDATE:
            raise NotCandidate("Require candidate role for vote response.")

        if current_term > self.current_term:
            return [], Role.FOLLOWER

        role_change = None

        if success:
            self.voted_for[source] = target

            if self.has_won_election():
                role_change = Role.LEADER

        return [], role_change

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
