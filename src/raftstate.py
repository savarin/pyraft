"""
Layer around the core append_entries operation to keep state of the log and
handle well-defined events.
"""
from typing import Dict, List, Optional, Tuple
import dataclasses

import raftconfig
import raftlog
import raftmessage
import raftrole


@dataclasses.dataclass
class RaftState:
    identifier: int

    def __post_init__(self) -> None:
        self.log: List[raftlog.LogEntry] = []
        self.role: raftrole.Role = raftrole.Role.FOLLOWER
        self.current_term: int = -1
        self.next_index: Optional[Dict[int, int]] = None
        self.match_index: Optional[Dict[int, Optional[int]]] = None
        self.commit_index: int = -1
        self.voted_for: Optional[int] = None
        self.current_votes: Optional[Dict[int, Optional[int]]] = None
        self.config: Dict[int, Tuple[str, int]] = raftconfig.ADDRESS_BY_IDENTIFIER

    ###
    ###   MULTI-PURPOSE HELPERS
    ###

    def count_majority(self) -> int:
        return 1 + len(self.config) // 2

    def create_followers_list(self) -> List[int]:
        followers = list(self.config.keys())
        followers.remove(self.identifier)

        return followers

    def implement_state_change(self, state_change: raftrole.StateChange) -> None:
        if state_change["role_change"] is not None:
            assert state_change["role_change"][0] == self.role
            self.role = state_change["role_change"][1]

        self.current_term = state_change["current_term"]

        if state_change["next_index"] == raftrole.Operation.RESET_TO_NONE:
            self.next_index = None
        elif state_change["next_index"] == raftrole.Operation.INITIALIZE:
            self.next_index = {identifier: len(self.log) for identifier in self.config}

        if state_change["match_index"] == raftrole.Operation.RESET_TO_NONE:
            self.match_index = None
        elif state_change["match_index"] == raftrole.Operation.INITIALIZE:
            self.match_index = {identifier: None for identifier in self.config}

        # Exception to RESET_TO_NONE, where reset is to -1. This is to simplify
        # message passing since integers are handled in the encoding/decoding
        # step, but None needs an extra step. Setting to -1 skip this step, but
        # care is needed at call sites to make sure change is via assignment
        # rather than addition.
        if state_change["commit_index"] == raftrole.Operation.RESET_TO_NONE:
            self.commit_index = -1
        elif state_change["commit_index"] == raftrole.Operation.INITIALIZE:
            raise Exception("Invalid initialization operation for commit index.")

        if state_change["voted_for"] == raftrole.Operation.RESET_TO_NONE:
            self.voted_for = None
        elif state_change["voted_for"] == raftrole.Operation.INITIALIZE:
            self.voted_for = self.identifier

        if state_change["current_votes"] == raftrole.Operation.RESET_TO_NONE:
            self.current_votes = None
        elif state_change["current_votes"] == raftrole.Operation.INITIALIZE:
            self.current_votes = {identifier: None for identifier in self.config}
            self.current_votes[self.identifier] = self.identifier

    ###
    ###   CLIENT-RELATED HELPERS AND HANDLERS
    ###

    def handle_client_log_append(
        self, source: int, target: int, item: str
    ) -> Tuple[
        List[raftmessage.Message], Optional[Tuple[raftrole.Role, raftrole.Role]]
    ]:
        """
        Client adds a log entry (received by leader).
        """
        if self.role != raftrole.Role.LEADER:
            raise Exception("Not able to generate leader heartbeat when not leader.")

        self.log.append(raftlog.LogEntry(self.current_term, item))

        assert self.next_index is not None
        self.next_index[target] = len(self.log)

        assert self.match_index is not None
        self.match_index[target] = len(self.log) - 1

        return [], None

    ###
    ###   LEADER-RELATED HELPERS AND HANDLERS
    ###

    def create_append_entries_arguments(
        self, target: int
    ) -> Tuple[int, int, int, List[raftlog.LogEntry], int]:
        assert self.next_index is not None
        next_index = self.next_index[target]

        assert next_index is not None
        previous_index = next_index - 1
        previous_term = self.log[previous_index].term if len(self.log) > 0 and previous_index >= 0 else -1

        assert next_index is not None
        return (
            self.current_term,
            previous_index,
            previous_term,
            self.log[next_index:],
            self.commit_index,
        )

    def count_null_match_index(self) -> int:
        assert self.match_index is not None
        return len(
            [
                identifier
                for identifier in self.match_index.values()
                if identifier is None
            ]
        )

    def get_index_metrics(self) -> Tuple[int, int]:
        assert self.match_index is not None
        non_null_match_index_values = sorted(
            [value for value in self.match_index.values() if value is not None]
        )
        non_null_match_index_count = len(non_null_match_index_values)

        # Get median value with index corrected for null values
        median_match_index = self.count_majority() - 1 - self.count_null_match_index()
        potential_commit_index = non_null_match_index_values[median_match_index]

        return non_null_match_index_count, potential_commit_index

    def update_indexes(self, target: int) -> None:
        assert self.next_index is not None
        self.next_index[target] = len(self.log)

        assert self.match_index is not None
        self.match_index[target] = len(self.log) - 1

        # Change to leader's commit_index is only relevant after a successful
        # append entry response from follower.
        non_null_match_index_count, potential_commit_index = self.get_index_metrics()

        # Require at least majority of next_index to be non-null.
        if non_null_match_index_count < self.count_majority():
            return None

        # Require latest be entry from leader's current term.
        if len(self.log) > 0 and self.log[potential_commit_index].term == self.current_term:
            self.commit_index = potential_commit_index

    def handle_leader_heartbeat(
        self, source: int, target: int, followers: Optional[List[int]] = None
    ) -> Tuple[
        List[raftmessage.Message], Optional[Tuple[raftrole.Role, raftrole.Role]]
    ]:
        """
        Leader heartbeat. Send AppendEntries to all followers.
        """
        if self.role != raftrole.Role.LEADER:
            raise Exception("Not able to generate leader heartbeat when not leader.")

        if followers is None:
            followers = self.create_followers_list()

        messages: List[raftmessage.Message] = []

        for follower in followers:
            message = raftmessage.AppendEntryRequest(
                self.identifier,
                follower,
                *self.create_append_entries_arguments(follower),
            )
            messages.append(message)

        return messages, None

    def handle_append_entries_request(
        self,
        source: int,
        target: int,
        current_term: int,
        previous_index: int,
        previous_term: int,
        entries: List[raftlog.LogEntry],
        commit_index: int,
    ) -> Tuple[
        List[raftmessage.Message], Optional[Tuple[raftrole.Role, raftrole.Role]]
    ]:
        """
        Update to the log (received by a follower).
        """
        state_change = raftrole.enumerate_state_change(
            raftrole.Role.LEADER, current_term, self.role, self.current_term
        )
        self.implement_state_change(state_change)

        # If not follower, then early return with no log changes.
        if self.role != raftrole.Role.FOLLOWER:
            return [
                raftmessage.AppendEntryResponse(
                    target,
                    source,
                    self.current_term,
                    False,
                    len(entries),
                )
            ], state_change["role_change"]

        success = raftlog.append_entries(
            self.log, previous_index, previous_term, entries
        )

        # Movement of commit_index by follower is based on commit_index on
        # leader and length of own log.
        if commit_index > self.commit_index:
            self.commit_index = min(commit_index, len(self.log) - 1)

        return [
            raftmessage.AppendEntryResponse(
                target, source, self.current_term, success, len(entries)
            )
        ], state_change["role_change"]

    def handle_append_entries_response(
        self,
        source: int,
        target: int,
        current_term: int,
        success: bool,
        entries_length: int,
    ) -> Tuple[
        List[raftmessage.Message], Optional[Tuple[raftrole.Role, raftrole.Role]]
    ]:
        """
        Follower response (received by leader).
        """
        # Since leader target response is the same irrespective of source role,
        # simply use follower as source role.
        state_change = raftrole.enumerate_state_change(
            raftrole.Role.FOLLOWER, current_term, self.role, self.current_term
        )
        self.implement_state_change(state_change)

        # If not leader, then early return with no log changes.
        if self.role != raftrole.Role.LEADER:
            return [], state_change["role_change"]

        # If successful, update indexes.
        if success:
            self.update_indexes(source)
            return [], state_change["role_change"]

        # If not successful, retry with earlier entries.
        assert self.next_index is not None
        next_index = self.next_index[source]

        assert next_index is not None
        self.next_index[source] = next_index - 1

        return [
            raftmessage.AppendEntryRequest(
                target,
                source,
                *self.create_append_entries_arguments(source),
            )
        ], state_change["role_change"]

    ###
    ###   CANDIDATE-RELATED HELPERS AND HANDLERS
    ###

    def handle_candidate_solicitation(
        self, source: int, target: int, followers: Optional[List[int]] = None
    ) -> Tuple[
        List[raftmessage.Message], Optional[Tuple[raftrole.Role, raftrole.Role]]
    ]:
        messages: List[raftmessage.Message] = []
        """
        Candidate soliciting votes. Send RequestVoteRequest to all followers.
        """
        if self.role != raftrole.Role.CANDIDATE:
            raise Exception("Not able to solicit votes when not candidate.")

        if followers is None:
            followers = self.create_followers_list()

        previous_term = self.log[-1].term if len(self.log) > 0 else -1

        for follower in followers:
            message = raftmessage.RequestVoteRequest(
                self.identifier,
                follower,
                self.current_term,
                len(self.log) - 1,
                previous_term,
            )
            messages.append(message)

        return messages, None

    def count_self_votes(self) -> int:
        assert self.current_votes is not None
        return len(
            [
                identifier
                for identifier in self.current_votes.values()
                if identifier == self.identifier
            ]
        )

    def count_null_votes(self) -> int:
        assert self.current_votes is not None
        return len(
            [
                identifier
                for identifier in self.current_votes.values()
                if identifier is None
            ]
        )

    def has_won_election(self) -> bool:
        return self.count_self_votes() >= self.count_majority()

    def handle_request_vote_request(
        self,
        source: int,
        target: int,
        current_term: int,
        last_log_index: int,
        last_log_term: int,
    ) -> Tuple[
        List[raftmessage.Message], Optional[Tuple[raftrole.Role, raftrole.Role]]
    ]:
        state_change = raftrole.enumerate_state_change(
            raftrole.Role.CANDIDATE, current_term, self.role, self.current_term
        )
        self.implement_state_change(state_change)

        # If not follower, then early return with failed vote response.
        if self.role != raftrole.Role.FOLLOWER:
            return [
                raftmessage.RequestVoteResponse(
                    target, source, False, self.current_term
                )
            ], state_change["role_change"]

        # Require candidate have higher term.
        if current_term < self.current_term:
            success = False

        # Require candidate have last entry having at least the same term.
        elif len(self.log) > 0 and last_log_term < self.log[-1].term:
            success = False

        # Require candidate have at least same log length.
        elif last_log_index < len(self.log) - 1:
            success = False

        else:
            assert current_term == self.current_term

            # Require vote not already cast to a different candidate.
            if self.voted_for is not None and self.voted_for != source:
                success = False

            else:
                if self.voted_for is None:
                    self.voted_for = source

                success = True

        return [
            raftmessage.RequestVoteResponse(target, source, success, self.current_term)
        ], state_change["role_change"]

    def handle_request_vote_response(
        self,
        source: int,
        target: int,
        success: bool,
        current_term: int,
    ) -> Tuple[
        List[raftmessage.Message], Optional[Tuple[raftrole.Role, raftrole.Role]]
    ]:
        # Since state change from candidate to follower on the back of a message
        # with equal term from a leader is only relevant for append entry
        # requests, can assume message from follower.
        state_change = raftrole.enumerate_state_change(
            raftrole.Role.FOLLOWER, current_term, self.role, self.current_term
        )
        self.implement_state_change(state_change)

        # If not candidate, then early return with no log changes.
        if self.role != raftrole.Role.CANDIDATE:
            return [], state_change["role_change"]

        if success:
            assert self.current_votes is not None
            self.current_votes[source] = target

            if self.has_won_election():
                state_change = raftrole.enumerate_state_change(
                    raftrole.Role.ELECTION_COMMISSION,
                    self.current_term,
                    self.role,
                    self.current_term,
                )
                self.implement_state_change(state_change)

        return [], state_change["role_change"]

    ###
    ###   CUSTOM HELPERS AND HANDLERS
    ###

    def color(self):
        return raftrole.color(self.role)

    def handle_text(
        self, source: int, target: int, text: str
    ) -> Tuple[
        List[raftmessage.Message], Optional[Tuple[raftrole.Role, raftrole.Role]]
    ]:
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

        print(self.color() + f"\n{text}\n{target} > ", end="")
        return [], None

    ###
    ###   PUBLIC INTERFACE TO HANDLERS
    ###

    # TODO: Create message handler for timer.
    def handle_message(
        self, message: raftmessage.Message
    ) -> Tuple[
        List[raftmessage.Message], Optional[Tuple[raftrole.Role, raftrole.Role]]
    ]:
        match message:
            case raftmessage.ClientLogAppend():
                return self.handle_client_log_append(**vars(message))

            case raftmessage.UpdateFollowers():
                return self.handle_leader_heartbeat(**vars(message))

            case raftmessage.AppendEntryRequest():
                return self.handle_append_entries_request(**vars(message))

            case raftmessage.AppendEntryResponse():
                return self.handle_append_entries_response(**vars(message))

            case raftmessage.RunElection():
                return self.handle_candidate_solicitation(**vars(message))

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


###
###   HELPERS FOR RAFTSERVER AND TESTS
###


def change_role_from_follower_to_candidate(
    state: RaftState, current_term: Optional[int] = None
) -> None:
    state_change = raftrole.enumerate_state_change(
        raftrole.Role.TIMER,
        current_term or state.current_term,
        raftrole.Role.FOLLOWER,
        current_term or state.current_term,
    )
    state.implement_state_change(state_change)


def change_role_from_candidate_to_leader(
    state: RaftState, current_term: Optional[int] = None
) -> None:
    state_change = raftrole.enumerate_state_change(
        raftrole.Role.ELECTION_COMMISSION,
        current_term or state.current_term,
        raftrole.Role.CANDIDATE,
        current_term or state.current_term,
    )
    state.implement_state_change(state_change)
