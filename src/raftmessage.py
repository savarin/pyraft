"""
Messages as a thin-layered class to simplify events-based interaction.


Relevant items from AppendEntries RPCs section in Figure 2 of Raft paper:

Invoked by leader to replicate log entries (§5.3); also used as heartbeat
(§5.2).

Arguments:
term            leader’s term
leaderId
prevLogIndex    index of log entry immediately preceding new ones
prevLogTerm     term of prevLogIndex entry
entries[]       log entries to store (empty for heartbeat; may send more than
                one for efficiency)
leaderCommit    leader’s commitIndex

Results:
term            currentTerm, for leader to update itself
success         true if follower contained entry matching prevLogIndex and
                prevLogTerm


Relevant items from RequestVote RPCs section in Figure 2 of Raft paper:

Invoked by candidates to gather votes (§5.2).

Arguments:
term            candidate’s term
candidateId     candidate requesting vote
lastLogIndex    index of candidate’s last log entry (§5.4)
lastLogTerm     term of candidate’s last log entry (§5.4)

Results:
term            currentTerm, for candidate to update itself
voteGranted     true means candidate received vote
"""

from typing import List
import dataclasses
import enum

import rafthelpers
import raftlog
import raftrole


class MessageType(enum.Enum):
    CLIENT_LOG_APPEND = "CLIENT_LOG_APPEND"
    UPDATE_FOLLOWERS = "UPDATE_FOLLOWERS"
    APPEND_REQUEST = "APPEND_REQUEST"
    APPEND_RESPONSE = "APPEND_RESPONSE"
    RUN_ELECTION = "RUN_ELECTION"
    VOTE_REQUEST = "VOTE_REQUEST"
    VOTE_RESPONSE = "VOTE_RESPONSE"
    ROLE_CHANGE = "ROLE_CHANGE"
    TEXT = "TEXT"


@dataclasses.dataclass
class Message:
    source: int
    target: int


@dataclasses.dataclass
class Text(Message):
    text: str


@dataclasses.dataclass
class ClientLogAppend(Message):
    item: str


@dataclasses.dataclass
class UpdateFollowers(Message):
    followers: List[int]


@dataclasses.dataclass
class AppendEntryRequest(Message):
    current_term: int
    previous_index: int
    previous_term: int
    entries: List[raftlog.LogEntry]
    commit_index: int


@dataclasses.dataclass
class AppendEntryResponse(Message):
    current_term: int
    success: bool
    entries_length: int


@dataclasses.dataclass
class RunElection(Message):
    followers: List[int]


@dataclasses.dataclass
class RequestVoteRequest(Message):
    current_term: int
    last_log_index: int
    last_log_term: int


@dataclasses.dataclass
class RequestVoteResponse(Message):
    success: bool
    current_term: int


@dataclasses.dataclass
class RoleChange(Message):
    from_role: raftrole.Role
    to_role: raftrole.Role


def encode_message(message: Message) -> str:
    attributes = vars(message).copy()

    match message:
        case ClientLogAppend():
            attributes["message_type"] = MessageType.CLIENT_LOG_APPEND.value

        case UpdateFollowers():
            attributes["message_type"] = MessageType.UPDATE_FOLLOWERS.value

        case AppendEntryRequest():
            entries = []

            for entry in message.entries:
                entries.append(vars(entry))

            attributes["message_type"] = MessageType.APPEND_REQUEST.value
            attributes["entries"] = entries

        case AppendEntryResponse():
            attributes["message_type"] = MessageType.APPEND_RESPONSE.value
            attributes["success"] = int(attributes["success"])

        case RunElection():
            attributes["message_type"] = MessageType.RUN_ELECTION.value

        case RequestVoteRequest():
            attributes["message_type"] = MessageType.VOTE_REQUEST.value

        case RequestVoteResponse():
            attributes["message_type"] = MessageType.VOTE_RESPONSE.value
            attributes["success"] = int(attributes["success"])

        case RoleChange():
            attributes["message_type"] = MessageType.ROLE_CHANGE.value
            attributes["from_role"] = attributes["from_role"].value
            attributes["to_role"] = attributes["to_role"].value

        case Text():
            attributes["message_type"] = MessageType.TEXT.value

        case _:
            raise Exception(
                f"Exhaustive switch error in encoding message with attributes {attributes}."
            )

    return rafthelpers.encode_item(attributes)


def decode_message(string: str) -> Message:
    attributes = rafthelpers.decode_item(string)

    message_type = MessageType(attributes["message_type"])
    del attributes["message_type"]

    match message_type:
        case MessageType.CLIENT_LOG_APPEND:
            return ClientLogAppend(**attributes)

        case MessageType.UPDATE_FOLLOWERS:
            return UpdateFollowers(**attributes)

        case MessageType.APPEND_REQUEST:
            entries = []

            for entry in attributes["entries"]:
                entries.append(raftlog.LogEntry(**entry))

            attributes["entries"] = entries
            return AppendEntryRequest(**attributes)

        case MessageType.APPEND_RESPONSE:
            attributes["success"] = bool(attributes["success"])
            return AppendEntryResponse(**attributes)

        case MessageType.RUN_ELECTION:
            return RunElection(**attributes)

        case MessageType.VOTE_REQUEST:
            return RequestVoteRequest(**attributes)

        case MessageType.VOTE_RESPONSE:
            attributes["success"] = bool(attributes["success"])
            return RequestVoteResponse(**attributes)

        case MessageType.ROLE_CHANGE:
            attributes["from_role"] = raftrole.Role(attributes["from_role"])
            attributes["to_role"] = raftrole.Role(attributes["to_role"])
            return RoleChange(**attributes)

        case MessageType.TEXT:
            return Text(**attributes)

        case _:
            raise Exception(
                f"Exhaustive switch error in decoding message with attributes {attributes}."
            )
