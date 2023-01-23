from typing import Dict, List
import dataclasses
import enum

import rafthelpers
import raftlog


class MessageType(enum.Enum):
    TEXT = "TEXT"
    CLIENT_LOG_APPEND = "CLIENT_LOG_APPEND"
    APPEND_REQUEST = "APPEND_REQUEST"
    APPEND_RESPONSE = "APPEND_RESPONSE"
    UPDATE_FOLLOWERS = "UPDATE_FOLLOWERS"
    VOTE_REQUEST = "VOTE_REQUEST"
    VOTE_RESPONSE = "VOTE_RESPONSE"


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
class AppendEntryRequest(Message):
    previous_index: int
    previous_term: int
    entries: List[raftlog.LogEntry]
    commit_index: int


@dataclasses.dataclass
class AppendEntryResponse(Message):
    success: bool
    entries_length: int
    properties: Dict[str, int]


@dataclasses.dataclass
class UpdateFollowers(Message):
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


def encode_message(message: Message) -> str:
    attributes = vars(message).copy()

    match message:
        case Text():
            attributes["message_type"] = MessageType.TEXT.value

        case ClientLogAppend():
            attributes["message_type"] = MessageType.CLIENT_LOG_APPEND.value

        case AppendEntryRequest():
            entries = []

            for entry in message.entries:
                entries.append(vars(entry))

            attributes["message_type"] = MessageType.APPEND_REQUEST.value
            attributes["entries"] = entries

        case AppendEntryResponse():
            attributes["message_type"] = MessageType.APPEND_RESPONSE.value
            attributes["success"] = int(attributes["success"])

        case UpdateFollowers():
            attributes["message_type"] = MessageType.UPDATE_FOLLOWERS.value

        case RequestVoteRequest():
            attributes["message_type"] = MessageType.VOTE_REQUEST.value

        case RequestVoteResponse():
            attributes["message_type"] = MessageType.VOTE_RESPONSE.value

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

        case MessageType.APPEND_REQUEST:
            entries = []

            for entry in attributes["entries"]:
                entries.append(raftlog.LogEntry(**entry))

            attributes["entries"] = entries
            return AppendEntryRequest(**attributes)

        case MessageType.APPEND_RESPONSE:
            attributes["success"] = bool(attributes["success"])
            return AppendEntryResponse(**attributes)

        case MessageType.UPDATE_FOLLOWERS:
            return UpdateFollowers(**attributes)

        case MessageType.VOTE_REQUEST:
            return RequestVoteRequest(**attributes)

        case MessageType.VOTE_RESPONSE:
            return RequestVoteResponse(**attributes)

        case MessageType.TEXT:
            return Text(**attributes)

        case _:
            raise Exception(
                f"Exhaustive switch error in decoding message with attributes {attributes}."
            )
