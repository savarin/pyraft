from typing import Dict, List
import dataclasses

import rafthelpers
import raftlog


@dataclasses.dataclass
class Message:
    source: int
    target: int


@dataclasses.dataclass
class Text(Message):
    text: str


@dataclasses.dataclass
class AppendEntryRequest(Message):
    previous_index: int
    previous_term: int
    entries: List[raftlog.LogEntry]


@dataclasses.dataclass
class AppendEntryResponse(Message):
    success: bool
    properties: Dict[str, int]


@dataclasses.dataclass
class UpdateFollowers(Message):
    followers: List[int]


def encode_message(message: Message) -> str:
    attributes = vars(message).copy()

    match message:
        case Text():
            pass

        case AppendEntryRequest():
            entries = []

            for entry in message.entries:
                entries.append(vars(entry))

            attributes["entries"] = entries

        case AppendEntryResponse():
            attributes["success"] = int(attributes["success"])

        case UpdateFollowers():
            pass

    return rafthelpers.encode_item(attributes)


def decode_message(string: str) -> Message:
    # TODO: Create enum for message type.
    attributes = rafthelpers.decode_item(string)

    if len(attributes) == 5:
        entries = []

        for entry in attributes["entries"]:
            entries.append(raftlog.LogEntry(**entry))

        attributes["entries"] = entries
        return AppendEntryRequest(**attributes)

    elif len(attributes) == 4:
        attributes["success"] = bool(attributes["success"])
        return AppendEntryResponse(**attributes)

    elif len(attributes) == 3 and "followers" in attributes:
        return UpdateFollowers(**attributes)

    elif len(attributes) == 3 and "text" in attributes:
        return Text(**attributes)

    raise Exception(
        f"Exhaustive switch error in decoding message with attributes {attributes}."
    )
