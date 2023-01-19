from typing import Dict, List
import dataclasses

import rafthelpers
import raftlog


class Message:
    pass


@dataclasses.dataclass
class AppendEntryRequest(Message):
    source: int
    target: int
    previous_index: int
    previous_term: int
    entries: List[raftlog.LogEntry]


@dataclasses.dataclass
class AppendEntryResponse(Message):
    source: int
    target: int
    success: bool
    properties: Dict[str, int]


@dataclasses.dataclass
class UpdateFollowers(Message):
    source: int
    target: int
    followers: List[int]


def encode_message(message):
    attributes = vars(message).copy()

    match message:
        case AppendEntryRequest():
            entries = []

            for entry in message.entries:
                entries.append(vars(entry))

            attributes["entries"] = entries
            return rafthelpers.encode_item(attributes)

        case AppendEntryResponse():
            attributes["success"] = int(attributes["success"])
            return rafthelpers.encode_item(attributes)

        case UpdateFollowers():
            return rafthelpers.encode_item(attributes)


def decode_message(string):
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

    elif len(attributes) == 3:
        return UpdateFollowers(**attributes)


if __name__ == "__main__":
    message = AppendEntryRequest(
        1, 2, 3, 4, [raftlog.LogEntry(5, "a"), raftlog.LogEntry(6, "b")]
    )
    string = encode_message(message)
    breakpoint()
