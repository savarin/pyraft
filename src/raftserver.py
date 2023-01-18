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


def encode_message(message):
    match message:
        case AppendEntryRequest():
            result = vars(message).copy()
            entries = []

            for entry in message.entries:
                entries.append(vars(entry))

            result["entries"] = entries
            return rafthelpers.encode_item(result)

        case AppendEntryResponse():
            return rafthelpers.encode_item(message)


def decode_message(string):
    attributes = rafthelpers.decode_item(string)

    if len(attributes) == 5:
        entries = []

        for entry in attributes["entries"]:
            entries.append(raftlog.LogEntry(**entry))

        attributes["entries"] = entries
        return AppendEntryRequest(**attributes)

    elif len(attributes) == 4:
        return AppendEntryResponse(**attributes)


if __name__ == "__main__":
    message = AppendEntryRequest(
        1, 2, 3, 4, [raftlog.LogEntry(5, "a"), raftlog.LogEntry(6, "b")]
    )
    string = encode_message(message)
    breakpoint()
