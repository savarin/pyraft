from typing import List
import dataclasses


@dataclasses.dataclass
class LogEntry:
    term: int
    item: str


def append_entries(
    log: List[LogEntry], prev_index: int, prev_term: int, entries: List[LogEntry]
):
    """
    Choose index starting from 0.

    Suppose have 3 elements. Either append starting at prev_index 3 or modify at
    1, 2 or 3, returning True. Can append from index 3, replaces index 0 or 1 or
    2, but return False if try to specify prev_index 4. or more

    [a b c]  d e
     ^ ^ ^   ^ ^
     0 1 2   3 4
    """
    # Check index rewrite does not create gaps. If it does, return False.
    if prev_index > len(log):
        return False

    while True:
        if len(entries) == 0:
            break

        entry = entries.pop(0)

        # Replace entry if prev_index refers to location with existing entry.
        if prev_index < len(log):
            log[prev_index] = entry

        # Otherwise append to log.
        elif prev_index == len(log):
            log.append(entry)

        else:
            raise Exception("Invalid index error.")

        prev_index += 1

    return True
