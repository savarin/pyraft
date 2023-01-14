from typing import List
import dataclasses


@dataclasses.dataclass
class LogEntry:
    term: int
    item: str


def append_entries(
    log: List[LogEntry],
    previous_index: int,
    previous_term: int,
    entries: List[LogEntry],
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
    entries_index = 0

    # Check index rewrite does not create gaps. If it does, return False.
    if previous_index > len(log):
        return False

    while True:
        # When no more entries to add, break out of loop.
        if len(entries) == entries_index:
            break

        entry = entries[entries_index]

        # Check term number of previous entry matches current term.
        if len(log) > 0 and log[previous_index - 1].term != previous_term:
            return False

        # Replace entry if prev_index refers to location with existing entry.
        if previous_index < len(log):
            # Check value is different.
            if log[previous_index].item != entry.item:
                log[previous_index] = entry

        # Otherwise append to log.
        elif previous_index == len(log):
            log.append(entry)

        else:
            raise Exception("Invalid index error.")

        entries_index += 1

    return True
