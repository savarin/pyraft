from typing import List
import dataclasses
import enum
import json
import os
import sys
import threading

import raftlog
import raftnode


class StateEnum(enum.Enum):
    LEADER = "LEADER"
    FOLLOWER = "FOLLOWER"


@dataclasses.dataclass
class RaftState:
    def __post_init__(self) -> None:
        self.log: List[raftlog.LogEntry] = []
        self.current_term: int = 1
        self.current_state: StateEnum = StateEnum.FOLLOWER
        self.next_index: int = 0

    def change_state(self, state_enum: StateEnum):
        self.current_state = state_enum

    def create_append_entries_arguments(self):
        if self.next_index == -1:
            raise Exception("Invalid follower state.")

        previous_index = self.next_index - 1
        previous_term = self.log[previous_index].term
        return previous_index, previous_term, self.log[self.next_index :]

    def handle_append_entries(
        self,
        previous_index: int,
        previous_term: int,
        entries: List[raftlog.LogEntry],
    ):
        # Update to the log (received by a follower).

        pre_length = len(self.log)
        response = raftlog.append_entries(
            self.log, previous_index, previous_term, entries
        )

        properties = {
            "pre_length": pre_length,
            "post_length": len(self.log),
            "entries_length": len(entries),
        }

        return response, properties

    def handle_client_log_append(self, item: str):
        # Client adds a log entry (received by leader).
        self.log.append(raftlog.LogEntry(self.current_term, item))

    def handle_append_entries_response(self, response, properties, callback):
        # Follower response (received by leader).
        if response:
            self.next_index += properties["entries_length"]
            return True, properties

        self.next_index -= 1
        arguments = self.create_append_entries_arguments()
        return callback(*arguments)

    def handle_leader_heartbeat(self, callback):
        # Leader heartbeat. Send AppendEntries to all followers.
        arguments = self.create_append_entries_arguments()
        return callback(*arguments)


def run(identifier):
    state = RaftState()
    node = raftnode.RaftNode(identifier)
    node.start()

    def decode(payload):
        items = payload["entries"].split(",")
        entries = []

        for i in range(0, len(items), 2):
            entries.append(raftlog.LogEntry(int(items[i]), items[i + 1]))

        payload["previous_index"] = int(payload["previous_index"])
        payload["previous_term"] = int(payload["previous_term"])
        payload["entries"] = entries
        return payload

    def receive():
        while True:
            message = node.receive()
            print(f"\n{identifier}: receive: {message}\n{identifier} > ", end="")

            try:
                payload = json.loads(message)
                payload = decode(payload)
                state.handle_append_entries(
                    payload["previous_index"],
                    payload["previous_term"],
                    payload["entries"],
                )

            except json.JSONDecodeError:
                pass

    threading.Thread(target=receive, args=()).start()

    if identifier == 0:
        state.handle_client_log_append("a")
        state.handle_client_log_append("b")

    while True:
        prompt = input(f"{identifier} > ")

        if not prompt:
            break

        target, message = prompt.split(maxsplit=1)

        if message == "replicate":

            def callback(previous_index, previous_term, entries):
                payload = {
                    "previous_index": previous_index,
                    "previous_term": previous_term,
                    "entries": ",".join([str(entry) for entry in entries]),
                }

                message = json.dumps(payload, separators=(",", ":"))
                print(message)

                node.send(int(target), message.encode("ascii"))

            state.handle_leader_heartbeat(callback)

        elif message == "expose":
            print(",".join([str(entry) for entry in state.log]))

        else:
            node.send(int(target), message.encode("ascii"))

    print("end.")
    os._exit(0)


if __name__ == "__main__":
    run(int(sys.argv[1]))
