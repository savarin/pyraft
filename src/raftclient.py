"""
Client to enable sending of append entries and expose state instructions to the
Raft server.
"""

from typing import List
import dataclasses
import sys

import raftconfig
import raftmessage
import raftnode


@dataclasses.dataclass
class RaftClient:
    identifier: int

    def __post_init__(self) -> None:
        self.node: raftnode.RaftNode = raftnode.RaftNode(self.identifier)

    def send(self, messages: List[raftmessage.Message]) -> None:
        for message in messages:
            self.node.send(message.target, raftmessage.encode_message(message))

    def instruct(self) -> None:
        while True:
            prompt = input(f"{self.identifier} > ")

            if not prompt:
                return None

            if prompt == "self":
                self.send(
                    [
                        raftmessage.Text(self.identifier, target, prompt)
                        for target in raftconfig.ADDRESS_BY_IDENTIFIER
                    ]
                )
                continue

            target, command = int(prompt[0]), prompt[2:]
            messages: List[raftmessage.Message] = []

            if command.startswith("append"):
                for item in command.replace("append ", "").split():
                    messages.append(
                        raftmessage.ClientLogAppend(self.identifier, target, item)
                    )

            else:
                messages.append(raftmessage.Text(self.identifier, target, command))

            self.send(messages)

    def run(self):
        self.node.start()
        self.instruct()

        print(self.color() + "end.")


if __name__ == "__main__":
    client = RaftClient(int(sys.argv[1]))
    client.run()
