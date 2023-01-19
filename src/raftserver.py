from typing import List
import dataclasses
import os
import sys
import threading

import raftconfig
import raftmessage
import raftnode
import raftstate


@dataclasses.dataclass
class RaftServer:
    identifier: int

    def __post_init__(self) -> None:
        self.state: raftstate.RaftState = raftstate.RaftState()
        self.node: raftnode.RaftNode = raftnode.RaftNode(self.identifier)

    def send(self, messages: List[raftmessage.Message]) -> None:
        for message in messages:
            self.node.send(
                message.target, raftmessage.encode_message(message).encode("ascii")
            )

    def respond(self) -> None:
        while True:
            payload = self.node.receive()

            try:
                request = raftmessage.decode_message(payload)
                print(f"\n{request.source} > {request.target} {payload}", end="")

                if not isinstance(request, raftmessage.Text):
                    print(f"\n{request.target} > ", end="")

                response = self.state.handle_message(request)
                self.send(response)

            except Exception as e:
                print(f"Exception: {e}")

    def instruct(self) -> None:
        while True:
            prompt = input(f"{self.identifier} > ")

            if not prompt:
                return None

            elif prompt == "test":
                for _ in range(len(self.state.log)):
                    self.state.log.pop()

                self.state.handle_client_log_append("a")
                self.state.handle_client_log_append("b")
                self.state.handle_client_log_append("c")

                followers = list(raftconfig.ADDRESS_BY_IDENTIFIER.keys())
                followers.remove(self.identifier)

                messages = self.state.handle_message(
                    raftmessage.UpdateFollowers(0, 0, followers)
                )
                assert messages is not None

                for follower in followers:
                    messages.append(
                        raftmessage.Text(self.identifier, follower, "expose")
                    )

                self.send(messages)

            elif prompt[0].isdigit() and prompt[1] == " ":
                target, command = int(prompt[0]), prompt[2:]
                messages = [raftmessage.Text(self.identifier, target, command)]
                self.send(messages)

            else:
                try:
                    exec(f"print({prompt})")

                except Exception as e:
                    print(f"Exception: {e}")

    def run(self):
        self.node.start()
        threading.Thread(target=self.respond, args=()).start()

        self.instruct()

        print("end.")
        os._exit(0)


if __name__ == "__main__":
    server = RaftServer(int(sys.argv[1]))
    server.run()
