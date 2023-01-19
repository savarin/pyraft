import dataclasses
import os
import sys
import threading

import raftmessage
import raftnode
import raftstate


@dataclasses.dataclass
class RaftServer:
    identifier: int

    def __post_init__(self):
        self.state = raftstate.RaftState()
        self.node = raftnode.RaftNode(self.identifier)

    def send(self, messages):
        for message in messages:
            self.node.send(
                message.target, raftmessage.encode_message(message).encode("ascii")
            )

    def respond(self):
        while True:
            payload = self.node.receive()

            try:
                request = raftmessage.decode_message(payload)
                print(f"\n{request.source} > {request.target} {payload}", end="")

                if not isinstance(request, raftmessage.Text):
                    print(f"\n{request.target} > ", end="")

                response = self.state.handle_message(request)

                if response is not None:
                    self.send(response)

            except Exception as e:
                print(f"Exception: {e}")

    def instruct(self):
        while True:
            prompt = input(f"{self.identifier} > ")

            if not prompt:
                return None

            elif not prompt[0].isdigit():
                try:
                    exec(f"print({prompt})")

                except Exception as e:
                    print(f"Exception: {e}")

                continue

            target, command = int(prompt[0]), prompt[2:]

            if command == "replicate":
                messages = self.state.handle_message(
                    raftmessage.UpdateFollowers(0, 0, [1])
                )

            else:
                messages = [raftmessage.Text(self.identifier, target, command)]

            self.send(messages)

    def run(self):
        self.node.start()
        threading.Thread(target=self.respond, args=()).start()

        if self.identifier == 0:
            self.state.handle_client_log_append("a")
            self.state.handle_client_log_append("b")

        self.instruct()

        print("end.")
        os._exit(0)


if __name__ == "__main__":
    server = RaftServer(int(sys.argv[1]))
    server.run()
