import dataclasses
import os
import sys
import threading

import rafthelpers
import raftmessage
import raftnode
import raftstate


@dataclasses.dataclass
class RaftServer:
    identifier: int

    def __post_init__(self):
        self.state = raftstate.RaftState()
        self.node = raftnode.RaftNode(self.identifier)

    def pprint(self, string):
        print(f"{string}\n{self.identifier} > ", end="")

    def receive(self):
        while True:
            string = self.node.receive()
            self.pprint(f"\n{self.identifier}: {string}")

            try:
                message = raftmessage.decode_message(string)
                print(
                    self.state.handle_append_entries(
                        message.previous_index,
                        message.previous_term,
                        message.entries,
                    )
                )
                self.pprint(self.state.log)

            except rafthelpers.DecodeError:
                pass

            except Exception as e:
                self.pprint(e)

    def instruct(self):
        while True:
            prompt = input(f"{self.identifier} > ")

            if not prompt:
                return None

            elif not prompt[0].isdigit():
                exec(f"print({prompt})")
                continue

            target, command = int(prompt[0]), prompt[2:]

            if command == "replicate":

                def callback(previous_index, previous_term, entries):
                    message = raftmessage.AppendEntryRequest(
                        self.identifier,
                        int(target),
                        previous_index,
                        previous_term,
                        entries,
                    )
                    self.node.send(
                        target, raftmessage.encode_message(message).encode("ascii")
                    )

                self.state.handle_leader_heartbeat(callback)

            else:
                self.node.send(target, command.encode("ascii"))

    def run(self):
        self.node.start()
        threading.Thread(target=self.receive, args=()).start()

        if self.identifier == 0:
            self.state.handle_client_log_append("a")
            self.state.handle_client_log_append("b")

        self.instruct()

        print("end.")
        os._exit(0)


if __name__ == "__main__":
    server = RaftServer(int(sys.argv[1]))
    server.run()
