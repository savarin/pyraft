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

    def run(self):
        self.node.start()

        def receive():
            while True:
                string = self.node.receive()
                print(
                    f"\n{self.identifier}: receive {string}\n{self.identifier} > ",
                    end="",
                )

                try:
                    message = raftmessage.decode_message(string)
                    print(
                        self.state.handle_append_entries(
                            message.previous_index,
                            message.previous_term,
                            message.entries,
                        )
                    )
                    print(self.state.log)

                except rafthelpers.DecodeError:
                    print(string)

                except Exception as e:
                    print(e)

        threading.Thread(target=receive, args=()).start()

        if self.identifier == 0:
            self.state.handle_client_log_append("a")
            self.state.handle_client_log_append("b")

        while True:
            prompt = input(f"{self.identifier} > ")

            if not prompt:
                break

            target, command = prompt.split(maxsplit=1)

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
                        int(target), raftmessage.encode_message(message).encode("ascii")
                    )

                self.state.handle_leader_heartbeat(callback)

            else:
                self.node.send(int(target), command.encode("ascii"))

        print("end.")
        os._exit(0)


if __name__ == "__main__":
    server = RaftServer(int(sys.argv[1]))
    server.run()
