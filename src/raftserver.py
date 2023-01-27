from typing import List
import dataclasses
import os
import sys
import threading

import raftconfig
import raftmessage
import raftnode
import raftrole
import raftstate


@dataclasses.dataclass
class RaftServer:
    identifier: int

    def __post_init__(self) -> None:
        self.state: raftstate.RaftState = raftstate.RaftState(self.identifier)
        self.node: raftnode.RaftNode = raftnode.RaftNode(self.identifier)

    def send(self, messages: List[raftmessage.Message]) -> None:
        for message in messages:
            self.node.send(message.target, raftmessage.encode_message(message))

    def respond(self) -> None:
        while True:
            payload = self.node.receive()

            try:
                request = raftmessage.decode_message(payload)
                print(
                    f"\n{request.source} > {request.target} {payload}",
                    end="",
                )

                # Text messages received will print out commands and the cursor,
                # here print out cursor separately if non-Text.
                if not isinstance(request, raftmessage.Text):
                    print(f"\n{request.target} > ", end="")

                response, role_change = self.state.handle_message(request)

                if role_change == (raftrole.Role.CANDIDATE, raftrole.Role.LEADER):
                    assert len(response) == 0
                    response += self.state.handle_leader_heartbeat(
                        self.identifier, self.identifier
                    )[0]

                elif role_change == (raftrole.Role.FOLLOWER, raftrole.Role.CANDIDATE):
                    assert len(response) == 0
                    response += self.state.handle_candidate_solicitation(
                        self.identifier, self.identifier
                    )[0]

                self.send(response)

            except Exception as e:
                print(f"Exception: {e}")

    def instruct(self) -> None:
        messages: List[raftmessage.Message] = []

        while True:
            prompt = input(f"{self.identifier} > ")

            if not prompt:
                return None

            try:
                # If identifier not specified, use as shell.
                if not (prompt[0].isdigit() and prompt[1] == " "):
                    exec(f"print({prompt})")
                    continue

                target, command = int(prompt[0]), prompt[2:]

                # If identifier specified but not own identifier, send message to
                # target.
                if target != self.identifier:
                    messages = [raftmessage.Text(self.identifier, target, command)]
                    self.send(messages)
                    continue

                # If identifier specified and is own identifier, act on own server.
                # Here by exposing log.
                if command.startswith("expose"):
                    print(
                        f"+ {str(self.state.commit_index)} {str(self.state.log)}\n",
                        end="",
                    )

                # Append entries to own log.
                elif command.startswith("append"):
                    for item in command.replace("append ", "").split():
                        self.state.handle_message(
                            raftmessage.ClientLogAppend(
                                self.identifier, self.identifier, item
                            )
                        )

                # Send out hearbeats to followers.
                elif command.startswith("update"):
                    followers = list(raftconfig.ADDRESS_BY_IDENTIFIER.keys())
                    followers.remove(self.identifier)

                    messages, change_role = self.state.handle_message(
                        raftmessage.UpdateFollowers(
                            self.identifier, self.identifier, followers
                        )
                    )

                    self.send(messages)

            except Exception as e:
                print(f"Exception: {e}")

    def run(self):
        self.node.start()
        threading.Thread(target=self.respond, args=()).start()

        if self.identifier == 0:
            self.state.role = raftstate.Role.LEADER

        self.instruct()

        print("end.")
        os._exit(0)


if __name__ == "__main__":
    server = RaftServer(int(sys.argv[1]))
    server.run()
