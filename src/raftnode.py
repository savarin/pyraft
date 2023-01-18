import dataclasses
import os
import queue
import socket
import sys
import threading


ADDRESS_BY_IDENTIFIER = {
    0: ("localhost", 8000),
    1: ("localhost", 9000),
}


def initialize_socket(identifier):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, True)

    sock.bind(ADDRESS_BY_IDENTIFIER[identifier])
    sock.listen()
    return sock


@dataclasses.dataclass
class RaftNode:
    identifier: int

    def __post_init__(self):
        self.socket = initialize_socket(self.identifier)
        self.incoming = queue.Queue()
        self.outgoing = {i: queue.Queue() for i in ADDRESS_BY_IDENTIFIER}

    def send(self, identifier, message):
        self.outgoing[identifier].put(message)

    def receive(self):
        return self.incoming.get()

    def _listen(self, client):
        try:
            while True:
                length = int.from_bytes(client.recv(1), byteorder="big")

                if length == 0:
                    raise IOError

                message = client.recv(length).decode("ascii")
                self.incoming.put(message)

        except IOError:
            client.close()

    def listen(self):
        while True:
            client, address = self.socket.accept()
            threading.Thread(target=self._listen, args=(client,)).start()

    def _deliver(self, sock, address, message):
        try:
            if sock is None:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect(address)

            sock.sendall(len(message).to_bytes(1, byteorder="big"))
            sock.sendall(message)

        except Exception as e:
            print(e)
            sock = None

        return sock

    def deliver(self, identifier):
        sock = None
        address = ADDRESS_BY_IDENTIFIER[identifier]

        try:
            while True:
                message = self.outgoing[identifier].get()
                sock = self._deliver(sock, address, message)

        finally:
            print("panic!")
            os._exit(1)

    def start(self):
        threading.Thread(target=self.listen, args=()).start()

        for i in ADDRESS_BY_IDENTIFIER:
            threading.Thread(target=self.deliver, args=(i,)).start()

        print("start.")


def run(identifier):
    node = RaftNode(identifier)
    node.start()

    def receive():
        while True:
            message = node.receive()
            print(f"\n{identifier}: receive: {message}\n{identifier} > ", end="")

    threading.Thread(target=receive, args=()).start()

    while True:
        prompt = input(f"{identifier} > ")

        if not prompt:
            break

        target, message = prompt.split(maxsplit=1)
        node.send(int(target), message.encode("ascii"))

    print("end.")
    os._exit(0)


if __name__ == "__main__":
    run(int(sys.argv[1]))
