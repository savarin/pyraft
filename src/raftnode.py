"""
Network runtime that operates in the background, allowing servers to send and
receive messages from each other. Minor changes to Dave's code. Wraps up a
combination of threads, queues and sockets.
"""
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
    """
    Represents environment that sends/receives raw messages in the cluster. No
    logic to parse messages, mainly abstraction to get messages to go to the
    intended target.

    To send a message to any other node in the cluster, use `send`. This
    operation is non-blocking and returns immediately. There is no guarantee of
    message delivery.

    > node.send(1, b"hello")

    To receive a single message, use `receive`. This is a blocking operation
    that waits for a message to arrive from anywhere.

    > message = node.receive()
    """

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
        """
        Run in background thread to listen for incoming connections and places
        messages in incoming queue.
        """
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
        """
        Run in background thread to deliver outgoing messages to other nodes.
        The delivery is best-effort attempt, in which the message is discarded
        if the remote server is not operational.
        """
        sock = None
        address = ADDRESS_BY_IDENTIFIER[identifier]

        try:
            while True:
                message = self.outgoing[identifier].get()
                sock = self._deliver(sock, address, message)

        finally:
            # Defensive coding to avoid partial system failure.
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

    # Ensures all threads are handled.
    print("end.")
    os._exit(0)


if __name__ == "__main__":
    run(int(sys.argv[1]))
