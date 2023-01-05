from socket import socket, AF_INET, SOCK_STREAM


class KVClient:
    def __init__(self):
        self.sock = socket(AF_INET, SOCK_STREAM)
        self.sock.connect(("localhost", 12345))


if __name__ == "__main__":
    client = KVClient()
