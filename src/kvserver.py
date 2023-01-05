from socket import socket, AF_INET, SOCK_STREAM


class KVServer:
    def __init__(self):
        self.sock = socket(AF_INET, SOCK_STREAM)
        self.sock.bind(("localhost", 12345))
        self.sock.listen()

    def run(self):
        while True:
            client, address = self.sock.accept()
            print("Connection from", address)


if __name__ == "__main__":
    server = KVServer()
    server.run()
