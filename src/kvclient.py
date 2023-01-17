from socket import socket, AF_INET, SOCK_STREAM


class KVClient:
    def __init__(self):
        self.sock = socket(AF_INET, SOCK_STREAM)
        self.sock.connect(("localhost", 8000))

    def run(self):
        while True:
            message = input("send > ")

            if len(message) == 0 or message == "exit":
                break

            self.sock.sendall(message.encode("ascii"))

            response = self.sock.recv(32)
            print("received >", response.decode("ascii"))

        self.sock.close()


if __name__ == "__main__":
    client = KVClient()
    client.run()
