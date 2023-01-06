from socket import socket, AF_INET, SO_REUSEADDR, SOCK_STREAM, SOL_SOCKET


class KVApplication:
    def __init__(self):
        self.data = {}

    def get(key):
        return data.get(key)

    def set(key, value):
        data[key] = value

    def delete(key):
        if key in data:
            del data[key]


class KVServer:
    def __init__(self):
        self.sock = socket(AF_INET, SOCK_STREAM)
        self.sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, True)
        self.sock.bind(("localhost", 8000))
        self.sock.listen()

    def run(self):
        while True:
            client, address = self.sock.accept()
            print("connection from:", address)

            try:
                while True:
                    request = client.recv(10)

                    if request != b"":
                        print("request", request.decode("ascii"))
                        client.sendall(request)

            except IOError:
                client.close()


if __name__ == "__main__":
    server = KVServer()
    server.run()
