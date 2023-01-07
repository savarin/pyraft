from socket import socket, AF_INET, SO_REUSEADDR, SOCK_STREAM, SOL_SOCKET


class KVApplication:
    def __init__(self):
        self.data = {}

    def has(self, key):
        return key in self.data

    def get(self, key):
        return self.data.get(key)

    def set(self, key, value):
        self.data[key] = value

    def delete(self, key):
        if key in self.data:
            del self.data[key]


class KVServer:
    def __init__(self):
        self.app = KVApplication()

        # TODO: Carve out in separate method.
        self.sock = socket(AF_INET, SOCK_STREAM)
        self.sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, True)
        # TODO: Create config.
        self.sock.bind(("localhost", 8000))
        self.sock.listen()

    def handle(self, request):
        command, *arguments = request.split(" ")

        # TODO: Check request validity and return codes in separate method.
        if command == "set":
            assert len(arguments) == 2
            self.app.set(arguments[0], arguments[1])
            return True, None

        # TODO: Consider removing existence check to simplify interface.
        elif command == "get":
            assert len(arguments) == 1

            if self.app.has(arguments[0]):
                return True, self.app.get(arguments[0])

            return False, None

        elif command == "delete":
            assert len(arguments) == 1

            if self.app.has(arguments[0]):
                self.app.delete(arguments[0])
                return True, None

            return False, None

    def run(self):
        while True:
            client, address = self.sock.accept()
            print("connection from:", address)

            try:
                while True:
                    # TODO: Log requests and response.
                    request = client.recv(32)

                    if request != b"":
                        # TODO: Use bencode for request and response.
                        is_valid, result = self.handle(request.decode("ascii"))

                        if result is not None:
                            response = result
                        else:
                            response = "ok" if is_valid else "invalid"

                        print(request.decode("ascii"), response)

                        client.sendall(response.encode("ascii"))

            except IOError:
                client.close()


if __name__ == "__main__":
    server = KVServer()
    server.run()
