import socket
import queue


class KVApplication:
    def __init__(self):
        self.data = {}

    def has(self, key):
        return key in self.data

    def get(self, key):
        return self.data.get(key, None)

    def set(self, key, value):
        self.data[key] = value

    def delete(self, key):
        if key in self.data:
            del self.data[key]


def initialize_socket():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, True)

    # TODO: Create config.
    sock.bind(("localhost", 8000))
    sock.listen()
    return sock


class KVServer:
    def __init__(self):
        self.app = KVApplication()
        self.socket = initialize_socket()
        self.queue = queue.Queue()
        self.clients = {}

    def put(self, identifier, message):
        self.queue.put((identifier, message))

    def get(self):
        return self.queue.get()

    def receive(self, client):
        return client.recv(32).decode("ascii")

    def send(self, client, message):
        client.sendall(message.encode("ascii"))

    def handle(self, request):
        command, *arguments = request.split(" ")

        if command == "set":
            assert len(arguments) == 2
            self.app.set(arguments[0], arguments[1])
            return None

        elif command == "get":
            assert len(arguments) == 1
            return self.app.get(arguments[0])

        elif command == "delete":
            assert len(arguments) == 1
            self.app.delete(arguments[0])
            return None

    def register(self, client):
        identifier = len(self.clients)
        self.clients[identifier] = client

        return identifier

    def produce(self):
        while True:
            client, address = self.socket.accept()
            print("connection from:", address)

            identifier = self.register(client)

            try:
                while True:
                    # TODO: Use bencode for request and response.
                    request = self.receive(client)
                    self.put(identifier, request)

                    self.consume()

            except IOError:
                client.close()

    def consume(self):
        identifier, request = self.get()
        response = self.handle(request)
        print(request, response or "None")

        self.send(self.clients[identifier], response or "ok")


if __name__ == "__main__":
    server = KVServer()
    server.produce()
