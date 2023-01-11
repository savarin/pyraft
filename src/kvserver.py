import socket
import queue
import threading


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


class KVHandler:
    def __init__(self):
        self.app = KVApplication()

    def handle(self, request):
        command, *arguments = request.split(" ")

        # TODO: Consider using return codes.
        if command == "set" and len(arguments) == 2:
            self.app.set(arguments[0], arguments[1])
            return True, None

        elif command == "get" and len(arguments) == 1:
            return True, self.app.get(arguments[0])

        elif command == "delete" and len(arguments) == 1:
            self.app.delete(arguments[0])
            return True, None

        return False, None


def initialize_socket(address):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, True)

    # TODO: Create config.
    sock.bind(address)
    sock.listen()
    return sock


class KVServer:
    def __init__(self):
        self.socket = initialize_socket(("localhost", 8000))
        self.queue = queue.Queue()
        self.clients = {}

    def put(self, message):
        self.queue.put(message)

    def get(self):
        return self.queue.get()

    def receive(self, identifier):
        return self.clients[identifier].recv(32).decode("ascii")

    def send(self, identifier, message):
        self.clients[identifier].sendall(message.encode("ascii"))

    def register(self, client):
        identifier = len(self.clients)
        self.clients[identifier] = client

        return identifier

    def direct(self, identifier):
        try:
            while True:
                # TODO: Use bencode for request and response.
                request = self.receive(identifier)
                self.put((identifier, request))

        except IOError:
            self.clients[identifier].close()

    def listen(self):
        while True:
            client, address = self.socket.accept()
            identifier = self.register(client)

            print("connection from:", address)
            threading.Thread(target=self.direct, args=(identifier,)).start()


def run():
    handler = KVHandler()
    server = KVServer()

    threading.Thread(target=server.listen, args=()).start()

    while True:
        identifier, request = server.get()
        result, value = handler.handle(request)

        if result:
            response = value or "ok"
        else:
            response = "invalid request"

        print(request, response)
        server.send(identifier, response)


if __name__ == "__main__":
    run()
