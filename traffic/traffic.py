# traffic.py
#
# Implement traffic light control software.
#
# Challenge:  Make something that can be tested/debugged.
from typing import Dict, Tuple, Optional
import dataclasses
import enum
import queue
import socket
import time
import threading

colors_by_state: Dict[int, Tuple[str, str]] = {
    0: ("G", "R"),
    1: ("G", "R"),
    2: ("Y", "R"),
    3: ("R", "G"),
    4: ("R", "G"),
    5: ("R", "Y"),
}

sleep_time_by_state: Dict[int, int] = {
    0: 15,
    1: 15,
    2: 5,
    3: 15,
    4: 45,
    5: 5,
}

port_by_direction: Dict[str, int] = {
    "localhost": 7000,
    "EW": 8000,
    "NS": 9000,
}


class Response(enum.Enum):
    UPDATE_STATE = "UPDATE_STATE"
    UPDATE_BUTTON = "UPDATE_BUTTON"
    IGNORE_BUTTON = "IGNORE_BUTTON"
    IGNORE_EVENT = "IGNORE_EVENT"


@dataclasses.dataclass
class TrafficLight:
    """
    state 0 - 15 seconds, NS button only changes flag
        EW G
        NS R
    state 1 - 15 seconds, NS button switches state
        EW G
        NS R
    state 2 - 5 seconds
        EW Y
        NS R
    state 3 - 15 seconds, EW button only changes flag
        EW R
        NS G
    state 4 - 45 seconds, EW button switches state
        EW R
        NS G
    state 5 - 5 seconds
        EW R
        NS Y

    v1 - simply change lights according to timer, with 4 states as the button is
    not taken into consideration.

    v2 - how to deal with timer set for 30 seconds and button pressed? set up
    incrementing counter and simply ignore ticks from a previous counter.

    v3 - implement 15 second check by expanding to 6 states by breaking up the
    red-green combinations into two based on the effect of the button.

    v4 (current) - how to enable better testing? carve out socket-related
    functionality into separate class. handle based on output.

    v5 - write tests for queue functionality by patching network and simulating
    enqueued messages. set up enums for states, ports and actions. combine
    colors and sleep time dictionaries.
    """

    def __post_init__(self) -> None:
        self._counter: int = -1
        self._button: bool = False

    def get_counter(self) -> int:
        return self._counter

    def get_button(self) -> bool:
        return self._button

    def get_state(self) -> int:
        return self._counter % 6

    def increment_counter(self) -> None:
        self._counter += 1

    def toggle_button(self) -> None:
        self._button = True

    def reset_button(self) -> None:
        self._button = False

    def handle_clock_tick(self) -> Tuple[Response, Optional[int]]:
        # If button pressed in first 15 seconds, then change state so that the
        # second part of red-green is skipped to yellow-green.
        if self.get_button() and self.get_state() in {0, 3}:
            self.increment_counter()

        self.increment_counter()
        self.reset_button()
        return Response.UPDATE_STATE, self.get_state()

    def handle_north_south_button(self) -> Tuple[Response, Optional[int]]:
        # Set button to true only when in state 0.
        if self.get_state() == 0:
            self.toggle_button()
            return Response.UPDATE_BUTTON, None

        # Change state only when in state 1.
        elif self.get_state() == 1:
            self.increment_counter()
            self.reset_button()
            return Response.UPDATE_STATE, self.get_state()

        return Response.IGNORE_BUTTON, None

    def handle_east_west_button(self) -> Tuple[Response, Optional[int]]:
        # Set button to true only when in state 3.
        if self.get_state() == 3:
            self.toggle_button()
            return Response.UPDATE_BUTTON, None

        # Change state only when in state 4.
        elif self.get_state() == 4:
            self.increment_counter()
            self.reset_button()
            return Response.UPDATE_STATE, self.get_state()

        return Response.IGNORE_BUTTON, None

    def handle_event(
        self, event: str, event_counter: int
    ) -> Tuple[Response, Optional[int]]:
        # Ignore timer event if have moved on from timer.
        if event_counter < self.get_counter():
            return Response.IGNORE_EVENT, None

        match event:
            case "tick":
                return self.handle_clock_tick()

            case "NS":
                return self.handle_north_south_button()

            case "EW":
                return self.handle_east_west_button()

            case _:
                raise Exception("Exhaustive switch error.")


@dataclasses.dataclass
class Server:
    def __post_init__(self) -> None:
        self.socket: socket.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind(("localhost", port_by_direction["localhost"]))

        self.queue: queue.Queue = queue.Queue()

    def send_message(self, message: str, direction: str) -> None:
        address = ("localhost", port_by_direction[direction])
        self.socket.sendto(message.encode("ascii"), address)

    def receive_message(self) -> str:
        message, _ = self.socket.recvfrom(8192)
        return message.decode("ascii")

    def put_message(self, message: str, counter: int) -> None:
        self.queue.put((message, counter))

    def get_message(self) -> Tuple[str, int]:
        return self.queue.get()


def run() -> None:
    traffic_light = TrafficLight()
    server = Server()

    def listen() -> None:
        while True:
            message = server.receive_message()
            counter = traffic_light.get_counter()

            print(f"enqueue {message} with counter {counter}")
            server.put_message(message, counter)

    def sleep(interval: int) -> None:
        time.sleep(interval)
        counter = traffic_light.get_counter()

        print(f"enqueue tick with counter {counter}")
        server.put_message("tick", counter)

    def start() -> None:
        threading.Thread(target=listen, args=()).start()
        threading.Thread(target=sleep, args=(1,)).start()

    def update(state) -> int:
        east_west_color, north_south_color = colors_by_state[state]
        sleep_time = sleep_time_by_state[state]

        server.send_message(east_west_color, "EW")
        server.send_message(north_south_color, "NS")

        threading.Thread(target=sleep, args=(sleep_time,)).start()
        return sleep_time

    start()

    # TODO: Create test with server and traffic light.
    while True:
        event, event_counter = server.get_message()
        response, state = traffic_light.handle_event(event, event_counter)

        match response:
            case Response.IGNORE_EVENT:
                print("ignore stale event.")

            case Response.IGNORE_BUTTON:
                print(f"ignore {event} button.")

            case Response.UPDATE_BUTTON:
                print("button state switched to true.")

            case Response.UPDATE_STATE:
                sleep_time = update(state)
                print(f"switch to {str(state)} for {str(sleep_time)} seconds...")

            case _:
                raise Exception("Exhaustive switch error.")


if __name__ == "__main__":
    run()
