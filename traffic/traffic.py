# traffic.py
#
# Implement traffic light control software.
#
# Challenge:  Make something that can be tested/debugged.
from typing import Dict, Tuple
import dataclasses
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
    "EW": 8000,
    "NS": 9000,
}


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
    enqueued messages.
    """

    def __post_init__(self) -> None:
        self.counter: int = -1
        self.button: bool = False

    def get_counter(self) -> int:
        return self.counter

    def get_button(self) -> bool:
        return self.button

    def get_state(self) -> int:
        return self.counter % 6

    def handle_clock_tick(self) -> int:
        # If button pressed in first 15 seconds, then change state so that the
        # second part of red-green is skipped to yellow-green.
        if self.button and self.get_state() in {0, 3}:
            self.increment_counter()

        self.increment_counter()
        self.reset_button()
        return self.counter

    def handle_north_south_button(self) -> int:
        # Set button to true only when in state 0.
        if self.get_state() == 0:
            self.button = True
            return -2

        # Change state only when in state 1.
        elif self.get_state() == 1:
            self.increment_counter()
            self.reset_button()
            return self.counter

        return -1

    def handle_east_west_button(self) -> int:
        # Set button to true only when in state 3.
        if self.get_state() == 3:
            self.button = True
            return -2

        # Change state only when in state 4.
        elif self.get_state() == 4:
            self.increment_counter()
            self.reset_button()
            return self.counter

        return -1

    def increment_counter(self) -> None:
        self.counter += 1

    def reset_button(self) -> None:
        self.button = False


@dataclasses.dataclass
class Server:
    def __post_init__(self) -> None:
        self.socket: socket.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind(("localhost", 7000))

        self.queue: queue.Queue = queue.Queue()

    def send_message(self, direction: str, message: str) -> None:
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

    def update() -> None:
        state = traffic_light.get_state()
        east_west_color, north_south_color = colors_by_state[state]
        sleep_time = sleep_time_by_state[state]

        server.send_message("EW", east_west_color)
        server.send_message("NS", north_south_color)

        print(f"switch to {str(state)} for {str(sleep_time)} seconds...")
        threading.Thread(target=sleep, args=(sleep_time,)).start()

    def start() -> None:
        threading.Thread(target=listen, args=()).start()
        threading.Thread(target=sleep, args=(1,)).start()

    start()

    while True:
        event, event_counter = server.get_message()

        # Ignore timer event if have moved on from timer.
        if event_counter < traffic_light.get_counter():
            print("ignore stale event.")
            continue

        match event:
            case "tick":
                light_counter = traffic_light.handle_clock_tick()

            case "NS":
                light_counter = traffic_light.handle_north_south_button()

            case "EW":
                light_counter = traffic_light.handle_east_west_button()

        if light_counter == -1:
            print(f"ignore {event} button.")
            continue

        elif light_counter == -2:
            print(f"button state switched to true only.")
            continue

        update()


if __name__ == "__main__":
    run()
