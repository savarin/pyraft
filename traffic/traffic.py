# traffic.py
#
# Implement traffic light control software.
#
# Challenge:  Make something that can be tested/debugged.
import dataclasses
import queue
import socket
import time
import threading


colors_by_state = {
    0: ("G", "R"),
    1: ("Y", "R"),
    2: ("R", "G"),
    3: ("R", "Y"),
}

sleep_time_by_state = {
    0: 30,
    1: 5,
    2: 60,
    3: 5,
}

port_by_direction = {
    "ew": 8000,
    "ns": 9000,
}


@dataclasses.dataclass
class TrafficLight:
    """
    v1
    - simply change lights according to timer.

        state 0 - 30 seconds
            EW G
            NS R
        state 1 - 5 seconds
            EW Y
            NS R
        state 2 - 60 seconds
            EW R
            NS G
        state 3 - 5 seconds
            EW R
            NS Y
        back to state 0

    v2
    - how to deal with timer set for 30 seconds and button pressed? set up
      incrementing counter and simply ignore ticks from a previous counter.
    - ignore 15 second check

    v3
    - implement 15 second check

    v4
    - how to enable better testing? carve out socket-related functionality into
      separate class. handle based on output.

    """

    def __post_init__(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind(("localhost", 7000))

        self.counter = -1

    def get_counter(self):
        return self.counter

    def receive_message(self):
        return self.socket.recvfrom(8192)

    def handle_clock_tick(self):
        self.update()
        return self.counter

    def handle_ns_button(self):
        # Action only when in state 0.
        if self.counter % 4 != 0:
            return None

        self.update()
        return self.counter

    def handle_ew_button(self):
        # Action only when in state 2.
        if self.counter % 4 != 2:
            return None

        self.update()
        return self.counter

    def update(self):
        self.update_counter()
        self.update_lights()

    def update_counter(self):
        self.counter += 1

    def update_lights(self):
        state = self.counter % 4
        ew_color, ns_color = colors_by_state[state]

        self.send_message("ew", ew_color)
        self.send_message("ns", ns_color)

    def send_message(self, direction, message):
        self.socket.sendto(
            message.encode("ascii"), ("localhost", port_by_direction[direction])
        )


def convert_counter(counter):
    previous_state = (counter - 1) % 4
    current_state = counter % 4
    sleep_time = sleep_time_by_state[current_state]
    status_update = f"previous: {previous_state}, current: {current_state} for {sleep_time} seconds..."

    return sleep_time, status_update


if __name__ == "__main__":
    # TODO: Refactor so that event queue handles dispatch, light handles state.
    light = TrafficLight()
    event_queue = queue.Queue()

    def enqueue(message):
        print(f"enqueue {message}, counter {light.get_counter()}")
        event_queue.put((message, light.get_counter()))

    def listen():
        while True:
            message, _ = light.receive_message()
            enqueue(message.decode("ascii"))

    def sleep(interval, message):
        time.sleep(interval)
        enqueue(message)

    def status():
        sleep_time, status_update = convert_counter(light.counter)
        print(status_update)

        threading.Thread(target=sleep, args=(sleep_time, "tick")).start()

    threading.Thread(target=listen, args=()).start()
    threading.Thread(target=sleep, args=(1, "tick")).start()

    while True:
        event, event_counter = event_queue.get()

        # Ignore timer event if have moved on from timer.
        if event_counter < light.get_counter():
            print("ignore stale event.")
            continue

        match event.lower():
            case "tick":
                light_counter = light.handle_clock_tick()

            case "ns":
                light_counter = light.handle_ns_button()

            case "ew":
                light_counter = light.handle_ew_button()

        if light_counter is None:
            print(f"ignore {event} button.")
            continue

        status()
