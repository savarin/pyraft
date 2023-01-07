# traffic.py
#
# Implement traffic light control software.
#
# Challenge:  Make something that can be tested/debugged.
from dataclasses import dataclass
from socket import socket, AF_INET, SOCK_DGRAM
import queue
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


@dataclass
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
        self.socket = socket(AF_INET, SOCK_DGRAM)
        self.counter = -1

    def get_state(self):
        return self.counter

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
    status_update = f"change from {previous_state} to {current_state}, stay for {sleep_time} seconds..."

    return sleep_time, status_update


if __name__ == "__main__":
    light = TrafficLight()
    event_queue = queue.Queue()

    def generate_tick(interval, counter):
        time.sleep(interval)
        event_queue.put(("tick", counter))

    threading.Thread(target=generate_tick, args=(1, -1)).start()

    def update(counter):
        sleep_time, status_update = convert_counter(counter)
        print(status_update)

        threading.Thread(target=generate_tick, args=(sleep_time, counter)).start()

    while True:
        event, event_counter = event_queue.get()

        match event:
            case "tick":
                # Ignore timer event if have moved on from timer.
                if event_counter < light.get_state():
                    print("ignore stale tick.")
                    continue

                light_counter = light.handle_clock_tick()
                update(light_counter)

            case "ns":
                light_counter = light.handle_ns_button()

                if light_counter is None:
                    print("ignore ns button")
                    continue

                update(light_counter)

            case "ew":
                light_counter = light.handle_ew_button()

                if light_counter is None:
                    print("ignore ew button")
                    continue

                update(light_counter)
