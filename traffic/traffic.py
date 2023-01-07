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

    """
    def __post_init__(self):
        self.socket = socket(AF_INET, SOCK_DGRAM)
        self.state = -1

    def handle_clock_tick(self):
        previous_state = self.state
        self.state = (self.state + 1) % 4
        self.update_lights()

        return previous_state, self.state, sleep_time_by_state[self.state]

    def handle_ew_button(self):
        ...

    def handle_ns_button(self):
        ...

    def update_lights(self):
        ew_color, ns_color = colors_by_state[self.state]

        self.send_message("ew", ew_color)
        self.send_message("ns", ns_color)

    def send_message(self, direction, message):
        self.socket.sendto(message.encode("ascii"), ("localhost", port_by_direction[direction]))


if __name__ == "__main__":
    light = TrafficLight()
    event_queue = queue.Queue()

    def generate_ticks(interval):
        time.sleep(interval)
        event_queue.put('tick')

    threading.Thread(target=generate_ticks, args=(1,)).start()

    while True:
        event = event_queue.get()

        if event == "tick":
            previous_state, current_state, sleep_time = light.handle_clock_tick()
            print(f"change from {previous_state} to {current_state}, stay for {sleep_time} seconds...")
            threading.Thread(target=generate_ticks, args=(sleep_time,)).start()
