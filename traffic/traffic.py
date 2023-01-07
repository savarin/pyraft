# traffic.py
#
# Implement traffic light control software.  
#
# Challenge:  Make something that can be tested/debugged. 
from dataclasses import dataclass
from socket import socket, AF_INET, SOCK_DGRAM


colors_by_state = {
    0: ("G", "R"),
    1: ("Y", "R"),
    2: ("R", "G"),
    3: ("R", "Y"),
}

port_by_direction = {
    "ew": 8000,
    "ns": 9000,
}

@dataclass
class TrafficLight:
    """
    v1 - simply change lights according to timer.

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
    """
    def __post_init__(self):
        self.socket = socket(AF_INET, SOCK_DGRAM)
        self.state = 0

    def handle_clock_tick(self):
        self.state = (self.state + 1) % 4
        self.update_lights()

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
