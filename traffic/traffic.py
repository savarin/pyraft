# traffic.py
#
# Implement traffic light control software.  
#
# Challenge:  Make something that can be tested/debugged. 


class TrafficLight:
    """
    v1 - simply change lights according to timer.

        state 0 - 5 seconds
            EW R
            NS R
        state 1 - 30 seconds
            EW G
            NS R
        state 2 - 5 seconds
            EW Y
            NS R
        state 3 - 60 seconds
            EW R
            NS G
        state 4 - 5 seconds
            EW R
            NS Y
        back to state 1
    """

    def handle_clock_tick(self):
        ...

    def handle_ew_button(self):
        ...

    def handle_ns_button(self):
        ...
