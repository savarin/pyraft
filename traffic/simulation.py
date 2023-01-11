"""
transitions:
    CLOCK clock += 1
        R R                                 -> G R
        G R + clock = 15 + NS_BUTTON = True -> Y R
        G R + clock = 30                    -> Y R
        Y R + clock = 5                     -> R G
        R G + clock = 15 + EW_BUTTON = True -> R Y
        R G + clock = 60                    -> R Y
        R Y + clock = 5                     -> G R

    EW_BUTTON east_west_button = True
        R G + clock >= 15                   -> R Y

    NS_BUTTON north_south_button = True
        G R + clock >= 15                   -> Y R

checks:
    individual
        colors
        buttons
        clock
        color pair

    sequential
        color pair - same || next in sequence
        buttons - same || False > True and else same || reset with color change
        clock - same || increase and else same || resets with color change
"""
import collections
import enum
import random
import sys


class ColorEnum(enum.Enum):
    RED = "RED"
    YELLOW = "YELLOW"
    GREEN = "GREEN"


class EventEnum(enum.Enum):
    CLOCK = "CLOCK"
    EW_BUTTON = "EW_BUTTON"
    NS_BUTTON = "NS_BUTTON"


def initial_state():
    return {
        "east_west_color": ColorEnum.RED,
        "north_south_color": ColorEnum.RED,
        "east_west_button": False,
        "north_south_button": False,
        "clock": 0,
    }


def handle_event(state, event):
    if event == EventEnum.CLOCK:
        state["clock"] += 1

    elif event == EventEnum.EW_BUTTON:
        state["east_west_button"] = True

    elif event == EventEnum.NS_BUTTON:
        state["north_south_button"] = True

    return state


def apply_transition(state):
    if (
        state["east_west_color"] == ColorEnum.RED
        and state["north_south_color"] == ColorEnum.RED
    ):
        state = initial_state()
        state.update({"east_west_color": ColorEnum.GREEN})

    elif (
        state["east_west_color"] == ColorEnum.GREEN
        and state["north_south_color"] == ColorEnum.RED
    ):
        if (state["clock"] == 15 and state["north_south_button"]) or state[
            "clock"
        ] == 30:
            state = initial_state()
            state.update({"east_west_color": ColorEnum.YELLOW})

    elif (
        state["east_west_color"] == ColorEnum.YELLOW
        and state["north_south_color"] == ColorEnum.RED
    ):
        if state["clock"] == 5:
            state = initial_state()
            state.update(
                {
                    "east_west_color": ColorEnum.RED,
                    "north_south_color": ColorEnum.GREEN,
                }
            )

    elif (
        state["east_west_color"] == ColorEnum.RED
        and state["north_south_color"] == ColorEnum.GREEN
    ):
        if (state["clock"] == 15 and state["east_west_button"]) or state["clock"] == 60:
            state = initial_state()
            state.update({"north_south_color": ColorEnum.YELLOW})

    elif (
        state["east_west_color"] == ColorEnum.RED
        and state["north_south_color"] == ColorEnum.YELLOW
    ):
        if state["clock"] == 5:
            state = initial_state()
            state.update(
                {
                    "east_west_color": ColorEnum.GREEN,
                    "north_south_color": ColorEnum.RED,
                }
            )

    elif (
        state["east_west_color"] == ColorEnum.RED
        and state["north_south_color"] == ColorEnum.GREEN
    ):
        if state["clock"] >= 15:
            state = initial_state()
            state.update({"north_south_color": ColorEnum.YELLOW})

    elif (
        state["east_west_color"] == ColorEnum.GREEN
        and state["north_south_color"] == ColorEnum.RED
    ):
        if state["clock"] >= 15:
            state = initial_state()
            state.update({"east_west_color": ColorEnum.YELLOW})

    return state


def next_state(state, event):
    state = handle_event(state, event)
    return apply_transition(state)


current_color_pair_by_previous_color_pair = {
    (ColorEnum.RED, ColorEnum.RED): (ColorEnum.GREEN, ColorEnum.RED),
    (ColorEnum.GREEN, ColorEnum.RED): (ColorEnum.YELLOW, ColorEnum.RED),
    (ColorEnum.YELLOW, ColorEnum.RED): (ColorEnum.RED, ColorEnum.GREEN),
    (ColorEnum.RED, ColorEnum.GREEN): (ColorEnum.RED, ColorEnum.YELLOW),
    (ColorEnum.RED, ColorEnum.YELLOW): (ColorEnum.GREEN, ColorEnum.RED),
}


def get_color_pair(state):
    return (state["east_west_color"], state["north_south_color"])


def assert_valid_individual_state(state):
    assert isinstance(state["east_west_color"], ColorEnum)
    assert isinstance(state["north_south_color"], ColorEnum)
    assert isinstance(state["east_west_button"], bool)
    assert isinstance(state["north_south_button"], bool)
    assert isinstance(state["clock"], int) and state["clock"] >= 0
    assert get_color_pair(state) in current_color_pair_by_previous_color_pair


def is_specific_transition(previous_state, current_state, transition):
    for k, v in previous_state.items():
        if k in transition:
            if v != transition[k][0] or current_state[k] != transition[k][1]:
                return False

        else:
            if v != current_state[k]:
                return False

    return True


def is_state_reset(previous_state, current_state):
    previous_color_pair = get_color_pair(previous_state)
    next_color_pair = current_color_pair_by_previous_color_pair[previous_color_pair]

    next_state = initial_state()
    next_state.update(
        {"east_west_color": next_color_pair[0], "north_south_color": next_color_pair[1]}
    )

    return next_state == current_state


def assert_valid_sequential_states(previous_state, current_state):
    previous_color_pair = get_color_pair(previous_state)
    current_color_pair = get_color_pair(current_state)
    next_color_pair = current_color_pair_by_previous_color_pair[previous_color_pair]

    assert (
        previous_color_pair == current_color_pair
        or next_color_pair == current_color_pair
    )

    is_east_west_button_same = (
        previous_state["east_west_button"] == current_state["east_west_button"]
    )
    is_east_west_button_transition = is_specific_transition(
        previous_state, current_state, {"east_west_button": (False, True)}
    )
    is_east_west_button_reset = (
        previous_state["east_west_button"]
        and not current_state["east_west_button"]
        and is_state_reset(previous_state, current_state)
    )

    assert (
        is_east_west_button_same
        or is_east_west_button_transition
        or is_east_west_button_reset
    )

    is_north_south_button_same = (
        previous_state["north_south_button"] == current_state["north_south_button"]
    )
    is_north_south_button_transition = is_specific_transition(
        previous_state, current_state, {"north_south_button": (False, True)}
    )
    is_north_south_button_reset = (
        previous_state["north_south_button"]
        and not current_state["north_south_button"]
        and is_state_reset(previous_state, current_state)
    )

    assert (
        is_north_south_button_same
        or is_north_south_button_transition
        or is_north_south_button_reset
    )

    is_clock_same = previous_state["clock"] == current_state["clock"]
    is_clock_transition = previous_state["clock"] + 1 == current_state["clock"]
    is_clock_reset = previous_state["clock"] > 0 and is_state_reset(
        previous_state, current_state
    )

    assert is_clock_same or is_clock_transition or is_clock_reset


if __name__ == "__main__":
    iterations = int(sys.argv[1])

    state = initial_state()
    result = []

    print(f"generating {str(iterations)} time-sequential iterations...")

    for _ in range(iterations):
        state = next_state(state, EventEnum.CLOCK)
        result.append(state.copy())

    print(f"validating {str(iterations)} time-sequential iterations individually...")

    for i in range(len(result)):
        assert_valid_individual_state(result[i])

    print(f"validating {str(iterations)} time-sequential iterations sequentially...")

    for i in range(1, len(result)):
        assert_valid_sequential_states(result[i - 1], result[i])

    state = initial_state()
    result, events = [], []

    print(f"generating {str(iterations)} random iterations...")

    for i in range(iterations):
        event = random.choice(list(EventEnum))
        events.append(event)

        state = next_state(state, event)
        result.append(state.copy())

        if i % (iterations // 10) == 0:
            print(f"0.{i // (iterations // 10)}", event)

    print(f"validating {str(iterations)} random iterations individually...")

    for i in range(len(result)):
        assert_valid_individual_state(result[i])

    print(f"validating {str(iterations)} random iterations sequentially...")

    for i in range(1, len(result)):
        assert_valid_sequential_states(result[i - 1], result[i])

    for k, v in sorted(collections.Counter(events).items(), key=lambda x: x[0].value):
        print(k, v)
