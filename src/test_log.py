import log

import pytest


@pytest.fixture
def simple_log():
    return [log.LogEntry(1, "a"), log.LogEntry(1, "b"), log.LogEntry(1, "c")]


def test_append_entries(simple_log):
    current_log = []
    assert log.append_entries(current_log, 0, 1, [])
    assert current_log == []

    current_log = []
    assert log.append_entries(current_log, 0, 1, [log.LogEntry(1, "a")])
    assert current_log == [log.LogEntry(1, "a")]

    assert log.append_entries(simple_log.copy(), 0, 1, [])
    assert not log.append_entries(simple_log.copy(), 0, 2, [])

    # Check replacement works as expected.
    for i in range(3):
        current_log = simple_log.copy()
        assert log.append_entries(
            current_log,
            i,
            1,
            [log.LogEntry(1, "x")],
        )
        assert current_log[i].item == "x"

    # Check append works as expected.
    current_log = simple_log.copy()
    assert log.append_entries(
        current_log,
        3,
        1,
        [log.LogEntry(1, "x"), log.LogEntry(1, "y")],
    )
    assert len(current_log) == 5
    assert current_log[3].item == "x"
    assert current_log[4].item == "y"

    # Check append and replace works as expected.
    current_log = simple_log.copy()
    assert log.append_entries(
        current_log,
        2,
        1,
        [log.LogEntry(1, "x"), log.LogEntry(1, "y")],
    )
    assert len(current_log) == 4
    assert current_log[0].item == "a"
    assert current_log[1].item == "b"
    assert current_log[2].item == "x"
    assert current_log[3].item == "y"

    # Check for idempotency.
    current_log = simple_log.copy()
    assert log.append_entries(
        current_log,
        3,
        1,
        [log.LogEntry(1, "x")],
    )
    assert log.append_entries(
        current_log,
        3,
        1,
        [log.LogEntry(1, "x")],
    )
    assert len(current_log) == 4
    assert current_log[3].item == "x"

    # Check for invalid term.
    assert not log.append_entries(
        simple_log,
        3,
        2,
        [log.LogEntry(1, "x")],
    )
