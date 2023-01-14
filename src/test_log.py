import log

import pytest


@pytest.fixture
def simple_log():
    return [log.LogEntry(1, "a"), log.LogEntry(1, "b"), log.LogEntry(1, "c")]


def test_append_entry(simple_log):
    # Check replacement works as expected.
    for i in range(3):
        current_log = simple_log.copy()
        assert log.append_entry(
            current_log,
            i,
            1,
            log.LogEntry(1, "x"),
        )
        assert current_log[i].item == "x"

    # Check append works as expected.
    current_log = simple_log.copy()
    assert log.append_entry(
        current_log,
        3,
        1,
        log.LogEntry(1, "x"),
    )
    assert len(current_log) == 4
    assert current_log[3].item == "x"

    # Check idempotency.
    current_log = simple_log.copy()
    log.append_entry(
        current_log,
        3,
        1,
        log.LogEntry(1, "x"),
    )
    log.append_entry(
        current_log,
        3,
        1,
        log.LogEntry(1, "x"),
    )
    assert len(current_log) == 4
    assert current_log[3].item == "x"


def test_append_entries(simple_log):
    # Check for previous_index.
    assert log.append_entries([], 0, 1, [])
    assert not log.append_entries([], 1, 1, [])

    # Check for previous_term.
    assert log.append_entries(simple_log.copy(), 0, 1, [])
    assert not log.append_entries(simple_log.copy(), 0, 2, [])

    # Check for simple append.
    current_log = []
    assert log.append_entries(current_log, 0, 1, [log.LogEntry(1, "a")])
    assert current_log == [log.LogEntry(1, "a")]

    # Check replace and append work as expected.
    current_log = simple_log.copy()
    assert log.append_entries(
        current_log,
        2,
        1,
        [log.LogEntry(1, "x"), log.LogEntry(1, "y"), log.LogEntry(1, "z")],
    )
    assert len(current_log) == 5
    assert current_log[0].item == "a"
    assert current_log[1].item == "b"
    assert current_log[2].item == "x"
    assert current_log[3].item == "y"
    assert current_log[4].item == "z"
