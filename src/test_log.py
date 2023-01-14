import log

import pytest


@pytest.fixture
def simple_log():
    return [log.LogEntry(1, "a"), log.LogEntry(1, "b"), log.LogEntry(1, "c")]


def test_append_entries(simple_log):
    assert log.append_entries([], 0, 1, [])
    assert log.append_entries([], 0, 1, [log.LogEntry(1, "a")])

    for i in range(3):
        assert log.append_entries(
            simple_log,
            i,
            1,
            [log.LogEntry(1, "x")],
        )

    assert log.append_entries(
        simple_log,
        3,
        1,
        [log.LogEntry(1, "x")],
    )

    assert log.append_entries(
        simple_log,
        3,
        1,
        [log.LogEntry(1, "x"), log.LogEntry(1, "x")],
    )

    assert not log.append_entries(
        simple_log,
        3,
        2,
        [log.LogEntry(1, "x")],
    )
