import log

import pytest


@pytest.fixture
def simple_log():
    return [log.LogEntry(1, "a"), log.LogEntry(1, "b"), log.LogEntry(1, "c")]


@pytest.fixture
def paper_log():
    paper_log = [log.LogEntry(1, "0"), log.LogEntry(1, "1"), log.LogEntry(1, "2")]
    paper_log += [log.LogEntry(4, "3"), log.LogEntry(4, "4")]
    paper_log += [log.LogEntry(5, "5"), log.LogEntry(5, "6")]
    paper_log += [log.LogEntry(6, "7"), log.LogEntry(6, "8"), log.LogEntry(6, "9")]
    return paper_log


def test_append_entry(simple_log):
    # Check return False when previous index past last index in log.
    current_log = simple_log.copy()
    assert not log.append_entry(
        current_log,
        3,
        1,
        log.LogEntry(1, "x"),
    )

    # Check append and return True when previous index is last index in log.
    current_log = simple_log.copy()
    assert log.append_entry(
        current_log,
        2,
        1,
        log.LogEntry(1, "x"),
    )
    assert len(current_log) == 4
    assert current_log[0].item == "a"
    assert current_log[1].item == "b"
    assert current_log[2].item == "c"
    assert current_log[3].item == "x"

    # Check replace and return True when previous index is less than the last
    # index in log.
    current_log = simple_log.copy()
    assert log.append_entry(
        current_log,
        0,
        1,
        log.LogEntry(1, "x"),
    )
    assert len(current_log) == 3
    assert current_log[0].item == "a"
    assert current_log[1].item == "x"
    assert current_log[2].item == "c"


def test_append_entries_simple(simple_log):
    # Check append and return True when previous index is -1 and log is empty.
    current_log = []
    assert log.append_entries(
        current_log,
        -1,
        1,
        [log.LogEntry(1, "x"), log.LogEntry(1, "y")],
    )
    assert len(current_log) == 2
    assert current_log[0].item == "x"
    assert current_log[1].item == "y"

    # Check return False when previous index is -1 and log is not empty.
    current_log = simple_log.copy()
    assert not log.append_entries(
        current_log, -1, 1, [log.LogEntry(1, "x"), log.LogEntry(1, "y")]
    )


def test_append_entries_paper(paper_log):
    log_1 = paper_log.copy()
    log_1.pop()
    assert not log.append_entries(log_1, 9, 6, [log.LogEntry(8, "x")])

    log_2 = paper_log.copy()
    for _ in range(5):
        log_2.pop()
    assert not log.append_entries(log_2, 9, 6, [log.LogEntry(8, "x")])

    log_3 = paper_log.copy()
    log_3.append(log.LogEntry(6, "10"))
    assert log.append_entries(log_3, 9, 6, [log.LogEntry(8, "x")])
    assert len(log_3) == 11
    assert log_3[10].item == "x"

    log_4 = paper_log.copy()
    log_4 += [log.LogEntry(7, "10"), log.LogEntry(7, "11")]
    assert log.append_entries(log_4, 9, 6, [log.LogEntry(8, "x")])
    assert len(log_4) == 11
    assert log_4[10].item == "x"

    log_5 = paper_log.copy()
    for _ in range(5):
        log_5.pop()
    log_5 += [log.LogEntry(4, "5"), log.LogEntry(4, "6")]
    assert not log.append_entries(log_5, 9, 6, [log.LogEntry(8, "x")])

    log_6 = paper_log.copy()
    for _ in range(7):
        log_6.pop()
    log_6 += [log.LogEntry(2, "3"), log.LogEntry(2, "4"), log.LogEntry(2, "5")]
    log_6 += [
        log.LogEntry(3, "6"),
        log.LogEntry(3, "7"),
        log.LogEntry(3, "8"),
        log.LogEntry(3, "9"),
        log.LogEntry(3, "10"),
    ]
    assert not log.append_entries(log_6, 9, 6, [log.LogEntry(8, "x")])
