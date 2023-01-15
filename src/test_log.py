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
    assert not log.append_entry(
        simple_log.copy(),
        3,
        1,
        log.LogEntry(1, "x"),
    )

    # Check append and return True when previous index is last index in log.
    log_1 = simple_log.copy()
    assert log.append_entry(
        log_1,
        2,
        1,
        log.LogEntry(1, "x"),
    )
    assert len(log_1) == 4
    assert log_1[0].item == "a"
    assert log_1[1].item == "b"
    assert log_1[2].item == "c"
    assert log_1[3].item == "x"

    # Check replace and return True when previous index is less than the last
    # index in log.
    log_2 = simple_log.copy()
    assert log.append_entry(
        log_2,
        0,
        1,
        log.LogEntry(1, "x"),
    )
    assert len(log_2) == 3
    assert log_2[0].item == "a"
    assert log_2[1].item == "x"
    assert log_2[2].item == "c"


def test_append_entries_simple(simple_log):
    # Check for previous_index.
    assert log.append_entries([], -1, 1, [])
    assert not log.append_entries([], 0, 1, [])

    # Check for previous_term.
    assert log.append_entries(simple_log.copy(), 0, 1, [])
    assert not log.append_entries(simple_log.copy(), 0, 0, [])

    # Check for simple append.
    log_1 = []
    assert log.append_entries(log_1, -1, 1, [log.LogEntry(1, "a")])
    assert log_1 == [log.LogEntry(1, "a")]

    # Check append only works as expected.
    log_2 = simple_log.copy()
    assert log.append_entries(
        log_2,
        2,
        1,
        [log.LogEntry(1, "x"), log.LogEntry(1, "y"), log.LogEntry(1, "z")],
    )
    assert len(log_2) == 6
    assert log_2[0].item == "a"
    assert log_2[1].item == "b"
    assert log_2[2].item == "c"
    assert log_2[3].item == "x"
    assert log_2[4].item == "y"
    assert log_2[5].item == "z"

    # Check replace and append work as expected.
    log_3 = simple_log.copy()
    assert log.append_entries(
        log_3,
        0,
        1,
        [log.LogEntry(2, "x"), log.LogEntry(2, "y"), log.LogEntry(2, "z")],
    )
    assert len(log_3) == 4
    assert log_3[0].item == "a"
    assert log_3[1].item == "x"
    assert log_3[2].item == "y"
    assert log_3[3].item == "z"

    # Check append and return True when previous index is -1 and log is empty.
    log_4 = []
    assert log.append_entries(
        log_4,
        -1,
        1,
        [log.LogEntry(1, "x"), log.LogEntry(1, "y")],
    )
    assert len(log_4) == 2
    assert log_4[0].item == "x"
    assert log_4[1].item == "y"

    # Check return False when previous index is -1 and log is not empty.
    assert not log.append_entries(
        simple_log.copy(), -1, 1, [log.LogEntry(1, "x"), log.LogEntry(1, "y")]
    )


def test_append_entries_paper(paper_log):
    log_1 = paper_log.copy()
    log_1.pop()
    assert not log.append_entries(log_1, 9, 6, [log.LogEntry(8, "x")])

    log_2 = paper_log.copy()
    [log_2.pop() for _ in range(5)]
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
    [log_5.pop() for _ in range(5)]
    log_5 += [log.LogEntry(4, "5"), log.LogEntry(4, "6")]
    assert not log.append_entries(log_5, 9, 6, [log.LogEntry(8, "x")])

    log_6 = paper_log.copy()
    [log_6.pop() for _ in range(7)]
    log_6 += [log.LogEntry(2, "3"), log.LogEntry(2, "4"), log.LogEntry(2, "5")]
    log_6 += [
        log.LogEntry(3, "6"),
        log.LogEntry(3, "7"),
        log.LogEntry(3, "8"),
        log.LogEntry(3, "9"),
        log.LogEntry(3, "10"),
    ]
    assert not log.append_entries(log_6, 9, 6, [log.LogEntry(8, "x")])
