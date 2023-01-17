import raftlog

import pytest


@pytest.fixture
def simple_log():
    return [
        raftlog.LogEntry(1, "a"),
        raftlog.LogEntry(1, "b"),
        raftlog.LogEntry(1, "c"),
    ]


@pytest.fixture
def paper_log():
    paper_log = [
        raftlog.LogEntry(1, "0"),
        raftlog.LogEntry(1, "1"),
        raftlog.LogEntry(1, "2"),
    ]
    paper_log += [raftlog.LogEntry(4, "3"), raftlog.LogEntry(4, "4")]
    paper_log += [raftlog.LogEntry(5, "5"), raftlog.LogEntry(5, "6")]
    paper_log += [
        raftlog.LogEntry(6, "7"),
        raftlog.LogEntry(6, "8"),
        raftlog.LogEntry(6, "9"),
    ]

    return paper_log


@pytest.fixture
def logs_by_identifier(paper_log):
    logs_by_identifier = {}

    log_a = paper_log.copy()
    log_a.pop()
    logs_by_identifier["a"] = log_a

    log_b = paper_log.copy()
    [log_b.pop() for _ in range(6)]
    logs_by_identifier["b"] = log_b

    log_c = paper_log.copy()
    log_c.append(raftlog.LogEntry(6, "10"))
    logs_by_identifier["c"] = log_c

    log_d = paper_log.copy()
    log_d += [raftlog.LogEntry(7, "10"), raftlog.LogEntry(7, "11")]
    logs_by_identifier["d"] = log_d

    log_e = paper_log.copy()
    [log_e.pop() for _ in range(5)]
    log_e += [raftlog.LogEntry(4, "5"), raftlog.LogEntry(4, "6")]
    logs_by_identifier["e"] = log_e

    log_f = paper_log.copy()
    [log_f.pop() for _ in range(7)]
    log_f += [
        raftlog.LogEntry(2, "3"),
        raftlog.LogEntry(2, "4"),
        raftlog.LogEntry(2, "5"),
    ]
    log_f += [
        raftlog.LogEntry(3, "6"),
        raftlog.LogEntry(3, "7"),
        raftlog.LogEntry(3, "8"),
        raftlog.LogEntry(3, "9"),
        raftlog.LogEntry(3, "10"),
    ]
    logs_by_identifier["f"] = log_f

    return logs_by_identifier


def test_append_entry(simple_log):
    # Check return False when previous index past last index in log.
    assert not raftlog.append_entry(
        simple_log.copy(),
        3,
        1,
        raftlog.LogEntry(1, "x"),
    )

    # Check append and return True when previous index is last index in log.
    log_1 = simple_log.copy()
    assert raftlog.append_entry(
        log_1,
        2,
        1,
        raftlog.LogEntry(1, "x"),
    )
    assert len(log_1) == 4
    assert log_1[0].item == "a"
    assert log_1[1].item == "b"
    assert log_1[2].item == "c"
    assert log_1[3].item == "x"

    # Check replace and return True when previous index is less than the last
    # index in log.
    log_2 = simple_log.copy()
    assert raftlog.append_entry(
        log_2,
        0,
        1,
        raftlog.LogEntry(1, "x"),
    )
    assert len(log_2) == 3
    assert log_2[0].item == "a"
    assert log_2[1].item == "x"
    assert log_2[2].item == "c"


def test_append_entries_simple(simple_log):
    # Check for previous_index.
    assert raftlog.append_entries([], -1, 1, [])
    assert not raftlog.append_entries([], 0, 1, [])

    # Check for previous_term.
    assert raftlog.append_entries(simple_log.copy(), 0, 1, [])
    assert not raftlog.append_entries(simple_log.copy(), 0, 0, [])

    # Check for simple append.
    log_1 = []
    assert raftlog.append_entries(log_1, -1, 1, [raftlog.LogEntry(1, "a")])
    assert log_1 == [raftlog.LogEntry(1, "a")]

    # Check append only works as expected.
    log_2 = simple_log.copy()
    assert raftlog.append_entries(
        log_2,
        2,
        1,
        [raftlog.LogEntry(1, "x"), raftlog.LogEntry(1, "y"), raftlog.LogEntry(1, "z")],
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
    assert raftlog.append_entries(
        log_3,
        0,
        1,
        [raftlog.LogEntry(2, "x"), raftlog.LogEntry(2, "y"), raftlog.LogEntry(2, "z")],
    )
    assert len(log_3) == 4
    assert log_3[0].item == "a"
    assert log_3[1].item == "x"
    assert log_3[2].item == "y"
    assert log_3[3].item == "z"

    # Check append and return True when previous index is -1 and log is empty.
    log_4 = []
    assert raftlog.append_entries(
        log_4,
        -1,
        1,
        [raftlog.LogEntry(1, "x"), raftlog.LogEntry(1, "y")],
    )
    assert len(log_4) == 2
    assert log_4[0].item == "x"
    assert log_4[1].item == "y"

    # Check return False when previous index is -1 and log is not empty.
    assert not raftlog.append_entries(
        simple_log.copy(), -1, 1, [raftlog.LogEntry(1, "x"), raftlog.LogEntry(1, "y")]
    )


def test_append_entries_paper(logs_by_identifier):
    # Figure 7a
    assert not raftlog.append_entries(
        logs_by_identifier["a"], 9, 6, [raftlog.LogEntry(8, "x")]
    )

    # Figure 7b
    assert not raftlog.append_entries(
        logs_by_identifier["b"], 9, 6, [raftlog.LogEntry(8, "x")]
    )

    # Figure 7c
    log_c = logs_by_identifier["c"]
    assert raftlog.append_entries(log_c, 9, 6, [raftlog.LogEntry(8, "x")])
    assert len(log_c) == 11
    assert log_c[10].item == "x"

    # Figure 7d
    log_d = logs_by_identifier["d"]
    assert raftlog.append_entries(log_d, 9, 6, [raftlog.LogEntry(8, "x")])
    assert len(log_d) == 11
    assert log_d[10].item == "x"

    # Figure 7e
    assert not raftlog.append_entries(
        logs_by_identifier["e"], 9, 6, [raftlog.LogEntry(8, "x")]
    )

    # Figure 7f
    assert not raftlog.append_entries(
        logs_by_identifier["e"], 9, 6, [raftlog.LogEntry(8, "x")]
    )
