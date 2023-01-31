import raftlog

import pytest


@pytest.fixture
def paper_log():
    paper_log = [
        raftlog.LogEntry(1, "1"),
        raftlog.LogEntry(1, "1"),
        raftlog.LogEntry(1, "1"),
    ]
    paper_log += [raftlog.LogEntry(4, "4"), raftlog.LogEntry(4, "4")]
    paper_log += [raftlog.LogEntry(5, "5"), raftlog.LogEntry(5, "5")]
    paper_log += [
        raftlog.LogEntry(6, "6"),
        raftlog.LogEntry(6, "6"),
        raftlog.LogEntry(6, "6"),
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
    log_c.append(raftlog.LogEntry(6, "6"))
    logs_by_identifier["c"] = log_c

    log_d = paper_log.copy()
    log_d += [raftlog.LogEntry(7, "7"), raftlog.LogEntry(7, "7")]
    logs_by_identifier["d"] = log_d

    log_e = paper_log.copy()
    [log_e.pop() for _ in range(5)]
    log_e += [raftlog.LogEntry(4, "4"), raftlog.LogEntry(4, "4")]
    logs_by_identifier["e"] = log_e

    log_f = paper_log.copy()
    [log_f.pop() for _ in range(7)]
    log_f += [
        raftlog.LogEntry(2, "2"),
        raftlog.LogEntry(2, "2"),
        raftlog.LogEntry(2, "2"),
    ]
    log_f += [
        raftlog.LogEntry(3, "3"),
        raftlog.LogEntry(3, "3"),
        raftlog.LogEntry(3, "3"),
        raftlog.LogEntry(3, "3"),
        raftlog.LogEntry(3, "3"),
    ]
    logs_by_identifier["f"] = log_f

    return logs_by_identifier


def test_append_entries_paper(logs_by_identifier):
    # Figure 7a
    assert not raftlog.append_entries(
        logs_by_identifier["a"], 9, 6, [raftlog.LogEntry(6, "6")]
    )

    # Figure 7b
    assert not raftlog.append_entries(
        logs_by_identifier["b"], 9, 6, [raftlog.LogEntry(6, "6")]
    )

    # Figure 7c
    log_c = logs_by_identifier["c"]
    assert raftlog.append_entries(log_c, 9, 6, [raftlog.LogEntry(6, "6")])
    assert len(log_c) == 11
    assert log_c[10].item == "6"

    # Figure 7d
    log_d = logs_by_identifier["d"]
    assert raftlog.append_entries(log_d, 9, 6, [raftlog.LogEntry(6, "6")])
    assert len(log_d) == 11
    assert log_d[10].item == "6"

    # Figure 7e
    assert not raftlog.append_entries(
        logs_by_identifier["e"], 9, 6, [raftlog.LogEntry(6, "6")]
    )

    # Figure 7f
    assert not raftlog.append_entries(
        logs_by_identifier["e"], 9, 6, [raftlog.LogEntry(6, "6")]
    )
