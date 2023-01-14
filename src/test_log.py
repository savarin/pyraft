import log


def test_append_entries():
    assert log.append_entries([], 0, 1, [])
    assert log.append_entries([], 0, 1, [log.LogEntry(1, "a")])

    for i in range(3):
        assert log.append_entries(
            [log.LogEntry(1, "a"), log.LogEntry(1, "b"), log.LogEntry(1, "c")],
            i,
            1,
            [log.LogEntry(1, "x")],
        )

    assert log.append_entries(
        [log.LogEntry(1, "a"), log.LogEntry(1, "b"), log.LogEntry(1, "c")],
        3,
        1,
        [log.LogEntry(1, "x")],
    )
