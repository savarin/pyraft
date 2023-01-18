import raftlog
import raftserver


def test_message_translation():
    message = raftserver.AppendEntryRequest(
        1, 2, 3, 4, [raftlog.LogEntry(5, "a"), raftlog.LogEntry(6, "b")]
    )

    string = (
        "d7:entriesld4:item1:a4:termi5eed4:item1:b4:termi6eee"
        + "14:previous_indexi3e13:previous_termi4e6:sourcei1e6:targeti2ee"
    )

    assert raftserver.encode_message(message) == string
    assert raftserver.decode_message(string) == message
