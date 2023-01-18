import rafthelpers


def test_encode_items():
    assert rafthelpers.encode_item(None) == ""
    assert rafthelpers.encode_item("") == "0:"
    assert rafthelpers.encode_item([]) == "le"
    assert rafthelpers.encode_item({}) == "de"

    assert rafthelpers.encode_item(1) == "i1e"
    assert rafthelpers.encode_item(-1) == "i-1e"
    assert rafthelpers.encode_item(0) == "i0e"
    assert rafthelpers.encode_item("foo") == "3:foo"

    assert rafthelpers.encode_item([1]) == "li1ee"
    assert rafthelpers.encode_item(["foo"]) == "l3:fooe"
    assert rafthelpers.encode_item([1, "foo"]) == "li1e3:fooe"
    assert rafthelpers.encode_item({"foo": 1}) == "d3:fooi1ee"

    assert rafthelpers.encode_item([1, ["foo"]]) == "li1el3:fooee"
    assert rafthelpers.encode_item({"foo": [1]}) == "d3:fooli1eee"
    assert rafthelpers.encode_item([{"foo": 1}]) == "ld3:fooi1eee"
    assert rafthelpers.encode_item({"foo": {"bar": "baz"}}) == "d3:food3:bar3:bazee"


def test_decode_items():
    assert rafthelpers.decode_item("") == None
    assert rafthelpers.decode_item("0:") == ""
    assert rafthelpers.decode_item("le") == []
    assert rafthelpers.decode_item("de") == {}
    assert rafthelpers.decode_item("i1e") == 1
    assert rafthelpers.decode_item("i-1e") == -1
    assert rafthelpers.decode_item("i0e") == 0
    assert rafthelpers.decode_item("3:foo") == "foo"
    assert rafthelpers.decode_item("li1ee") == [1]
    assert rafthelpers.decode_item("l3:fooe") == ["foo"]
    assert rafthelpers.decode_item("li1e3:fooe") == [1, "foo"]
    assert rafthelpers.decode_item("d3:fooi1ee") == {"foo": 1}
    assert rafthelpers.decode_item("li1el3:fooee") == [1, ["foo"]]
    assert rafthelpers.decode_item("d3:fooli1eee") == {"foo": [1]}
    assert rafthelpers.decode_item("ld3:fooi1eee") == [{"foo": 1}]
    assert rafthelpers.decode_item("d3:food3:bar3:bazee") == {"foo": {"bar": "baz"}}
