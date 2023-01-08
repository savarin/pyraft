import traffic


def test_clock_tick() -> None:
    traffic_light = traffic.TrafficLight()

    for i in range(6):
        traffic_light.handle_clock_tick()

        assert traffic_light.get_counter() == i
        assert traffic_light.get_state() == i
        assert not traffic_light.get_button()

    traffic_light.handle_clock_tick()

    assert traffic_light.get_counter() == 6
    assert traffic_light.get_state() == 0
    assert not traffic_light.get_button()


def test_north_south_button() -> None:
    traffic_light = traffic.TrafficLight()
    traffic_light.handle_clock_tick()

    assert traffic_light.get_counter() == 0
    assert traffic_light.get_state() == 0
    assert not traffic_light.get_button()

    traffic_light.handle_north_south_button()
    assert traffic_light.get_counter() == 0
    assert traffic_light.get_state() == 0
    assert traffic_light.get_button()

    traffic_light.handle_clock_tick()
    assert traffic_light.get_counter() == 2
    assert traffic_light.get_state() == 2
    assert not traffic_light.get_button()

    for i in range(4):
        traffic_light.handle_north_south_button()
        assert traffic_light.get_counter() == 2 + i
        assert traffic_light.get_state() == 2 + i
        assert not traffic_light.get_button()

        traffic_light.handle_clock_tick()

    assert traffic_light.get_counter() == 6
    assert traffic_light.get_state() == 0
    assert not traffic_light.get_button()


def test_east_west_button() -> None:
    traffic_light = traffic.TrafficLight()
    traffic_light.handle_clock_tick()

    for i in range(3):
        traffic_light.handle_east_west_button()
        assert traffic_light.get_counter() == i
        assert traffic_light.get_state() == i
        assert not traffic_light.get_button()

        traffic_light.handle_clock_tick()

    assert traffic_light.get_counter() == 3
    assert traffic_light.get_state() == 3
    assert not traffic_light.get_button()

    traffic_light.handle_east_west_button()
    assert traffic_light.get_counter() == 3
    assert traffic_light.get_state() == 3
    assert traffic_light.get_button()

    traffic_light.handle_clock_tick()
    assert traffic_light.get_counter() == 5
    assert traffic_light.get_state() == 5
    assert not traffic_light.get_button()

    traffic_light.handle_clock_tick()
    assert traffic_light.get_counter() == 6
    assert traffic_light.get_state() == 0
    assert not traffic_light.get_button()
