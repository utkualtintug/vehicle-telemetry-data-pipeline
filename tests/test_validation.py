from pipeline.validation import validate_event
from datetime import datetime

def base_event():
    return {
        "timestamp": datetime.now(),
        "vehicle_speed_kph": 80,
        "engine_rpm": 2000,
        "fuel_level_percent": 50,
        "battery_voltage_v": 12
    }


def test_negative_speed():
    event = base_event()
    event["vehicle_speed_kph"] = -5

    is_valid, reason = validate_event(event)

    assert is_valid is False
    assert reason == "NEGATIVE_SPEED"


def test_speed_too_high():
    event = base_event()
    event["vehicle_speed_kph"] = 300

    is_valid, reason = validate_event(event)

    assert is_valid is False
    assert reason == "SPEED_TOO_HIGH"


def test_invalid_fuel_level():
    event = base_event()
    event["fuel_level_percent"] = 150

    is_valid, reason = validate_event(event)

    assert is_valid is False
    assert reason == "FUEL_LEVEL_OUT_OF_RANGE"


def test_low_battery_voltage():
    event = base_event()
    event["battery_voltage_v"] = 6

    is_valid, reason = validate_event(event)

    assert is_valid is False
    assert reason == "LOW_BATTERY_VOLTAGE"


def test_valid_event():
    event = base_event()

    is_valid, reason = validate_event(event)

    assert is_valid is True
    assert reason is None