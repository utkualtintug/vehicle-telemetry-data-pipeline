from pipeline.validation import validate_event
from datetime import datetime


def test_valid_event():
    is_valid, reason = validate_event(
        speed=80,
        distance=100,
        event_time=datetime.now()
    )
    assert is_valid is True
    assert reason is None


def test_missing_event_time():
    is_valid, reason = validate_event(
        speed=50,
        distance=20,
        event_time=None
    )
    assert is_valid is False
    assert reason == "MISSING_EVENTTIME"


def test_invalid_speed_negative():
    is_valid, reason = validate_event(
        speed=-5,
        distance=10,
        event_time=datetime.now()
    )
    assert is_valid is False
    assert reason == "SPEED_INVALID"


def test_invalid_speed_too_high():
    is_valid, reason = validate_event(
        speed=200,
        distance=10,
        event_time=datetime.now()
    )
    assert is_valid is False
    assert reason == "SPEED_INVALID"


def test_invalid_distance():
    is_valid, reason = validate_event(
        speed=60,
        distance=-3,
        event_time=datetime.now()
    )
    assert is_valid is False
    assert reason == "DISTANCE_INVALID"
