def validate_event(speed, distance, event_time):
    if event_time is None:
        return False, "MISSING_EVENTTIME"

    if speed is None or speed < 0 or speed > 120:
        return False, "SPEED_INVALID"

    if distance is None or distance < 0 or distance > 600:
        return False, "DISTANCE_INVALID"

    return True, None
