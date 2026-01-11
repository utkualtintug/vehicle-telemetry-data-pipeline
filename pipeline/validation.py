def validate_event(event):
    if event["timestamp"] is None:
        return False, "MISSING_TIMESTAMP"

    if event["vehicle_speed_kph"] is not None:
        if event["vehicle_speed_kph"] < 0:
            return False, "NEGATIVE_SPEED"

    if event["engine_rpm"] is not None:
        if event["engine_rpm"] < 0:
            return False, "NEGATIVE_RPM"

    if event["fuel_level_percent"] is not None:
        if event["fuel_level_percent"] < 0 or event["fuel_level_percent"] > 100:
            return False, "FUEL_LEVEL_OUT_OF_RANGE"

    if event["battery_voltage_v"] is not None:
        if event["battery_voltage_v"] < 8:
            return False, "LOW_BATTERY_VOLTAGE"
    
    if event["vehicle_speed_kph"] is not None:
        if event["vehicle_speed_kph"] > 250:
            return False, "SPEED_TOO_HIGH"


    return True, None
