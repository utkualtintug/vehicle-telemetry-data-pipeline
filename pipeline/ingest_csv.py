import pandas as pd
import psycopg2
from logger import logger
from config import DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT
from validation import validate_event


def run_ingest():
    # Read raw unput data from csv file
    df = pd.read_csv("data/raw/synthetic_telemetry_data.csv")


    # Connect to PostgreSQL
    conn = psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT
    )
    cur = conn.cursor()

    # Insert into raw table, so we don't lose them
    insert_raw_sql = """
    INSERT INTO raw_vehicle_events (
        vehicle_id,
        brand,
        event_time,
        vehicle_speed_kph,
        engine_rpm,
        fuel_level_percent,
        battery_voltage_v
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    -- If the same event already exists, skip insert
    ON CONFLICT (vehicle_id, event_time) DO NOTHING;
    """

    # Insert valid events only
    insert_clean_sql = """
    INSERT INTO clean_vehicle_events (
        vehicle_id,
        brand,
        event_time,
        vehicle_speed_kph,
        engine_rpm,
        fuel_level_percent,
        battery_voltage_v
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (vehicle_id, event_time) DO NOTHING;
    """

    # Insert invalid events with reason
    insert_rejected_sql = """
    INSERT INTO rejected_vehicle_events (
        vehicle_id,
        event_time,
        reject_reason
    )
    VALUES (%s, %s, %s)
    ON CONFLICT (vehicle_id, event_time, reject_reason) DO NOTHING;
    """

    # Counters
    raw_inserted = 0
    clean_inserted = 0
    rejected_inserted = 0

    # Process events one by one
    for i, row in df.iterrows():
        event = row.to_dict()

        # Inject invalid data periodically to test validation
        if i % 500 == 0:
            event["vehicle_speed_kph"] = -5

        # Normalize event time field
        event_time = event.get("event_time") or event.get("timestamp")

        # Always write to raw table first
        cur.execute(
            insert_raw_sql,
            (
                event.get("vehicle_id"),
                event.get("brand"),
                event_time,
                event.get("vehicle_speed_kph"),
                event.get("engine_rpm"),
                event.get("fuel_level_percent"),
                event.get("battery_voltage_v"),
            )
        )
        raw_inserted += cur.rowcount

        # Apply domain validation rules. (is_valid == true or false)
        is_valid, reason = validate_event(event)

        # Invalid events go to rejected table
        if not is_valid:
            cur.execute(
                insert_rejected_sql,
                (
                    event.get("vehicle_id"),
                    event_time,
                    reason
                )
            )
            rejected_inserted += cur.rowcount
            continue # Skip clean insert

        # Valid events go to clean table
        cur.execute(
            insert_clean_sql,
            (
                event.get("vehicle_id"),
                event.get("brand"),
                event_time,
                event.get("vehicle_speed_kph"),
                event.get("engine_rpm"),
                event.get("fuel_level_percent"),
                event.get("battery_voltage_v"),
            )
        )
        clean_inserted += cur.rowcount

    conn.commit()
    cur.close()
    conn.close()

    logger.info(f"Raw inserted (deduped): {raw_inserted}")
    logger.info(f"Clean inserted (deduped): {clean_inserted}")
    logger.info(f"Rejected inserted (deduped): {rejected_inserted}")
