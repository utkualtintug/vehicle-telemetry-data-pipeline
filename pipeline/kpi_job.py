import psycopg2
from config import DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT
from logger import logger


def run_kpi_job():

    conn = psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT
    )

    cur = conn.cursor()

    cur.execute("""
        SELECT
            vehicle_id,
            DATE(event_time) AS kpi_date, -- (vehicle_id, day) = one KPI row
            COUNT(*) AS total_events,     -- Total event
            SUM(vehicle_speed_kph) AS total_distance, -- Basic total distance (we cannot calculate it this way in real life)
            AVG(vehicle_speed_kph) AS avg_speed,
            MAX(vehicle_speed_kph) AS max_speed,
            SUM(
                CASE
                    WHEN vehicle_speed_kph = 0 AND engine_rpm > 0 THEN 1
                    ELSE 0
                END
            )::DOUBLE PRECISION / COUNT(*) AS idle_ratio -- Idle ratio
        FROM clean_vehicle_events -- Calculate from clean datas 
        GROUP BY vehicle_id, DATE(event_time); -- Generate separate KPIs for each vehicle and each day
    """)

    rows = cur.fetchall() # Example row -> [('V1', date(2025,1,1), 20, 500.0, 30.0, 120.0, 0.1)]

    insert_sql = """
    INSERT INTO daily_kpis (
        vehicle_id,
        kpi_date,
        total_events,
        total_distance,
        avg_speed,
        max_speed,
        idle_ratio
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (vehicle_id, kpi_date)
    DO UPDATE SET
        total_events = EXCLUDED.total_events, -- EXCLUDED = “The new value I tried to INSERT”
        total_distance = EXCLUDED.total_distance,
        avg_speed = EXCLUDED.avg_speed,
        max_speed = EXCLUDED.max_speed,
        idle_ratio = EXCLUDED.idle_ratio;
    """

    for row in rows:
        cur.execute(insert_sql, row)

    conn.commit()
    cur.close()
    conn.close()

    logger.info(f"KPI rows written: {len(rows)}")
