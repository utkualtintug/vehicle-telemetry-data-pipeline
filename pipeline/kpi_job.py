import psycopg2
from config import DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT
from logger import logger

def run_kpi_job():
    logger.info("KPI job started")

    conn = None
    cur = None

    try:
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
                DATE(event_time) AS kpi_date,
                COUNT(id) AS total_events,
                ROUND(AVG(speed), 2) AS avg_speed,
                SUM(distance) AS total_distance
            FROM clean_events
            GROUP BY DATE(event_time);
        """)

        rows = cur.fetchall()
        logger.info(f"KPI rows calculated: {len(rows)}")

        for kpi_date, total_events, avg_speed, total_distance in rows:
            cur.execute(
                """
                INSERT INTO daily_kpis (kpi_date, total_events, avg_speed, total_distance)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (kpi_date)
                DO UPDATE SET
                    total_events = EXCLUDED.total_events,
                    avg_speed = EXCLUDED.avg_speed,
                    total_distance = EXCLUDED.total_distance;
                """,
                (kpi_date, total_events, avg_speed, total_distance)
            )

        conn.commit()
        logger.info("KPI job finished successfully")

    except Exception as e:
        logger.error(f"KPI job failed: {e}")
        if conn:
            conn.rollback()
        raise

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
