import psycopg2
from config import DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT

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
            DATE(eventtime) AS kpi_date,
            COUNT(id) AS total_events,
            ROUND(AVG(speed), 2) AS avg_speed,
            SUM(distance) AS total_distance
        FROM clean_events
        GROUP BY DATE(eventtime);
    """)

    rows = cur.fetchall()

    for row in rows:
        cur.execute(
            """
            INSERT INTO daily_kpis (kpi_date, total_events, avg_speed, total_distance)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT(kpi_date)
            DO UPDATE SET
                total_events = EXCLUDED.total_events,
                avg_speed = EXCLUDED.avg_speed,
                total_distance = EXCLUDED.total_distance
            """,
            (row[0], row[1], row[2], row[3])
        )

    print("Datas are saved.")

    conn.commit()

    cur.close()
    conn.close()
