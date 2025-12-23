import psycopg2
import csv
import os
from config import DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT

def export_csv():
    os.makedirs("output", exist_ok=True)

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
            kpi_date,
            total_events,
            avg_speed,
            total_distance
        FROM daily_kpis
        ORDER BY kpi_date;
    """)

    rows = cur.fetchall()

    with open("output/daily_kpis.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "kpi_date",
            "total_events",
            "avg_speed",
            "total_distance"
        ])
        writer.writerows(rows)

    print("daily_kpis.csv created.")

    cur.close()
    conn.close()
    