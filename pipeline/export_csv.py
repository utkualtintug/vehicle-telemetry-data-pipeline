import psycopg2
import csv
import os
from config import DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT
from logger import logger


def export_csv():

    logger.info("CSV export started")

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
            vehicle_id,
            kpi_date,
            total_events,
            total_distance,
            avg_speed,
            max_speed,
            idle_ratio
        FROM daily_kpis
        ORDER BY vehicle_id, kpi_date;
    """)

    rows = cur.fetchall()
    logger.info(f"Rows fetched for CSV export: {len(rows)}")

    output_path = "output/daily_kpis.csv"

    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "vehicle_id",
            "kpi_date",
            "total_events",
            "total_distance",
            "avg_speed",
            "max_speed",
            "idle_ratio"
        ])
        writer.writerows(rows)

    cur.close()
    conn.close()

    logger.info(f"CSV export completed: {output_path}")
