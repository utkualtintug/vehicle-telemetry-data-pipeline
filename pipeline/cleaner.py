import psycopg2
from config import DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT

def run_cleaner():
    conn = psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT
    )

    cur = conn.cursor()

    cur.execute("""
        SELECT id
        FROM raw_events
        WHERE id NOT IN (SELECT id FROM clean_events)
    """)
    raw_records = cur.fetchall()
    total_raw = len(raw_records)

    cur.execute("""
        SELECT id, speed, distance, eventtime
        FROM raw_events
        WHERE id NOT IN (SELECT id FROM clean_events)
            AND speed >= 0
            AND speed <= 120
            AND distance >= 0
            AND distance <= 600
            AND eventtime IS NOT NULL;
    """)
    rows = cur.fetchall()
    accepted = len(rows)

    for row in rows:
        cur.execute(
            """
            INSERT INTO clean_events (id, speed, distance, eventtime)
            VALUES (%s, %s, %s, %s)
            """,
            (row[0], row[1], row[2], row[3])
        )

    conn.commit()

    rejected = total_raw - accepted
    reject_rate = (rejected / total_raw) if total_raw > 0 else 0

    print(
        f"Cleaner finished.\n"
        f"Total raw records: {total_raw}\n"
        f"Accepted records: {accepted}\n"
        f"Rejected records: {rejected}\n"
        f"Reject rate: {reject_rate:.2f}"
    )

    cur.close()
    conn.close()
