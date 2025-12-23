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

    for row in rows:
        cur.execute(
            """
            INSERT INTO clean_events (id, speed, distance, eventtime)
            VALUES (%s, %s, %s, %s)
            """,
            (row[0], row[1], row[2], row[3])
        )

    conn.commit()

    print(f"{len(rows)} datas are saved. ")

    cur.close()
    conn.close()
