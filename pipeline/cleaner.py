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
        SELECT id, speed, distance, event_time
        FROM raw_events
        WHERE id NOT IN (SELECT id FROM clean_events)
            AND id NOT IN (SELECT raw_id FROM rejected_events);
    """)

    rows = cur.fetchall()
    candidate_count = len(rows)

    rejected_count = 0
    accepted_count = 0

    for row in rows:
        raw_id, speed, distance, event_time = row

        reject_reason = None

        if event_time is None:
            reject_reason = "MISSING_EVENTTIME"
        elif speed is None or speed < 0 or speed > 120:
            reject_reason = "SPEED_INVALID"
        elif distance is None or distance < 0 or distance > 600:
            reject_reason = "DISTANCE_INVALID"

        if reject_reason is not None:
            rejected_count += 1
            cur.execute(
                """
                INSERT INTO rejected_events (raw_id, reject_reason)
                VALUES (%s, %s)
                ON CONFLICT (raw_id) DO NOTHING;
                """,
                (raw_id, reject_reason)
            )
            continue

        accepted_count += 1
        cur.execute(
            """
            INSERT INTO clean_events (id, speed, distance, event_time)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING;
            """,
            (raw_id, speed, distance, event_time)
        )

    conn.commit()

    evaluated = accepted_count + rejected_count
    reject_rate = (rejected_count / evaluated) if evaluated > 0 else 0

    print(
        f"Cleaner finished.\n"
        f"Candidate records: {candidate_count}\n"
        f"Accepted records: {accepted_count}\n"
        f"Rejected records: {rejected_count}\n"
        f"Reject rate: {reject_rate:.2f}"
    )

    cur.close()
    conn.close()
