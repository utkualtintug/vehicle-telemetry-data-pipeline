import psycopg2
from config import DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT
from logger import logger

def run_cleaner():
    logger.info("Cleaner started")

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

        logger.info(f"Candidate records: {candidate_count}")
        logger.info(f"Accepted records: {(accepted_count)}")
        logger.info(f"Rejected records: {(rejected_count)}")
        logger.info(f"Reject rate: {reject_rate:.2f}")

        logger.info("Cleaner finished")

    except Exception as e:
        logger.error(f"Cleaner failed: {e}")
        conn.rollback()
        raise

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
