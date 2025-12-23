import time
import random
import psycopg2
from config import DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT

conn = psycopg2.connect(
    host=DB_HOST,
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    port=DB_PORT
    )

cur = conn.cursor()

while True:
    speed = random.randint(0, 220)
    distance = random.randint(0, 1100)

    cur.execute(
        "INSERT INTO raw_events (speed, distance) VALUES (%s, %s)",
        (speed, distance)
    )

    conn.commit()

    print(f"speed {speed} distance {distance}")
    time.sleep(5)
