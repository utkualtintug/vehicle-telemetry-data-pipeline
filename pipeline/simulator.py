import random
import psycopg2
from datetime import datetime, timedelta
from config import DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT

TOTAL_EVENTS = 20000

SCENARIOS = [
    ("urban", 0.65),
    ("highway", 0.25),
    ("fault", 0.10),
]

def choose_scenario():
    r = random.random()
    cumulative = 0
    for name, prob in SCENARIOS:
        cumulative += prob
        if r <= cumulative:
            return name
    return "urban"

def generate_event(scenario, last_speed):
    event_time = datetime.now() - timedelta(days=random.randint(0, 30),
    seconds=random.randint(0, 86400))

    if scenario == "urban":
        speed = max(0, min(60, last_speed + random.randint(-10, 10)))
        if random.random() < 0.1:
            speed = 0
        distance = round(random.uniform(0.1, 3.0), 2)

    elif scenario == "highway":
        speed = max(60, min(120, last_speed + random.randint(-15, 15)))
        if random.random() < 0.05:
            speed = random.randint(121, 150)
        distance = round(random.uniform(5.0, 50.0), 2)

    else:
        fault_type = random.choice(["speed_null", "speed_high", "distance_neg", "time_null"])
        speed = random.randint(0, 120)
        distance = round(random.uniform(1.0, 10.0), 2)

        if fault_type == "speed_null":
            speed = None
        elif fault_type == "speed_high":
            speed = random.randint(180, 300)
        elif fault_type == "distance_neg":
            distance = -random.uniform(1.0, 10.0)
        elif fault_type == "time_null":
            event_time = None

    return speed, distance, event_time

def run_simulator():
    conn = psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT
    )
    cur = conn.cursor()

    last_speed = random.randint(20, 60)

    for _ in range(TOTAL_EVENTS):
        scenario = choose_scenario()
        speed, distance, event_time = generate_event(scenario, last_speed)

        if speed is not None:
            last_speed = speed

        cur.execute(
            """
            INSERT INTO raw_events (speed, distance, event_time)
            VALUES (%s, %s, %s)
            """,
            (speed, distance, event_time)
        )

    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    run_simulator()
