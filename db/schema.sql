CREATE TABLE raw_events (
    id SERIAL PRIMARY KEY,
    speed INT,
    distance INT,
    eventtime TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE clean_events (
    id INT,
    speed INT,
    distance INT,
    eventtime TIMESTAMP
);

CREATE TABLE daily_kpis(
    total_events INT,
    avg_speed INT,
    total_distance INT,
    kpi_date DATE UNIQUE
);