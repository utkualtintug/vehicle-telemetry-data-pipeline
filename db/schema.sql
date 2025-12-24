CREATE TABLE raw_events (
    id SERIAL PRIMARY KEY,
    speed INT,
    distance INT,
    event_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE clean_events (
    id INT PRIMARY KEY REFERENCES raw_events(id),
    speed INT NOT NULL,
    distance INT NOT NULL,
    event_time TIMESTAMP NOT NULL
);

CREATE TABLE daily_kpis (
    kpi_date DATE NOT NULL PRIMARY KEY,
    total_events INT NOT NULL,
    avg_speed NUMERIC(5,2) NOT NULL,
    total_distance INT NOT NULL
);

CREATE TABLE rejected_events (
    raw_id INT PRIMARY KEY REFERENCES raw_events(id),
    reject_reason TEXT NOT NULL,
    rejected_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
