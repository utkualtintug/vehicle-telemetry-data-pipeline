CREATE TABLE raw_vehicle_events (
    id SERIAL PRIMARY KEY,
    vehicle_id TEXT NOT NULL,
    brand TEXT NOT NULL,
    event_time TIMESTAMP NOT NULL,
    vehicle_speed_kph DOUBLE PRECISION,
    engine_rpm DOUBLE PRECISION,
    fuel_level_percent DOUBLE PRECISION,
    battery_voltage_v DOUBLE PRECISION,
    CONSTRAINT raw_vehicle_events_uniq UNIQUE (vehicle_id, event_time)
);

CREATE TABLE clean_vehicle_events (
    id SERIAL PRIMARY KEY,
    vehicle_id TEXT NOT NULL,
    brand TEXT NOT NULL,
    event_time TIMESTAMP NOT NULL,
    vehicle_speed_kph DOUBLE PRECISION NOT NULL,
    engine_rpm DOUBLE PRECISION NOT NULL,
    fuel_level_percent DOUBLE PRECISION,
    battery_voltage_v DOUBLE PRECISION,
    CONSTRAINT clean_vehicle_events_uniq UNIQUE (vehicle_id, event_time)
);

CREATE TABLE rejected_vehicle_events (
    id SERIAL PRIMARY KEY,
    vehicle_id TEXT,
    event_time TIMESTAMP,
    reject_reason TEXT NOT NULL,
    rejected_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT rejected_vehicle_events_uniq UNIQUE (vehicle_id, event_time, reject_reason)
);

CREATE TABLE daily_kpis (
    vehicle_id TEXT NOT NULL,
    kpi_date DATE NOT NULL,
    total_events INT NOT NULL,
    total_distance DOUBLE PRECISION NOT NULL,
    avg_speed DOUBLE PRECISION NOT NULL,
    max_speed DOUBLE PRECISION NOT NULL,
    idle_ratio DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (vehicle_id, kpi_date)
);
