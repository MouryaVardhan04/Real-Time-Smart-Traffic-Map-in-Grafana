CREATE TABLE road_metadata (
    road_id INTEGER PRIMARY KEY,
    road_name VARCHAR NOT NULL,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    city VARCHAR DEFAULT 'Bengaluru'
);

CREATE TABLE traffic_events (
    event_id VARCHAR PRIMARY KEY,
    road_id INTEGER REFERENCES road_metadata(road_id),
    road_name VARCHAR,
    timestamp TIMESTAMP NOT NULL,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    vehicle_count INTEGER,
    avg_speed_kmph DOUBLE PRECISION,
    occupancy_pct DOUBLE PRECISION,
    weather VARCHAR,
    day_of_week VARCHAR,
    hour_of_day INTEGER,
    event_type VARCHAR,
    source VARCHAR,
    incident_type VARCHAR,
    severity VARCHAR
);

CREATE TABLE traffic_predictions (
    prediction_id SERIAL PRIMARY KEY,
    road_id INTEGER REFERENCES road_metadata(road_id),
    prediction_timestamp TIMESTAMP NOT NULL,
    target_timestamp TIMESTAMP NOT NULL,
    predicted_congestion VARCHAR,
    prediction_confidence DOUBLE PRECISION,
    model_version VARCHAR,
    source VARCHAR
);

CREATE TABLE traffic_incidents (
    incident_id SERIAL PRIMARY KEY,
    road_id INTEGER REFERENCES road_metadata(road_id),
    event_timestamp TIMESTAMP NOT NULL,
    incident_type VARCHAR,
    severity VARCHAR,
    description TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    source VARCHAR
);

INSERT INTO road_metadata (road_id, road_name, latitude, longitude, city)
VALUES
(1, 'Main Avenue', 12.9716, 77.5946, 'Bengaluru'),
(2, 'Ring Road', 12.9650, 77.5836, 'Bengaluru'),
(3, 'City Center Blvd', 12.9751, 77.5937, 'Bengaluru'),
(4, 'Old Airport Road', 12.9862, 77.6060, 'Bengaluru'),
(5, 'Tech Park Expressway', 12.9248, 77.6110, 'Bengaluru');