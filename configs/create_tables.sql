CREATE TABLE IF NOT EXISTS machine_predictions (
    id SERIAL PRIMARY KEY,
    machine_id INTEGER,
    datetime TIMESTAMP,
    volt FLOAT,
    rotate FLOAT,
    pressure FLOAT,
    vibration FLOAT
);