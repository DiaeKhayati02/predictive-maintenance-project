CREATE TABLE raw_data (
    id SERIAL PRIMARY KEY,
    air_temperature FLOAT,
    process_temperature FLOAT,
    rotational_speed INTEGER,
    torque FLOAT,
    tool_wear INTEGER,
    machine_failure VARCHAR(50),
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE predictions (
    id SERIAL PRIMARY KEY,
    raw_data_id INTEGER REFERENCES raw_data(id),
    predicted_failure BOOLEAN,
    failure_probability FLOAT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);