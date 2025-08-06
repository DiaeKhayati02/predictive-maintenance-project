CREATE TABLE machine_predictions (
    id SERIAL PRIMARY KEY,
    datetime TIMESTAMP NOT NULL,
    machine_id INTEGER NOT NULL,
    sample_id INTEGER NOT NULL,
    
    -- Pressure values for 5 timesteps
    pressure_t0 FLOAT NOT NULL,
    pressure_t1 FLOAT NOT NULL,
    pressure_t2 FLOAT NOT NULL,
    pressure_t3 FLOAT NOT NULL,
    pressure_t4 FLOAT NOT NULL,
    
    -- Hour one-hot encoded features (120 columns total: 24 hours × 5 timesteps)
    -- You can add these individually or store as JSON/array
    hour_features JSONB,  -- Store all hour features as JSON for flexibility
    
    -- Target and prediction
    target_pressure FLOAT NOT NULL,
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Indexes for efficient querying
    CONSTRAINT unique_sample UNIQUE(machine_id, sample_id)
);

-- Create indexes for efficient querying
CREATE INDEX idx_machine_predictions_datetime ON machine_predictions(datetime);
CREATE INDEX idx_machine_predictions_machine_id ON machine_predictions(machine_id);
CREATE INDEX idx_machine_predictions_processed ON machine_predictions(processed);
CREATE INDEX idx_machine_predictions_created_at ON machine_predictions(created_at);