-- First, ensure the extension is enabled
CREATE EXTENSION IF NOT EXISTS postgis;

-- Create the table using a GEOGRAPHY type
CREATE TABLE flight_positions (
    position_id SERIAL PRIMARY KEY,
    flight_id INTEGER NOT NULL REFERENCES flights(flight_id) ON DELETE CASCADE,
    
    "timestamp" TIMESTAMPTZ NOT NULL,
    
    -- A single column to store the location, optimized for geographic queries
    location GEOGRAPHY(POINT, 4326),
    
    altitude INTEGER,
    ground_speed INTEGER,
    vertical_rate INTEGER
);

-- Create a spatial index for extremely fast location-based queries
CREATE INDEX idx_flight_positions_location ON flight_positions USING GIST (location);