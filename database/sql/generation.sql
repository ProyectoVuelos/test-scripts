-- #################################
-- ##      TABLE 1: FLIGHTS       ##
-- #################################
-- Stores the main summary for each flight.

CREATE TABLE flights (
    -- Core Identifiers
    flight_id SERIAL PRIMARY KEY, -- Internal auto-incrementing primary key
    fr24_id VARCHAR(20) NOT NULL UNIQUE, -- The unique ID from the FlightRadar24 API
    flight VARCHAR(20), -- Flight number, e.g., 'UAL173'
    callsign VARCHAR(20), -- Flight callsign, e.g., 'SWA2914'
    
    -- Aircraft Details
    aircraft_model VARCHAR(100),
    aircraft_reg VARCHAR(20), -- Aircraft registration, e.g., 'N123UA'
    
    -- Route & Distance
    departure VARCHAR(4), -- 4-letter ICAO airport code
    arrival VARCHAR(4),   -- 4-letter ICAO airport code
    distance_km NUMERIC(10, 2),
    circle_distance NUMERIC(10, 2),

    -- Phase Durations (in seconds)
    duration_takeoff_s INTEGER,
    duration_climb_s INTEGER,
    duration_cruise_s INTEGER,
    duration_descent_s INTEGER,
    duration_landing_s INTEGER,

    -- Fuel Estimations (in kg)
    fuel_takeoff_kg NUMERIC(10, 2),
    fuel_climb_kg NUMERIC(10, 2),
    fuel_cruise_kg NUMERIC(10, 2),
    fuel_descent_kg NUMERIC(10, 2),
    fuel_landing_kg NUMERIC(10, 2),
    
    -- CO2 Estimations (in kg)
    co2_takeoff_kg NUMERIC(10, 2),
    co2_climb_kg NUMERIC(10, 2),
    co2_cruise_kg NUMERIC(10, 2),
    co2_descent_kg NUMERIC(10, 2),
    co2_landing_kg NUMERIC(10, 2),
    co2_total_kg NUMERIC(10, 2),
    co2_per_passenger_kg NUMERIC(10, 2),
    
    -- Record Timestamps
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ######################################
-- ##  TABLE 2: FLIGHT_POSITIONS       ##
-- ######################################
-- Stores the time-series GPS data for each flight path.

CREATE TABLE flight_positions (
    position_id SERIAL PRIMARY KEY,
    
    -- Foreign key to link back to the flights table
    flight_id INTEGER NOT NULL REFERENCES flights(flight_id) ON DELETE CASCADE,
    
    -- Position Data
    "timestamp" TIMESTAMPTZ NOT NULL, -- Storing as a proper timestamp is better than an integer
    latitude NUMERIC(9, 6) NOT NULL,
    longitude NUMERIC(9, 6) NOT NULL,
    altitude INTEGER,
    ground_speed INTEGER,
    vertical_rate INTEGER
);


-- #################################
-- ##          INDEXES            ##
-- #################################
-- Create indexes on frequently queried columns for performance.

CREATE INDEX idx_flights_callsign ON flights(callsign);
CREATE INDEX idx_flights_departure ON flights(departure);
CREATE INDEX idx_flights_arrival ON flights(arrival);

CREATE INDEX idx_flight_positions_flight_id ON flight_positions(flight_id);
CREATE INDEX idx_flight_positions_timestamp ON flight_positions("timestamp");