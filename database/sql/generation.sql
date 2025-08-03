-- ##################################################################
-- ##      CLEANUP: Drop existing objects to ensure a fresh start    ##
-- ##################################################################
-- The CASCADE option automatically drops dependent objects like indexes and triggers.

DROP TABLE IF EXISTS flight_positions CASCADE;
DROP TABLE IF EXISTS flights CASCADE;
DROP FUNCTION IF EXISTS trigger_set_timestamp() CASCADE;


-- ##################################################################
-- ##                TABLE 1: FLIGHTS                              ##
-- ##################################################################
-- Stores the main summary for each flight.

CREATE TABLE flights (
    -- Core Identifiers
    flight_id SERIAL PRIMARY KEY,
    fr24_id VARCHAR(20) NOT NULL UNIQUE,
    flight VARCHAR(20),
    callsign VARCHAR(20),
    
    -- Aircraft Details
    aircraft_model VARCHAR(100),
    aircraft_reg VARCHAR(20),
    
    -- Route, Time & Distance
    departure_icao CHAR(4),
    arrival_icao CHAR(4),
    departure_time_utc TIMESTAMPTZ,
    arrival_time_utc TIMESTAMPTZ,
    flight_duration_s INTEGER,
    distance_calculated_km NUMERIC(10, 2),
    great_circle_distance_km NUMERIC(10, 2),

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
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_updated TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


-- ##################################################################
-- ##                TABLE 2: FLIGHT_POSITIONS                     ##
-- ##################################################################
-- Stores the time-series GPS data for each flight path.

CREATE TABLE flight_positions (
    position_id SERIAL PRIMARY KEY,
    flight_id INTEGER NOT NULL REFERENCES flights(flight_id) ON DELETE CASCADE,
    
    -- Position Data
    "timestamp" TIMESTAMPTZ NOT NULL,
    latitude NUMERIC(9, 6) NOT NULL,
    longitude NUMERIC(9, 6) NOT NULL,
    altitude INTEGER,
    ground_speed INTEGER,
    vertical_rate INTEGER
);


-- ##################################################################
-- ##             INDEXES (Corrected Column Names)                 ##
-- ##################################################################

CREATE INDEX idx_flights_callsign ON flights(callsign);
CREATE INDEX idx_flights_departure_icao ON flights(departure_icao); -- Corrected
CREATE INDEX idx_flights_arrival_icao ON flights(arrival_icao);     -- Corrected

CREATE INDEX idx_flight_positions_flight_id ON flight_positions(flight_id);
CREATE INDEX idx_flight_positions_timestamp ON flight_positions("timestamp");


-- ##################################################################
-- ##          TRIGGERS: Automatically update timestamps           ##
-- ##################################################################

CREATE OR REPLACE FUNCTION trigger_set_timestamp()
RETURNS TRIGGER AS $$
BEGIN
  NEW.last_updated = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER set_timestamp
BEFORE UPDATE ON flights
FOR EACH ROW
EXECUTE FUNCTION trigger_set_timestamp();
